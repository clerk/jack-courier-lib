package courier

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/clerk/jack-service/proto/jackpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultShutdownTimeout = 10 * time.Second
	defaultSubmitTimeout   = 30 * time.Second
	defaultHealthPort      = "8080"
)

type courier struct {
	driver  Driver
	address string

	shutdownTimeout time.Duration
	submitTimeout   time.Duration
	logger          *slog.Logger
	tlsOverride     *bool
	grpcDialOpts    []grpc.DialOption
	statsd          statsd.ClientInterface

	conn   *grpc.ClientConn
	client jackpb.BackgroundJobsClient
}

// Main is the entry point for a courier service. It loads configuration
// from environment variables, sets up the gRPC connection to jack-service,
// starts a health check HTTP server, and runs the registered driver.
//
// Main blocks until a SIGINT or SIGTERM signal is received, then performs
// a graceful shutdown.
//
// Required environment variables:
//   - JACK_SERVICE_ADDR: jack-service gRPC address (e.g., "jack-service:50051")
//
// Optional environment variables:
//   - PORT: health check HTTP server port (default "8080")
//   - JACK_COURIER_SHUTDOWN_TIMEOUT: graceful shutdown timeout (default "10s")
//   - JACK_COURIER_SUBMIT_TIMEOUT: per-call EnqueueBulk timeout (default "30s")
func Main(opts ...Option) {
	os.Exit(run(opts...))
}

func run(opts ...Option) int {
	driver := getDriver()
	if driver == nil {
		fmt.Fprintln(os.Stderr, "courier: no driver registered, call RegisterDriver before Main")
		return 1
	}

	addr := os.Getenv("JACK_SERVICE_ADDR")
	if addr == "" {
		fmt.Fprintln(os.Stderr, "courier: JACK_SERVICE_ADDR environment variable is required")
		return 1
	}

	c := &courier{
		driver:          driver,
		address:         addr,
		shutdownTimeout: defaultShutdownTimeout,
		submitTimeout:   defaultSubmitTimeout,
		logger:          slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	}

	for _, opt := range opts {
		if err := opt(c); err != nil {
			fmt.Fprintf(os.Stderr, "courier: option error: %v\n", err)
			return 1
		}
	}

	if c.statsd == nil {
		c.statsd = &statsd.NoOpClient{}
	}

	defer func() { _ = c.statsd.Flush() }()

	if v := os.Getenv("JACK_COURIER_SHUTDOWN_TIMEOUT"); v != "" {
		d, err := parsePositiveDuration("JACK_COURIER_SHUTDOWN_TIMEOUT", v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "courier: %v\n", err)
			return 1
		}
		c.shutdownTimeout = d
	}

	if v := os.Getenv("JACK_COURIER_SUBMIT_TIMEOUT"); v != "" {
		d, err := parsePositiveDuration("JACK_COURIER_SUBMIT_TIMEOUT", v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "courier: %v\n", err)
			return 1
		}
		c.submitTimeout = d
	}

	if err := c.connect(); err != nil {
		c.logger.Error("failed to connect to jack-service", slog.String("error", err.Error()))
		return 1
	}
	defer func() { _ = c.conn.Close() }()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start health check HTTP server.
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultHealthPort
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		c.logger.Info("health server started", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.logger.Error("health server error", slog.String("error", err.Error()))
		}
	}()

	c.logger.Info("courier started",
		slog.String("jack_service_addr", c.address),
		slog.String("health_port", port),
	)

	// Run the driver with the submit callback. This blocks until
	// the context is cancelled or the driver returns an error.
	err := c.driver.Run(ctx, c.submit)

	// Graceful shutdown.
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), c.shutdownTimeout)
	defer shutdownCancel()

	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		c.logger.Error("health server shutdown error", slog.String("error", shutdownErr.Error()))
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		c.logger.Error("driver exited with error", slog.String("error", err.Error()))
		return 1
	}

	c.logger.Info("courier stopped")
	return 0
}

func parsePositiveDuration(name, value string) (time.Duration, error) {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", name, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be > 0, got %s", name, d)
	}
	return d, nil
}

// submit delivers a batch of jobs to jack-service via EnqueueBulk in a
// single RPC. Per-job jack-service control fields (shadow, etc.) ride on
// each job's InternalJackMeta bytes — no per-call gRPC metadata header,
// no batch-splitting.
func (c *courier) submit(ctx context.Context, jobs []Job) ([]SubmitResult, error) {
	if len(jobs) == 0 {
		return nil, nil
	}
	return c.submitBatch(ctx, jobs)
}

func (c *courier) submitBatch(ctx context.Context, jobs []Job) ([]SubmitResult, error) {
	req := buildBulkRequest(jobs)

	if c.submitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.submitTimeout)
		defer cancel()
	}

	start := time.Now()
	resp, err := c.client.EnqueueBulk(ctx, req)
	if err != nil {
		_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:error"}, 1)
		return nil, fmt.Errorf("courier: EnqueueBulk: %w", err)
	}

	_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:success"}, 1)
	_ = c.statsd.Distribution("jack.courier.submit.duration", time.Since(start).Seconds(), nil, 1)
	_ = c.statsd.Distribution("jack.courier.submit.batch_size", float64(len(jobs)), nil, 1)

	results := collectResults(resp)

	var failCount int
	for _, r := range results {
		if r.Err != "" {
			failCount++
		}
	}
	_ = c.statsd.Count("jack.courier.submit.jobs", int64(len(results)-failCount), []string{"status:success"}, 1)
	if failCount > 0 {
		_ = c.statsd.Count("jack.courier.submit.jobs", int64(failCount), []string{"status:error"}, 1)
	}

	return results, nil
}

// buildBulkRequest converts a slice of Job to an EnqueueBulkRequest.
func buildBulkRequest(jobs []Job) *jackpb.EnqueueBulkRequest {
	reqs := make([]*jackpb.EnqueueRequest, len(jobs))
	for i, j := range jobs {
		req := &jackpb.EnqueueRequest{
			ProducerId:       j.ProducerID,
			JobType:          j.JobType,
			Payload:          j.Payload,
			TraceId:          j.TraceID,
			CorrelationId:    j.CorrelationID,
			JobId:            j.ID,
			InternalJackMeta: j.InternalJackMeta,
		}
		if !j.RunAt.IsZero() {
			req.RunAt = timestamppb.New(j.RunAt)
		}
		reqs[i] = req
	}
	return &jackpb.EnqueueBulkRequest{Jobs: reqs}
}

// collectResults maps BulkResult entries to SubmitResults.
// CorrelationID is echoed directly from the proto response.
func collectResults(resp *jackpb.EnqueueBulkResponse) []SubmitResult {
	if resp == nil {
		return nil
	}

	results := make([]SubmitResult, len(resp.Results))
	for i, r := range resp.Results {
		results[i] = SubmitResult{
			CorrelationID: r.CorrelationId,
			JobID:         r.JobId,
			Err:           r.Error,
			Reason:        r.Reason,
		}
	}
	return results
}

// connect establishes the gRPC connection to jack-service.
func (c *courier) connect() error {
	useTLS := c.shouldUseTLS()

	var transportCreds grpc.DialOption
	if useTLS {
		transportCreds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{}))
	} else {
		transportCreds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	dialOpts := append([]grpc.DialOption{transportCreds}, c.grpcDialOpts...)

	conn, err := grpc.NewClient(c.address, dialOpts...)
	if err != nil {
		return err
	}

	c.conn = conn
	c.client = jackpb.NewBackgroundJobsClient(conn)
	return nil
}

func (c *courier) shouldUseTLS() bool {
	if c.tlsOverride != nil {
		return *c.tlsOverride
	}
	return strings.HasSuffix(c.address, ":443")
}
