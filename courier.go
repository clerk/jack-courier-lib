package courier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
)

const (
	defaultShutdownTimeout = 10 * time.Second
	defaultSubmitTimeout   = 30 * time.Second
	defaultHealthPort      = "8080"
)

type courier struct {
	driver  Driver
	project string

	shutdownTimeout time.Duration
	submitTimeout   time.Duration
	sinkNoop        bool
	logger          *slog.Logger
	statsd          statsd.ClientInterface

	publishers map[string]*queuePublisher
}

// Main is the entry point for a courier service. It loads configuration
// from environment variables, builds one Pub/Sub publisher per configured
// queue, starts a health check HTTP server, and runs the registered driver.
//
// Main blocks until a SIGINT or SIGTERM signal is received, then performs
// a graceful shutdown.
//
// Required environment variables (not required when JACK_COURIER_SINK_NOOP
// is enabled):
//   - JACK_COURIER_PUBSUB_PROJECT: GCP project ID of the Pub/Sub topics
//   - JACK_COURIER_PUBSUB_TOPICS: comma-separated queue:topic pairs mapping
//     each queue to the topic it publishes to
//     (e.g., "high:clerk_jobs_high,low:clerk_jobs_low")
//
// Optional environment variables:
//   - PORT: health check HTTP server port (default "8080")
//   - JACK_COURIER_SHUTDOWN_TIMEOUT: graceful shutdown timeout (default "10s")
//   - JACK_COURIER_SUBMIT_TIMEOUT: per-submit publish deadline (default "30s")
//   - JACK_COURIER_SINK_NOOP: if "true" or "1", never publish to Pub/Sub;
//     submitted jobs are acknowledged as accepted and dropped. Used to
//     validate drivers in production before the real sink is in place.
//   - PUBSUB_EMULATOR_HOST: standard Pub/Sub emulator override for local dev
func Main(opts ...Option) {
	os.Exit(run(opts...))
}

func run(opts ...Option) int {
	driver := getDriver()
	if driver == nil {
		fmt.Fprintln(os.Stderr, "courier: no driver registered, call RegisterDriver before Main")
		return 1
	}

	sinkNoop := false
	if v := os.Getenv("JACK_COURIER_SINK_NOOP"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "courier: invalid JACK_COURIER_SINK_NOOP: %v\n", err)
			return 1
		}
		sinkNoop = b
	}

	project := os.Getenv("JACK_COURIER_PUBSUB_PROJECT")
	if project == "" && !sinkNoop {
		fmt.Fprintln(os.Stderr, "courier: JACK_COURIER_PUBSUB_PROJECT environment variable is required")
		return 1
	}

	rawTopics := os.Getenv("JACK_COURIER_PUBSUB_TOPICS")
	if rawTopics == "" && !sinkNoop {
		fmt.Fprintln(os.Stderr, "courier: JACK_COURIER_PUBSUB_TOPICS environment variable is required")
		return 1
	}

	// The noop sink ignores the Pub/Sub configuration entirely; the deploy
	// that turns noop off validates it.
	var topics map[string]string
	if !sinkNoop {
		var err error
		topics, err = parseTopicMap(rawTopics)
		if err != nil {
			fmt.Fprintf(os.Stderr, "courier: invalid JACK_COURIER_PUBSUB_TOPICS: %v\n", err)
			return 1
		}
	}

	c := &courier{
		driver:          driver,
		project:         project,
		shutdownTimeout: defaultShutdownTimeout,
		submitTimeout:   defaultSubmitTimeout,
		sinkNoop:        sinkNoop,
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

	if c.sinkNoop {
		c.logger.Info("JACK_COURIER_SINK_NOOP enabled: jobs will be acknowledged without being published to Pub/Sub")
	} else if err := c.buildPublishers(context.Background(), topics); err != nil {
		c.logger.Error("failed to create pubsub publishers", slog.String("error", err.Error()))
		return 1
	}

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
		slog.String("pubsub_project", project),
		slog.Any("pubsub_topics", topics),
		slog.String("health_port", port),
	)

	return c.run(ctx, server)
}

// run runs the driver and, once it returns or ctx is cancelled, shuts down
// within shutdownTimeout, returning the process exit code.
func (c *courier) run(ctx context.Context, server *http.Server) int {
	driverDone := make(chan error, 1)
	go func() {
		driverDone <- c.driver.Run(ctx, c.submit)
	}()

	var err error
	driverExited := false
	select {
	case err = <-driverDone:
		driverExited = true
	case <-ctx.Done():
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), c.shutdownTimeout)
	defer shutdownCancel()

	// The publisher flush shares the shutdown budget with the driver drain,
	// so the shutdown timeout bounds total termination time as documented.
	defer c.stopPublishers(shutdownCtx)

	if !driverExited {
		var abandoned bool
		abandoned, err = drainDriver(shutdownCtx, driverDone)
		if abandoned {
			// Driver did not exit in time; exit now and let process teardown close the server.
			c.logger.Warn("driver did not exit within shutdown timeout, abandoning",
				slog.Duration("timeout", c.shutdownTimeout))
			return 1
		}
	}

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

// drainDriver waits for the driver to return before ctx is done,
// reporting abandoned=true if the budget expires first.
func drainDriver(ctx context.Context, done <-chan error) (abandoned bool, err error) {
	select {
	case err := <-done:
		return false, err
	case <-ctx.Done():
		return true, nil
	}
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
