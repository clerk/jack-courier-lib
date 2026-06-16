package courier

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/clerk/jack-service/proto/jackpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// --- Mock BackgroundJobs gRPC server ---

type mockBackgroundJobsServer struct {
	jackpb.UnimplementedBackgroundJobsServer
	enqueueBulkFn func(context.Context, *jackpb.EnqueueBulkRequest) (*jackpb.EnqueueBulkResponse, error)
}

func (m *mockBackgroundJobsServer) EnqueueBulk(ctx context.Context, req *jackpb.EnqueueBulkRequest) (*jackpb.EnqueueBulkResponse, error) {
	if m.enqueueBulkFn != nil {
		return m.enqueueBulkFn(ctx, req)
	}
	return &jackpb.EnqueueBulkResponse{}, nil
}

// startMockServer starts an in-process gRPC server and returns a client connection.
func startMockServer(t *testing.T, srv *mockBackgroundJobsServer) *grpc.ClientConn {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	s := grpc.NewServer()
	jackpb.RegisterBackgroundJobsServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Logf("mock server stopped: %v", err)
		}
	}()

	t.Cleanup(func() {
		s.GracefulStop()
		lis.Close()
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	)
	if err != nil {
		t.Fatalf("failed to create bufconn client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// echoCorrelationServer returns a mock server that echoes CorrelationId
// and assigns a job ID based on the job type.
func echoCorrelationServer() *mockBackgroundJobsServer {
	return &mockBackgroundJobsServer{
		enqueueBulkFn: func(_ context.Context, req *jackpb.EnqueueBulkRequest) (*jackpb.EnqueueBulkResponse, error) {
			results := make([]*jackpb.BulkResult, len(req.Jobs))
			for i, j := range req.Jobs {
				results[i] = &jackpb.BulkResult{
					Index:         int32(i),
					JobId:         "pjob_" + j.JobType,
					CorrelationId: j.CorrelationId,
				}
			}
			return &jackpb.EnqueueBulkResponse{Results: results}, nil
		},
	}
}

// --- Tests ---

func TestBuildBulkRequest(t *testing.T) {
	now := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)

	jobs := []Job{
		{
			CorrelationID: "corr-1",
			ID:            "psjob_abc",
			ProducerID:    "prod_abc",
			JobType:       "send_email",
			Payload:       []byte(`{"to":"user@example.com"}`),
			RunAt:         now,
			TraceID:       "trace-123",
		},
		{
			CorrelationID: "corr-2",
			ProducerID:    "prod_abc",
			JobType:       "send_sms",
			// ID + RunAt zero = jack generates ID, runs immediately
		},
	}

	req := buildBulkRequest(jobs)

	if len(req.Jobs) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(req.Jobs))
	}

	// First job: all fields set
	j0 := req.Jobs[0]
	if j0.ProducerId != "prod_abc" {
		t.Errorf("expected ProducerId=prod_abc, got %s", j0.ProducerId)
	}
	if j0.JobType != "send_email" {
		t.Errorf("expected JobType=send_email, got %s", j0.JobType)
	}
	if string(j0.Payload) != `{"to":"user@example.com"}` {
		t.Errorf("unexpected payload: %s", j0.Payload)
	}
	if j0.RunAt == nil {
		t.Fatal("expected RunAt to be set")
	}
	if !j0.RunAt.AsTime().Equal(now) {
		t.Errorf("expected RunAt=%v, got %v", now, j0.RunAt.AsTime())
	}
	if j0.TraceId != "trace-123" {
		t.Errorf("expected TraceId=trace-123, got %s", j0.TraceId)
	}
	if j0.CorrelationId != "corr-1" {
		t.Errorf("expected CorrelationId=corr-1, got %s", j0.CorrelationId)
	}
	if j0.JobId != "psjob_abc" {
		t.Errorf("expected JobId=psjob_abc, got %s", j0.JobId)
	}

	// Second job: zero RunAt should not set timestamp; empty ID stays empty
	j1 := req.Jobs[1]
	if j1.RunAt != nil {
		t.Errorf("expected RunAt to be nil for zero time, got %v", j1.RunAt)
	}
	if j1.CorrelationId != "corr-2" {
		t.Errorf("expected CorrelationId=corr-2, got %s", j1.CorrelationId)
	}
	if j1.JobId != "" {
		t.Errorf("expected empty JobId (jack will generate), got %s", j1.JobId)
	}
}

// TestBuildBulkRequestThreadsID is a focused regression test: Job.ID must
// reach EnqueueRequest.JobId verbatim. This is the producer-side half of
// PLAT-2887 — without it, the producer's psjob_<KSUID> never reaches
// jack-service and legacy↔jack correlation by attribute fails.
func TestBuildBulkRequestThreadsID(t *testing.T) {
	jobs := []Job{
		{ID: "psjob_alpha", ProducerID: "p", JobType: "t1", Payload: []byte(`{}`)},
		{ID: "", ProducerID: "p", JobType: "t2", Payload: []byte(`{}`)},
		{ID: "psjob_gamma", ProducerID: "p", JobType: "t3", Payload: []byte(`{}`)},
	}

	req := buildBulkRequest(jobs)
	got := []string{req.Jobs[0].JobId, req.Jobs[1].JobId, req.Jobs[2].JobId}
	want := []string{"psjob_alpha", "", "psjob_gamma"}

	for i := range got {
		if got[i] != want[i] {
			t.Errorf("Jobs[%d].JobId = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestCollectResults_FullSuccess(t *testing.T) {
	resp := &jackpb.EnqueueBulkResponse{
		Results: []*jackpb.BulkResult{
			{Index: 0, JobId: "pjob_aaa", CorrelationId: "corr-1"},
			{Index: 1, JobId: "pjob_bbb", CorrelationId: "corr-2"},
		},
	}

	results := collectResults(resp)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].CorrelationID != "corr-1" || results[0].JobID != "pjob_aaa" || results[0].Err != "" {
		t.Errorf("unexpected result[0]: %+v", results[0])
	}
	if results[1].CorrelationID != "corr-2" || results[1].JobID != "pjob_bbb" || results[1].Err != "" {
		t.Errorf("unexpected result[1]: %+v", results[1])
	}
}

func TestCollectResults_PartialFailure(t *testing.T) {
	resp := &jackpb.EnqueueBulkResponse{
		Results: []*jackpb.BulkResult{
			{Index: 0, JobId: "pjob_aaa", CorrelationId: "corr-1"},
			{Index: 1, Error: "unknown job type", Reason: "validation_error", CorrelationId: "corr-2"},
			{Index: 2, JobId: "pjob_ccc", CorrelationId: "corr-3"},
		},
	}

	results := collectResults(resp)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	if results[0].Err != "" {
		t.Errorf("expected result[0] success, got err=%s", results[0].Err)
	}
	if results[0].CorrelationID != "corr-1" {
		t.Errorf("expected CorrelationID=corr-1, got %s", results[0].CorrelationID)
	}

	if results[1].Err != "unknown job type" {
		t.Errorf("expected result[1] error, got err=%s", results[1].Err)
	}
	if results[1].Reason != "validation_error" {
		t.Errorf("expected result[1] reason=validation_error, got %s", results[1].Reason)
	}
	if results[1].CorrelationID != "corr-2" {
		t.Errorf("expected CorrelationID=corr-2, got %s", results[1].CorrelationID)
	}

	if results[2].Err != "" {
		t.Errorf("expected result[2] success, got err=%s", results[2].Err)
	}
}

func TestCollectResults_NilResponse(t *testing.T) {
	results := collectResults(nil)
	if results != nil {
		t.Errorf("expected nil results for nil response, got %v", results)
	}
}

func TestShouldUseTLS(t *testing.T) {
	tests := []struct {
		name     string
		address  string
		override *bool
		want     bool
	}{
		{"port 443 auto-detects TLS", "jack-service:443", nil, true},
		{"non-443 auto-detects no TLS", "jack-service:50051", nil, false},
		{"override forces TLS on", "jack-service:50051", boolPtr(true), true},
		{"override forces TLS off", "jack-service:443", boolPtr(false), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &courier{address: tt.address, tlsOverride: tt.override}
			if got := c.shouldUseTLS(); got != tt.want {
				t.Errorf("shouldUseTLS()=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestSubmit_FullSuccess(t *testing.T) {
	conn := startMockServer(t, echoCorrelationServer())
	c := &courier{
		client: jackpb.NewBackgroundJobsClient(conn),
		logger: slog.Default(),
		statsd: &statsd.NoOpClient{},
	}

	jobs := []Job{
		{CorrelationID: "c1", ProducerID: "prod_1", JobType: "email"},
		{CorrelationID: "c2", ProducerID: "prod_1", JobType: "sms"},
	}

	results, err := c.submit(context.Background(), jobs)
	if err != nil {
		t.Fatalf("submit returned error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].CorrelationID != "c1" || results[0].JobID != "pjob_email" || results[0].Err != "" {
		t.Errorf("unexpected result[0]: %+v", results[0])
	}
	if results[1].CorrelationID != "c2" || results[1].JobID != "pjob_sms" || results[1].Err != "" {
		t.Errorf("unexpected result[1]: %+v", results[1])
	}
}

func TestSubmit_PartialFailure(t *testing.T) {
	srv := &mockBackgroundJobsServer{
		enqueueBulkFn: func(_ context.Context, req *jackpb.EnqueueBulkRequest) (*jackpb.EnqueueBulkResponse, error) {
			return &jackpb.EnqueueBulkResponse{
				Results: []*jackpb.BulkResult{
					{Index: 0, JobId: "pjob_ok", CorrelationId: req.Jobs[0].CorrelationId},
					{Index: 1, Error: "invalid job type", CorrelationId: req.Jobs[1].CorrelationId},
				},
			}, nil
		},
	}

	conn := startMockServer(t, srv)
	c := &courier{
		client: jackpb.NewBackgroundJobsClient(conn),
		logger: slog.Default(),
		statsd: &statsd.NoOpClient{},
	}

	jobs := []Job{
		{CorrelationID: "c1", ProducerID: "prod_1", JobType: "email"},
		{CorrelationID: "c2", ProducerID: "prod_1", JobType: "bad_type"},
	}

	results, err := c.submit(context.Background(), jobs)
	if err != nil {
		t.Fatalf("submit returned error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}

	if results[0].Err != "" {
		t.Errorf("expected result[0] success, got err=%s", results[0].Err)
	}
	if results[0].CorrelationID != "c1" {
		t.Errorf("expected CorrelationID=c1, got %s", results[0].CorrelationID)
	}
	if results[1].Err != "invalid job type" {
		t.Errorf("expected result[1] error='invalid job type', got err=%s", results[1].Err)
	}
	if results[1].CorrelationID != "c2" {
		t.Errorf("expected CorrelationID=c2, got %s", results[1].CorrelationID)
	}
}

func TestSubmit_GRPCError(t *testing.T) {
	srv := &mockBackgroundJobsServer{
		enqueueBulkFn: func(_ context.Context, _ *jackpb.EnqueueBulkRequest) (*jackpb.EnqueueBulkResponse, error) {
			return nil, grpc.ErrServerStopped
		},
	}

	conn := startMockServer(t, srv)
	c := &courier{
		client: jackpb.NewBackgroundJobsClient(conn),
		logger: slog.Default(),
		statsd: &statsd.NoOpClient{},
	}

	jobs := []Job{{CorrelationID: "c1", ProducerID: "prod_1", JobType: "email"}}

	results, err := c.submit(context.Background(), jobs)
	if err == nil {
		t.Fatal("expected error from submit, got nil")
	}
	if results != nil {
		t.Errorf("expected nil results on gRPC error, got %v", results)
	}
}

// TestSubmit_SingleBatchPassesThroughInternalJackMeta verifies the new
// flow: per-job InternalJackMeta bytes flow into EnqueueRequest unchanged,
// no batch splitting, no gRPC metadata header.
func TestSubmit_SingleBatchPassesThroughInternalJackMeta(t *testing.T) {
	var callCount int
	var seen []*jackpb.EnqueueRequest
	srv := &mockBackgroundJobsServer{
		enqueueBulkFn: func(ctx context.Context, req *jackpb.EnqueueBulkRequest) (*jackpb.EnqueueBulkResponse, error) {
			callCount++
			seen = append(seen, req.Jobs...)
			results := make([]*jackpb.BulkResult, len(req.Jobs))
			for i, j := range req.Jobs {
				results[i] = &jackpb.BulkResult{Index: int32(i), CorrelationId: j.CorrelationId}
			}
			return &jackpb.EnqueueBulkResponse{Results: results}, nil
		},
	}

	conn := startMockServer(t, srv)
	c := &courier{
		client: jackpb.NewBackgroundJobsClient(conn),
		logger: slog.Default(),
		statsd: &statsd.NoOpClient{},
	}

	jobs := []Job{
		{CorrelationID: "c1", ProducerID: "p", JobType: "email", InternalJackMeta: nil},
		{CorrelationID: "c2", ProducerID: "p", JobType: "sms", InternalJackMeta: []byte{0xde, 0xad}},
		{CorrelationID: "c3", ProducerID: "p", JobType: "email", InternalJackMeta: []byte{0xbe, 0xef}},
	}

	if _, err := c.submit(context.Background(), jobs); err != nil {
		t.Fatalf("submit: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected single batched RPC, got %d calls", callCount)
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 jobs in the call, got %d", len(seen))
	}
	for i, want := range [][]byte{nil, {0xde, 0xad}, {0xbe, 0xef}} {
		if !bytesEqual(seen[i].InternalJackMeta, want) {
			t.Errorf("job %d: InternalJackMeta = %x, want %x", i, seen[i].InternalJackMeta, want)
		}
	}
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSubmit_EmptyJobs(t *testing.T) {
	c := &courier{logger: slog.Default()}

	results, err := c.submit(context.Background(), nil)
	if err != nil {
		t.Fatalf("expected no error for empty jobs, got %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty jobs, got %v", results)
	}
}

func TestRegisterDriver_Panics(t *testing.T) {
	driverMu.Lock()
	origDriver := registeredDriver
	origRegistered := driverRegistered
	registeredDriver = nil
	driverRegistered = false
	driverMu.Unlock()

	defer func() {
		driverMu.Lock()
		registeredDriver = origDriver
		driverRegistered = origRegistered
		driverMu.Unlock()
	}()

	RegisterDriver(&noopDriver{})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on double RegisterDriver")
		}
	}()
	RegisterDriver(&noopDriver{})
}

func TestRegisterDriver_NilPanics(t *testing.T) {
	driverMu.Lock()
	origDriver := registeredDriver
	origRegistered := driverRegistered
	registeredDriver = nil
	driverRegistered = false
	driverMu.Unlock()

	defer func() {
		driverMu.Lock()
		registeredDriver = origDriver
		driverRegistered = origRegistered
		driverMu.Unlock()
	}()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil RegisterDriver")
		}
	}()
	RegisterDriver(nil)
}

// --- Helpers ---

type noopDriver struct{}

func (d *noopDriver) Run(ctx context.Context, submit SubmitFunc) error {
	<-ctx.Done()
	return ctx.Err()
}

func boolPtr(b bool) *bool {
	return &b
}
