package courier

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"cloud.google.com/go/pubsub/v2/pstest"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/clerk/jack/proto/jackpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestParseTopicMap(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    map[string]string
		wantErr bool
	}{
		{"single pair", "high:clerk_jobs_high", map[string]string{"high": "clerk_jobs_high"}, false},
		{
			"multiple pairs with spaces",
			"high:clerk_jobs_high, session-minter:clerk_jobs_session_minter",
			map[string]string{"high": "clerk_jobs_high", "session-minter": "clerk_jobs_session_minter"},
			false,
		},
		{
			"spaces around colon are trimmed",
			"high: clerk_jobs_high,low : clerk_jobs_low",
			map[string]string{"high": "clerk_jobs_high", "low": "clerk_jobs_low"},
			false,
		},
		{"whitespace-only topic", "high: ", nil, true},
		{"missing colon", "high", nil, true},
		{"empty queue", ":topic", nil, true},
		{"empty topic", "high:", nil, true},
		{"duplicate queue", "high:t1,high:t2", nil, true},
		{"trailing comma", "high:t1,", nil, true},
		{"empty string", "", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTopicMap(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got %v", tt.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.raw, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("parseTopicMap(%q) = %v, want %v", tt.raw, got, tt.want)
			}
			for q, topic := range tt.want {
				if got[q] != topic {
					t.Errorf("parseTopicMap(%q)[%q] = %q, want %q", tt.raw, q, got[q], topic)
				}
			}
		})
	}
}

func TestClassifyPublishError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"oversized message is permanent", pubsub.ErrOversizedMessage, reasonPayloadTooLarge},
		{"wrapped oversized message is permanent", fmt.Errorf("wrap: %w", pubsub.ErrOversizedMessage), reasonPayloadTooLarge},
		// InvalidArgument is request-level (one Publish RPC carries the whole
		// batch), so it may reflect config problems and must not dead-letter.
		{"invalid argument is retryable", status.Error(codes.InvalidArgument, "bad topic"), ""},
		{"unavailable is retryable", status.Error(codes.Unavailable, "down"), ""},
		{"not found is retryable", status.Error(codes.NotFound, "no topic"), ""},
		{"deadline is retryable", context.DeadlineExceeded, ""},
		{"cancelled is retryable", context.Canceled, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyPublishError(tt.err); got != tt.want {
				t.Errorf("classifyPublishError(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestShadowFromMeta(t *testing.T) {
	shadowTrue, err := proto.Marshal(&jackpb.InternalJackMeta{Shadow: true})
	if err != nil {
		t.Fatal(err)
	}
	shadowFalse, err := proto.Marshal(&jackpb.InternalJackMeta{Shadow: false})
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name    string
		meta    []byte
		want    bool
		wantErr bool
	}{
		{"empty means not shadow", nil, false, false},
		{"shadow true", shadowTrue, true, false},
		{"shadow false", shadowFalse, false, false},
		{"garbage errors", []byte{0xff, 0xff, 0xff, 0xff}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shadowFromMeta(tt.meta)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("shadowFromMeta = %v, want %v", got, tt.want)
			}
		})
	}
}

// newPubsubCourier builds a courier whose publishers point at an in-process
// pstest server. Topics listed in create are created on the server; the
// topic map may reference others to exercise missing-topic behavior.
func newPubsubCourier(t *testing.T, topics map[string]string, create []string) (*courier, *pstest.Server) {
	t.Helper()

	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })
	t.Setenv("PUBSUB_EMULATOR_HOST", srv.Addr)

	const project = "test-project"

	admin, err := pubsub.NewClient(t.Context(), project)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	for _, topic := range create {
		name := fmt.Sprintf("projects/%s/topics/%s", project, topic)
		if _, err := admin.TopicAdminClient.CreateTopic(t.Context(), &pubsubpb.Topic{Name: name}); err != nil {
			t.Fatalf("create topic %s: %v", name, err)
		}
	}

	c := &courier{
		project: project,
		logger:  discardLogger(),
		statsd:  &statsd.NoOpClient{},
	}
	if err := c.buildPublishers(t.Context(), topics); err != nil {
		t.Fatalf("buildPublishers: %v", err)
	}
	t.Cleanup(func() { c.stopPublishers(context.Background()) })

	return c, srv
}

func metaBytes(t *testing.T, shadow bool) []byte {
	t.Helper()
	b, err := proto.Marshal(&jackpb.InternalJackMeta{Shadow: shadow})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSubmit_RoutesPerQueue(t *testing.T) {
	c, srv := newPubsubCourier(t,
		map[string]string{"high": "topic_high", "low": "topic_low"},
		[]string{"topic_high", "topic_low"},
	)

	jobs := []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("h1")},
		{CorrelationID: "2", ID: "b", Queue: "low", Payload: []byte("l1")},
		{CorrelationID: "3", ID: "c", Queue: "high", Payload: []byte("h2")},
	}

	results, err := c.submit(t.Context(), jobs)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	for i, r := range results {
		if r.Err != "" {
			t.Fatalf("job %d failed: %s", i, r.Err)
		}
	}

	perTopic := map[string]int{}
	for _, m := range srv.Messages() {
		perTopic[m.Topic]++
	}
	if len(perTopic) != 2 {
		t.Fatalf("expected messages on 2 topics, got %v", perTopic)
	}
	for topic, want := range map[string]int{"topic_high": 2, "topic_low": 1} {
		found := false
		for name, n := range perTopic {
			if strings.HasSuffix(name, "/"+topic) {
				found = true
				if n != want {
					t.Errorf("topic %s: %d messages, want %d", topic, n, want)
				}
			}
		}
		if !found {
			t.Errorf("no messages on topic %s: %v", topic, perTopic)
		}
	}
}

func TestSubmit_PassthroughPayloadAndAttributes(t *testing.T) {
	c, srv := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	payload := []byte(`{"id":"psjob_123","args":{"nested":true}}`)
	jobs := []Job{{
		CorrelationID:    "42",
		ID:               "psjob_123",
		Queue:            "high",
		ProducerID:       "clerk_go",
		TraceID:          "trace_abc",
		Payload:          payload,
		InternalJackMeta: metaBytes(t, true),
	}}

	if _, err := c.submit(t.Context(), jobs); err != nil {
		t.Fatalf("submit: %v", err)
	}

	msgs := srv.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if string(msgs[0].Data) != string(payload) {
		t.Errorf("Data = %q, want payload verbatim %q", msgs[0].Data, payload)
	}
	want := map[string]string{
		"shadow":      "true",
		"job_id":      "psjob_123",
		"trace_id":    "trace_abc",
		"producer_id": "clerk_go",
	}
	if len(msgs[0].Attributes) != len(want) {
		t.Errorf("Attributes = %v, want exactly %v", msgs[0].Attributes, want)
	}
	for k, v := range want {
		if msgs[0].Attributes[k] != v {
			t.Errorf("Attributes[%q] = %q, want %q", k, msgs[0].Attributes[k], v)
		}
	}
}

func TestSubmit_OmitsEmptyAttributes(t *testing.T) {
	// A present-but-empty job_id would collide distinct jobs on the same
	// dedup key; empty attributes must be omitted, not published.
	c, srv := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	jobs := []Job{{CorrelationID: "1", Queue: "high", Payload: []byte("x")}}

	if _, err := c.submit(t.Context(), jobs); err != nil {
		t.Fatalf("submit: %v", err)
	}

	msgs := srv.Messages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	want := map[string]string{"shadow": "false"}
	if len(msgs[0].Attributes) != len(want) || msgs[0].Attributes["shadow"] != "false" {
		t.Errorf("Attributes = %v, want exactly %v", msgs[0].Attributes, want)
	}
}

func TestSubmit_ResultsAlignWithJobs(t *testing.T) {
	c, _ := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	jobs := make([]Job, 5)
	for i := range jobs {
		jobs[i] = Job{
			CorrelationID: fmt.Sprintf("row-%d", i),
			ID:            fmt.Sprintf("psjob_%d", i),
			Queue:         "high",
			Payload:       []byte("x"),
		}
	}

	results, err := c.submit(t.Context(), jobs)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if len(results) != len(jobs) {
		t.Fatalf("got %d results for %d jobs", len(results), len(jobs))
	}
	for i, r := range results {
		if r.CorrelationID != jobs[i].CorrelationID {
			t.Errorf("result %d: CorrelationID = %q, want %q", i, r.CorrelationID, jobs[i].CorrelationID)
		}
		if r.JobID != jobs[i].ID {
			t.Errorf("result %d: JobID = %q, want %q", i, r.JobID, jobs[i].ID)
		}
		if r.Err != "" {
			t.Errorf("result %d: unexpected Err %q", i, r.Err)
		}
	}
}

func TestSubmit_MalformedMetaFailsJobNotBatch(t *testing.T) {
	c, srv := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	jobs := []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("ok"), InternalJackMeta: []byte{0xff, 0xff, 0xff, 0xff}},
		{CorrelationID: "2", ID: "b", Queue: "high", Payload: []byte("ok")},
	}

	results, err := c.submit(t.Context(), jobs)
	if err != nil {
		t.Fatalf("expected per-job failure, got batch error: %v", err)
	}
	if results[0].Err == "" || results[0].Reason != reasonValidationError {
		t.Errorf("bad-meta job: Err=%q Reason=%q, want validation_error", results[0].Err, results[0].Reason)
	}
	if results[1].Err != "" {
		t.Errorf("sibling job failed: %s", results[1].Err)
	}
	if n := len(srv.Messages()); n != 1 {
		t.Errorf("expected only the sibling published, got %d messages", n)
	}
}

func TestSubmit_UnmappedQueueRejectsBatch(t *testing.T) {
	c, srv := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	jobs := []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("ok")},
		{CorrelationID: "2", ID: "b", Queue: "unknown", Payload: []byte("ok")},
	}

	results, err := c.submit(t.Context(), jobs)
	if err == nil {
		t.Fatal("expected batch error for unmapped queue")
	}
	if results != nil {
		t.Fatalf("expected nil results on batch error, got %v", results)
	}
	if n := len(srv.Messages()); n != 0 {
		t.Errorf("expected nothing published, got %d messages", n)
	}
}

func TestSubmit_MissingTopicFailsBatchRetryably(t *testing.T) {
	// The topic is mapped but never created on the server: every publish
	// fails with NotFound (retryable class), which must surface as a batch
	// error so the driver retries instead of dead-lettering.
	c, srv := newPubsubCourier(t,
		map[string]string{"high": "topic_missing"},
		nil,
	)

	jobs := []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("x")},
		{CorrelationID: "2", ID: "b", Queue: "high", Payload: []byte("y")},
	}

	results, err := c.submit(t.Context(), jobs)
	if err == nil {
		t.Fatalf("expected batch error for missing topic, got results %v", results)
	}
	if n := len(srv.Messages()); n != 0 {
		t.Errorf("expected nothing published, got %d messages", n)
	}
}

func TestSubmit_OversizedJobFailsPermanentlyNotBatch(t *testing.T) {
	// A batch made entirely of permanently-failing jobs must resolve per-job
	// (payload_too_large → DLQ), not as a batch error: a batch error would
	// make the driver retry the same poison batch forever.
	c, _ := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	jobs := []Job{{
		CorrelationID: "1",
		ID:            "a",
		Queue:         "high",
		Payload:       make([]byte, pubsub.MaxPublishRequestBytes+1),
	}}

	results, err := c.submit(t.Context(), jobs)
	if err != nil {
		t.Fatalf("expected per-job failure, got batch error: %v", err)
	}
	if results[0].Err == "" || results[0].Reason != reasonPayloadTooLarge {
		t.Errorf("oversized job: Err=%q Reason=%q, want payload_too_large", results[0].Err, results[0].Reason)
	}
}

func TestSubmit_EmitsMetrics(t *testing.T) {
	c, _ := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)
	rec := &recordingStatsd{ClientInterface: &statsd.NoOpClient{}}
	c.statsd = rec

	future := time.Now().Add(time.Hour)
	jobs := []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", JobType: "email", Payload: []byte("x")},
		{CorrelationID: "2", ID: "b", Queue: "high", JobType: "email", Payload: []byte("y"), RunAt: future},
		{CorrelationID: "3", ID: "c", Queue: "high", JobType: "sms", Payload: []byte("z"), RunAt: future},
	}

	if _, err := c.submit(t.Context(), jobs); err != nil {
		t.Fatalf("submit: %v", err)
	}

	if got := rec.incrTags("jack.courier.submit.count"); !hasTag(got, "status:success") || !hasTag(got, "queue:high") {
		t.Errorf("submit.count tags = %v, want status:success and queue:high", got)
	}
	if got := rec.countValue("jack.courier.submit.jobs", "status:success"); got != 3 {
		t.Errorf("submit.jobs success = %d, want 3", got)
	}
	if got := rec.distributionValue("jack.courier.submit.batch_size"); got != 3 {
		t.Errorf("submit.batch_size = %v, want 3", got)
	}
	if got := rec.countValue("jack.courier.submit.future_jobs", "job_type:email"); got != 1 {
		t.Errorf("future_jobs{email} = %d, want 1", got)
	}
	if got := rec.countValue("jack.courier.submit.future_jobs", "job_type:sms"); got != 1 {
		t.Errorf("future_jobs{sms} = %d, want 1", got)
	}
}

func TestSubmit_SinkNoop(t *testing.T) {
	rec := &recordingStatsd{ClientInterface: &statsd.NoOpClient{}}
	// No publishers configured: any publish or queue lookup would reject the
	// batch, so passing results prove the noop branch short-circuits.
	c := &courier{
		logger:   discardLogger(),
		statsd:   rec,
		sinkNoop: true,
	}

	jobs := []Job{
		{CorrelationID: "c1", ID: "psjob_abc", Queue: "high", ProducerID: "prod_1", JobType: "email"},
		{CorrelationID: "c2", Queue: "low", ProducerID: "prod_1", JobType: "sms"},
	}

	results, err := c.submit(t.Context(), jobs)
	if err != nil {
		t.Fatalf("submit returned error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].CorrelationID != "c1" || results[0].JobID != "psjob_abc" || results[0].Err != "" {
		t.Errorf("unexpected result[0]: %+v", results[0])
	}
	if results[1].CorrelationID != "c2" || results[1].JobID != "" || results[1].Err != "" {
		t.Errorf("unexpected result[1]: %+v", results[1])
	}
	if got := rec.incrTags("jack.courier.submit.count"); !hasTag(got, "status:noop") || !hasTag(got, "queue:high") {
		t.Errorf("submit.count tags = %v, want status:noop and queue:high", got)
	}
	if got := rec.distributionValue("jack.courier.submit.batch_size"); got != 2 {
		t.Errorf("submit.batch_size = %v, want 2", got)
	}
}

// newStalledCourier builds a courier whose Publish RPCs always fail with a
// retryable code, so publishes stay pending in the client indefinitely.
func newStalledCourier(t *testing.T) *courier {
	t.Helper()

	srv := pstest.NewServer(pstest.WithErrorInjection("Publish", codes.Unavailable, "injected outage"))
	t.Cleanup(func() { _ = srv.Close() })
	t.Setenv("PUBSUB_EMULATOR_HOST", srv.Addr)

	client, err := pubsub.NewClient(context.Background(), "test-project")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	c := &courier{
		logger:     discardLogger(),
		statsd:     &statsd.NoOpClient{},
		publishers: map[string]*queuePublisher{"high": {client: client, pub: client.Publisher("topic_high")}},
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		c.stopPublishers(ctx)
	})
	return c
}

func TestStopPublishers_BoundedByContext(t *testing.T) {
	// A pending publish makes Publisher.Stop block on flushing it far beyond
	// any shutdown budget. stopPublishers must give up when its context
	// expires.
	c := newStalledCourier(t)

	// The result is deliberately not awaited: the publish stays pending.
	c.publishers["high"].pub.Publish(context.Background(), &pubsub.Message{Data: []byte("x")})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	c.stopPublishers(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("stopPublishers took %v, want it bounded by its context", elapsed)
	}
}

func TestSubmit_TimesOutOnStalledPublish(t *testing.T) {
	// A stalled Pub/Sub must not hang submit past the configured submit
	// timeout: every publish fails with the context error and the batch
	// surfaces as a retryable batch error to the driver.
	c := newStalledCourier(t)
	c.submitTimeout = 100 * time.Millisecond

	start := time.Now()
	results, err := c.submit(context.Background(), []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("x")},
	})
	if err == nil {
		t.Fatalf("expected batch error from stalled publishes, got results %v", results)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want it to wrap context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("submit took %v, want it bounded by the submit timeout", elapsed)
	}
}

func TestSubmit_RejectsDeadContext(t *testing.T) {
	// The client publishes bundles on a background context, so a dead caller
	// context must fail the batch before anything is enqueued, and the error
	// must keep its identity so shutdown is not mistaken for a failure.
	c, srv := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := c.submit(ctx, []Job{{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("x")}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", err)
	}
	if n := len(srv.Messages()); n != 0 {
		t.Errorf("expected nothing published, got %d messages", n)
	}
}

func TestSubmit_MixedQueueBatchAttributesJobMetricsPerQueue(t *testing.T) {
	// The DLQ retry path can submit one batch spanning queues; per-job
	// metrics must attribute failures to the failing job's queue.
	c, _ := newPubsubCourier(t,
		map[string]string{"high": "topic_high", "low": "topic_low"},
		[]string{"topic_high", "topic_low"},
	)
	rec := &recordingStatsd{ClientInterface: &statsd.NoOpClient{}}
	c.statsd = rec

	jobs := []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("x")},
		{CorrelationID: "2", ID: "b", Queue: "low", Payload: []byte("y"), InternalJackMeta: []byte{0xff, 0xff, 0xff, 0xff}},
	}

	if _, err := c.submit(t.Context(), jobs); err != nil {
		t.Fatalf("submit: %v", err)
	}

	if got := rec.countValue("jack.courier.submit.jobs", "status:success", "queue:high"); got != 1 {
		t.Errorf("success{high} = %d, want 1", got)
	}
	if got := rec.countValue("jack.courier.submit.jobs", "status:error", "queue:low"); got != 1 {
		t.Errorf("error{low} = %d, want 1", got)
	}
	if got := rec.countValue("jack.courier.submit.jobs", "status:error", "queue:high"); got != 0 {
		t.Errorf("error{high} = %d, want 0", got)
	}
}

func TestBuildPublishers_AppliesSubmitTimeout(t *testing.T) {
	// PublishSettings.Timeout must follow the submit deadline, or an
	// abandoned publish keeps retrying in the background for the client's
	// 60s default and can land after the driver already retried the batch.
	srv := pstest.NewServer()
	t.Cleanup(func() { _ = srv.Close() })
	t.Setenv("PUBSUB_EMULATOR_HOST", srv.Addr)

	c := &courier{
		project:       "test-project",
		logger:        discardLogger(),
		statsd:        &statsd.NoOpClient{},
		submitTimeout: 5 * time.Second,
	}
	if err := c.buildPublishers(t.Context(), map[string]string{"high": "topic_high"}); err != nil {
		t.Fatalf("buildPublishers: %v", err)
	}
	t.Cleanup(func() { c.stopPublishers(context.Background()) })

	if got := c.publishers["high"].pub.PublishSettings.Timeout; got != 5*time.Second {
		t.Errorf("PublishSettings.Timeout = %v, want 5s", got)
	}
}

func TestSubmit_FutureJobsMetricSkipsFailures(t *testing.T) {
	c, _ := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)
	rec := &recordingStatsd{ClientInterface: &statsd.NoOpClient{}}
	c.statsd = rec

	future := time.Now().Add(time.Hour)
	jobs := []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", JobType: "email", Payload: []byte("x"), RunAt: future},
		{CorrelationID: "2", ID: "b", Queue: "high", JobType: "email", Payload: []byte("y"), RunAt: future, InternalJackMeta: []byte{0xff, 0xff, 0xff, 0xff}},
	}

	if _, err := c.submit(t.Context(), jobs); err != nil {
		t.Fatalf("submit: %v", err)
	}

	if got := rec.countValue("jack.courier.submit.future_jobs", "job_type:email"); got != 1 {
		t.Errorf("future_jobs{email} = %d, want 1 (the failed job must not count)", got)
	}
}

// --- Metric recording helpers ---

type metricCall struct {
	name  string
	tags  []string
	value float64
}

type recordingStatsd struct {
	statsd.ClientInterface
	mu    sync.Mutex
	calls []metricCall
}

func (r *recordingStatsd) Incr(name string, tags []string, _ float64) error {
	r.record(name, tags, 1)
	return nil
}

func (r *recordingStatsd) Count(name string, value int64, tags []string, _ float64) error {
	r.record(name, tags, float64(value))
	return nil
}

func (r *recordingStatsd) Distribution(name string, value float64, tags []string, _ float64) error {
	r.record(name, tags, value)
	return nil
}

func (r *recordingStatsd) record(name string, tags []string, value float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, metricCall{name: name, tags: tags, value: value})
}

func (r *recordingStatsd) incrTags(name string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.calls {
		if c.name == name {
			return c.tags
		}
	}
	return nil
}

// countValue sums the values of every Count call carrying all given tags.
func (r *recordingStatsd) countValue(name string, tags ...string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total int64
	for _, c := range r.calls {
		if c.name != name {
			continue
		}
		matched := true
		for _, tag := range tags {
			if !hasTag(c.tags, tag) {
				matched = false
				break
			}
		}
		if matched {
			total += int64(c.value)
		}
	}
	return total
}

func (r *recordingStatsd) distributionValue(name string) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.calls {
		if c.name == name {
			return c.value
		}
	}
	return -1
}

func hasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}
