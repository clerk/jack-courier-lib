package courier

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// startFakeSentry starts a local Sentry ingestion endpoint, so delivery
// tests involve no network. Delivered event bodies arrive on the returned
// channel. The global client is reset to noop reporting when the test
// finishes.
func startFakeSentry(t *testing.T) (chan string, string) {
	t.Helper()
	t.Setenv("SENTRY_DSN", "") // keep the SDK's env fallback out of tests
	bodies := make(chan string, 8)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies <- string(b)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() {
		if err := initSentry("", "", ""); err != nil {
			t.Errorf("reset sentry to noop: %v", err)
		}
	})

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return bodies, fmt.Sprintf("http://publickey@%s/1", u.Host)
}

// newFakeSentry points the courier's reporting at a fake ingestion endpoint.
func newFakeSentry(t *testing.T) chan string {
	t.Helper()
	bodies, dsn := startFakeSentry(t)
	if err := initSentry(dsn, "test", "deadbeef"); err != nil {
		t.Fatalf("initSentry: %v", err)
	}
	return bodies
}

// awaitEvent waits for one delivered event body. Flush normally guarantees
// delivery, but it can also return on its internal timeout under load, so
// wait for the body instead of checking the channel non-blockingly.
func awaitEvent(t *testing.T, bodies chan string) string {
	t.Helper()
	select {
	case body := <-bodies:
		return body
	case <-time.After(5 * time.Second):
		t.Fatal("no event was delivered to the fake Sentry server")
		return ""
	}
}

// Without WithSentry (or with an empty DSN) the courier must run exactly as
// before: init succeeds and every capture is a noop.
func TestSentry_NoopWithoutDSN(t *testing.T) {
	t.Setenv("SENTRY_DSN", "")
	if err := initSentry("", "", ""); err != nil {
		t.Fatalf("initSentry with empty DSN returned error: %v", err)
	}
	captureException(errors.New("boom"))
	captureWarning(errors.New("boom"))
	flushSentry(sentryFlushTimeout)
}

// Reporting is configured only through WithSentry: the SDK's SENTRY_DSN env
// fallback must neither enable it nor fail startup.
func TestRun_IgnoresSentryDSNEnv(t *testing.T) {
	t.Setenv("SENTRY_DSN", "not-a-dsn")
	t.Setenv("JACK_COURIER_SINK_NOOP", "true")
	swapDriver(t, fakeDriver{run: func(context.Context, SubmitFunc) error { return nil }})

	if code := run(WithLogger(discardLogger())); code != 0 {
		t.Fatalf("expected exit code 0 without WithSentry despite SENTRY_DSN, got %d", code)
	}
}

// A malformed DSN is a config error and must fail startup, like the other
// invalid-config cases.
func TestRun_MalformedSentryDSNFails(t *testing.T) {
	t.Setenv("JACK_COURIER_SINK_NOOP", "true")
	swapDriver(t, fakeDriver{run: func(context.Context, SubmitFunc) error { return nil }})

	if code := run(WithLogger(discardLogger()), WithSentry("not-a-dsn", "test", "")); code != 1 {
		t.Fatalf("expected exit code 1 for a malformed DSN, got %d", code)
	}
}

// A failed publish batch is the motivating case for PLAT-3457: it must
// reach Sentry, not just the logs and metrics.
func TestSubmit_PublishFailureReportsToSentry(t *testing.T) {
	bodies := newFakeSentry(t)

	c := newStalledCourier(t)
	c.submitTimeout = 100 * time.Millisecond

	if _, err := c.submit(context.Background(), []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("x")},
	}); err == nil {
		t.Fatal("expected batch error from stalled publishes")
	}
	flushSentry(sentryFlushTimeout)

	if body := awaitEvent(t, bodies); !strings.Contains(body, "publish batch") {
		t.Errorf("delivered event does not mention the publish batch failure; body: %.500s", body)
	}
}

// A driver that dies takes the courier down; that crash must reach Sentry.
func TestRun_DriverErrorReportsToSentry(t *testing.T) {
	bodies := newFakeSentry(t)

	c := &courier{
		driver:          fakeDriver{run: func(context.Context, SubmitFunc) error { return errors.New("driver kaboom") }},
		logger:          discardLogger(),
		shutdownTimeout: time.Second,
	}
	if code := c.run(t.Context(), &http.Server{}); code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	flushSentry(sentryFlushTimeout)

	if body := awaitEvent(t, bodies); !strings.Contains(body, "driver kaboom") {
		t.Errorf("delivered event does not contain the driver error; body: %.500s", body)
	}
}

// An abandoned driver is a degraded shutdown the courier healed around; it
// must surface at warning level.
func TestRun_AbandonedDriverReportsWarning(t *testing.T) {
	bodies := newFakeSentry(t)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	c := &courier{
		driver: fakeDriver{run: func(context.Context, SubmitFunc) error {
			<-release // ignores ctx cancellation
			return nil
		}},
		logger:          discardLogger(),
		shutdownTimeout: 50 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // simulate signal

	if code := c.run(ctx, &http.Server{}); code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	flushSentry(sentryFlushTimeout)

	body := awaitEvent(t, bodies)
	if !strings.Contains(body, "did not exit within shutdown timeout") {
		t.Errorf("delivered event does not mention the abandoned driver; body: %.500s", body)
	}
	if !strings.Contains(body, `"level":"warning"`) {
		t.Errorf("delivered event is not warning level; body: %.500s", body)
	}
}

// A queue with no topic mapping is retried by the driver forever; Sentry
// must get one event per queue, not one per retry.
func TestSubmit_UnmappedQueueReportsOnce(t *testing.T) {
	bodies := newFakeSentry(t)
	c, _ := newPubsubCourier(t, map[string]string{"high": "topic_high"}, []string{"topic_high"})

	for range 2 {
		if _, err := c.submit(context.Background(), []Job{{CorrelationID: "1", ID: "a", Queue: "low"}}); err == nil {
			t.Fatal("expected error for unmapped queue")
		}
	}
	if !flushSentry(sentryFlushTimeout) {
		t.Fatal("sentry flush timed out")
	}

	if body := awaitEvent(t, bodies); !strings.Contains(body, "no topic configured") {
		t.Errorf("delivered event does not mention the unmapped queue; body: %.500s", body)
	}
	select {
	case body := <-bodies:
		t.Errorf("unmapped queue reported more than once; body: %.500s", body)
	default:
	}
}

// Persistent publish failures are retried by the driver on a tight loop;
// repeats within the cooldown must produce a single event.
func TestSubmit_RepeatedPublishFailuresThrottled(t *testing.T) {
	bodies := newFakeSentry(t)

	c := newStalledCourier(t)
	c.submitTimeout = 100 * time.Millisecond

	for range 2 {
		if _, err := c.submit(context.Background(), []Job{
			{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("x")},
		}); err == nil {
			t.Fatal("expected batch error from stalled publishes")
		}
	}
	if !flushSentry(sentryFlushTimeout) {
		t.Fatal("sentry flush timed out")
	}

	if body := awaitEvent(t, bodies); !strings.Contains(body, "publish batch") {
		t.Errorf("delivered event does not mention the publish batch failure; body: %.500s", body)
	}
	select {
	case body := <-bodies:
		t.Errorf("repeated publish failure reported more than once within the cooldown; body: %.500s", body)
	default:
	}
}

// A canceled submit must not consume the queue's report slot and suppress
// a later real failure.
func TestReportSubmitFailure_CanceledDoesNotConsumeCooldown(t *testing.T) {
	bodies := newFakeSentry(t)
	c := &courier{}

	c.reportSubmitFailure("high", fmt.Errorf("courier: submit: %w", context.Canceled))
	c.reportSubmitFailure("high", errors.New("boom"))
	if !flushSentry(sentryFlushTimeout) {
		t.Fatal("sentry flush timed out")
	}

	if body := awaitEvent(t, bodies); !strings.Contains(body, "boom") {
		t.Errorf("real failure was not reported; body: %.500s", body)
	}
}

// Shutdown cancellation is the expected path out of submit and the driver;
// it must never turn into a Sentry event, at either level.
func TestCapture_SkipsContextCanceled(t *testing.T) {
	bodies := newFakeSentry(t)

	captureException(fmt.Errorf("courier: submit: %w", context.Canceled))
	captureWarning(fmt.Errorf("courier: close: %w", context.Canceled))
	if !flushSentry(sentryFlushTimeout) {
		t.Fatal("sentry flush timed out")
	}

	select {
	case body := <-bodies:
		t.Errorf("context.Canceled was reported to Sentry; body: %.500s", body)
	default:
	}
}

// Timeout env vars are parsed after Sentry is initialized, so their config
// errors are reported, unlike the config read before the options.
func TestRun_TimeoutConfigErrorReportsToSentry(t *testing.T) {
	bodies, dsn := startFakeSentry(t)
	t.Setenv("JACK_COURIER_SINK_NOOP", "true")
	t.Setenv("JACK_COURIER_SHUTDOWN_TIMEOUT", "nope")
	swapDriver(t, fakeDriver{run: func(context.Context, SubmitFunc) error { return nil }})

	if code := run(WithLogger(discardLogger()), WithSentry(dsn, "test", "")); code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
	if body := awaitEvent(t, bodies); !strings.Contains(body, "JACK_COURIER_SHUTDOWN_TIMEOUT") {
		t.Errorf("delivered event does not mention the invalid timeout; body: %.500s", body)
	}
}
