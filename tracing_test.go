package courier

import (
	"context"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/mocktracer"
)

// submitSpan returns the single courier.submit span recorded by mt.
func submitSpan(t *testing.T, mt mocktracer.Tracer) mocktracer.Span {
	t.Helper()

	var spans []mocktracer.Span
	for _, s := range mt.FinishedSpans() {
		if s.OperationName() == "courier.submit" {
			spans = append(spans, s)
		}
	}
	if len(spans) != 1 {
		t.Fatalf("expected 1 courier.submit span, got %d", len(spans))
	}
	return spans[0]
}

func TestSubmit_SpanTagsAndPerJobFailures(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	c, _ := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	jobs := []Job{
		{CorrelationID: "1", ID: "a", Queue: "high", Payload: []byte("p1")},
		{CorrelationID: "2", ID: "b", Queue: "high", Payload: []byte("p2"), InternalJackMeta: []byte{0xff}},
	}
	if _, err := c.submit(t.Context(), jobs); err != nil {
		t.Fatalf("submit: %v", err)
	}

	span := submitSpan(t, mt)
	if got := span.Tag(ext.ResourceName); got != "high" {
		t.Errorf("resource = %v, want %q", got, "high")
	}
	if got := span.Tag(ext.SpanKind); got != ext.SpanKindProducer {
		t.Errorf("span.kind = %v, want %q", got, ext.SpanKindProducer)
	}
	if got := span.Tag("jobs.count"); got != 2 {
		t.Errorf("jobs.count = %v, want 2", got)
	}
	if got := span.Tag("jobs.failed"); got != 1 {
		t.Errorf("jobs.failed = %v, want 1", got)
	}
	if got := span.Tag(ext.Error); got != nil {
		t.Errorf("per-job failures must not mark the span as error, got %v", got)
	}
}

func TestSubmit_SpanBatchErrorIsMarked(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	c, _ := newPubsubCourier(t,
		map[string]string{"high": "topic_high"},
		[]string{"topic_high"},
	)

	jobs := []Job{{CorrelationID: "1", ID: "a", Queue: "unmapped", Payload: []byte("p")}}
	if _, err := c.submit(t.Context(), jobs); err == nil {
		t.Fatal("expected batch error for unmapped queue")
	}

	span := submitSpan(t, mt)
	if got := span.Tag(ext.Error); got == nil {
		t.Error("expected span to be marked as error")
	}
	if got := span.Tag("jobs.failed"); got != nil {
		t.Errorf("batch failure has no per-job outcomes, jobs.failed = %v", got)
	}
}

func TestSubmit_SpanCancellationIsNotAnError(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	c := &courier{sinkNoop: true, logger: discardLogger(), statsd: &statsd.NoOpClient{}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.submit(ctx, []Job{{CorrelationID: "1", Queue: "high"}}); err == nil {
		t.Fatal("expected error for cancelled context")
	}

	span := submitSpan(t, mt)
	if got := span.Tag(ext.Error); got != nil {
		t.Errorf("cancellation must not mark the span as error, got %v", got)
	}
}
