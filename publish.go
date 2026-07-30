package courier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/clerk/jack/proto/jackpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

// Reason values reported on SubmitResult.Reason. They are exported so drivers
// can branch on the rejection class instead of matching the error text.
const (
	ReasonValidationError = "validation_error"
	ReasonPayloadTooLarge = "payload_too_large"
	ReasonNotYetDue       = "not_yet_due"
)

// ErrQueueUnmapped means no topic is configured for a queue.
var ErrQueueUnmapped = errors.New("courier: no topic configured for queue")

// futureJobLeeway is how far ahead of now a job may be scheduled and still be
// published immediately. Jobs due further out are held: nothing delays a
// published message yet (see PLAT-3376), so publishing them would run them
// early. A minute of leeway keeps jobs that are due imminently on the fast
// path rather than bouncing them back to the driver for one more round trip.
const futureJobLeeway = time.Minute

// queuePublisher owns the Pub/Sub client for a single queue. Each queue gets
// its own client — not just its own publisher handle — so one queue's
// backpressure or connection trouble cannot affect another's.
type queuePublisher struct {
	client *pubsub.Client
	pub    *pubsub.Publisher
}

// parseTopicMap parses a comma-separated list of queue:topic pairs, e.g.
// "high:clerk_jobs_high,low:clerk_jobs_low". Whitespace around either part
// is trimmed: a stray space (e.g. after the colon) would otherwise produce
// a topic name that can never publish.
func parseTopicMap(raw string) (map[string]string, error) {
	topics := make(map[string]string)
	for pair := range strings.SplitSeq(raw, ",") {
		queue, topic, ok := strings.Cut(pair, ":")
		queue, topic = strings.TrimSpace(queue), strings.TrimSpace(topic)
		if !ok || queue == "" || topic == "" {
			return nil, fmt.Errorf("invalid queue:topic pair %q", pair)
		}
		if _, dup := topics[queue]; dup {
			return nil, fmt.Errorf("duplicate queue %q", queue)
		}
		topics[queue] = topic
	}
	return topics, nil
}

func (c *courier) buildPublishers(ctx context.Context, topics map[string]string) error {
	c.publishers = make(map[string]*queuePublisher, len(topics))
	for queue, topic := range topics {
		client, err := pubsub.NewClient(ctx, c.project)
		if err != nil {
			c.stopPublishers(ctx)
			return fmt.Errorf("courier: create pubsub client for queue %q: %w", queue, err)
		}
		pub := client.Publisher(topic)
		// The submit deadline also bounds the client's own publish attempts:
		// otherwise an abandoned publish keeps retrying in the background
		// (60s client default) and can land after the driver already
		// retried the batch.
		if c.submitTimeout > 0 {
			pub.PublishSettings.Timeout = c.submitTimeout
		}
		c.publishers[queue] = &queuePublisher{client: client, pub: pub}
	}
	return nil
}

// stopPublishers flushes pending publishes and closes the per-queue clients,
// abandoning the flush when ctx expires so shutdown stays within its budget.
// Abandoning is safe: publishes that never resolved were not acked to the
// driver, so those jobs survive in the outbox or the driver's retryable DLQ
// and are redelivered later.
func (c *courier) stopPublishers(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		for _, qp := range c.publishers {
			wg.Go(func() {
				qp.pub.Stop()
				if err := qp.client.Close(); err != nil {
					c.logger.Warn("pubsub client close error", slog.String("error", err.Error()))
					captureWarning(err)
				}
			})
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-ctx.Done():
		c.logger.Warn("publisher shutdown exceeded the shutdown timeout, abandoning flush")
		captureWarning(errors.New("courier: publisher shutdown exceeded the shutdown timeout, abandoned flush"))
	}
}

// submit publishes a batch of jobs to their queues' Pub/Sub topics and maps
// per-message outcomes onto SubmitResults. When sinkNoop is set, nothing is
// published and every job is reported as accepted.
func (c *courier) submit(ctx context.Context, jobs []Job) (results []SubmitResult, err error) {
	if len(jobs) == 0 {
		return nil, nil
	}

	spanOptions := []tracer.StartSpanOption{
		tracer.ResourceName(jobs[0].Queue),
		tracer.Tag("jobs.count", len(jobs)),
	}
	if c.sinkNoop {
		spanOptions = append(spanOptions, tracer.Tag("sink.noop", true))
	} else {
		spanOptions = append(spanOptions,
			tracer.SpanType(ext.SpanTypeMessageProducer),
			tracer.Tag(ext.SpanKind, ext.SpanKindProducer),
			tracer.Tag(ext.MessagingSystem, ext.MessagingSystemGCPPubsub),
		)
	}
	span, ctx := tracer.StartSpanFromContext(ctx, "courier.submit", spanOptions...)
	defer func() {
		// The tag is only set when per-job outcomes exist; a batch-level
		// failure has none, and the span error already covers it.
		if results != nil {
			var failed int
			for _, r := range results {
				if r.Err != "" && r.Reason != ReasonNotYetDue {
					failed++
				}
			}
			span.SetTag("jobs.failed", failed)
		}
		// Shutdown cancellation (e.g. deployments) is not an unexpected error
		if errors.Is(err, context.Canceled) {
			span.Finish()
			return
		}
		span.Finish(tracer.WithError(err))
	}()

	// Refuse a dead context before enqueueing anything: the client publishes
	// bundles on a background context, so messages accepted here could still
	// reach the wire while the driver retries the batch.
	err = ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("courier: submit: %w", err)
	}

	if c.sinkNoop {
		// Acknowledge every job as accepted without publishing. Queues are
		// not validated: noop mode runs without any topic configuration.
		results = make([]SubmitResult, len(jobs))
		for i, j := range jobs {
			results[i] = SubmitResult{CorrelationID: j.CorrelationID, JobID: j.ID}
		}
		_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:noop", "queue:" + jobs[0].Queue}, 1)
		_ = c.statsd.Distribution("jack.courier.submit.batch_size", float64(len(jobs)), []string{"queue:" + jobs[0].Queue}, 1)
		return results, nil
	}

	// An unmapped queue is a config error. Reject the batch before publishing
	// anything: half-publishing would turn the driver's retry of the batch
	// into duplicates.
	for _, job := range jobs {
		if _, ok := c.publishers[job.Queue]; !ok {
			_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:error", "queue:" + job.Queue}, 1)
			err := fmt.Errorf("%w %q", ErrQueueUnmapped, job.Queue)
			c.reportSubmitFailure(job.Queue, err)
			return nil, err
		}
	}

	if c.submitTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.submitTimeout)
		defer cancel()
	}

	// Call-level metrics are tagged with the first job's queue: drivers
	// normally deliver per-queue batches, though the DLQ retry path can mix
	// queues; per-job metrics below attribute by each job's own queue.
	queueTag := "queue:" + jobs[0].Queue

	start := time.Now()
	notDueAfter := start.Add(futureJobLeeway)
	results = make([]SubmitResult, len(jobs))
	futures := make([]*pubsub.PublishResult, len(jobs))
	for i, job := range jobs {
		results[i] = SubmitResult{CorrelationID: job.CorrelationID, JobID: job.ID}

		shadow, err := shadowFromMeta(job.InternalJackMeta)
		if err != nil {
			results[i].Err = fmt.Sprintf("decode internal_jack_meta: %v", err)
			results[i].Reason = ReasonValidationError
			continue
		}

		// Hand a job scheduled beyond the leeway back to the driver rather
		// than publishing it early. It was never sent, so it is neither a
		// delivery failure nor delivered: the driver keeps it and submits it
		// again once it is due. Validation runs first, so a job that can
		// never publish is rejected now rather than after its RunAt.
		if job.RunAt.After(notDueAfter) {
			results[i].Err = fmt.Sprintf("not due until %s", job.RunAt.UTC().Format(time.RFC3339))
			results[i].Reason = ReasonNotYetDue
			continue
		}

		// Hoisted attributes let consumers act without decoding the payload
		// (dedup on job_id, tracing, producer attribution). Empty values are
		// omitted rather than published: workers key dedup on the attribute,
		// and a present-but-empty job_id would collide distinct jobs.
		attrs := map[string]string{"shadow": strconv.FormatBool(shadow)}
		if job.ID != "" {
			attrs["job_id"] = job.ID
		}
		if job.TraceID != "" {
			attrs["trace_id"] = job.TraceID
		}
		if job.ProducerID != "" {
			attrs["producer_id"] = job.ProducerID
		}

		futures[i] = c.publishers[job.Queue].pub.Publish(ctx, &pubsub.Message{
			Data:       job.Payload,
			Attributes: attrs,
		})
	}

	// retryable counts failures that a batch retry could fix; the rest —
	// deferrals and permanent rejections — carry a Reason and must resolve
	// per job. pubFailed counts only real publish failures, excluding jobs
	// that never reached Pub/Sub (deferrals, validation rejections).
	var failed, retryable, pubFailed int
	var firstPubErr error
	for i, f := range futures {
		if f == nil { // deferred or rejected before publish
			failed++
			continue
		}
		if _, err := f.Get(ctx); err != nil {
			if firstPubErr == nil {
				firstPubErr = err
			}
			pubFailed++
			results[i].Err = err.Error()
			results[i].Reason = classifyPublishError(err)
			failed++
			if results[i].Reason == "" {
				retryable++
			}
		}
	}

	// A batch where every job failed retryably is a transport-level problem:
	// surface it as a call error so the driver retries the whole batch with
	// backoff instead of dead-lettering everything. Anything carrying a Reason
	// must resolve per-job: a poison job would make its batch retry forever,
	// and a batch error would hide a deferral behind a transport failure.
	// Wrapping the publish error keeps its identity so shutdown cancellation
	// is not mistaken for a driver failure.
	if failed == len(jobs) && retryable == failed {
		_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:error", queueTag}, 1)
		err := fmt.Errorf("courier: publish batch: all %d jobs failed: %w", len(jobs), firstPubErr)
		c.reportSubmitFailure(jobs[0].Queue, err)
		return nil, err
	}

	// Publish failures that resolve per-job are reported too: the driver
	// retries or dead-letters them without any error reporting of its own.
	if firstPubErr != nil {
		c.reportSubmitFailure(jobs[0].Queue, fmt.Errorf("courier: publish: %d of %d jobs failed: %w", pubFailed, len(jobs), firstPubErr))
	}

	// submit.count reflects the call outcome (resolved per-job vs failed as
	// a batch); per-job failures are visible on submit.jobs{status:error}.
	_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:success", queueTag}, 1)
	_ = c.statsd.Distribution("jack.courier.submit.duration", time.Since(start).Seconds(), []string{queueTag}, 1)
	_ = c.statsd.Distribution("jack.courier.submit.batch_size", float64(len(jobs)), []string{queueTag}, 1)

	// Per-job metrics attribute by each job's own queue so a mixed batch
	// cannot hide which queue is failing. Deferrals get their own status:
	// nothing went wrong, and counting them as errors would put every future
	// job on the error rate.
	perQueueStatus := make(map[[2]string]int64)
	for i := range jobs {
		status := "success"
		switch {
		case results[i].Reason == ReasonNotYetDue:
			status = ReasonNotYetDue
		case results[i].Err != "":
			status = "error"
		}
		perQueueStatus[[2]string{jobs[i].Queue, status}]++
	}
	for k, n := range perQueueStatus {
		_ = c.statsd.Count("jack.courier.submit.jobs", n, []string{"status:" + k[1], "queue:" + k[0]}, 1)
	}

	c.countFutureJobs(jobs, results)

	return results, nil
}

// countFutureJobs measures how many submitted jobs are scheduled in the
// future, whether they were published (due within futureJobLeeway) or handed
// back to the driver as not yet due. The metric exists so we know the volume
// per job type before routing types that schedule ahead; see PLAT-3376.
// Failed jobs are excluded so rejections and their retries do not inflate the
// volume — a deferral is not a failure and still counts.
func (c *courier) countFutureJobs(jobs []Job, results []SubmitResult) {
	now := time.Now()
	type group struct{ queue, jobType string }
	perType := make(map[group]int64)
	for i := range jobs {
		if results[i].Err != "" && results[i].Reason != ReasonNotYetDue {
			continue
		}
		if jobs[i].RunAt.After(now) {
			perType[group{jobs[i].Queue, jobs[i].JobType}]++
		}
	}
	for g, n := range perType {
		_ = c.statsd.Count("jack.courier.submit.future_jobs", n, []string{"queue:" + g.queue, "job_type:" + g.jobType}, 1)
	}
}

// shadowFromMeta reports whether the job was enqueued in shadow mode. The
// outbox row carries a serialized jackpb.InternalJackMeta; workers ack
// shadow jobs without executing them, so the flag must reach the message
// attributes.
func shadowFromMeta(b []byte) (bool, error) {
	if len(b) == 0 {
		return false, nil
	}
	var meta jackpb.InternalJackMeta
	if err := proto.Unmarshal(b, &meta); err != nil {
		return false, err
	}
	return meta.GetShadow(), nil
}

// sentryReportCooldown bounds how often submit failures are reported to
// Sentry per queue: drivers retry failing batches on a tight loop, and
// unthrottled captures would burn the Sentry quota during a sustained
// outage. Logs and metrics still reflect every occurrence.
const sentryReportCooldown = time.Minute

func (c *courier) reportSubmitFailure(queue string, err error) {
	// Cancellation is never reported, so it must not consume the queue's
	// report slot and suppress a later real failure.
	if errors.Is(err, context.Canceled) {
		return
	}
	if last, ok := c.sentryLastReport.Load(queue); ok && time.Since(last.(time.Time)) < sentryReportCooldown {
		return
	}
	c.sentryLastReport.Store(queue, time.Now())
	captureException(err)
}

// classifyPublishError maps a publish failure to a SubmitResult.Reason.
// Empty means retryable. Only ErrOversizedMessage is permanent: it is the
// one failure proven per-message, checked client-side before sending.
// Server statuses (including InvalidArgument) stay retryable because a
// publish RPC carries a whole batch and stamps one error onto every message
// in it, so a request-level problem (e.g. a malformed topic in config) would
// otherwise permanently dead-letter valid committed jobs.
func classifyPublishError(err error) string {
	if errors.Is(err, pubsub.ErrOversizedMessage) {
		return ReasonPayloadTooLarge
	}
	return ""
}
