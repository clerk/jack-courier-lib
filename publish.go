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
)

const (
	reasonValidationError = "validation_error"
	reasonPayloadTooLarge = "payload_too_large"
)

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
	for _, pair := range strings.Split(raw, ",") {
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
				}
			})
		}
		wg.Wait()
	}()
	select {
	case <-done:
	case <-ctx.Done():
		c.logger.Warn("publisher shutdown exceeded the shutdown timeout, abandoning flush")
	}
}

// submit publishes a batch of jobs to their queues' Pub/Sub topics and maps
// per-message outcomes onto SubmitResults.
func (c *courier) submit(ctx context.Context, jobs []Job) ([]SubmitResult, error) {
	if len(jobs) == 0 {
		return nil, nil
	}

	// Refuse a dead context before enqueueing anything: the client publishes
	// bundles on a background context, so messages accepted here could still
	// reach the wire while the driver retries the batch. Wrapping keeps the
	// error identity for the shutdown path (errors.Is(context.Canceled)).
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("courier: submit: %w", err)
	}

	// An unmapped queue is a config error. Reject the batch before publishing
	// anything: half-publishing would turn the driver's retry of the batch
	// into duplicates.
	for i := range jobs {
		if _, ok := c.publishers[jobs[i].Queue]; !ok {
			_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:error", "queue:" + jobs[i].Queue}, 1)
			return nil, fmt.Errorf("courier: no topic configured for queue %q", jobs[i].Queue)
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
	results := make([]SubmitResult, len(jobs))
	futures := make([]*pubsub.PublishResult, len(jobs))
	for i := range jobs {
		j := &jobs[i]
		results[i] = SubmitResult{CorrelationID: j.CorrelationID, JobID: j.ID}

		shadow, err := shadowFromMeta(j.InternalJackMeta)
		if err != nil {
			results[i].Err = fmt.Sprintf("decode internal_jack_meta: %v", err)
			results[i].Reason = reasonValidationError
			continue
		}

		// Hoisted attributes let consumers act without decoding the payload
		// (dedup on job_id, tracing, producer attribution). Empty values are
		// omitted rather than published: workers key dedup on the attribute,
		// and a present-but-empty job_id would collide distinct jobs.
		attrs := map[string]string{"shadow": strconv.FormatBool(shadow)}
		if j.ID != "" {
			attrs["job_id"] = j.ID
		}
		if j.TraceID != "" {
			attrs["trace_id"] = j.TraceID
		}
		if j.ProducerID != "" {
			attrs["producer_id"] = j.ProducerID
		}

		futures[i] = c.publishers[j.Queue].pub.Publish(ctx, &pubsub.Message{
			Data:       j.Payload,
			Attributes: attrs,
		})
	}

	var failed, permanent int
	var firstPubErr error
	for i, f := range futures {
		if f == nil { // rejected before publish
			failed++
			permanent++
			continue
		}
		if _, err := f.Get(ctx); err != nil {
			if firstPubErr == nil {
				firstPubErr = err
			}
			results[i].Err = err.Error()
			results[i].Reason = classifyPublishError(err)
			failed++
			if results[i].Reason != "" {
				permanent++
			}
		}
	}

	// A batch where every job failed retryably is a transport-level problem:
	// surface it as a call error so the driver retries the whole batch with
	// backoff instead of dead-lettering everything. Any permanent failure
	// must resolve per-job, or a poison job would make its batch retry
	// forever. Wrapping the publish error keeps its identity so shutdown
	// cancellation is not mistaken for a driver failure.
	if failed == len(jobs) && permanent == 0 {
		_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:error", queueTag}, 1)
		return nil, fmt.Errorf("courier: publish batch: all %d jobs failed: %w", len(jobs), firstPubErr)
	}

	// submit.count reflects the call outcome (resolved per-job vs failed as
	// a batch); per-job failures are visible on submit.jobs{status:error}.
	_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:success", queueTag}, 1)
	_ = c.statsd.Distribution("jack.courier.submit.duration", time.Since(start).Seconds(), []string{queueTag}, 1)
	_ = c.statsd.Distribution("jack.courier.submit.batch_size", float64(len(jobs)), []string{queueTag}, 1)

	// Per-job metrics attribute by each job's own queue so a mixed batch
	// cannot hide which queue is failing.
	perQueueOK := make(map[string]int64)
	perQueueFailed := make(map[string]int64)
	for i := range jobs {
		if results[i].Err == "" {
			perQueueOK[jobs[i].Queue]++
		} else {
			perQueueFailed[jobs[i].Queue]++
		}
	}
	for q, n := range perQueueOK {
		_ = c.statsd.Count("jack.courier.submit.jobs", n, []string{"status:success", "queue:" + q}, 1)
	}
	for q, n := range perQueueFailed {
		_ = c.statsd.Count("jack.courier.submit.jobs", n, []string{"status:error", "queue:" + q}, 1)
	}

	c.countFutureJobs(jobs, results)

	return results, nil
}

// countFutureJobs measures how many published jobs are scheduled in the
// future. The courier publishes them like everything else (nothing delays
// them yet; see PLAT-3376); the metric exists so we know the volume per
// job type before routing types that schedule ahead. Failed jobs are
// excluded so rejections and their retries do not inflate the volume.
func (c *courier) countFutureJobs(jobs []Job, results []SubmitResult) {
	now := time.Now()
	type group struct{ queue, jobType string }
	perType := make(map[group]int64)
	for i := range jobs {
		if results[i].Err == "" && jobs[i].RunAt.After(now) {
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

// classifyPublishError maps a publish failure to a SubmitResult.Reason.
// Empty means retryable. Only ErrOversizedMessage is permanent: it is the
// one failure proven per-message, checked client-side before sending.
// Server statuses (including InvalidArgument) stay retryable because a
// publish RPC carries a whole batch and stamps one error onto every message
// in it, so a request-level problem (e.g. a malformed topic in config) would
// otherwise permanently dead-letter valid committed jobs.
func classifyPublishError(err error) string {
	if errors.Is(err, pubsub.ErrOversizedMessage) {
		return reasonPayloadTooLarge
	}
	return ""
}
