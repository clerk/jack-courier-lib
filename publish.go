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
// "high:clerk_jobs_high,low:clerk_jobs_low".
func parseTopicMap(raw string) (map[string]string, error) {
	topics := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		queue, topic, ok := strings.Cut(strings.TrimSpace(pair), ":")
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
		c.publishers[queue] = &queuePublisher{client: client, pub: client.Publisher(topic)}
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

	// Drivers deliver per-queue batches, so the batch-level metric tags
	// reflect the first job's queue.
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

		futures[i] = c.publishers[j.Queue].pub.Publish(ctx, &pubsub.Message{
			Data: j.Payload,
			Attributes: map[string]string{
				"shadow": strconv.FormatBool(shadow),
				"job_id": j.ID,
			},
		})
	}

	var failed, permanent int
	for i, f := range futures {
		if f == nil { // rejected before publish
			failed++
			permanent++
			continue
		}
		if _, err := f.Get(ctx); err != nil {
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
	// forever.
	if failed == len(jobs) && permanent == 0 {
		_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:error", queueTag}, 1)
		return nil, fmt.Errorf("courier: publish batch: all %d jobs failed: %s", len(jobs), firstErr(results))
	}

	_ = c.statsd.Incr("jack.courier.submit.count", []string{"status:success", queueTag}, 1)
	_ = c.statsd.Distribution("jack.courier.submit.duration", time.Since(start).Seconds(), []string{queueTag}, 1)
	_ = c.statsd.Distribution("jack.courier.submit.batch_size", float64(len(jobs)), []string{queueTag}, 1)

	_ = c.statsd.Count("jack.courier.submit.jobs", int64(len(jobs)-failed), []string{"status:success", queueTag}, 1)
	if failed > 0 {
		_ = c.statsd.Count("jack.courier.submit.jobs", int64(failed), []string{"status:error", queueTag}, 1)
	}

	c.countFutureJobs(jobs, queueTag)

	return results, nil
}

// countFutureJobs measures how many published jobs are scheduled in the
// future. The courier publishes them like everything else (nothing delays
// them yet — see PLAT-3376); the metric exists so we know the volume per
// job type before routing types that schedule ahead.
func (c *courier) countFutureJobs(jobs []Job, queueTag string) {
	now := time.Now()
	perType := make(map[string]int64)
	for i := range jobs {
		if jobs[i].RunAt.After(now) {
			perType[jobs[i].JobType]++
		}
	}
	for jobType, n := range perType {
		_ = c.statsd.Count("jack.courier.submit.future_jobs", n, []string{queueTag, "job_type:" + jobType}, 1)
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

func firstErr(results []SubmitResult) string {
	for i := range results {
		if results[i].Err != "" {
			return results[i].Err
		}
	}
	return ""
}
