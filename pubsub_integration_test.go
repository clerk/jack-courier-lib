package courier

import (
	"context"
	"crypto/rand"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// run this with make up/pubsub after ensuring the emulator is running

const (
	emulatorHostEnv = "JACK_COURIER_TEST_PUBSUB_HOST"
	emulatorProject = "clerk-local"
	receiveTimeout  = 10 * time.Second
	setupTimeout    = 10 * time.Second
)

func TestIntegration_SubmitRoundTrip(t *testing.T) {
	requireEmulator(t)

	admin, err := pubsub.NewClient(t.Context(), emulatorProject)
	require.NoError(t, err, "admin client")
	t.Cleanup(func() { _ = admin.Close() })

	topicID, subID := newEmulatorTopic(t, admin, "clerk_jobs_high")
	c := newEmulatorCourier(t, map[string]string{"high": topicID})

	cases := []struct {
		job        Job
		wantShadow string
	}{
		{
			job: Job{
				CorrelationID:    "outbox-1",
				ID:               "psjob_live",
				Queue:            "high",
				ProducerID:       "svc_courier_test",
				JobType:          "test.round_trip",
				Payload:          []byte(`{"hello":"fousekis"}`),
				TraceID:          "trace_live",
				InternalJackMeta: metaBytes(t, false),
			},
			wantShadow: "false",
		},
		{
			job: Job{
				CorrelationID:    "outbox-2",
				ID:               "psjob_shadow",
				Queue:            "high",
				ProducerID:       "svc_courier_test",
				JobType:          "test.round_trip",
				Payload:          []byte(`{"hello":"shadow fousekis"}`),
				TraceID:          "trace_shadow",
				InternalJackMeta: metaBytes(t, true),
			},
			wantShadow: "true",
		},
	}

	jobs := make([]Job, len(cases))
	for i, tc := range cases {
		jobs[i] = tc.job
	}

	results, err := c.submit(t.Context(), jobs)
	require.NoError(t, err, "submit")
	for i, r := range results {
		require.Emptyf(t, r.Err, "job %d (%s) failed to publish", i, r.JobID)
	}

	got := receiveN(t, admin, subID, len(jobs))

	for _, tc := range cases {
		t.Run(tc.job.ID, func(t *testing.T) {
			msg, ok := got[tc.job.ID]
			require.Truef(t, ok, "no message received for job %s", tc.job.ID)

			assert.Equal(t, tc.job.Payload, msg.Data, "payload")
			assert.Equal(t, tc.job.ID, msg.Attributes["job_id"], "job_id attribute")
			assert.Equal(t, tc.job.TraceID, msg.Attributes["trace_id"], "trace_id attribute")
			assert.Equal(t, tc.job.ProducerID, msg.Attributes["producer_id"], "producer_id attribute")
			assert.Equal(t, tc.wantShadow, msg.Attributes["shadow"], "shadow attribute")
		})
	}
}

// TestIntegration_RunRoutesQueuesToTopics drives the whole entry point rather
// than submit() alone: run() parses JACK_COURIER_PUBSUB_TOPICS, builds a
// publisher per queue, and routes a mixed batch to three real topics.
func TestIntegration_RunRoutesQueuesToTopics(t *testing.T) {
	requireEmulator(t)

	admin, err := pubsub.NewClient(t.Context(), emulatorProject)
	require.NoError(t, err, "admin client")
	t.Cleanup(func() { _ = admin.Close() })

	queues := []struct{ name, topic, sub string }{
		{name: "high"}, {name: "medium"}, {name: "low"},
	}
	pairs := make([]string, len(queues))
	for i, q := range queues {
		topicID, subID := newEmulatorTopic(t, admin, "clerk_jobs_"+q.name)
		queues[i].topic, queues[i].sub = topicID, subID
		pairs[i] = q.name + ":" + topicID
	}

	t.Setenv("JACK_COURIER_PUBSUB_PROJECT", emulatorProject)
	t.Setenv("JACK_COURIER_PUBSUB_TOPICS", strings.Join(pairs, ","))
	// An exported noop would ack every job without publishing, so pin it off.
	t.Setenv("JACK_COURIER_SINK_NOOP", "")
	t.Setenv("PORT", "0")

	var results []SubmitResult
	var submitErr error
	swapDriver(t, fakeDriver{run: func(ctx context.Context, submit SubmitFunc) error {
		jobs := make([]Job, len(queues))
		for i, q := range queues {
			jobs[i] = Job{
				CorrelationID: "outbox-" + q.name,
				ID:            "psjob_" + q.name,
				Queue:         q.name,
				ProducerID:    "svc_courier_test",
				JobType:       "test.run",
				Payload:       []byte(`{"queue":"` + q.name + `"}`),
			}
		}
		results, submitErr = submit(ctx, jobs)
		return nil
	}})

	require.Equal(t, 0, run(WithLogger(discardLogger())), "run exit code")
	require.NoError(t, submitErr, "submit")
	require.Len(t, results, len(queues))
	for _, r := range results {
		require.Emptyf(t, r.Err, "job %s failed to publish", r.JobID)
	}

	// Each queue's subscription must carry that queue's own job: a routing bug
	// delivers the wrong job_id here, or nothing at all.
	for _, q := range queues {
		t.Run(q.name, func(t *testing.T) {
			got := receiveN(t, admin, q.sub, 1)

			msg, ok := got["psjob_"+q.name]
			require.Truef(t, ok, "queue %s: topic %s carried %v", q.name, q.topic, slices.Collect(maps.Keys(got)))
			assert.JSONEq(t, `{"queue":"`+q.name+`"}`, string(msg.Data), "payload")
		})
	}
}

// ----- helpers ----

func requireEmulator(t *testing.T) {
	t.Helper()

	host := os.Getenv(emulatorHostEnv)
	if host == "" {
		t.Skipf("%s is not set. Start the emulator with `cd ../clerk_go && make up/pubsub`, then run `make test/pubsub`", emulatorHostEnv)
	}
	t.Setenv("PUBSUB_EMULATOR_HOST", host)
}

func newEmulatorTopic(t *testing.T, admin *pubsub.Client, base string) (topicID, subID string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), setupTimeout)
	defer cancel()

	suffix := strings.ToLower(rand.Text()[:10])
	topicID, subID = base+"_"+suffix, base+"_sub_"+suffix
	topicName := fmt.Sprintf("projects/%s/topics/%s", emulatorProject, topicID)
	subName := fmt.Sprintf("projects/%s/subscriptions/%s", emulatorProject, subID)

	_, err := admin.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topicName})
	require.NoErrorf(t, err, "create topic %s", topicName)

	t.Cleanup(func() {
		req := &pubsubpb.DeleteTopicRequest{Topic: topicName}
		if err := admin.TopicAdminClient.DeleteTopic(context.Background(), req); err != nil {
			t.Logf("cleanup: delete topic %s: %v", topicName, err)
		}
	})

	sub := &pubsubpb.Subscription{Name: subName, Topic: topicName}
	_, err = admin.SubscriptionAdminClient.CreateSubscription(ctx, sub)
	require.NoErrorf(t, err, "create subscription %s", subName)

	t.Cleanup(func() {
		req := &pubsubpb.DeleteSubscriptionRequest{Subscription: subName}
		if err := admin.SubscriptionAdminClient.DeleteSubscription(context.Background(), req); err != nil {
			t.Logf("cleanup: delete subscription %s: %v", subName, err)
		}
	})

	return topicID, subID
}

func newEmulatorCourier(t *testing.T, topics map[string]string) *courier {
	t.Helper()

	c := &courier{
		project:       emulatorProject,
		submitTimeout: receiveTimeout,
		logger:        discardLogger(),
		statsd:        &statsd.NoOpClient{},
	}
	require.NoError(t, c.buildPublishers(t.Context(), topics), "buildPublishers")
	t.Cleanup(func() { c.stopPublishers(context.Background()) })

	return c
}

func receiveN(t *testing.T, client *pubsub.Client, subID string, n int) map[string]*pubsub.Message {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), receiveTimeout)
	defer cancel()

	var mu sync.Mutex
	got := make(map[string]*pubsub.Message, n)

	err := client.Subscriber(subID).Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		m.Ack()
		mu.Lock()
		defer mu.Unlock()
		got[m.Attributes["job_id"]] = m
		if len(got) == n {
			cancel()
		}
	})
	require.NoErrorf(t, err, "receive from %s", subID)

	mu.Lock()
	defer mu.Unlock()
	require.Lenf(t, got, n, "received %d of %d messages within %s", len(got), n, receiveTimeout)

	return got
}
