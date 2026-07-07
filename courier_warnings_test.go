package courier

import (
	"context"
	"testing"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/clerk/jack/proto/jackpb"
)

type incrCall struct {
	name string
	tags []string
}

type recordingStatsd struct {
	statsd.ClientInterface
	incrs []incrCall
}

func (r *recordingStatsd) Incr(name string, tags []string, _ float64) error {
	r.incrs = append(r.incrs, incrCall{name: name, tags: tags})
	return nil
}

func (r *recordingStatsd) countByName(name string) int {
	n := 0
	for _, c := range r.incrs {
		if c.name == name {
			n++
		}
	}
	return n
}

func (r *recordingStatsd) tagsForName(name string) []string {
	for _, c := range r.incrs {
		if c.name == name {
			return c.tags
		}
	}
	return nil
}

func hasTag(tags []string, want string) bool {
	for _, tag := range tags {
		if tag == want {
			return true
		}
	}
	return false
}

func TestCollectResults_PreservesWarnings(t *testing.T) {
	resp := &jackpb.EnqueueBulkResponse{
		Results: []*jackpb.BulkResult{
			{
				Index:         0,
				JobId:         "pjob_ok",
				CorrelationId: "c1",
				ErrorMessages: []string{"unregistered producer_id: p"},
			},
		},
	}

	results := collectResults(resp)

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if len(results[0].ErrorMessages) != 1 || results[0].ErrorMessages[0] != "unregistered producer_id: p" {
		t.Errorf("expected ErrorMessages preserved, got %v", results[0].ErrorMessages)
	}
}

func TestSubmit_WarningResultEmitsWarningCounter(t *testing.T) {
	srv := &mockBackgroundJobsServer{
		enqueueBulkFn: func(_ context.Context, req *jackpb.EnqueueBulkRequest) (*jackpb.EnqueueBulkResponse, error) {
			return &jackpb.EnqueueBulkResponse{
				Results: []*jackpb.BulkResult{
					{
						Index:         0,
						JobId:         "pjob_ok",
						CorrelationId: req.Jobs[0].CorrelationId,
						ErrorMessages: []string{"unregistered job_type: email"},
					},
				},
			}, nil
		},
	}

	conn := startMockServer(t, srv)
	rec := &recordingStatsd{ClientInterface: &statsd.NoOpClient{}}
	c := &courier{
		client: jackpb.NewBackgroundJobsClient(conn),
		logger: discardLogger(),
		statsd: rec,
	}

	jobs := []Job{{CorrelationID: "c1", ProducerID: "p", JobType: "email"}}

	results, err := c.submit(t.Context(), jobs)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}

	if results[0].Err != "" {
		t.Errorf("warning-only result must not be a failure, got err=%q", results[0].Err)
	}

	if got := rec.countByName("jack.courier.warnings.count"); got != 1 {
		t.Errorf("expected 1 jack.courier.warnings.count emission, got %d", got)
	}
	if tags := rec.tagsForName("jack.courier.warnings.count"); !hasTag(tags, "job_type:email") {
		t.Errorf("expected job_type:email tag on warnings counter, got %v", tags)
	}
}
