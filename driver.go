package courier

import (
	"context"
	"sync"
	"time"
)

// Job represents a single background job to be published to Pub/Sub.
type Job struct {
	// CorrelationID is a driver-assigned reference that is echoed back in
	// SubmitResult. The driver uses this to track which jobs were successfully
	// published (e.g., outbox row primary key, WAL LSN).
	CorrelationID string

	// ID is the producer-side job identifier, extracted from the producer's
	// payload. It is attached to the published message as the `job_id`
	// attribute; consumers use it for deduplication.
	ID string

	// Queue selects the Pub/Sub topic this job is published to. Drivers
	// populate it from the outbox row's queue column; the courier maps it
	// to a topic via its configured queue:topic map.
	Queue string

	// ProducerID identifies the producing service.
	ProducerID string

	// JobType is the job type name.
	JobType string

	// Payload is the opaque job data, published verbatim as the message
	// body. Its shape is the producer↔consumer contract; the courier does
	// not inspect it.
	Payload []byte

	// RunAt is the scheduled execution time. Zero value means run immediately.
	RunAt time.Time

	// TraceID is an optional distributed tracing correlation ID.
	TraceID string

	// InternalJackMeta carries the producer-supplied control header for this
	// job (a serialized jackpb.InternalJackMeta proto read from the outbox
	// row). The courier decodes it to derive the `shadow` message attribute.
	// Empty means no control fields are set.
	InternalJackMeta []byte
}

// SubmitResult represents the outcome of publishing a single job.
type SubmitResult struct {
	// CorrelationID is echoed from the submitted Job.CorrelationID.
	CorrelationID string

	// JobID is echoed from the submitted Job.ID.
	JobID string

	// Err is non-empty if this job failed to publish.
	Err string

	// Reason is the rejection class, set only for failures that can never
	// succeed: "payload_too_large", "validation_error". Empty means the
	// failure is retryable.
	Reason string
}

// SubmitFunc publishes a batch of jobs to Pub/Sub and returns per-job results.
// Calling it with an empty jobs slice is a no-op and returns nil, nil.
//
// If the batch fails as a whole (e.g. a transport outage where every publish
// failed retryably), err is non-nil and results is nil. The driver should
// retry the batch with backoff.
//
// On partial failure, results contains a mix of successful and failed entries.
// Each result has a `CorrelationID` echoed from the submitted job.
type SubmitFunc func(ctx context.Context, jobs []Job) ([]SubmitResult, error)

// Driver sources jobs for publishing. The courier calls Driver.Run, passing
// a submit callback. The driver controls when and how often to call submit —
// it owns the dispatch loop.
//
// When building a new driver, ensure it:
// - Populates all required Job fields
// - Batches per queue where possible: call-level metrics take the first job's queue (per-job metrics attribute by each job's own queue)
// - Handles batch errors from `submit` (e.g. retry with backoff)
// - Handles per-job failures in `SubmitResult.Err` (retry, dead-letter, or log)
// - Tracks delivery state so jobs are not re-submitted after success (e.g., advance WAL LSN, delete outbox rows, update cursor)
// - Batches efficiently — larger batches amortize per-call overhead; smaller batches reduce latency
// - Implements backpressure — if publishing is slow or erroring, the driver should back off rather than flood
type Driver interface {
	// Run starts the driver. The driver calls submit whenever it has a batch
	// of jobs ready for delivery. Run should exit when ctx is cancelled or an
	// unrecoverable error occurs.
	//
	// The driver is responsible for:
	//   - Sourcing jobs (e.g., Postgres WAL, polling, channel)
	//   - Deciding batch size and dispatch rate
	//   - Handling SubmitResult (advancing LSN, deleting rows, retrying failures)
	Run(ctx context.Context, submit SubmitFunc) error
}

var (
	driverMu         sync.Mutex
	registeredDriver Driver
	driverRegistered bool
)

// RegisterDriver registers a Driver to be used by Main.
// It panics if called more than once.
func RegisterDriver(d Driver) {
	driverMu.Lock()
	defer driverMu.Unlock()

	if driverRegistered {
		panic("courier: RegisterDriver called twice")
	}
	if d == nil {
		panic("courier: RegisterDriver called with nil driver")
	}

	registeredDriver = d
	driverRegistered = true
}

func getDriver() Driver {
	driverMu.Lock()
	defer driverMu.Unlock()
	return registeredDriver
}
