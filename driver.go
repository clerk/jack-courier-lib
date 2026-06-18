package courier

import (
	"context"
	"sync"
	"time"
)

// Job represents a single background job to be delivered to jack-service.
type Job struct {
	// CorrelationID is a driver-assigned reference that is echoed back in
	// SubmitResult. The driver uses this to track which jobs were successfully
	// enqueued (e.g., outbox row primary key, WAL LSN).
	CorrelationID string

	// ID is an optional caller-supplied job ID. When non-empty, jack-service
	// uses it as the canonical job identifier instead of generating one
	// server-side. Drivers (e.g., jack-courier-driver-pglg) populate this
	// from the producer's payload so the producer-side ID is preserved
	// end-to-end through the pipeline.
	ID string

	// ProducerID identifies the producer in jack-service.
	ProducerID string

	// JobType is the registered job type name in jack-service.
	JobType string

	// Payload is the opaque job data delivered to consumers.
	Payload []byte

	// RunAt is the scheduled execution time. Zero value means run immediately.
	RunAt time.Time

	// TraceID is an optional distributed tracing correlation ID.
	TraceID string

	// InternalJackMeta carries the producer-supplied jack-service control
	// header for this job. The driver reads the bytes from the producer's
	// outbox row (a serialized jack.InternalJackMeta proto) and the courier
	// ships them through to jack-service in EnqueueRequest.InternalJackMeta
	// unchanged. Empty means no jack-service control fields are set.
	InternalJackMeta []byte
}

// SubmitResult represents the outcome of submitting a single job to jack-service.
type SubmitResult struct {
	// CorrelationID is echoed from the submitted Job.CorrelationID.
	CorrelationID string

	// JobID is the jack-service assigned job identifier.
	JobID string

	// Err is non-empty if this job failed to enqueue.
	Err string

	// Reason is the rejection class from jack-service, set only when
	// Err is non-empty. Values: "validation_error", "payload_too_large",
	// "internal_error".
	Reason string
}

// SubmitFunc delivers a batch of jobs to jack-service and returns per-job results.
// Calling it with an empty jobs slice is a no-op and returns nil, nil.
//
// If the gRPC call itself fails, err is non-nil and results is nil. The driver
// should probably retry or back off.
//
// On partial failure, results contains a mix of successful and failed entries.
// Each result has a `CorrelationID` echoed from the submitted job.
type SubmitFunc func(ctx context.Context, jobs []Job) ([]SubmitResult, error)

// Driver sources jobs for delivery to jack-service. The courier calls
// Driver.Run, passing a submit callback. The driver controls when and
// how often to call submit — it owns the dispatch loop.
//
// When building a new driver, ensure it:
// - Populates all required Job fields
// - Handles gRPC errors from `submit` (e.g. retry with backoff)
// - Handles per-job failures in `SubmitResult.Err` (retry, dead-letter, or log)
// - Tracks delivery state so jobs are not re-submitted after success (e.g., advance WAL LSN, delete outbox rows, update cursor)
// - Batches efficiently — larger batches reduce gRPC round-trips; smaller batches reduce latency
// - Implements backpressure — if jack-service is slow or erroring, the driver should back off rather than flood it
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
