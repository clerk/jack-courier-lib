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

	// Shadow marks the job for ack-without-execute at the worker. Used
	// during migration to validate the pipeline without side effects.
	Shadow bool
}

// SubmitResult represents the outcome of submitting a single job to jack-service.
type SubmitResult struct {
	// CorrelationID is echoed from the submitted Job.CorrelationID.
	CorrelationID string

	// JobID is the jack-service assigned job identifier.
	JobID string

	// Err is non-empty if this job failed to enqueue.
	Err string
}

// SubmitFunc delivers a batch of jobs to jack-service and returns per-job results.
// If the gRPC call itself fails, err is non-nil and results is nil.
// On partial failure, results contains a mix of successful and failed entries.
type SubmitFunc func(ctx context.Context, jobs []Job) ([]SubmitResult, error)

// Driver sources jobs for delivery to jack-service. The courier calls
// Driver.Run, passing a submit callback. The driver controls when and
// how often to call submit — it owns the dispatch loop.
type Driver interface {
	// Run starts the driver. The driver calls submit whenever it has a batch
	// of jobs ready for delivery. Run blocks until ctx is cancelled or an
	// unrecoverable error occurs.
	//
	// The driver is responsible for:
	//   - Sourcing jobs (e.g., Postgres WAL, polling, channel)
	//   - Deciding batch size and dispatch rate
	//   - Handling SubmitResult (advancing LSN, deleting rows, retrying failures)
	Run(ctx context.Context, submit SubmitFunc) error
}

var (
	driverMu          sync.Mutex
	registeredDriver  Driver
	driverRegistered  bool
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
