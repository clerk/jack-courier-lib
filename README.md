# jack-courier-lib

A Go library that delivers background jobs to Clerk's Jack service. It provides a pluggable **Driver** interface so that different job-sourcing strategies (Postgres WAL, polling, message queues, etc.) can be implemented independently.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│  courier (this library)                              │
│                                                      │
│   ┌────────┐    submit([]Job)    ┌───────────────┐   │
│   │ Driver │ ──────────────────► │ gRPC client   │   │
│   │  .Run()│ ◄────────────────── │ (EnqueueBulk) │   │
│   └────────┘   []SubmitResult    └───────────────┘   │
│        ▲                                │            │
│        │ ctx cancel                     │            │
│        │ (SIGINT/SIGTERM)               ▼            │
│   ┌──────────────┐             ┌─────────────────┐   │
│   │ Health HTTP  │             │  jack-service   │   │
│   │ GET /health  │             │  (gRPC server)  │   │
│   └──────────────┘             └─────────────────┘   │
└──────────────────────────────────────────────────────┘
```

## Driver Interface

A driver is the **only thing you need to implement** to create a new courier service. The interface is intentionally minimal:

```go
type Driver interface {
    Run(ctx context.Context, submit SubmitFunc) error
}
```

### Contract

1. **`Run` blocks** until `ctx` is cancelled or an unrecoverable error occurs.
2. **The driver owns the dispatch loop.** It decides when, how often, and in what batch sizes to call `submit`.
3. **The driver sources jobs.** It is responsible for connecting to whatever data source provides jobs (Postgres table, WAL stream, Redis queue, etc.).
4. **The driver handles results.** After each `submit` call, the driver receives `[]SubmitResult` and must handle successes (e.g., advance a cursor, delete outbox rows) and failures (e.g., retry, log, move to dead letter).
5. **The driver must respect context cancellation.** When `ctx` is cancelled, `Run` should clean up and return promptly (within the courier's shutdown timeout, default 10s).

### SubmitFunc

```go
type SubmitFunc func(ctx context.Context, jobs []Job) ([]SubmitResult, error)
```

- If the gRPC call itself fails, `err` is non-nil and `results` is nil. The driver should retry or back off.
- On **partial failure**, `results` contains a mix of successful and failed entries. Each result has a `CorrelationID` echoed from the submitted job.
- Calling `submit` with an empty slice is a no-op (returns `nil, nil`).

### Job

```go
type Job struct {
    CorrelationID string    // Driver-assigned reference, echoed back in SubmitResult
    ProducerID    string    // Identifies the producer in jack-service
    JobType       string    // Registered job type name in jack-service
    Payload       []byte    // Opaque job data delivered to consumers
    RunAt         time.Time // Scheduled execution time (zero = immediate)
    TraceID       string    // Optional distributed tracing ID
}
```

| Field           | Required | Notes |
|-----------------|----------|-------|
| `CorrelationID` | Yes      | The driver uses this to track which jobs succeeded/failed. Typically an outbox row ID, WAL LSN, or sequence number. |
| `ProducerID`    | Yes      | Must match a registered producer in jack-service. |
| `JobType`       | Yes      | Must match a registered job type in jack-service. |
| `Payload`       | Yes      | Opaque bytes — typically JSON. jack-service passes this through to consumers. |
| `RunAt`         | No       | Zero value means run immediately. Non-zero schedules the job for future execution. |
| `TraceID`       | No       | For distributed tracing correlation. |

### SubmitResult

```go
type SubmitResult struct {
    CorrelationID string // Echoed from Job.CorrelationID
    JobID         string // jack-service assigned job ID (empty on failure)
    Err           string // Non-empty if this job failed to enqueue
}
```

The driver should check `Err` on each result. A non-empty `Err` means that specific job was rejected by jack-service (e.g., unknown job type, invalid payload).

## Driver Registration

Drivers are registered globally before calling `Main`:

```go
func main() {
    courier.RegisterDriver(myDriver)
    courier.Main()
}
```

- `RegisterDriver` must be called exactly once. Calling it twice panics.
- Passing `nil` panics.

## Entry Point

`courier.Main()` handles the full lifecycle:

1. Reads `JACK_SERVICE_ADDR` (required) and connects to jack-service via gRPC.
2. Starts an HTTP health server on `PORT` (default `8080`) at `GET /health`.
3. Calls `driver.Run(ctx, submit)` — this blocks.
4. On `SIGINT`/`SIGTERM`, cancels the context and waits up to `JACK_COURIER_SHUTDOWN_TIMEOUT` (default `10s`) for graceful shutdown.

### Environment Variables

| Variable                       | Required | Default | Description |
|--------------------------------|----------|---------|-------------|
| `JACK_SERVICE_ADDR`            | Yes      | —       | jack-service gRPC address (e.g., `jack-service:50051`) |
| `PORT`                         | No       | `8080`  | Health check HTTP server port |
| `JACK_COURIER_SHUTDOWN_TIMEOUT`| No       | `10s`   | Graceful shutdown timeout (Go duration) |

## Options

Options customize the courier instance passed to `Main`:

```go
courier.Main(
    courier.WithLogger(logger),
    courier.WithTLS(true),
    courier.WithGRPCDialOptions(grpc.WithBlock()),
    courier.WithShutdownTimeout(30 * time.Second),
)
```

| Option                  | Description |
|-------------------------|-------------|
| `WithLogger`            | Custom `*slog.Logger` (default: JSON to stdout) |
| `WithTLS`               | Force TLS on/off (default: auto-detect, enabled when address ends with `:443`) |
| `WithGRPCDialOptions`   | Append additional gRPC dial options |
| `WithShutdownTimeout`   | Override shutdown timeout (must be > 0) |

## Implementing a Driver — Checklist

When building a new driver, ensure it:

- [ ] **Blocks in `Run`** until `ctx.Done()` or unrecoverable error
- [ ] **Respects context cancellation** and shuts down within the timeout
- [ ] **Populates all required Job fields** (`CorrelationID`, `ProducerID`, `JobType`, `Payload`)
- [ ] **Handles gRPC errors** from `submit` (retry with backoff)
- [ ] **Handles per-job failures** in `SubmitResult.Err` (retry, dead-letter, or log)
- [ ] **Tracks delivery state** so jobs are not re-submitted after success (e.g., advance WAL LSN, delete outbox rows, update cursor)
- [ ] **Batches efficiently** — larger batches reduce gRPC round-trips; smaller batches reduce latency
- [ ] **Implements backpressure** — if jack-service is slow or erroring, the driver should back off rather than flood it
