# jack-courier-lib

A Go library that publishes background jobs to GCP Pub/Sub. It provides a pluggable **Driver** interface so that different job-sourcing strategies (Postgres polling, WAL, message queues, etc.) can be implemented independently.

Each job carries a queue name; the courier publishes it to that queue's configured topic. The payload is published verbatim as the message body — its shape is the producer↔consumer contract, not the courier's concern. The message carries `shadow` (decoded from the job's control header, so workers can ack shadow jobs without executing them), plus `job_id`, `trace_id` and `producer_id` attributes so consumers can dedup, trace and attribute jobs without decoding the payload. Attributes with empty values are omitted.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  courier (this library)                                  │
│                                                          │
│   ┌────────┐    submit([]Job)    ┌───────────────────┐   │
│   │ Driver │ ──────────────────► │ per-queue Pub/Sub │   │
│   │  .Run()│ ◄────────────────── │ publishers        │   │
│   └────────┘   []SubmitResult    └───────────────────┘   │
│        ▲                                  │              │
│        │ ctx cancel                       │              │
│        │ (SIGINT/SIGTERM)                 ▼              │
│   ┌──────────────┐              ┌──────────────────┐     │
│   │ Health HTTP  │              │  Pub/Sub topics  │     │
│   │ GET /health  │              │  (one per queue) │     │
│   └──────────────┘              └──────────────────┘     │
└──────────────────────────────────────────────────────────┘
```

Each queue gets its own Pub/Sub client so one queue's backpressure or connection trouble cannot affect another's.

## Usage

A `Driver` is the **only thing you need to implement** to create a new courier service. The interface is intentionally minimal:

Drivers are registered globally before calling `Main`:

```go
func main() {
    courier.RegisterDriver(myDriver)
    courier.Main()
}
```

## Configuration

Required:

- `JACK_COURIER_PUBSUB_PROJECT` — GCP project ID of the Pub/Sub topics.
- `JACK_COURIER_PUBSUB_TOPICS` — comma-separated `queue:topic` pairs mapping each queue to the topic it publishes to, e.g. `high:clerk_jobs_high,low:clerk_jobs_low`. A job whose queue has no mapping fails its whole batch before anything is published.

Optional:

- `JACK_COURIER_SUBMIT_TIMEOUT` — deadline applied to each submit call's publishes, parsed as a Go duration (e.g. `30s`, `1m`). Defaults to `30s`. The driver's lifecycle context still controls overall shutdown; this only bounds an individual submit so a stalled Pub/Sub cannot hang the courier. It also caps the client's own publish attempts, so an abandoned publish stops retrying in the background.
- `JACK_COURIER_SHUTDOWN_TIMEOUT` — graceful shutdown timeout, parsed as a Go duration. Defaults to `10s`. On `SIGINT`/`SIGTERM` this bounds total termination time. If the driver does not return within it, the driver is abandoned and the process exits anyway, so the courier never takes longer than the timeout to shut down.
- `PUBSUB_EMULATOR_HOST` — standard Pub/Sub emulator override for local development.

## Error semantics

`submit` returns per-job results. A failure is permanent (`Reason` set to `payload_too_large` or `validation_error`) only when retrying can never succeed; drivers should dead-letter those. All other failures are retryable: the driver retries them on its own schedule. When every job in a batch fails retryably (e.g. a transport outage), `submit` returns a single batch error instead, so drivers retry the whole batch with backoff.
