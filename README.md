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

- `JACK_COURIER_SUBMIT_TIMEOUT` — per-call deadline applied to each `EnqueueBulk` RPC, parsed as a Go duration (e.g. `30s`, `1m`). Defaults to `30s`. The driver's lifecycle context still controls overall shutdown; this only bounds an individual submit so a stalled jack-service cannot hang the courier.
- `JACK_COURIER_SHUTDOWN_TIMEOUT` — graceful shutdown timeout, parsed as a Go duration. Defaults to `10s`. On `SIGINT`/`SIGTERM` this bounds total termination time. If the driver does not return within it, the driver is abandoned and the process exits anyway, so the courier never takes longer than the timeout to shut down.
- `JACK_COURIER_SINK_NOOP` — if `true` or `1`, the courier never talks to jack-service: submitted jobs are acknowledged as accepted and dropped, and `JACK_SERVICE_ADDR` is not required. Used to validate drivers end-to-end in production (partitioning, batching, cursor advancement) before the real sink is in place.
