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

## Batch sizing — per-batch byte ceiling

The gRPC client uses a default per-call message size limit of **4 MiB** (gRPC's own default) for both send and receive. A single `EnqueueBulk` request whose marshaled size exceeds this fails the **entire batch** with a `ResourceExhausted` error — not just the offending row. Drivers that ship variable-size payloads must therefore size batches in **bytes**, not just row count.

- `WithMaxCallSendMsgSize(bytes)` raises the outgoing limit.
- `WithMaxCallRecvMsgSize(bytes)` raises the incoming limit (rarely needed; responses are small).

When in doubt, leave them at the default and have your driver track per-batch byte accumulation against the same ceiling. Drivers like [jack-courier-driver-pglg](https://github.com/clerk/jack-courier-driver-pglg) document this trade-off alongside their `MaxBatchSize` knob.

## Observability

The client wires a dd-trace-go gRPC interceptor so every `EnqueueBulk` call emits standard DataDog APM trace metrics:

- `trace.grpc.client.hits` — request count
- `trace.grpc.client.errors` — error count (tagged with `grpc.code` — `Unavailable`, `ResourceExhausted`, `DeadlineExceeded`, etc.)
- `trace.grpc.client.duration` — distribution

Tag them by `resource_name` for per-method breakdown, by `grpc.code` for throttling / bulkhead / timeout visibility. Use `WithTraceServiceName` to override the default service tag (`background-jobs-courier`) so client spans group cleanly under your service in DD APM.
