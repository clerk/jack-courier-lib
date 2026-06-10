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
