package courier

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"google.golang.org/grpc"
)

// Option configures a courier instance.
type Option func(*courier) error

// WithLogger sets the structured logger. Defaults to slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(c *courier) error {
		if logger == nil {
			return fmt.Errorf("courier: logger must not be nil")
		}
		c.logger = logger
		return nil
	}
}

// WithTLS forces TLS on or off for the gRPC connection to jack-service.
// By default, TLS is auto-detected: enabled when the address ends with ":443".
func WithTLS(enabled bool) Option {
	return func(c *courier) error {
		c.tlsOverride = &enabled
		return nil
	}
}

// WithGRPCDialOptions appends additional gRPC dial options to the connection.
func WithGRPCDialOptions(opts ...grpc.DialOption) Option {
	return func(c *courier) error {
		c.grpcDialOpts = append(c.grpcDialOpts, opts...)
		return nil
	}
}

// WithStatsd sets the DogStatsD client for metrics. Defaults to a no-op client.
func WithStatsd(sd statsd.ClientInterface) Option {
	return func(c *courier) error {
		if sd == nil {
			return fmt.Errorf("courier: statsd client must not be nil")
		}
		c.statsd = sd
		return nil
	}
}

// WithShutdownTimeout sets the maximum time allowed for graceful shutdown
// after a termination signal is received. Defaults to 10 seconds.
func WithShutdownTimeout(d time.Duration) Option {
	return func(c *courier) error {
		if d <= 0 {
			return fmt.Errorf("courier: shutdown timeout must be > 0, got %s", d)
		}
		c.shutdownTimeout = d
		return nil
	}
}

// WithSubmitTimeout sets the per-call deadline applied to each EnqueueBulk
// RPC. The driver's lifecycle context still controls overall shutdown.
// This only affects an individual submit so a stalled jack-service cannot hang the
// courier indefinitely. Defaults to 30 seconds.
func WithSubmitTimeout(d time.Duration) Option {
	return func(c *courier) error {
		if d <= 0 {
			return fmt.Errorf("courier: submit timeout must be > 0, got %s", d)
		}
		c.submitTimeout = d
		return nil
	}
}

// WithMaxCallSendMsgSize sets the maximum serialized size in bytes of an
// outgoing EnqueueBulk request. A batch whose marshaled size exceeds this
// limit fails the entire RPC with ResourceExhausted — the driver must size
// batches in bytes, not just row count, when this matters. Defaults to
// 4 MiB (gRPC's own default).
func WithMaxCallSendMsgSize(bytes int) Option {
	return func(c *courier) error {
		if bytes <= 0 {
			return fmt.Errorf("courier: max call send message size must be > 0, got %d", bytes)
		}
		c.maxCallSendMsgBytes = bytes
		return nil
	}
}

// WithMaxCallRecvMsgSize sets the maximum serialized size in bytes of an
// incoming response from jack-service. EnqueueBulk responses are small
// relative to requests, so this rarely needs raising; exposed for symmetry.
// Defaults to 4 MiB (gRPC's own default).
func WithMaxCallRecvMsgSize(bytes int) Option {
	return func(c *courier) error {
		if bytes <= 0 {
			return fmt.Errorf("courier: max call recv message size must be > 0, got %d", bytes)
		}
		c.maxCallRecvMsgBytes = bytes
		return nil
	}
}

// WithTraceServiceName sets the DataDog APM service tag applied to client
// gRPC spans. Callers SHOULD set this to their service identifier (e.g.
// "background-jobs-courier-core") so client spans group cleanly under the
// same DD APM service as the rest of their telemetry. Defaults to
// "background-jobs-courier".
func WithTraceServiceName(name string) Option {
	return func(c *courier) error {
		if name == "" {
			return fmt.Errorf("courier: trace service name must not be empty")
		}
		c.traceServiceName = name
		return nil
	}
}
