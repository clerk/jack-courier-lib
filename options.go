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
