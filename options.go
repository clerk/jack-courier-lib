package courier

import (
	"fmt"
	"log/slog"
	"time"

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
