package courier

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
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

// WithSentry configures Sentry error reporting.
//
// An empty DSN disables reporting, so the option can be passed unconditionally.
// The courier owns the process's global Sentry client; nothing else in the
// process should initialize sentry-go.
func WithSentry(dsn, environment, release string) Option {
	return func(c *courier) error {
		c.sentryDSN = dsn
		if environment != "" {
			c.env = environment
		}
		if release != "" {
			c.serviceVersion = release
		}
		return nil
	}
}

// WithDatadogTracing enables the Datadog APM tracer for the process, tagging
// every span with the given service, environment and version. Each submit
// call emits one span covering its Pub/Sub publishes.
//
// An empty service disables tracing, so the option can be passed
// unconditionally. The courier owns the process's global tracer; nothing else
// in the process should call tracer.Start. Agent connectivity comes from the
// standard DD_* environment variables.
func WithDatadogTracing(service, environment, version string) Option {
	return func(c *courier) error {
		c.traceService = service
		if environment != "" {
			c.env = environment
		}
		if version != "" {
			c.serviceVersion = version
		}
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

// WithSubmitTimeout sets the deadline applied to each submit call's publishes.
// The driver's lifecycle context still controls overall shutdown. This only
// affects an individual submit so a stalled Pub/Sub cannot hang the courier
// indefinitely. Defaults to 30 seconds.
func WithSubmitTimeout(d time.Duration) Option {
	return func(c *courier) error {
		if d <= 0 {
			return fmt.Errorf("courier: submit timeout must be > 0, got %s", d)
		}
		c.submitTimeout = d
		return nil
	}
}
