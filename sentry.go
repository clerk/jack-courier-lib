package courier

import (
	"context"
	"errors"
	"time"

	sentrygo "github.com/getsentry/sentry-go"
)

func initSentry(dsn, environment, release string) error {
	return sentrygo.Init(sentrygo.ClientOptions{
		Dsn:         dsn,
		Environment: environment,
		Release:     release,
	})
}

func captureException(err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	sentrygo.CurrentHub().CaptureException(err)
}

func captureWarning(err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	// The hub is cloned so the level cannot leak into captures on other
	// goroutines.
	hub := sentrygo.CurrentHub().Clone()
	hub.Scope().SetLevel(sentrygo.LevelWarning)
	hub.CaptureException(err)
}

const sentryFlushTimeout = 2 * time.Second

// flushSentry waits up to limit for buffered events to reach Sentry, since
// events are delivered asynchronously, reporting whether the queue fully
// drained.
func flushSentry(limit time.Duration) bool {
	if limit <= 0 {
		return false
	}
	return sentrygo.Flush(limit)
}
