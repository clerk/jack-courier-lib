package courier

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	sentrygo "github.com/getsentry/sentry-go"
)

// sentryEnabled gates all captures on our own successful Init, so a hub
// initialized by the embedding binary is never reported to when courier
// reporting is disabled.
var sentryEnabled atomic.Bool

func initSentry(dsn, environment, release string) error {
	err := sentrygo.Init(sentrygo.ClientOptions{
		Dsn:         dsn,
		Environment: environment,
		Release:     release,
	})
	sentryEnabled.Store(err == nil && dsn != "")
	return err
}

func captureException(err error) {
	if !sentryEnabled.Load() || errors.Is(err, context.Canceled) {
		return
	}
	sentrygo.CurrentHub().CaptureException(err)
}

func captureWarning(err error) {
	if !sentryEnabled.Load() || errors.Is(err, context.Canceled) {
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
// drained. It does nothing when courier reporting is disabled, so a hub
// initialized by the embedding binary is never flushed.
func flushSentry(limit time.Duration) bool {
	if !sentryEnabled.Load() || limit <= 0 {
		return false
	}
	return sentrygo.Flush(limit)
}
