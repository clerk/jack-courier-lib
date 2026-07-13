package courier

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

func TestParsePositiveDuration(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr bool
	}{
		{"valid duration", "30s", 30 * time.Second, false},
		{"zero is rejected", "0s", 0, true},
		{"negative is rejected", "-5s", 0, true},
		{"malformed is rejected", "nope", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePositiveDuration("JACK_COURIER_SUBMIT_TIMEOUT", tt.value)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got nil", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.value, err)
			}
			if got != tt.want {
				t.Errorf("parsePositiveDuration(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}

func TestSubmit_EmptyJobs(t *testing.T) {
	c := &courier{logger: slog.Default()}

	results, err := c.submit(context.Background(), nil)
	if err != nil {
		t.Fatalf("expected no error for empty jobs, got %v", err)
	}
	if results != nil {
		t.Errorf("expected nil results for empty jobs, got %v", results)
	}
}

func TestRegisterDriver_Panics(t *testing.T) {
	driverMu.Lock()
	origDriver := registeredDriver
	origRegistered := driverRegistered
	registeredDriver = nil
	driverRegistered = false
	driverMu.Unlock()

	defer func() {
		driverMu.Lock()
		registeredDriver = origDriver
		driverRegistered = origRegistered
		driverMu.Unlock()
	}()

	RegisterDriver(&noopDriver{})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on double RegisterDriver")
		}
	}()
	RegisterDriver(&noopDriver{})
}

func TestRegisterDriver_NilPanics(t *testing.T) {
	driverMu.Lock()
	origDriver := registeredDriver
	origRegistered := driverRegistered
	registeredDriver = nil
	driverRegistered = false
	driverMu.Unlock()

	defer func() {
		driverMu.Lock()
		registeredDriver = origDriver
		driverRegistered = origRegistered
		driverMu.Unlock()
	}()

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on nil RegisterDriver")
		}
	}()
	RegisterDriver(nil)
}

func TestRun_DriverExitsCleanly(t *testing.T) {
	c := &courier{
		driver:          fakeDriver{run: func(context.Context, SubmitFunc) error { return nil }},
		logger:          discardLogger(),
		shutdownTimeout: time.Second,
	}

	if code := c.run(t.Context(), &http.Server{}); code != 0 {
		t.Fatalf("expected exit code 0, got %d", code)
	}
}

func TestRun_DriverExitsWithError(t *testing.T) {
	c := &courier{
		driver:          fakeDriver{run: func(context.Context, SubmitFunc) error { return errors.New("boom") }},
		logger:          discardLogger(),
		shutdownTimeout: time.Second,
	}

	if code := c.run(t.Context(), &http.Server{}); code != 1 {
		t.Fatalf("expected exit code 1, got %d", code)
	}
}

func TestRun_SignalDriverShutsDownInTime(t *testing.T) {
	c := &courier{
		driver:          &noopDriver{}, // returns ctx.Err() once ctx is cancelled
		logger:          discardLogger(),
		shutdownTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // simulate SIGINT/SIGTERM

	if code := c.run(ctx, &http.Server{}); code != 0 {
		t.Fatalf("expected exit code 0 for a cooperative driver, got %d", code)
	}
}

func TestRun_SignalDriverAbandonedOnTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	c := &courier{
		driver: fakeDriver{run: func(context.Context, SubmitFunc) error {
			<-release // ignores ctx cancellation
			return nil
		}},
		logger:          discardLogger(),
		shutdownTimeout: 50 * time.Millisecond,
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // simulate signal

	if code := c.run(ctx, &http.Server{}); code != 1 {
		t.Fatalf("expected exit code 1 when the driver is abandoned, got %d", code)
	}
}

func TestDrainDriver_ReturnsWithinBudget(t *testing.T) {
	driverErr := errors.New("driver stopped")
	done := make(chan error, 1)
	done <- driverErr

	shutdownCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	abandoned, err := drainDriver(shutdownCtx, done)

	if abandoned {
		t.Fatal("expected abandoned=false when the driver returns within the budget")
	}
	if !errors.Is(err, driverErr) {
		t.Fatalf("expected driver error, got %v", err)
	}
}

func TestDrainDriver_ExceedsBudget(t *testing.T) {
	done := make(chan error, 1) // driver never returns

	shutdownCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	abandoned, err := drainDriver(shutdownCtx, done)

	if !abandoned {
		t.Fatal("expected abandoned=true when the driver exceeds the budget")
	}
	if err != nil {
		t.Fatalf("expected nil error when abandoning, got %v", err)
	}
}

// --- Helpers ---

type fakeDriver struct {
	run func(ctx context.Context, submit SubmitFunc) error
}

func (d fakeDriver) Run(ctx context.Context, submit SubmitFunc) error {
	return d.run(ctx, submit)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type noopDriver struct{}

func (d *noopDriver) Run(ctx context.Context, submit SubmitFunc) error {
	<-ctx.Done()
	return ctx.Err()
}
