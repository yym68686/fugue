package operationexecution

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrPersistenceInterrupted = errors.New("operation persistence unavailable during observation")

// RetryPersistence retries a bounded Store call while the leader context
// retains ownership. Backoff is capped so recovery can resume promptly.
func RetryPersistence(ctx context.Context, delay time.Duration, pending func(int), call func() error, transient func(error) bool) error {
	if delay <= 0 {
		delay = time.Second
	}
	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := call()
		if ctx.Err() != nil {
			if transient(err) {
				return fmt.Errorf("%w: %w", ErrPersistenceInterrupted, ctx.Err())
			}
			return ctx.Err()
		}
		if !transient(err) {
			return err
		}
		if pending != nil {
			pending(attempt)
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("%w: %w", ErrPersistenceInterrupted, ctx.Err())
		case <-timer.C:
		}
		if delay < 15*time.Second {
			delay *= 2
			if delay > 15*time.Second {
				delay = 15 * time.Second
			}
		}
	}
}

// ResumeObservation repeats a read-only observation interrupted by a Store
// outage. The workload outcome stays unknown until observation succeeds.
func ResumeObservation(ctx context.Context, observe func() error) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := observe()
		if !errors.Is(err, ErrPersistenceInterrupted) || ctx.Err() != nil {
			return err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
