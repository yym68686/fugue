package store

import (
	"context"
	"errors"
	"sync"
)

// Separate from the separated encoder's return counters: this covers the whole
// PostgreSQL input writer, including policy-disabled JSONB writes, transaction/
// queue acquisition and commit errors.
// A nil return is an outcome, not an independent durable-commit receipt.
var compilerInputWriterStats compilerInputWriterRecorder

type compilerInputWriterRecorder struct {
	mu                                                        sync.Mutex
	attempts, inFlight, succeeded, deadlines, canceled, other uint64
}

func (r *compilerInputWriterRecorder) begin() func(error) {
	r.mu.Lock()
	r.attempts++
	r.inFlight++
	r.mu.Unlock()
	return func(err error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.inFlight--
		switch {
		case err == nil:
			r.succeeded++
		case errors.Is(err, context.DeadlineExceeded):
			r.deadlines++
		case errors.Is(err, context.Canceled):
			r.canceled++
		default:
			r.other++
		}
	}
}

func (r *compilerInputWriterRecorder) snapshot() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return map[string]any{
		"schema":   "fugue.compiler-input-writer.observation.v1",
		"attempts": r.attempts, "in_flight": r.inFlight,
		"successful_returns": r.succeeded, "deadline_failures": r.deadlines,
		"canceled_failures": r.canceled, "other_failures": r.other,
	}
}
