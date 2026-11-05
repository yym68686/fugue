package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Attempt counters are process-local, not proof of a committed transaction.
// Successful maintenance commits also have durable storage_events receipts.
var separatedInputStats struct {
	writes, failures, fallback, logical, newPackBytes atomic.Uint64
	mu                                                sync.Mutex
	lastFailureCategory                               string
	lastFailureAt                                     time.Time
	lastWriteTimings                                  map[string]int64
	lastFailureTimings                                map[string]int64
	lastWriteProfile                                  map[string]int64
	lastFailureProfile                                map[string]int64
	lastFailurePhase                                  string
}

func separatedInputObservation() any {
	separatedInputStats.mu.Lock()
	category, at := separatedInputStats.lastFailureCategory, separatedInputStats.lastFailureAt
	timings := separatedInputStats.lastWriteTimings
	failureTimings, failurePhase := separatedInputStats.lastFailureTimings, separatedInputStats.lastFailurePhase
	profile, failureProfile := separatedInputStats.lastWriteProfile, separatedInputStats.lastFailureProfile
	separatedInputStats.mu.Unlock()
	return map[string]any{
		"verified_write_attempts": separatedInputStats.writes.Load(),
		"write_failures":          separatedInputStats.failures.Load(),
		"budget_fallbacks":        separatedInputStats.fallback.Load(),
		"logical_bytes":           separatedInputStats.logical.Load(),
		"new_pack_bytes":          separatedInputStats.newPackBytes.Load(),
		"last_failure_category":   category,
		"last_failure_at":         at,
		"last_write_timings_ms":   timings,
		"last_failure_timings_ms": failureTimings,
		"last_write_profile":      profile,
		"last_failure_profile":    failureProfile,
		"last_failure_phase":      failurePhase,
	}
}

func observeSeparatedFailure(err error) {
	category := "unclassified"
	var pg *pgconn.PgError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		category = "deadline"
	case errors.Is(err, context.Canceled):
		category = "canceled"
	case errors.Is(err, errSharedInputBudget):
		category = "metadata-budget"
	case errors.Is(err, ErrConflict):
		category = "integrity-conflict"
	case errors.As(err, &pg):
		category = "database-" + pg.Code
	}
	separatedInputStats.failures.Add(1)
	separatedInputStats.mu.Lock()
	separatedInputStats.lastFailureCategory, separatedInputStats.lastFailureAt = category, time.Now().UTC()
	separatedInputStats.mu.Unlock()
}
