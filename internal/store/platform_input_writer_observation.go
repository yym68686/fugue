package store

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// Separate from the separated encoder's return counters: this covers the whole
// PostgreSQL input writer, including policy-disabled JSONB writes, transaction/
// queue acquisition and commit errors.
// A nil return is an outcome, not an independent durable-commit receipt.
var compilerInputWriterStats compilerInputWriterRecorder

type compilerInputWriterPhase uint8

const (
	compilerInputWriterValidate compilerInputWriterPhase = iota
	compilerInputWriterEncode
	compilerInputWriterBeginTx
	compilerInputWriterBarrier
	compilerInputWriterPersist
	compilerInputWriterCommit
	compilerInputWriterAfterCommit
	compilerInputWriterPhaseCount
)

var compilerInputWriterPhaseNames = [...]string{"validate", "encode_snapshot", "begin_tx", "writer_barrier", "persist_verify", "commit", "after_commit_return"}

type compilerInputWriterFailure struct {
	At                         time.Time        `json:"at"`
	Phase                      string           `json:"phase"`
	DeadlineSource             string           `json:"deadline_source"`
	TotalMS                    int64            `json:"total_ms"`
	CleanupMS                  int64            `json:"cleanup_ms"`
	PhaseMS                    map[string]int64 `json:"phase_ms"`
	ParentBudgetEntryMS        int64            `json:"parent_budget_entry_ms"`
	WriterBudgetEntryMS        int64            `json:"writer_budget_entry_ms"`
	ParentBudgetAfterBeginMS   int64            `json:"parent_budget_after_begin_ms"`
	WriterBudgetAfterBeginMS   int64            `json:"writer_budget_after_begin_ms"`
	ParentBudgetAfterBarrierMS int64            `json:"parent_budget_after_barrier_ms"`
	WriterBudgetAfterBarrierMS int64            `json:"writer_budget_after_barrier_ms"`
	ParentStateAtFailure       string           `json:"parent_state_at_failure"`
	WriterStateAtFailure       string           `json:"writer_state_at_failure"`
}

type compilerInputWriterRecorder struct {
	mu                                                        sync.Mutex
	attempts, inFlight, succeeded, deadlines, canceled, other uint64
	lastFailure                                               *compilerInputWriterFailure
}

type compilerInputWriterAttempt struct {
	recorder         *compilerInputWriterRecorder
	started          time.Time
	phaseStarted     time.Time
	phase            compilerInputWriterPhase
	phaseTime        [compilerInputWriterPhaseCount]time.Duration
	workStopped      time.Time
	parentState      string
	writerState      string
	parentErr        error
	writerErr        error
	parent           context.Context
	writer           context.Context
	parentEntryMS    int64
	writerEntryMS    int64
	parentAfterBegin int64
	writerAfterBegin int64
	parentAfterLock  int64
	writerAfterLock  int64
}

func (r *compilerInputWriterRecorder) begin() func(error) {
	attempt := r.beginObserved(context.Background())
	return attempt.finish
}

func (r *compilerInputWriterRecorder) beginObserved(parent context.Context) *compilerInputWriterAttempt {
	now := time.Now()
	r.mu.Lock()
	r.attempts++
	r.inFlight++
	r.mu.Unlock()
	return &compilerInputWriterAttempt{
		recorder: r, started: now, phaseStarted: now, parent: parent,
		parentEntryMS: compilerInputRemainingMS(parent), writerEntryMS: -1,
		parentAfterBegin: -1, writerAfterBegin: -1, parentAfterLock: -1, writerAfterLock: -1,
	}
}

func (a *compilerInputWriterAttempt) setWriterContext(ctx context.Context) {
	a.writer = ctx
	a.writerEntryMS = compilerInputRemainingMS(ctx)
}

func (a *compilerInputWriterAttempt) advance(phase compilerInputWriterPhase) {
	if phase >= compilerInputWriterPhaseCount {
		return
	}
	now := time.Now()
	a.phaseTime[a.phase] += now.Sub(a.phaseStarted)
	a.phase, a.phaseStarted = phase, now
}

func (a *compilerInputWriterAttempt) afterBegin() {
	a.parentAfterBegin = compilerInputRemainingMS(a.parent)
	a.writerAfterBegin = compilerInputRemainingMS(a.writer)
}

func (a *compilerInputWriterAttempt) afterBarrier() {
	a.parentAfterLock = compilerInputRemainingMS(a.parent)
	a.writerAfterLock = compilerInputRemainingMS(a.writer)
}

// Stop the active phase before deferred rollback. Cleanup is still included in
// the outer total, but cannot masquerade as time spent acquiring the barrier.
// Context states are sampled here, before our own deferred cancel.
func (a *compilerInputWriterAttempt) stopWork() {
	if !a.workStopped.IsZero() {
		return
	}
	a.workStopped = time.Now()
	a.phaseTime[a.phase] += a.workStopped.Sub(a.phaseStarted)
	a.parentState, a.parentErr = compilerInputSampleContext(a.parent)
	a.writerState, a.writerErr = compilerInputSampleContext(a.writer)
}

func (a *compilerInputWriterAttempt) rollback(tx *sql.Tx) {
	a.stopWork()
	_ = tx.Rollback()
}

func (a *compilerInputWriterAttempt) commit(tx *sql.Tx) error {
	a.advance(compilerInputWriterCommit)
	err := tx.Commit()
	if err == nil {
		// This records the driver's return, not a separate commit receipt.
		a.advance(compilerInputWriterAfterCommit)
	}
	return err
}

func (a *compilerInputWriterAttempt) finish(err error) {
	if a == nil || a.recorder == nil {
		return
	}
	a.stopWork()
	now := time.Now()
	r := a.recorder
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
	if err != nil {
		phaseMS := make(map[string]int64, len(compilerInputWriterPhaseNames))
		for i, name := range compilerInputWriterPhaseNames {
			phaseMS[name] = a.phaseTime[i].Milliseconds()
		}
		r.lastFailure = &compilerInputWriterFailure{
			At: a.workStopped.UTC(), Phase: compilerInputWriterPhaseNames[a.phase],
			DeadlineSource: compilerInputWriterDeadlineSource(err, a.parent, a.writer, a.parentErr, a.writerErr),
			TotalMS:        now.Sub(a.started).Milliseconds(), CleanupMS: now.Sub(a.workStopped).Milliseconds(), PhaseMS: phaseMS,
			ParentBudgetEntryMS: a.parentEntryMS, WriterBudgetEntryMS: a.writerEntryMS,
			ParentBudgetAfterBeginMS: a.parentAfterBegin, WriterBudgetAfterBeginMS: a.writerAfterBegin,
			ParentBudgetAfterBarrierMS: a.parentAfterLock, WriterBudgetAfterBarrierMS: a.writerAfterLock,
			ParentStateAtFailure: a.parentState, WriterStateAtFailure: a.writerState,
		}
	}
}

// These labels describe sampled context state and the effective deadline's
// owner. They do not establish a database wait cause or a per-operation receipt.
func compilerInputWriterDeadlineSource(err error, parent, writer context.Context, parentErr, writerErr error) string {
	switch {
	case errors.Is(writerErr, context.DeadlineExceeded):
		if parent != nil && writer != nil {
			pd, pok := parent.Deadline()
			wd, wok := writer.Deadline()
			if pok && wok && !wd.Before(pd) {
				return "parent_deadline_observed"
			}
		}
		return "writer_budget_deadline_observed"
	case errors.Is(parentErr, context.DeadlineExceeded):
		return "parent_deadline_observed"
	case errors.Is(parentErr, context.Canceled):
		return "parent_canceled_observed"
	case errors.Is(writerErr, context.Canceled):
		return "writer_canceled_observed"
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "57014" {
		return "database_query_canceled"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_unattributed"
	case errors.Is(err, context.Canceled):
		return "canceled_unattributed"
	default:
		return "non_deadline_failure"
	}
}

func compilerInputRemainingMS(ctx context.Context) int64 {
	if ctx == nil {
		return -1
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return -1
	}
	remaining := time.Until(deadline).Milliseconds()
	if remaining < 0 {
		return 0
	}
	return remaining
}

func compilerInputSampleContext(ctx context.Context) (string, error) {
	if ctx == nil {
		return "unavailable", nil
	}
	err := ctx.Err()
	switch err {
	case context.DeadlineExceeded:
		return "deadline_exceeded", err
	case context.Canceled:
		return "canceled", err
	default:
		if _, ok := ctx.Deadline(); ok {
			return "active_with_deadline", err
		}
		return "active_without_deadline", err
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

func (r *compilerInputWriterRecorder) diagnosticSnapshot() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	var last any
	if r.lastFailure != nil {
		copy := *r.lastFailure
		phases := make(map[string]int64, len(copy.PhaseMS))
		for name, duration := range copy.PhaseMS {
			phases[name] = duration
		}
		copy.PhaseMS = phases
		last = copy
	}
	return map[string]any{"schema": "fugue.compiler-input-writer-diagnostic.observation.v1", "last_failure": last}
}
