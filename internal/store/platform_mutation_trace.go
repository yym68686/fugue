package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"fugue/internal/runtimeobservation"
)

// Trace only fixed code stages and elapsed time. It changes neither the SQL
// deadline/locks nor any admission decision, and never captures SQL or inputs.
// Keeping the operation on the existing driver observations makes DB durations
// distinguishable from CPU validation inside the same transaction deadline.
type platformMutationTrace struct {
	ctx                  context.Context
	operation            string
	started, timeOfStage time.Time
	stage                string
	durations            map[string]time.Duration
	order                []string
	now                  func() time.Time
}

func startPlatformMutationTrace(ctx context.Context, operation string) (context.Context, *platformMutationTrace) {
	ctx = runtimeobservation.WithDatabaseOperation(ctx, operation)
	now := time.Now()
	return ctx, &platformMutationTrace{ctx: ctx, operation: operation, started: now, timeOfStage: now, stage: "setup", durations: map[string]time.Duration{}, now: time.Now}
}

func (t *platformMutationTrace) advance(stage string) {
	now := t.now()
	if _, ok := t.durations[t.stage]; !ok {
		t.order = append(t.order, t.stage)
	}
	t.durations[t.stage] += now.Sub(t.timeOfStage)
	t.stage, t.timeOfStage = stage, now
}

func (t *platformMutationTrace) finish(err error) error {
	if err == nil {
		return nil
	}
	// Preserve exact ordinary store sentinel identities for existing callers.
	// Only annotate the transaction/cancellation failures this trace diagnoses.
	if !errors.Is(err, sql.ErrTxDone) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		return err
	}
	failedStage := t.stage
	t.advance("finished")
	values := make([]string, 0, len(t.order))
	for _, name := range t.order {
		values = append(values, fmt.Sprintf("%s:%d", name, t.durations[name].Milliseconds()))
	}
	return fmt.Errorf("%s failed phase=%s elapsed_ms=%d deadline_exceeded=%t stage_ms=[%s]: %w", t.operation, failedStage, t.timeOfStage.Sub(t.started).Milliseconds(), t.ctx.Err() == context.DeadlineExceeded, strings.Join(values, ","), err)
}
