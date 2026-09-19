package store

import (
	"context"
	"database/sql"
	"sort"
	"time"

	"fugue/internal/model"
)

// ReleaseMetricsSnapshot preserves the retained-attempt window while loading
// its steps in one database round trip. It never reads deployment secrets.
type ReleaseMetricsSnapshot struct {
	Attempts []model.ReleaseAttempt
	Steps    map[string][]model.ReleaseStep
}

func (s *Store) LoadReleaseMetricsSnapshot(ctx context.Context, limit int) (ReleaseMetricsSnapshot, error) {
	out := ReleaseMetricsSnapshot{Steps: map[string][]model.ReleaseStep{}}
	if limit <= 0 || limit > 500 {
		limit = 500
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
		if err != nil {
			return out, err
		}
		defer tx.Rollback()
		rows, err := tx.QueryContext(ctx, `SELECT id,trigger_type,status,started_at,finished_at FROM fugue_release_attempts ORDER BY started_at DESC, id DESC LIMIT $1`, limit)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var a model.ReleaseAttempt
			if err = rows.Scan(&a.ID, &a.TriggerType, &a.Status, &a.StartedAt, &a.FinishedAt); err != nil {
				rows.Close()
				return out, err
			}
			out.Attempts = append(out.Attempts, a)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
		ids := make([]string, 0, len(out.Attempts))
		for _, a := range out.Attempts {
			ids = append(ids, a.ID)
		}
		rows, err = tx.QueryContext(ctx, `SELECT release_attempt_id, id, step_type, status, started_at, finished_at, COALESCE(payload_json->>'phase','') FROM fugue_release_steps WHERE release_attempt_id = ANY($1::text[]) ORDER BY started_at ASC,id ASC`, ids)
		if err != nil {
			return out, err
		}
		for rows.Next() {
			var step model.ReleaseStep
			var phase string
			if err = rows.Scan(&step.ReleaseAttemptID, &step.ID, &step.Type, &step.Status, &step.StartedAt, &step.FinishedAt, &phase); err != nil {
				rows.Close()
				return out, err
			}
			step.Payload = map[string]any{"phase": phase}
			out.Steps[step.ReleaseAttemptID] = append(out.Steps[step.ReleaseAttemptID], step)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return out, err
		}
		return out, tx.Commit()
	}
	err := s.withLockedState(false, func(state *model.State) error {
		out.Attempts = append(out.Attempts, state.ReleaseAttempts...)
		sort.Slice(out.Attempts, func(i, j int) bool {
			a, b := out.Attempts[i], out.Attempts[j]
			if !a.StartedAt.Equal(b.StartedAt) {
				return a.StartedAt.After(b.StartedAt)
			}
			return a.ID > b.ID
		})
		if len(out.Attempts) > limit {
			out.Attempts = out.Attempts[:limit]
		}
		ids := map[string]bool{}
		for _, a := range out.Attempts {
			ids[a.ID] = true
		}
		for _, step := range state.ReleaseSteps {
			if ids[step.ReleaseAttemptID] {
				step.Payload = map[string]any{"phase": step.Payload["phase"]}
				out.Steps[step.ReleaseAttemptID] = append(out.Steps[step.ReleaseAttemptID], step)
			}
		}
		for id, steps := range out.Steps {
			sort.Slice(steps, func(i, j int) bool {
				if !steps[i].StartedAt.Equal(steps[j].StartedAt) {
					return steps[i].StartedAt.Before(steps[j].StartedAt)
				}
				return steps[i].ID < steps[j].ID
			})
			out.Steps[id] = steps
		}
		return ctx.Err()
	})
	return out, err
}
