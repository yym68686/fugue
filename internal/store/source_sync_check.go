package store

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"fugue/internal/model"
)

// RecordAppSourceSyncCheck writes runtime facts only. Compare the source and
// previous check state so a late response cannot undo a rebind, resume, or newer
// observation. In particular, this never rewrites spec/source or app.updated_at.
func (s *Store) RecordAppSourceSyncCheck(ctx context.Context, app model.App, observed *model.AppSourceSyncStatus) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if app.ID == "" || observed == nil || observed.LastCheckedAt == nil {
		return false, ErrInvalidInput
	}
	source := model.AppOriginSource(app)
	if source == nil {
		return false, ErrInvalidInput
	}
	if !s.usingDatabase() {
		recorded := false
		err := s.withLockedState(true, func(state *model.State) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			i := findApp(state, app.ID)
			if i < 0 || isDeletedApp(state.Apps[i]) {
				return ErrNotFound
			}
			current := &state.Apps[i]
			if !reflect.DeepEqual(model.AppOriginSource(*current), source) || !reflect.DeepEqual(current.Status.SourceSync, app.Status.SourceSync) {
				return nil
			}
			current.Status.SourceSync = model.CloneAppSourceSyncStatus(observed)
			recorded = true
			return nil
		})
		return recorded, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	sourceJSON, err := json.Marshal(source)
	if err != nil {
		return false, err
	}
	previousJSON, err := json.Marshal(app.Status.SourceSync)
	if err != nil {
		return false, err
	}
	observedJSON, err := json.Marshal(observed)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE fugue_apps
SET status_json = jsonb_set(COALESCE(NULLIF(status_json, 'null'::jsonb), '{}'::jsonb), '{source_sync}', $2::jsonb)
WHERE id = $1
  AND COALESCE(source_json->'origin_source', source_json->'build_source', source_json) = $3::jsonb
  AND COALESCE(status_json->'source_sync', 'null'::jsonb) = $4::jsonb`, app.ID, observedJSON, sourceJSON, previousJSON)
	if err != nil {
		return false, fmt.Errorf("record source check: %w", err)
	}
	n, err := result.RowsAffected()
	return n == 1, err
}
