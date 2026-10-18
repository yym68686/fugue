package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

// ListActiveSourceOperationIDsByApp projects the membership used by runtime
// inspection without transferring historical desired specs or source bodies.
// It is a fresh read; it is not an operation or authorization cache.
func (s *Store) ListActiveSourceOperationIDsByApp(ctx context.Context, tenantID string, platformAdmin bool, appID string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	appID = strings.TrimSpace(appID)
	ids := []string{}
	if appID == "" {
		return ids, nil
	}
	if !s.usingDatabase() {
		operations, err := s.ListOperationsWithDesiredSourceByApp(tenantID, platformAdmin, appID)
		if err != nil {
			return nil, err
		}
		for _, op := range operations {
			if isActiveOperationStatus(op.Status) {
				ids = append(ids, op.ID)
			}
		}
		return ids, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	args := []any{appID, model.OperationStatusPending, model.OperationStatusRunning, model.OperationStatusWaitingAgent}
	query := `SELECT id FROM fugue_operations WHERE app_id = $1 AND desired_source_json IS NOT NULL AND status IN ($2, $3, $4)`
	if !platformAdmin {
		args = append(args, tenantID)
		query += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	// Match the existing source-operation reader's ordering.
	query += " ORDER BY created_at ASC"
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list active source operation IDs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ListAppReleasesByIDs returns complete snapshots for the requested membership,
// preserving the ordinary list's tenant, app, role and retirement filters.
// An empty membership never means an unrestricted list.
func (s *Store) ListAppReleasesByIDs(ctx context.Context, filter model.AppReleaseFilter, releaseIDs []string) ([]model.AppRelease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ids := trimmedStringSet(releaseIDs)
	out := []model.AppRelease{}
	if len(ids) == 0 {
		return out, nil
	}
	filter = normalizeAppReleaseFilter(filter)
	if s.usingDatabase() {
		return s.pgListAppReleaseProjectionIDsContext(ctx, filter, appReleaseFullSpec, sortedTrimmedStringKeys(ids))
	}
	err := s.withLockedState(false, func(state *model.State) error {
		for _, release := range state.AppReleases {
			if _, found := ids[release.ID]; found && appReleaseMatchesFilter(release, filter) {
				out = append(out, release)
			}
		}
		sortAppReleases(out)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, ctx.Err()
}
