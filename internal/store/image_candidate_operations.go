package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"fugue/internal/model"
)

// CompactImageCandidateOperations retains the first source occurrence and the
// first greatest observation time for each identical source/runtime input.
// Candidate folding uses first source richness, first nonempty runtime ref and
// the greatest observation time; intermediate repeats cannot change its result.
func CompactImageCandidateOperations(ops []model.Operation) []model.Operation {
	type representatives struct{ first, latest int }
	groups := make(map[string]representatives, len(ops))
	for i, op := range ops {
		image := ""
		if op.DesiredSpec != nil {
			image = op.DesiredSpec.Image
		}
		keyBytes, _ := json.Marshal(struct {
			AppID  string
			Source *model.AppSource
			Image  string
		}{op.AppID, op.DesiredSource, image})
		key := string(keyBytes)
		group, exists := groups[key]
		if !exists {
			groups[key] = representatives{i, i}
			continue
		}
		if imageOperationObservedAt(op).After(imageOperationObservedAt(ops[group.latest])) {
			group.latest = i
			groups[key] = group
		}
	}
	keep := make(map[int]bool, len(groups)*2)
	for _, group := range groups {
		keep[group.first] = true
		keep[group.latest] = true
	}
	result := make([]model.Operation, 0, len(keep))
	for i, op := range ops {
		if keep[i] {
			result = append(result, op)
		}
	}
	return result
}

func imageOperationObservedAt(op model.Operation) time.Time {
	if op.CompletedAt != nil {
		return *op.CompletedAt
	}
	if op.StartedAt != nil {
		return *op.StartedAt
	}
	if !op.UpdatedAt.IsZero() {
		return op.UpdatedAt
	}
	return op.CreatedAt
}

func (s *Store) ListImageCandidateOperationsByApps(tenantID string, platformAdmin bool, appIDs []string) (map[string][]model.Operation, error) {
	if !s.usingDatabase() {
		groups, err := s.ListImageOperationsByApps(tenantID, platformAdmin, appIDs)
		if err != nil {
			return nil, err
		}
		for id, ops := range groups {
			groups[id] = CompactImageCandidateOperations(ops)
		}
		return groups, nil
	}
	ids := sortedTrimmedStringKeys(trimmedStringSet(appIDs))
	if len(ids) == 0 {
		return map[string][]model.Operation{}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	args := make([]any, 0, len(ids)+1)
	for _, id := range ids {
		args = append(args, id)
	}
	predicate := fmt.Sprintf("app_id IN (%s) AND desired_source_json IS NOT NULL", sqlPlaceholderList(1, len(ids)))
	if !platformAdmin {
		args = append(args, tenantID)
		predicate += fmt.Sprintf(" AND tenant_id = $%d", len(args))
	}
	query := `WITH inputs AS (
 SELECT id, tenant_id, type, status, app_id, desired_spec_json->>'image' AS image,
   CASE WHEN desired_source_json ? 'desired_source' OR desired_source_json ? 'desired_origin_source'
     THEN jsonb_build_object('desired_source', desired_source_json->'desired_source')
     ELSE desired_source_json END AS source,
   created_at, updated_at, started_at, completed_at
 FROM fugue_operations WHERE ` + predicate + `
), ranked AS (
 SELECT *,
   row_number() OVER (PARTITION BY app_id,image,source ORDER BY created_at,id) AS first_input,
   row_number() OVER (PARTITION BY app_id,image,source ORDER BY COALESCE(completed_at,started_at,NULLIF(updated_at,'0001-01-01 00:00:00+00'::timestamptz),created_at) DESC,created_at,id) AS latest_input
 FROM inputs
)
SELECT id,tenant_id,type,status,app_id,image,source,created_at,updated_at,started_at,completed_at
FROM ranked WHERE first_input=1 OR latest_input=1 ORDER BY app_id,created_at,id`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string][]model.Operation, len(ids))
	for rows.Next() {
		op, err := scanImageOperation(rows)
		if err != nil {
			return nil, err
		}
		result[op.AppID] = append(result[op.AppID], op)
	}
	return result, rows.Err()
}
