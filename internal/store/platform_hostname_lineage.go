package store

import (
	"context"
	"strings"
	"time"

	"fugue/internal/model"
)

// ListPlatformHostnameArtifacts is a bounded diagnostic read. Apply the recent
// candidate limit before hostname matching, preserving explicit coverage.
func (s *Store) ListPlatformHostnameArtifacts(ctx context.Context, kind, hostname string, limit int, includeContent bool) ([]model.PlatformArtifact, error) {
	if limit < 1 || limit > 200 || includeContent && limit > 5 {
		return nil, ErrInvalidInput
	}
	hostname = strings.Trim(strings.TrimSpace(strings.ToLower(hostname)), ".")
	if hostname == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		columns := "NULL::jsonb"
		if includeContent {
			columns = "content_json"
		}
		query := `WITH candidates AS MATERIALIZED (
SELECT * FROM fugue_platform_artifacts WHERE artifact_kind=$1 AND status=$2
ORDER BY updated_at DESC,id ASC LIMIT $3
)
SELECT id,artifact_kind,scope_key,scope_json,schema_version,generation,generation_sequence,status,content_hash,
` + columns + `,validation_results_json,compatibility_floor,metadata_json,created_by_type,created_by_id,provenance_json,created_at,updated_at
FROM candidates
WHERE EXISTS (
 SELECT 1 FROM jsonb_path_query(content_json, 'strict $.** ? (@.type() == "object").keyvalue() ? (@.key like_regex "hostname" flag "i" && @.value.type() == "string").value') AS names(value)
 WHERE btrim(btrim(lower(value #>> '{}'), E' \t\n\r\f\v'), '.')=$4
)
ORDER BY updated_at DESC,id ASC`
		rows, err := s.db.QueryContext(ctx, query, kind, model.PlatformArtifactStatusValidated, limit, hostname)
		if err != nil {
			return nil, mapDBErr(err)
		}
		defer rows.Close()
		out := []model.PlatformArtifact{}
		for rows.Next() {
			artifact, err := scanPlatformArtifact(rows)
			if err != nil {
				return nil, err
			}
			out = append(out, artifact)
		}
		return out, rows.Err()
	}
	artifacts, err := s.ListPlatformArtifacts(model.PlatformArtifactFilter{ArtifactKind: kind, Status: model.PlatformArtifactStatusValidated, Limit: limit})
	if err != nil {
		return nil, err
	}
	out := []model.PlatformArtifact{}
	for _, artifact := range artifacts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !platformContentHasHostname(artifact.Content, hostname) {
			continue
		}
		if !includeContent {
			artifact.Content = nil
		}
		out = append(out, artifact)
	}
	return out, nil
}

func platformContentHasHostname(value any, hostname string) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if strings.Contains(strings.ToLower(key), "hostname") {
				if candidate, ok := child.(string); ok && strings.Trim(strings.TrimSpace(strings.ToLower(candidate)), ".") == hostname {
					return true
				}
			}
			if platformContentHasHostname(child, hostname) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if platformContentHasHostname(child, hostname) {
				return true
			}
		}
	}
	return false
}
