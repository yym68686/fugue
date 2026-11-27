package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"fugue/internal/model"
)

// PlatformRuntimeFactFilter selects existing audit facts, without manufacturing
// observations or changing the signed audit payload.
type PlatformRuntimeFactFilter struct {
	ConsumerID   string
	ReleaseSetID string
	ArtifactKind string
	Limit        int
}

var producerRuntimeActions = []string{
	"platform_config.shadow_produced", "platform_config.gray_produced",
	"platform_config.full_produced", "platform_config.serving_verified",
	"platform_config.serving_rolled_back",
}

func (s *Store) ListPlatformRuntimeFacts(ctx context.Context, filter PlatformRuntimeFactFilter) ([]model.AuditEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	filter.ConsumerID, filter.ReleaseSetID, filter.ArtifactKind = strings.TrimSpace(filter.ConsumerID), strings.TrimSpace(filter.ReleaseSetID), strings.TrimSpace(filter.ArtifactKind)
	if filter.Limit == 0 {
		filter.Limit = 200
	}
	if filter.Limit < 1 || filter.Limit > 1000 {
		return nil, ErrInvalidInput
	}
	return s.pgListPlatformRuntimeFacts(ctx, filter)
}

func (s *Store) pgListPlatformRuntimeFacts(ctx context.Context, filter PlatformRuntimeFactFilter) ([]model.AuditEvent, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	args := []any{}
	bind := func(v any) string { args = append(args, v); return fmt.Sprintf("$%d", len(args)) }
	actions := []string{}
	for _, a := range producerRuntimeActions {
		actions = append(actions, bind(a))
	}
	producer := "(e.target_type='platform_release_set' AND e.action IN (" + strings.Join(actions, ",") + "))"
	kind := "(CASE WHEN " + producer + " THEN 'release_set' ELSE COALESCE(e.metadata_json->>'artifact_kind','') END)"
	query := `SELECT e.id,e.tenant_id,e.actor_type,e.actor_id,e.action,e.target_type,e.target_id,
        e.metadata_json,e.chain_id,e.chain_sequence,e.previous_hash,e.event_hash,e.provenance_json,e.created_at
        FROM fugue_audit_events e WHERE (e.action='platform_consumer.heartbeat_accepted'
        OR left(e.action,length('platform_artifact.'))='platform_artifact.' OR ` + producer + `)`
	metadataEquals := func(key, value string) string {
		raw, _ := json.Marshal(map[string]string{key: value})
		return "e.metadata_json @> " + bind(string(raw)) + "::jsonb"
	}
	if filter.ConsumerID != "" {
		// Resolve the stable legacy instance identities before selecting audit
		// rows. An EXISTS subquery inside the OR prevents bitmap index scans.
		ids := []string{filter.ConsumerID}
		rows, err := s.db.QueryContext(ctx, `SELECT id FROM fugue_platform_consumer_instances WHERE consumer_id=$1`, filter.ConsumerID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		query += " AND (" + metadataEquals("consumer_id", filter.ConsumerID) + " OR (e.target_type='platform_consumer' AND e.target_id=ANY(" + bind(ids) + ")))"
	}
	if filter.ArtifactKind != "" {
		if filter.ArtifactKind == model.PlatformArtifactKindReleaseSet {
			query += " AND (" + producer + " OR " + metadataEquals("artifact_kind", filter.ArtifactKind) + ")"
		} else {
			query += " AND (NOT " + producer + " AND " + metadataEquals("artifact_kind", filter.ArtifactKind) + ")"
		}
	}
	if filter.ReleaseSetID != "" {
		p := bind(filter.ReleaseSetID)
		query += " AND (" + metadataEquals("release_set_id", filter.ReleaseSetID) + " OR (" + kind + "='release_set' AND (" +
			"(e.target_type IN ('platform_release_set','platform_artifact') AND e.target_id=" + p + ") OR " + metadataEquals("artifact_id", filter.ReleaseSetID) +
			" OR (" + producer + " AND e.action='platform_config.serving_rolled_back' AND " + metadataEquals("lkg_artifact_id", filter.ReleaseSetID) + "))))"
	}
	// Filtered history may be far behind the newest heartbeat. Avoid the
	// chronological index's small-LIMIT plan that scans unrelated recent rows;
	// select via identity indexes, then perform the bounded top-N sort.
	order := "e.created_at"
	if filter.ConsumerID != "" || filter.ReleaseSetID != "" || filter.ArtifactKind != "" {
		order = "(e.created_at + interval '0 seconds')"
	}
	query += " ORDER BY " + order + " DESC,e.id DESC LIMIT " + bind(filter.Limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list platform runtime facts: %w", err)
	}
	defer rows.Close()
	events := []model.AuditEvent{}
	for rows.Next() {
		e, err := scanAuditEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
