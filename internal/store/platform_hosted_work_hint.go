package store

import (
	"context"
	"database/sql"
	"errors"
)

// Scheduling hint only. A first edit already has a committed project route
// table; a withdrawal may leave only an existing offer. Neither row is
// proof of project ownership, current selection, or a valid program.
func (s *Store) HostedPathWakeHint(ctx context.Context) (string, error) {

	var hint string
	e := s.db.QueryRowContext(ctx, hostedPathWakeSQL).Scan(&hint)
	if errors.Is(e, sql.ErrNoRows) {
		return "", nil
	}
	return hint, e
}

// Only transitions, not each heartbeat timestamp or our own freshly written
// draft, wake another expensive capture. A bounded periodic recheck handles
// other changes (e.g. a newly ready origin) not represented by this hint.
const hostedPathWakeSQL = `WITH full_parent AS MATERIALIZED (
 SELECT id,artifact_id FROM fugue_platform_artifact_releases
 WHERE artifact_kind='release_set' AND scope_key='global' AND release_channel='full' AND status='active' AND verification_state='verified'
 ORDER BY released_at DESC,id LIMIT 1
), members AS (
 SELECT DISTINCT i.node_id,i.artifact_kind,i.apply_status,i.probe_status,
 i.compatibility_capabilities_json::jsonb ? 'hosted_recipient_selection_v1' as can_select,
 i.compatibility_capabilities_json::jsonb ? 'hosted_shared_dns_hold_v1' as can_hold
 FROM full_parent f JOIN fugue_platform_consumer_instances i ON i.release_set_id=f.artifact_id
 WHERE i.scope_key='global' AND i.identity_verified AND i.last_heartbeat_at>now()-interval '90 seconds'
 AND i.artifact_kind IN ('dns_answer_bundle','caddy_route_config','edge_route_bundle')
 ORDER BY 1,2,3,4,5,6 LIMIT 1024
) SELECT jsonb_build_array(f.id,l.verification_evidence_hash,
 (SELECT max(updated_at) FROM fugue_project_route_tables),
 (SELECT md5(coalesce(jsonb_agg(m ORDER BY m.node_id,m.artifact_kind,m.apply_status,m.probe_status,m.can_select,m.can_hold)::text,'')) FROM members m),
 (SELECT max(fence) FROM fugue_hosted_selections),
 (SELECT count(*) FROM fugue_hosted_selection_receipts r JOIN fugue_hosted_selections s ON s.hostname=r.hostname AND s.fence=r.fence AND s.decision=r.decision WHERE r.passing AND r.valid_until>now()))::text
 FROM full_parent f JOIN fugue_platform_lkg_snapshots l ON l.artifact_id=f.artifact_id AND l.verified_by_release_id=f.id
 WHERE l.scope_key='global' AND l.artifact_kind='release_set' AND l.expires_at>now() AND
 (EXISTS(SELECT 1 FROM fugue_hosted_path_prepare_offers) OR EXISTS(SELECT 1 FROM fugue_project_route_tables))`
