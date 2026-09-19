package store

import (
	"context"
	"strings"
	"time"

	"fugue/internal/model"
)

// GetStandalonePlatformLKG reads the latest independently verified publication.
// A TrafficReleaseSet member's recovery reference cannot authorize traffic in
// groups outside that parent's rollout. Legacy readers use this lookup until
// an explicit parent publication selects them; it never falls back on expiry.
func (s *Store) GetStandalonePlatformLKG(kind, scopeKey string) (*model.PlatformLKGSnapshot, error) {
	kind = NormalizePlatformArtifactKind(kind)
	scopeKey = strings.ToLower(strings.TrimSpace(scopeKey))
	if scopeKey == "" {
		scopeKey = "global"
	}
	if kind == "" {
		return nil, ErrInvalidInput
	}
	if s.usingDatabase() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		snapshot, err := scanPlatformLKGSnapshot(s.db.QueryRowContext(ctx, `
SELECT h.id, h.artifact_id, h.artifact_kind, h.scope_key, h.scope_json, h.schema_version,
 h.generation, h.generation_sequence, h.content_hash, h.artifact_provenance_json,
 h.verified_by_release_id, h.verification_evidence_hash, h.snapshot_provenance_json,
 h.expires_at, h.created_at, h.updated_at
FROM (
 SELECT id, artifact_id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, content_hash, artifact_provenance_json, verified_by_release_id, verification_evidence_hash, snapshot_provenance_json, expires_at, created_at, updated_at FROM fugue_platform_lkg_snapshots WHERE artifact_kind=$1 AND scope_key=$2
 UNION ALL
 SELECT id, artifact_id, artifact_kind, scope_key, scope_json, schema_version, generation, generation_sequence, content_hash, artifact_provenance_json, verified_by_release_id, verification_evidence_hash, snapshot_provenance_json, expires_at, created_at, updated_at FROM fugue_platform_lkg_snapshot_history WHERE artifact_kind=$1 AND scope_key=$2
) h JOIN fugue_platform_artifact_releases r ON r.id=h.verified_by_release_id
WHERE r.artifact_kind=h.artifact_kind AND r.artifact_id=h.artifact_id AND r.scope_key=h.scope_key
ORDER BY h.updated_at DESC, h.created_at DESC, h.generation_sequence DESC, h.id DESC
LIMIT 1`, kind, scopeKey))
		if mapDBErr(err) == ErrNotFound {
			return nil, nil
		}
		if err != nil {
			return nil, mapDBErr(err)
		}
		return &snapshot, nil
	}
	var out *model.PlatformLKGSnapshot
	err := s.withLockedState(false, func(state *model.State) error {
		releases := make(map[string]model.PlatformArtifactRelease, len(state.PlatformArtifactReleases))
		for _, r := range state.PlatformArtifactReleases {
			releases[r.ID] = r
		}
		for _, snapshot := range state.PlatformLKGSnapshots {
			r, found := releases[snapshot.VerifiedByReleaseID]
			if !found || snapshot.ArtifactKind != kind || snapshot.ScopeKey != scopeKey || r.ArtifactKind != kind || r.ScopeKey != scopeKey || r.ArtifactID != snapshot.ArtifactID {
				continue
			}
			if out == nil || platformLKGSnapshotIsNewer(snapshot, *out) {
				copy := snapshot
				out = &copy
			}
		}
		return nil
	})
	return out, err
}
