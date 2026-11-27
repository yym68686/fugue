package api

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"fugue/internal/model"
)

// Seed already-signed publications exactly as issued by another producer.
// API methods re-sign or increment a generation, changing the source proof.
type platformPublicationFixture struct {
	artifacts []model.PlatformArtifact
	releases  []model.PlatformArtifactRelease
	lanes     []model.PlatformReleaseLane
	sets      []model.PlatformExpectedConsumerSet
	lkgs      []model.PlatformLKGSnapshot
}

func (f platformPublicationFixture) seed(t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := tx.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	encode := func(value any) []byte {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	now := time.Now().UTC()
	for _, a := range f.artifacts {
		exec(`INSERT INTO fugue_platform_artifacts
	(id,artifact_kind,scope_key,scope_json,schema_version,generation,generation_sequence,status,content_hash,content_json,
	 validation_results_json,compatibility_floor,metadata_json,created_by_type,created_by_id,provenance_json,created_at,updated_at)
VALUES ($1,$2,$3,$4::jsonb,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12,$13::jsonb,$14,$15,$16::jsonb,$17,$18)`,
			a.ID, a.ArtifactKind, a.ScopeKey, encode(a.Scope), a.SchemaVersion, a.Generation, a.GenerationSequence,
			a.Status, a.ContentHash, encode(a.Content), encode(a.ValidationResults), a.CompatibilityFloor, encode(a.Metadata),
			a.CreatedByType, a.CreatedByID, encode(a.Provenance), a.CreatedAt, a.UpdatedAt)
		exec(`INSERT INTO fugue_platform_artifact_generation_sequences (artifact_kind,scope_key,last_sequence,updated_at)
VALUES ($1,$2,$3,$4) ON CONFLICT (artifact_kind,scope_key) DO UPDATE
SET last_sequence=greatest(fugue_platform_artifact_generation_sequences.last_sequence,excluded.last_sequence)`,
			a.ArtifactKind, a.ScopeKey, a.GenerationSequence, now)
	}
	for _, r := range f.releases {
		exec(`INSERT INTO fugue_platform_artifact_releases
	(id,artifact_id,artifact_kind,scope_key,scope_json,generation,release_channel,status,canary_rule_ref,reason,
	 released_by_type,released_by_id,released_at,created_at,updated_at,lane_key,fencing_token,version,
	 verification_state,verified_lkg_generation,verified_at,verification_evidence_json,override_mode)
VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12,$13,$13,$13,$14,$15,greatest($16::bigint,1),$17,$18,$19,$20::jsonb,$21)`,
			r.ID, r.ArtifactID, r.ArtifactKind, r.ScopeKey, encode(r.Scope), r.Generation, r.ReleaseChannel, r.Status,
			r.CanaryRuleRef, r.Reason, r.ReleasedByType, r.ReleasedByID, r.ReleasedAt, r.LaneKey, r.FencingToken,
			r.Version, r.VerificationState, r.VerifiedLKGGeneration, r.VerifiedAt, encode(r.VerificationEvidence), r.OverrideMode)
	}
	for _, l := range f.lanes {
		exec(`INSERT INTO fugue_platform_release_lanes
	(lane_key,artifact_kind,scope_key,release_channel,fencing_token,version,active_release_id,frozen,freeze_reason,updated_at)
VALUES ($1,$2,$3,$4,$5,greatest($6::bigint,1),$7,$8,$9,$10)`,
			l.LaneKey, l.ArtifactKind, l.ScopeKey, l.ReleaseChannel, l.FencingToken, l.Version, l.ActiveReleaseID, l.Frozen, l.FreezeReason, now)
	}
	for _, s := range f.sets {
		exec(`INSERT INTO fugue_platform_expected_consumer_sets
	(id,release_set_id,artifact_release_id,artifact_kind,scope_key,scope_json,expected_generation,topology_revision,
	 revision,requires_consumers,required_cardinality,optional_cardinality,heartbeat_deadline,convergence_deadline,consumers_json,created_at,updated_at)
VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb,$16,$17)`,
			s.ID, s.ReleaseSetID, s.ArtifactReleaseID, s.ArtifactKind, s.ScopeKey, encode(s.Scope), s.ExpectedGeneration,
			s.TopologyRevision, s.Revision, s.RequiresConsumers, s.RequiredCardinality, s.OptionalCardinality,
			s.HeartbeatDeadline, s.ConvergenceDeadline, encode(s.Consumers), s.CreatedAt, s.UpdatedAt)
	}
	for _, s := range f.lkgs {
		exec(`INSERT INTO fugue_platform_lkg_snapshots
	(id,artifact_id,artifact_kind,scope_key,scope_json,schema_version,generation,generation_sequence,content_hash,
	 artifact_provenance_json,expires_at,created_at,updated_at,verified_by_release_id,verification_evidence_hash,snapshot_provenance_json)
VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16::jsonb)`,
			s.ID, s.ArtifactID, s.ArtifactKind, s.ScopeKey, encode(s.Scope), s.SchemaVersion, s.Generation, s.GenerationSequence,
			s.ContentHash, encode(s.ArtifactProvenance), s.ExpiresAt, s.CreatedAt, s.UpdatedAt,
			s.VerifiedByReleaseID, s.VerificationEvidenceHash, encode(s.SnapshotProvenance))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
