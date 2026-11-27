package api

import (
	"encoding/json"
	"strings"

	"fugue/internal/auth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/store"
	"testing"
	"time"
)

// Actual original input and a signed typed dependency graph with a positive
// full-release baseline. This is a restoration-boundary fixture, not a complete
// publication E2E. Receipt clocks remain historical; authority uses real time.
func qualityRestoreInputFixture(t *testing.T, snapshot platformconfig.RuntimeSnapshot) (*Server, model.PlatformArtifact, string) {
	t.Helper()
	fixture := model.NewID("quality-restore")
	st := newStoreTestDatabase(t, fixture)
	s := NewServer(st, auth.New(st, ""), nil, ServerConfig{BundleSigningKey: "synthetic-quality-input", BundleSigningKeyID: "quality-input"})
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []model.PlatformArtifact{
		{ArtifactKind: model.PlatformArtifactKindPolicySnapshot, Generation: "policy-a", Content: map[string]any{"generation": "policy-a", "scope": "global"}},
		{ID: "quality-child", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, Generation: "quality-child", Content: map[string]any{"lineage": map[string]any{"input_snapshot_digest": digest}}},
	} {
		artifact.Status = model.PlatformArtifactStatusValidated
		if _, err := st.CreatePlatformArtifact(artifact); err != nil {
			t.Fatal(err)
		}
	}
	parent, err := st.CreatePlatformArtifact(model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindReleaseSet,
		Generation: "quality-parent", Status: model.PlatformArtifactStatusValidated, Content: map[string]any{"artifact_ids": []string{"quality-child"}},
		Metadata: map[string]string{"input_snapshot_digest": digest, "intent_generation": "intent-a", "policy_generation": "policy-a"}})
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	release := model.PlatformArtifactRelease{ID: "quality-full", ArtifactID: parent.ID, ArtifactKind: parent.ArtifactKind,
		Scope: parent.Scope, ScopeKey: parent.ScopeKey, Generation: parent.Generation, ReleaseChannel: model.PlatformArtifactReleaseChannelFull,
		Status: model.PlatformArtifactReleaseStatusActive, FencingToken: 1, VerificationState: model.PlatformArtifactVerificationStateVerified,
		VerifiedLKGGeneration: parent.Generation, VerifiedAt: &at, ReleasedAt: at, UpdatedAt: at}
	lkg, err := platformsafety.SignPlatformLKGSnapshot(model.PlatformLKGSnapshot{ID: "quality-lkg", ArtifactID: parent.ID,
		ArtifactKind: parent.ArtifactKind, Scope: parent.Scope, ScopeKey: parent.ScopeKey, SchemaVersion: parent.SchemaVersion,
		Generation: parent.Generation, GenerationSequence: parent.GenerationSequence, ContentHash: parent.ContentHash, ArtifactProvenance: parent.Provenance,
		VerifiedByReleaseID: release.ID, VerificationEvidenceHash: "sha256:" + strings.Repeat("a", 64),
		ExpiresAt: at.Add(time.Hour), CreatedAt: at, UpdatedAt: at}, s.bundleKeyring())
	if err != nil {
		t.Fatal(err)
	}
	db := openStoreTestSQL(t, fixture)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	jsonValue := func(v any) string {
		t.Helper()
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	key := platformsafety.ReleaseLaneKey(release.ArtifactKind, release.ScopeKey, release.ReleaseChannel)
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO fugue_platform_artifact_releases
		(id,artifact_id,artifact_kind,scope_key,scope_json,generation,release_channel,status,lane_key,fencing_token,
		 verification_state,verified_lkg_generation,verified_at,released_at,created_at,updated_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,1,$10,$6,$11,$11,$11,$11)`,
		release.ID, release.ArtifactID, release.ArtifactKind, release.ScopeKey, jsonValue(release.Scope), release.Generation,
		release.ReleaseChannel, release.Status, key, release.VerificationState, at); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO fugue_platform_release_lanes
		(lane_key,artifact_kind,scope_key,release_channel,fencing_token,version,active_release_id,updated_at)
		VALUES($1,$2,$3,$4,1,1,$5,$6)`, key, release.ArtifactKind, release.ScopeKey, release.ReleaseChannel, release.ID, at); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"fugue_platform_lkg_snapshots", "fugue_platform_lkg_snapshot_history"} {
		if _, err := tx.ExecContext(t.Context(), `INSERT INTO `+table+`
			(id,artifact_id,artifact_kind,scope_key,scope_json,schema_version,generation,generation_sequence,content_hash,
			 artifact_provenance_json,verified_by_release_id,verification_evidence_hash,snapshot_provenance_json,expires_at,created_at,updated_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$15)`,
			lkg.ID, lkg.ArtifactID, lkg.ArtifactKind, lkg.ScopeKey, jsonValue(lkg.Scope), lkg.SchemaVersion, lkg.Generation, lkg.GenerationSequence,
			lkg.ContentHash, jsonValue(lkg.ArtifactProvenance), lkg.VerifiedByReleaseID, lkg.VerificationEvidenceHash,
			jsonValue(lkg.SnapshotProvenance), lkg.ExpiresAt, lkg.CreatedAt); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return s, parent, fixture
}

func TestDynamicQualityRestoreRejectsUnneededEvidence(t *testing.T) {
	now := time.Now().UTC()
	base := store.CompilerDNSSelectionSummary{HasSelection: true, HasEvidence: true, CapturedAt: now.Add(-time.Minute)}
	job := dynamicQualityJob{Route: platformconfig.PhysicalQualityRoute{Policy: model.PhysicalEdgeQualityPolicy{EvidenceMaxAgeSeconds: 120}}}
	for _, tc := range []struct {
		name    string
		summary store.CompilerDNSSelectionSummary
		entry   dynamicQualityEntry
		want    bool
	}{
		{name: "cold", summary: base, want: true},
		{name: "older", summary: base, entry: dynamicQualityEntry{Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now.Add(-90 * time.Second)}}}, want: true},
		{name: "same", summary: base, entry: dynamicQualityEntry{Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: base.CapturedAt}}}},
		{name: "newer", summary: base, entry: dynamicQualityEntry{Compiled: compiledPhysicalDNSSelection{Selection: &model.DNSPhysicalSelection{CapturedAt: now}}}},
		{name: "no receipt", summary: store.CompilerDNSSelectionSummary{HasSelection: true, CapturedAt: base.CapturedAt}},
		{name: "no selection", summary: store.CompilerDNSSelectionSummary{HasEvidence: true, CapturedAt: base.CapturedAt}},
		{name: "expired", summary: store.CompilerDNSSelectionSummary{HasSelection: true, HasEvidence: true, CapturedAt: now.Add(-120*time.Second - time.Nanosecond)}},
		{name: "expiry boundary", summary: store.CompilerDNSSelectionSummary{HasSelection: true, HasEvidence: true, CapturedAt: now.Add(-120 * time.Second)}, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := dynamicQualityRestoreNeeded(tc.summary, job, tc.entry, now); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
