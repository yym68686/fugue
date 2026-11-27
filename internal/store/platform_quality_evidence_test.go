package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"fugue/internal/bundleauth"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/schemamigrate"
)

func TestPostgresPeriodicQualityEvidenceOwnerFencesOnlyTheTimer(t *testing.T) {
	s := billingBatchPGStore(t)
	ctx := t.Context()
	if err := s.ensureDatabaseReady(); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{schemamigrate.CompilerInputLifecycleSQL, schemamigrate.CompilerInputZeroTempSQL} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().UTC()
	digest := func(n string) string { return "sha256:" + strings.Repeat(n, 64) }
	seedExpired := func(key string) {
		t.Helper()
		if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_platform_quality_evidence
  (content_hash, evidence, size_bytes, created_at, expires_at) VALUES ($1, $2, 1, $3, $4)`,
			key, []byte("a"), now.Add(-2*time.Hour), now.Add(-time.Hour)); err != nil {
			t.Fatal(err)
		}
	}
	assertPresent := func(key string, want bool) {
		t.Helper()
		var exists bool
		err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM fugue_platform_quality_evidence WHERE content_hash=$1)`, key).Scan(&exists)
		if err != nil || exists != want {
			t.Fatalf("evidence present=%t, want %t: %v", exists, want, err)
		}
	}
	seedExpired(digest("a"))
	if err := s.maintainPlatformQualityEvidenceForAPI(ctx, nil, now, true); err != nil {
		t.Fatal(err)
	}
	assertPresent(digest("a"), false)
	key := observationCheckpointKey(qualityEvidenceRetentionWriterCheckpoint)
	if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_meta (key,value,updated_at) VALUES ($1,'worker',NOW())`, key); err != nil {
		t.Fatal(err)
	}
	seedExpired(digest("b"))
	s.qualityEvidenceMetrics.attemptedAt.Store(1)
	if err := s.maintainPlatformQualityEvidenceForAPI(ctx, nil, now, true); err != nil {
		t.Fatal(err)
	}
	assertPresent(digest("b"), true)
	if got := s.qualityEvidenceMetrics.attemptedAt.Load(); got != 1 {
		t.Fatal("standby API claimed a timer pass")
	}
	newEvidence := model.PlatformQualityEvidence{ContentHash: digest("c"), Evidence: []byte("c"), SizeBytes: 1,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	if err := s.maintainPlatformQualityEvidence(ctx, &newEvidence, now); err != nil {
		t.Fatal(err)
	}
	assertPresent(digest("b"), false)
	assertPresent(digest("c"), true)
}

func compactOnlyStoreFixture(t *testing.T, s *Store) (platformconfig.CompileResult, model.PlatformArtifact) {
	t.Helper()
	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("synthetic-compact-key", "compact", "", "", nil))
	at := time.Now().UTC().Add(-time.Minute)
	c, err := platformconfig.Compile(platformconfig.CompileRequest{OmitOriginalInput: true,
		Intent: platformconfig.PlatformIntent{Generation: "intent"}, Policy: platformconfig.PolicySnapshot{Generation: "policy"},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{CapturedAt: &at, Facts: map[string]any{"replay": strings.Repeat("audit", 1000)}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePlatformCompilationInputsContext(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, a := range []model.PlatformArtifact{c.IntentArtifact, c.PolicyArtifact, c.RouteArtifact, c.DNSArtifact, c.TLSArtifact} {
		stored, _, err := s.EnsurePlatformArtifact(a)
		if err != nil {
			t.Fatal(err)
		}
		if a.ArtifactKind != model.PlatformArtifactKindPlatformIntent && a.ArtifactKind != model.PlatformArtifactKindPolicySnapshot {
			ids = append(ids, stored.ID)
		}
	}
	parentDraft := platformconfig.BuildReleaseSetArtifact(c.ReleaseSet, ids, time.Now().UTC())
	parentDraft.Metadata[platformconfig.QualityEvidenceDigestMetadata] = c.ReleaseArtifact.Metadata[platformconfig.QualityEvidenceDigestMetadata]
	parent, _, err := s.EnsurePlatformArtifact(parentDraft)
	if err != nil {
		t.Fatal(err)
	}
	return c, parent
}

func TestCompactOnlyColdFileReadsAndRetentionNeedNoOriginal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	c, parent := compactOnlyStoreFixture(t, s)
	s = New(path)
	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("synthetic-compact-key", "compact", "", "", nil))
	if _, err := s.GetPlatformCompilerInput(c.Lineage.InputSnapshotDigest); !errors.Is(err, ErrNotFound) {
		t.Fatal("complete input persisted", err)
	}
	if got, err := s.GetPlatformArtifactRuntimeInput(parent); err != nil || got.Facts != nil {
		t.Fatal("cold operational read required original", err)
	}
	if err := s.CheckPlatformInputRead(t.Context(), []PlatformInputReadSelection{{ArtifactID: parent.ID, ContentHash: parent.ContentHash}}); err != nil {
		node, _ := compilerInputArtifactRuntimeNode(parent)
		t.Fatal("operational dependency closure required original", err, node.References)
	}
	node, err := compilerInputArtifactHotNode(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, ref := range node.References {
		if ref.Kind == "input" && ref.ID == c.Lineage.InputSnapshotDigest {
			t.Fatal("new retention closure promised full original")
		}
	}
	if _, err := s.GetPlatformArtifactQualityInputContext(t.Context(), parent, nil); !errors.Is(err, ErrNotFound) {
		t.Fatal("empty quality history was fabricated", err)
	}
}

func TestQualityEvidenceIndependentPoolOwnsBytesChecksSignatureAndExpires(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := New(path)
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	_, parent := compactOnlyStoreFixture(t, s)
	at := time.Now().UTC().Add(-time.Hour)
	quality := platformconfig.RuntimeSnapshot{IntentGeneration: "intent", PolicyGeneration: "policy", CapturedAt: &at,
		DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: "dns-a", Hostname: "app.example.test", ObservedAt: at, PhysicalEvidence: json.RawMessage(`{"receipt":{"at":"original"}}`)}}}
	digest, _ := platformconfig.RuntimeSnapshotDigest(quality)
	if err := s.ensurePlatformQualityEvidence(t.Context(), quality, digest, time.Now()); err != nil {
		t.Fatal(err)
	}
	parent.Metadata[platformconfig.QualityEvidenceDigestMetadata] = digest
	parent, err := platformsafety.SignPlatformArtifact(parent, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	s = New(path)
	s.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("synthetic-compact-key", "compact", "", "", nil))
	got, err := s.GetPlatformArtifactQualityInputContext(t.Context(), parent, nil)
	gotDigest, _ := platformconfig.RuntimeSnapshotDigest(platformconfig.RuntimeSnapshot{IntentGeneration: got.IntentGeneration, PolicyGeneration: got.PolicyGeneration, CapturedAt: quality.CapturedAt, DNSSelections: got.DNSSelections})
	if err != nil || gotDigest != digest {
		t.Fatal("cold pool read changed evidence", got.IntentGeneration, got.PolicyGeneration, gotDigest, digest, err)
	}
	got.DNSSelections[0].PhysicalEvidence[0] = '!'
	if _, err := s.GetPlatformArtifactQualityInputContext(t.Context(), parent, nil); err != nil {
		t.Fatal("caller mutated persisted evidence", err)
	}
	parent.Metadata[platformconfig.QualityEvidenceDigestMetadata] = "sha256:" + strings.Repeat("a", 64)
	if _, err := s.GetPlatformArtifactQualityInputContext(t.Context(), parent, nil); !errors.Is(err, ErrConflict) {
		t.Fatal("unsigned binding changed", err)
	}
	if err := s.maintainPlatformQualityEvidence(t.Context(), nil, at.Add(QualityEvidenceRetention+time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.withLockedState(false, func(st *model.State) error {
		if len(st.PlatformQualityEvidence) != 0 {
			t.Fatal("expired pool retained")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestQualityEvidenceBudgetFIFOAndImmutableAge(t *testing.T) {
	now := time.Now().UTC()
	rows := []model.PlatformQualityEvidence{}
	for i := 0; i < 17; i++ {
		rows = append(rows, model.PlatformQualityEvidence{ContentHash: fmt.Sprint(i), SizeBytes: 64 << 20, CreatedAt: now.Add(time.Duration(i) * time.Second), ExpiresAt: now.Add(time.Hour)})
	}
	rows[16].ExpiresAt = now
	newest := model.PlatformQualityEvidence{ContentHash: "incoming", SizeBytes: 64 << 20}
	victims := qualityEvidenceVictims(rows, &newest, now)
	if !reflect.DeepEqual(victims, []string{"16", "0"}) {
		t.Fatal("not expired-first then FIFO", victims)
	}
	rows = make([]model.PlatformQualityEvidence, QualityEvidenceMaxObjects)
	for i := range rows {
		rows[i] = model.PlatformQualityEvidence{ContentHash: fmt.Sprintf("%04d", i), SizeBytes: 1, CreatedAt: now, ExpiresAt: now.Add(time.Hour)}
	}
	if victims := qualityEvidenceVictims(rows, &newest, now); !slices.Equal(victims, []string{"0000"}) {
		t.Fatal("row budget not enforced", victims)
	}
	at := now.Add(-QualityEvidenceRetention - time.Second)
	snapshot := platformconfig.RuntimeSnapshot{CapturedAt: &at, DNSSelections: []platformconfig.DNSSelectionObservation{{PhysicalEvidence: json.RawMessage(`{}`)}}}
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if _, retained, err := qualityEvidenceObject(snapshot, digest, now); err != nil || retained {
		t.Fatal("replay renewed expired capture", err)
	}
}

func TestCompactOnlyPostgresNeverPersistsOriginalAndPoolSurvivesColdStore(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated} {
		if err := migrate(t.Context(), address); err != nil {
			t.Fatal(err)
		}
	}
	s := &Store{db: db, databaseURL: address, dbReady: true}
	s.storagePolicy.value = StoragePolicy{Version: 1, CompilerInputChunks: true}
	s.storagePolicy.separated = compilerInputCodecPolicy{Version: 1, Enabled: true}
	c, parent := compactOnlyStoreFixture(t, s)
	if _, err := s.GetPlatformCompilerInput(c.Lineage.InputSnapshotDigest); !errors.Is(err, ErrNotFound) {
		t.Fatal("PG persisted full original", err)
	}
	fresh := &Store{db: s.db, databaseURL: s.databaseURL, dbReady: true}
	fresh.ConfigurePlatformArtifactSigning(bundleauth.NewKeyring("synthetic-compact-key", "compact", "", "", nil))
	if _, err := fresh.GetPlatformArtifactRuntimeInput(parent); err != nil {
		t.Fatal("PG cold read required original", err)
	}
	var bindings int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_audit_bindings WHERE original_digest=$1`, c.Lineage.InputSnapshotDigest).Scan(&bindings); err != nil || bindings != 0 {
		t.Fatal("new generation created legacy audit obligation", bindings, err)
	}
	at := time.Now().UTC().Add(-time.Minute)
	quality := platformconfig.RuntimeSnapshot{IntentGeneration: "intent", PolicyGeneration: "policy", CapturedAt: &at,
		DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: "dns-a", Hostname: "app.example.test", ObservedAt: at, PhysicalEvidence: json.RawMessage(`{"original":true}`)}}}
	digest, _ := platformconfig.RuntimeSnapshotDigest(quality)
	if err := s.ensurePlatformQualityEvidence(t.Context(), quality, digest, time.Now()); err != nil {
		t.Fatal("PG independent pool insert", err)
	}
	parent.Metadata[platformconfig.QualityEvidenceDigestMetadata] = digest
	parent, err := platformsafety.SignPlatformArtifact(parent, s.platformArtifactSigningKeyring())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := fresh.GetPlatformArtifactQualityInputContext(t.Context(), parent, nil); err != nil || len(got.DNSSelections) != 1 || string(got.DNSSelections[0].PhysicalEvidence) != `{"original":true}` {
		t.Fatal("PG cold comparison history unavailable", err)
	}
	if err := s.maintainPlatformQualityEvidence(t.Context(), nil, at.Add(QualityEvidenceRetention+time.Second)); err != nil {
		t.Fatal("PG expiry cleanup", err)
	}
	if _, err := fresh.GetPlatformArtifactQualityInputContext(t.Context(), parent, nil); !errors.Is(err, ErrNotFound) {
		t.Fatal("PG removed history became evidence", err)
	}
	// An unavailable optional pool must not abort independent operational writes.
	if _, err := db.Exec(`DROP TABLE fugue_platform_quality_evidence`); err != nil {
		t.Fatal(err)
	}
	c.InputSnapshot.DNSSelections = quality.DNSSelections
	c.Lineage.InputSnapshotDigest, _ = platformconfig.RuntimeSnapshotDigest(c.InputSnapshot)
	runtime := platformconfig.ConfigurationRuntimeInput(c.InputSnapshot)
	c.RuntimeInput = &runtime
	c.Lineage.RuntimeInputDigest, _ = platformconfig.RuntimeSnapshotDigest(runtime)
	projected := platformconfig.QualityEvidenceInput(c.InputSnapshot)
	c.QualityInput = &projected
	c.ReleaseArtifact.Metadata[platformconfig.QualityEvidenceDigestMetadata], _ = platformconfig.RuntimeSnapshotDigest(projected)
	if err := s.EnsurePlatformCompilationInputsContext(t.Context(), c); err != nil {
		t.Fatal("optional pool failure blocked operational input", err)
	}
	if s.qualityEvidenceMetrics.failed.Load() != 1 {
		t.Fatal("optional failure hidden from metrics")
	}
}
