package store

import (
	"context"
	"database/sql"
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
	for _, statement := range []string{schemamigrate.CompilerInputLifecycleSQL, schemamigrate.CompilerInputZeroTempSQL, schemamigrate.PlatformQualityEvidenceRetentionSQL} {
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
	if _, err := s.db.ExecContext(ctx, `CREATE ROLE synthetic_quality_retention_worker NOLOGIN;
GRANT USAGE ON SCHEMA fugue_observation_maintenance TO synthetic_quality_retention_worker;
GRANT SELECT ON fugue_observation_maintenance.quality_evidence_worker_progress TO synthetic_quality_retention_worker;
GRANT EXECUTE ON FUNCTION fugue_observation_maintenance.prune_quality_evidence_v1() TO synthetic_quality_retention_worker;`); err != nil {
		t.Fatal(err)
	}
	worker, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	if _, err := worker.ExecContext(ctx, `SET ROLE synthetic_quality_retention_worker`); err != nil {
		t.Fatal(err)
	}
	defer worker.ExecContext(ctx, `RESET ROLE`)
	if _, err := worker.ExecContext(ctx, `SELECT evidence FROM fugue_platform_quality_evidence`); err == nil {
		t.Fatal("retention role could read raw evidence")
	}
	if _, err := worker.ExecContext(ctx, `SELECT key FROM fugue_meta`); err == nil {
		t.Fatal("retention role could read shared metadata")
	}
	if _, err := worker.ExecContext(ctx, `UPDATE fugue_observation_maintenance.quality_evidence_progress SET total_deleted=0`); err == nil {
		t.Fatal("retention role could forge durable progress")
	}
	report, err := readQualityEvidenceRetentionProgress(ctx, worker)
	if err != nil || report.Owner != "api" || report.CompletedPasses != 0 || !report.CompletedAt.IsZero() {
		t.Fatalf("standby report: %+v, %v", report, err)
	}
	queryPass := func(db interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
	}, wantState string, wantDeleted int64) {
		t.Helper()
		var state string
		var deleted int64
		var at sql.NullTime
		err := db.QueryRowContext(ctx, `SELECT * FROM fugue_observation_maintenance.prune_quality_evidence_v1()`).Scan(&state, &deleted, &at)
		if err != nil || state != wantState || deleted != wantDeleted || at.Valid != (wantState == "completed") {
			t.Fatalf("unexpected expiry pass: state=%q deleted=%d timestamp=%v err=%v", state, deleted, at, err)
		}
	}
	queryPass(worker, "standby", 0)
	for range 2 {
		if _, err := s.db.ExecContext(ctx, schemamigrate.PlatformQualityEvidenceRetentionActivateSQL); err != nil {
			t.Fatal(err)
		}
	}
	seedExpired(digest("b"))
	legacy, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if _, err := legacy.ExecContext(ctx, `CREATE ROLE synthetic_quality_legacy_api NOLOGIN;
GRANT SELECT, DELETE ON public.fugue_platform_quality_evidence TO synthetic_quality_legacy_api;
SET ROLE synthetic_quality_legacy_api;`); err != nil {
		t.Fatal(err)
	}
	defer legacy.ExecContext(ctx, `RESET ROLE`)
	if _, err := legacy.ExecContext(ctx, `DELETE FROM fugue_platform_quality_evidence WHERE content_hash=$1`, digest("b")); err == nil ||
		!strings.Contains(err.Error(), "legacy periodic quality evidence deletion fenced") {
		t.Fatalf("rollback binary's old DELETE must be fenced: %v", err)
	}
	admission, err := legacy.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Rollback()
	if _, err := admission.ExecContext(ctx, `SET LOCAL fugue.quality_evidence_admission='v1'`); err != nil {
		t.Fatal(err)
	}
	result, err := admission.ExecContext(ctx, `DELETE FROM fugue_platform_quality_evidence WHERE content_hash=$1`, digest("b"))
	if err != nil {
		t.Fatalf("current foreground admission must preserve the old deletion behavior: %v", err)
	}
	if touched, err := result.RowsAffected(); err != nil || touched != 1 {
		t.Fatalf("foreground admission deleted %d rows: %v", touched, err)
	}
	if err := admission.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.ExecContext(ctx, `DELETE FROM fugue_platform_quality_evidence WHERE content_hash=$1`, digest("b")); err == nil {
		t.Fatal("foreground admission authorization leaked past the transaction")
	}
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

	if _, err := s.db.ExecContext(ctx, `INSERT INTO fugue_platform_quality_evidence
   (content_hash, evidence, size_bytes, created_at, expires_at)
   SELECT 'sha256:' || repeat('0',60) || lpad(to_hex(i),4,'0'), decode('61','hex'), 1, $1, $2
   FROM generate_series(1,257) AS seed(i)`, now.Add(-2*time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	inFlight, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer inFlight.Rollback()
	if _, err = inFlight.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, qualityEvidenceLockID); err != nil {
		t.Fatal(err)
	}
	queryPass(worker, "busy", 0)
	if err := inFlight.Rollback(); err != nil {
		t.Fatal(err)
	}
	interrupted, err := worker.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer interrupted.Rollback()
	queryPass(interrupted, "completed", 256)
	if err := interrupted.Rollback(); err != nil {
		t.Fatal(err)
	}
	queryPass(worker, "completed", 256)
	queryPass(worker, "completed", 1)
	assertPresent(digest("c"), true)
	if _, err := s.db.ExecContext(ctx, schemamigrate.PlatformQualityEvidenceRetentionSQL); err != nil {
		t.Fatalf("retry installation must leave authorization and progress intact: %v", err)
	}
	if _, err := worker.ExecContext(ctx, `RESET ROLE`); err != nil {
		t.Fatal(err)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := s.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.ExecContext(ctx, `SET ROLE synthetic_quality_retention_worker`); err != nil {
		t.Fatal(err)
	}
	defer reopened.ExecContext(ctx, `RESET ROLE`)
	report, err = readQualityEvidenceRetentionProgress(ctx, reopened)
	if err != nil || report.Owner != "worker" || report.CompletedPasses != 2 || report.TotalDeleted != 257 || report.CompletedAt.Before(now) {
		t.Fatalf("reopened worker must see committed progress: %+v, %v", report, err)
	}
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
