package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"fugue/internal/inputarchive"
	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

type archiveTestObjects struct {
	objects    map[string][]byte
	failSecond bool
	failGet    bool
}

func archiveTestStore(t *testing.T) *Store {
	t.Helper()
	s := boundedContentTestStore(t)
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated, schemamigrate.MigrateCompilerInputArchive} {
		if err := migrate(t.Context(), s.databaseURL); err != nil {
			t.Fatal(err)
		}
	}
	resetCompilerInputArchiveTestLedger(t, s)
	return s
}

func resetCompilerInputArchiveTestLedger(t *testing.T, s *Store) {
	t.Helper()
	// archiveTestStore has already verified this is a loopback fugue_test DB.
	for _, table := range []string{"fugue_platform_input_archive_control", "fugue_platform_input_archives", "fugue_platform_input_archive_segments", "fugue_platform_input_archive_leases"} {
		for _, level := range []string{"row", "truncate"} {
			if _, err := s.db.ExecContext(t.Context(), "DROP TRIGGER IF EXISTS "+table+"_retired_"+level+"_guard ON "+table); err != nil {
				t.Fatal("reset disposable archive guard", err)
			}
		}
	}
	if _, err := s.db.ExecContext(t.Context(), `DROP FUNCTION IF EXISTS fugue_platform_input_archive_retirement_guard()`); err != nil {
		t.Fatal("reset disposable archive guard function", err)
	}
	for _, statement := range []string{
		`DELETE FROM fugue_platform_input_archive_leases`,
		`DELETE FROM fugue_platform_input_archive_segments`,
		`DELETE FROM fugue_platform_input_archives`,
		`UPDATE fugue_platform_input_archive_control SET epoch=0,writes_fenced=false,retirement_started_at=NULL,retirement_plan_sha256='',retirement_reader_current='',retirement_reader_rollback='',retirement_archive_policy_sha256='' WHERE singleton`,
	} {
		if _, err := s.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal("reset disposable archive ledger", err)
		}
	}
}

func resetArchiveInputFixture(t *testing.T, s *Store, digest string) {
	t.Helper()
	clear := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, table := range []string{"fugue_platform_input_archive_leases", "fugue_platform_input_archive_segments", "fugue_platform_input_archives", "fugue_platform_input_hot_chunks"} {
			if _, err := s.db.ExecContext(ctx, `DELETE FROM `+table+` WHERE content_hash=$1`, digest); err != nil {
				return err
			}
		}
		if _, err := s.db.ExecContext(ctx, `DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest); err != nil {
			return err
		}
		_, err := s.db.ExecContext(ctx, `DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest)
		return err
	}
	if err := clear(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := clear(); err != nil {
			t.Errorf("clean archive fixture %s: %v", digest, err)
		}
	})
}

func (s *archiveTestObjects) Put(_ context.Context, key string, raw []byte) error {
	if s.failSecond && strings.Contains(key, "/00001-") {
		return errors.New("simulated upload failure")
	}
	if s.objects == nil {
		s.objects = map[string][]byte{}
	}
	if _, exists := s.objects[key]; exists {
		return errors.New("already exists")
	}
	s.objects[key] = bytes.Clone(raw)
	return nil
}
func (s *archiveTestObjects) Get(_ context.Context, key string) (io.ReadCloser, error) {
	if s.failGet {
		return nil, errors.New("simulated read failure")
	}
	raw, ok := s.objects[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(raw)), nil
}

func prepareVerifiedRoutedArchive(t *testing.T, s *Store) (string, *archiveTestObjects, string) {
	t.Helper()
	snapshot, _ := chunkedInputFixture(t)
	snapshot.Origins = append(snapshot.Origins, snapshot.Origins...)
	digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	objects := &archiveTestObjects{}
	backend := "sha256:" + strings.Repeat("b", 64)
	if _, err := copyCompilerInputArchive(t.Context(), s.db, objects, backend, CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true}); err != nil {
		t.Fatal("archive copy failed", err)
	}
	if _, err := routeCompilerInputArchive(t.Context(), s.db, objects, backend, digest); err != nil {
		t.Fatal("archive route failed", err)
	}
	s.compilerInputArchiveObjects = objects
	s.compilerInputArchiveReadLeaseEnabled = true
	return digest, objects, backend
}

const hotCopyTestBudget = 16 << 20

func TestCompilerInputArchiveHotCopyVerifiesThenPrefersLocalBytes(t *testing.T) {
	for _, codec := range []bool{false, true} {
		t.Run(map[bool]string{false: "v1", true: "v2"}[codec], func(t *testing.T) {
			testCompilerInputArchiveHotCopyVerifiesThenPrefersLocalBytes(t, codec)
		})
	}
}

func testCompilerInputArchiveHotCopyVerifiesThenPrefersLocalBytes(t *testing.T, codec bool) {
	s := archiveTestStore(t)
	if codec {
		s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: true}
	}
	digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
	options := CompilerInputArchiveHotOptions{Digest: digest, MaxStoredBytes: hotCopyTestBudget}
	preview, err := copyCompilerInputArchiveHot(t.Context(), s.db, nil, "", options)
	if err != nil || preview.Applied || preview.State != "source-metadata-observed" || preview.StoredBytes <= 0 {
		t.Fatal("hot-copy preview changed state or omitted its byte estimate", preview, err)
	}
	objects.failGet = true
	if _, err = copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}); err == nil {
		t.Fatal("promotion accepted an unreadable external archive")
	}
	var hotRows int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_hot_chunks WHERE content_hash=$1`, digest).Scan(&hotRows); err != nil || hotRows != 0 {
		t.Fatal("failed archive verification staged hot rows", hotRows, err)
	}
	objects.failGet = false
	if _, err = copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: preview.StoredBytes - 1}); !errors.Is(err, ErrConflict) {
		t.Fatal("hot-copy byte budget was not enforced", err)
	}
	report, err := copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget})
	if err != nil || !report.Applied || report.State != "hot-verified" || report.Chunks < 1 || report.StoredBytes != preview.StoredBytes {
		t.Fatal("verified hot copy did not complete", report, err)
	}
	var location, archiveState string
	var oldRows int
	if err = s.db.QueryRow(`SELECT storage_location FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&location); err != nil || location != "hot" {
		t.Fatal("hot copy was not atomically published", location, err)
	}
	if err = s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1),(SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1)`, digest).Scan(&oldRows, &archiveState); err != nil || oldRows != report.Chunks || archiveState != "routed" {
		t.Fatal("promotion removed source rows or changed its archive pin", oldRows, archiveState, err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_hot_chunks WHERE content_hash=$1`, digest).Scan(&hotRows); err != nil || hotRows != report.Chunks {
		t.Fatal("hot chunk coverage differs", hotRows, err)
	}
	// A broken R2 reader proves this hot input is read from local PostgreSQL,
	// not the old external route. The source relation remains as rollback.
	s.compilerInputArchiveObjects = &archiveTestObjects{failGet: true}
	got, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal("hot input depended on unavailable R2", err)
	}
	if actual, hashErr := platformconfig.RuntimeSnapshotDigest(got); hashErr != nil || actual != digest {
		t.Fatal("hot reader changed the canonical input", actual, hashErr)
	}
	if _, err = s.GetPlatformArtifactContent(digest); err != nil {
		t.Fatal("public artifact content path depended on R2 for a hot input", err)
	}
	for i := 0; i < 2; i++ {
		if _, err = s.GetPlatformCompilerInputDNSSelections(digest, nil); err != nil {
			t.Fatal("cold/warm selective input path depended on R2 for a hot input", err)
		}
	}
	if err = s.EnsurePlatformCompilerInput(got, digest); err != nil {
		t.Fatal("existing hot writer verification depended on R2", err)
	}
	objects.failGet = true
	if _, err = copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}); err != nil {
		t.Fatal("repeated promotion of verified hot input required the archive", err)
	}
}

func TestCompilerInputArchiveHotCopyRejectsCorruptHotBytes(t *testing.T) {
	s := archiveTestStore(t)
	digest, _, backend := prepareVerifiedRoutedArchive(t, s)
	report, err := copyCompilerInputArchiveHot(t.Context(), s.db, s.compilerInputArchiveObjects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget})
	if err != nil || !report.Applied {
		t.Fatal("hot copy fixture did not complete", report, err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_hot_chunks SET data=$2 WHERE content_hash=$1 AND ordinal=0`, digest, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetPlatformCompilerInput(digest); err == nil {
		t.Fatal("corrupt hot input silently fell back to R2")
	}
}

func TestCompilerInputArchiveHotCopyPartialPageFailureAndResume(t *testing.T) {
	s := archiveTestStore(t)
	digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
	m, epoch, err := verifiedArchiveSource(t.Context(), s.db, digest)
	if err != nil || m.Count <= inputChunkBatchSize {
		t.Fatal("fixture must span multiple bounded pages", m, err)
	}
	if _, err = copyCompilerInputHotPage(t.Context(), s.db, m, epoch+1, 0, inputChunkBatchSize, 8<<20); !errors.Is(err, ErrConflict) {
		t.Fatal("stale maintenance epoch was allowed to stage bytes", err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_archive_leases SET expires_at=now()+interval '1 minute' WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = copyCompilerInputHotPage(t.Context(), s.db, m, epoch, 0, inputChunkBatchSize, 8<<20); !errors.Is(err, ErrConflict) {
		t.Fatal("active archive owner was not respected", err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_archive_leases SET expires_at=now()-interval '1 second' WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`INSERT INTO fugue_platform_input_hot_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,$2,'sha256:'||encode(sha256($3::bytea),'hex'),$3)`, digest, inputChunkBatchSize, []byte("synthetic-conflict")); err != nil {
		t.Fatal(err)
	}
	options := CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}
	if report, err := copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, options); !errors.Is(err, ErrConflict) || report.Applied {
		t.Fatal("conflicting second page was accepted", report, err)
	}
	var location string
	var count int
	if err = s.db.QueryRow(`SELECT storage_location FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&location); err != nil || location != "legacy" {
		t.Fatal("incomplete hot copy was published", location, err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_hot_chunks WHERE content_hash=$1`, digest).Scan(&count); err != nil || count != inputChunkBatchSize+1 {
		t.Fatal("failed page did not roll back while retaining prior checkpoints", count, err)
	}
	// Remove only this deliberately injected test conflict. Production never
	// deletes or overwrites unexpected staging bytes to force a retry through.
	if _, err = s.db.Exec(`DELETE FROM fugue_platform_input_hot_chunks WHERE content_hash=$1 AND ordinal=$2`, digest, inputChunkBatchSize); err != nil {
		t.Fatal(err)
	}
	if report, err := copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, options); err != nil || !report.Applied {
		t.Fatal("exact retry did not preserve/reuse the completed page", report, err)
	}
}

func TestCompilerInputArchiveHotCopyRejectsSelfConsistentSourceCorruption(t *testing.T) {
	s := archiveTestStore(t)
	digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
	if _, err := s.db.Exec(`UPDATE fugue_platform_input_chunks SET data=set_byte(data,0,33),chunk_hash='sha256:'||encode(sha256(set_byte(data,0,33)),'hex') WHERE content_hash=$1 AND ordinal=0`, digest); err != nil {
		t.Fatal(err)
	}
	if report, err := copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}); !errors.Is(err, ErrConflict) || report.Applied {
		t.Fatal("full original digest did not reject self-consistent corrupted chunks", report, err)
	}
	var location string
	if err := s.db.QueryRow(`SELECT storage_location FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&location); err != nil || location != "legacy" {
		t.Fatal("corrupt hot copy changed the canonical location", location, err)
	}
	var hotRows, expected int
	if err := s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_input_hot_chunks WHERE content_hash=$1),(SELECT chunk_count FROM fugue_platform_input_manifests WHERE content_hash=$1)`, digest).Scan(&hotRows, &expected); err != nil || hotRows != expected {
		t.Fatal("test did not reach full target digest validation", hotRows, expected, err)
	}
}

func TestCompilerInputArchiveCopyLeaseResumePinsAndReadback(t *testing.T) {
	s := archiveTestStore(t)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	objects := &archiveTestObjects{failSecond: true}
	backend := "sha256:" + strings.Repeat("b", 64)
	options := CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true}
	if _, err = copyCompilerInputArchive(t.Context(), s.db, objects, backend, options); err == nil {
		t.Fatal("upload failure accepted")
	}
	var receipts int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_archive_segments WHERE content_hash=$1`, digest).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatal("verified checkpoint lost", receipts, err)
	}
	if _, err = claimCompilerInputArchive(t.Context(), s.db, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal("active lease was stolen", err)
	}
	if _, err = RunCompilerInputMaintenance(t.Context(), s.databaseURL, CompilerInputMaintenanceOptions{Action: "migrate", Digest: digest, MaxInputBytes: 128 << 20, Apply: true}); !errors.Is(err, ErrConflict) {
		t.Fatal("archive source representation was not pinned against migration", err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_archive_leases SET expires_at=now()-interval '1 second' WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	objects.failSecond = false
	report, err := copyCompilerInputArchive(t.Context(), s.db, objects, backend, options)
	if err != nil || !report.Applied || report.State != "verified" || report.FencingToken != 2 {
		t.Fatal("resume did not verify", report, err)
	}
	after, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil || m != after {
		t.Fatal("source identity changed", err)
	}
	var count int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest).Scan(&count); err != nil || count != m.Count {
		t.Fatal("copy changed source chunks", err)
	}
	var state string
	if err = s.db.QueryRow(`SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&state); err != nil || state != "verified" {
		t.Fatal("copy published a route", state, err)
	}
	var leases int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_archive_leases WHERE content_hash=$1 AND expires_at>now()`, digest).Scan(&leases); err != nil || leases != 0 {
		t.Fatal("completion retained an active lease", err)
	}

	// Confirm that an old worker cannot write a receipt under a newer token.
	old := inputArchiveClaim{source: m, owner: "obsolete-worker", epoch: 0, token: 1}
	if err = checkpointCompilerInputArchive(t.Context(), s.db, old, inputarchive.Segment{}); !errors.Is(err, ErrConflict) {
		t.Fatal("stale worker wrote a receipt", err)
	}
}

func TestCompilerInputWriteFenceRoutesFallbackToHotAndBlocksMaintenanceRewrite(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	var original inputManifest
	original, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_control SET writes_fenced=true WHERE singleton`); err != nil {
		t.Fatal(err)
	}

	// Existing identities remain usable while fallback writes move to the hot
	// relation, leaving the old relation fixed for retirement verification.
	if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal("existing input was rejected by the write fence", err)
	}
	changed := snapshot
	changed.IntentGeneration += "-new"
	newDigest, err := platformconfig.RuntimeSnapshotDigest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.EnsurePlatformCompilerInput(changed, newDigest); err != nil {
		t.Fatal("fenced writer did not use the separate hot relation", err)
	}
	var location string
	if err = s.db.QueryRowContext(t.Context(), `SELECT storage_location FROM fugue_platform_input_manifests WHERE content_hash=$1`, newDigest).Scan(&location); err != nil || location != "hot" {
		t.Fatal("fenced write location was not recorded", location, err)
	}
	var legacyRows, hotRows int
	if err = s.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1),(SELECT count(*) FROM fugue_platform_input_hot_chunks WHERE content_hash=$1)`, newDigest).Scan(&legacyRows, &hotRows); err != nil || legacyRows != 0 || hotRows == 0 {
		t.Fatal("fenced fallback did not isolate bytes in the hot relation", legacyRows, hotRows, err)
	}
	for i := 0; i < 2; i++ {
		got, readErr := s.GetPlatformCompilerInput(newDigest)
		if readErr != nil {
			t.Fatal("new hot input is unreadable", readErr)
		}
		if actual, digestErr := platformconfig.RuntimeSnapshotDigest(got); digestErr != nil || actual != newDigest {
			t.Fatal("hot input changed its canonical digest", actual, digestErr)
		}
		if err = s.EnsurePlatformCompilerInput(changed, newDigest); err != nil {
			t.Fatal("hot input did not support idempotent ensure", err)
		}
	}

	if _, err = s.maintainCompilerInput(t.Context(), CompilerInputMaintenanceOptions{Action: "migrate", Digest: digest, ExpectedFormat: original.Format, MaxInputBytes: 1 << 30, Apply: true}); !errors.Is(err, ErrConflict) {
		t.Fatal("maintenance rewrite passed the fence", err)
	}
	current, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil || current != original {
		t.Fatal("fenced maintenance changed the input manifest", current, err)
	}
}

func TestCompilerInputDatabaseWriteFenceBlocksOldWritersAndCanBeReversedBeforeRetirement(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}

	armed, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false)
	if err != nil || !armed.Applied || !armed.WritesFenced || !armed.TriggerInstalled {
		t.Fatal("database writer fence was not atomically armed", armed, err)
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_chunks SET data=data WHERE content_hash=$1 AND ordinal=0`, digest); err == nil {
		t.Fatal("database trigger allowed a legacy binary to mutate chunks")
	}
	readBack, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal("write fencing blocked existing readers", err)
	}
	if got, e := platformconfig.RuntimeSnapshotDigest(readBack); e != nil || got != digest {
		t.Fatal("fenced read changed input identity", got, e)
	}

	released, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, true)
	if err != nil || !released.Applied || released.WritesFenced || released.TriggerInstalled {
		t.Fatal("pre-retirement writer fence could not be reversed", released, err)
	}
	changed := snapshot
	changed.IntentGeneration += "-after-unfence"
	newDigest, err := platformconfig.RuntimeSnapshotDigest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.EnsurePlatformCompilerInput(changed, newDigest); err != nil {
		t.Fatal("writer did not resume after fence removal", err)
	}
	var location string
	if err = s.db.QueryRowContext(t.Context(), `SELECT storage_location FROM fugue_platform_input_manifests WHERE content_hash=$1`, newDigest).Scan(&location); err != nil || location != "legacy" {
		t.Fatal("unfenced writer did not return to the legacy relation", location, err)
	}
	if _, err = s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_control SET writes_fenced=true,retirement_started_at=now() WHERE singleton`); err != nil {
		t.Fatal(err)
	}
	if _, err = RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, true); !errors.Is(err, ErrConflict) {
		t.Fatal("retired relation was allowed to reopen for writes", err)
	}
}

func prepareRetirableCompilerInput(t *testing.T, s *Store) (platformconfig.RuntimeSnapshot, string, *archiveTestObjects, string) {
	t.Helper()
	for name, value := range map[string]string{
		"FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT":          "https://archive.example.invalid",
		"FUGUE_COMPILER_INPUT_ARCHIVE_REGION":            "auto",
		"FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET":            "compiler-input-test",
		"FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX":            "compiler-archive-test",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID":     "test-access",
		"FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY": "test-secret",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED":   "true",
	} {
		t.Setenv(name, value)
	}
	resetArchiveFenceFixture(t, s)
	if _, err := s.db.ExecContext(t.Context(), `TRUNCATE TABLE fugue_platform_input_chunks RESTRICT`); err != nil {
		t.Fatal("clear disposable legacy chunk fixtures", err)
	}
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	objects := &archiveTestObjects{}
	cfg, configured, err := inputarchive.ConfigFromEnv()
	if err != nil || !configured {
		t.Fatal("test archive config invalid", err)
	}
	backend, err := cfg.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := copyCompilerInputArchive(t.Context(), s.db, objects, backend, CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true}); err != nil {
		t.Fatal("archive source was not verified", err)
	}
	if _, err := routeCompilerInputArchive(t.Context(), s.db, objects, backend, digest); err != nil {
		t.Fatal("archive read route was not committed", err)
	}
	if report, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); err != nil || !report.WritesFenced || !report.TriggerInstalled {
		t.Fatal("database writer fence was not installed", report, err)
	}
	s.compilerInputArchiveObjects = objects
	s.compilerInputArchiveReadLeaseEnabled = true
	return snapshot, digest, objects, backend
}

func TestCompilerInputArchiveRetirementRequiresCompleteVerifiedRouteAndDrainsReaders(t *testing.T) {
	s := archiveTestStore(t)
	_, digest, objects, backend := prepareRetirableCompilerInput(t, s)
	plan, err := compilerInputArchiveRetirementPlan(t.Context(), s.db, objects, backend, true)
	if err != nil || !plan.CanRetire || plan.InputCount != 1 || plan.CoveredInputCount != 1 || plan.VerifiedArchiveInputs != 1 || plan.BlockedInputCount != 0 || plan.LegacyChunkRows == 0 || plan.ReclaimableRelationBytes == 0 || plan.Inputs[0].Digest != digest || !plan.Inputs[0].ReplacementVerified {
		t.Fatal("complete archive route did not produce a retireable plan", plan, err)
	}
	if plan.Inputs[0].LogicalBytes <= 0 || plan.ReclaimableLogicalBytes != plan.Inputs[0].LogicalBytes {
		t.Fatal("plan did not report logical bytes separately from relation bytes", plan.Inputs[0], plan.ReclaimableLogicalBytes)
	}

	readerConn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var locked bool
	if err = readerConn.QueryRowContext(t.Context(), `SELECT pg_try_advisory_lock_shared($1)`, compilerInputArchiveReadLeaseLockID).Scan(&locked); err != nil || !locked {
		readerConn.Close()
		t.Fatal("could not establish a live compiler-input reader lease", err)
	}
	policyHash := strings.Repeat("d", 64)
	blocked, err := applyCompilerInputArchiveRetirement(t.Context(), s.db, backend, CompilerInputArchiveRetirementOptions{RoutingEnabled: true, ExpectedPlanSHA256: plan.PlanSHA256, CurrentReaderRelease: strings.Repeat("a", 40), RollbackReaderRelease: strings.Repeat("b", 40), ArchiveRetentionPolicySHA256: policyHash, ReaderDrainTimeout: 100 * time.Millisecond}, plan)
	if err == nil || blocked.Applied {
		t.Fatal("retirement did not stop when a reader lease remained active", blocked, err)
	}
	var rows int64
	if err = s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest).Scan(&rows); err != nil || rows != plan.Inputs[0].LegacyChunkRows {
		t.Fatal("reader drain timeout changed source rows", rows, err)
	}
	if _, err = readerConn.ExecContext(t.Context(), `SELECT pg_advisory_unlock_shared($1)`, compilerInputArchiveReadLeaseLockID); err != nil {
		readerConn.Close()
		t.Fatal(err)
	}
	if err = readerConn.Close(); err != nil {
		t.Fatal(err)
	}

	options := CompilerInputArchiveRetirementOptions{RoutingEnabled: true, ExpectedPlanSHA256: plan.PlanSHA256, CurrentReaderRelease: strings.Repeat("a", 40), RollbackReaderRelease: strings.Repeat("b", 40), ArchiveRetentionPolicySHA256: policyHash, ReaderDrainTimeout: time.Second}
	retired, err := applyCompilerInputArchiveRetirement(t.Context(), s.db, backend, options, plan)
	if err != nil || !retired.Applied || retired.Action != "archive-retire" {
		t.Fatal("complete verified relation did not retire", retired, err)
	}
	if err = s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest).Scan(&rows); err != nil || rows != 0 {
		t.Fatal("retirement left legacy chunk rows", rows, err)
	}
	var retiredAt sql.NullTime
	var storedPlan, currentReader, rollbackReader, storedPolicy string
	if err = s.db.QueryRowContext(t.Context(), `SELECT retirement_started_at,retirement_plan_sha256,retirement_reader_current,retirement_reader_rollback,retirement_archive_policy_sha256 FROM fugue_platform_input_archive_control WHERE singleton`).Scan(&retiredAt, &storedPlan, &currentReader, &rollbackReader, &storedPolicy); err != nil || !retiredAt.Valid || storedPlan != plan.PlanSHA256 || currentReader != strings.Repeat("a", 40) || rollbackReader != strings.Repeat("b", 40) || storedPolicy != policyHash {
		t.Fatal("retirement audit marker was not committed atomically", retiredAt, storedPlan, currentReader, rollbackReader, storedPolicy, err)
	}
	got, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal("reader could not load equivalent bytes after retirement", err)
	}
	if actual, digestErr := platformconfig.RuntimeSnapshotDigest(got); digestErr != nil || actual != digest {
		t.Fatal("reader identity changed after relation retirement", actual, digestErr)
	}
	if _, err = RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, true); !errors.Is(err, ErrConflict) {
		t.Fatal("retired table was allowed to reopen for writes", err)
	}
	if _, err = unrouteCompilerInputArchive(t.Context(), s.db, digest); !errors.Is(err, ErrConflict) {
		t.Fatal("retired source was allowed to re-enter the legacy route", err)
	}
	for _, statement := range []string{
		`UPDATE fugue_platform_input_archive_control SET retirement_started_at=NULL WHERE singleton`,
		`TRUNCATE TABLE fugue_platform_input_archive_control RESTRICT`,
		`UPDATE fugue_platform_input_archives SET state='verified',routed_at=NULL`,
		`DELETE FROM fugue_platform_input_archive_segments`,
		`TRUNCATE TABLE fugue_platform_input_archive_segments RESTRICT`,
		`INSERT INTO fugue_platform_input_archive_leases(content_hash,owner,source_epoch,fencing_token,expires_at,updated_at) VALUES('` + digest + `','retirement-test',0,0,now()+interval '1 minute',now())`,
	} {
		if _, err = s.db.ExecContext(t.Context(), statement); err == nil {
			t.Fatal("retirement allowed archive recovery metadata to mutate", statement)
		}
	}
	if err = schemamigrate.MigrateCompilerInputArchive(t.Context(), s.databaseURL); err != nil {
		t.Fatal("idempotent archive schema migration failed after retirement", err)
	}
}

func TestCompilerInputArchiveRetirementBlocksUnroutedInputs(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	if _, err := s.db.ExecContext(t.Context(), `TRUNCATE TABLE fugue_platform_input_chunks RESTRICT`); err != nil {
		t.Fatal("clear disposable legacy chunk fixtures", err)
	}
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	objects := &archiveTestObjects{}
	if report, err := RunCompilerInputArchiveWriteFence(t.Context(), s.databaseURL, true, false); err != nil || !report.WritesFenced {
		t.Fatal("writer fence was not installed", report, err)
	}
	plan, err := compilerInputArchiveRetirementPlan(t.Context(), s.db, objects, "sha256:"+strings.Repeat("c", 64), true)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CanRetire || plan.BlockedInputCount != 1 || plan.ReclaimableRelationBytes != 0 || plan.Inputs[0].Blocker != "verified-archive-route-missing" {
		t.Fatal("unrouted legacy source was treated as retireable", plan)
	}
}

func TestCompilerInputArchiveRetirementBlocksActiveMigrationLease(t *testing.T) {
	s := archiveTestStore(t)
	_, digest, objects, backend := prepareRetirableCompilerInput(t, s)
	if _, err := s.db.ExecContext(t.Context(), `INSERT INTO fugue_platform_input_archive_leases(content_hash,owner,source_epoch,fencing_token,expires_at,updated_at) SELECT content_hash,'active-test-migration',source_epoch,fencing_token+1,now()+interval '2 minutes',now() FROM fugue_platform_input_archives WHERE content_hash=$1 ON CONFLICT(content_hash) DO UPDATE SET owner=excluded.owner,source_epoch=excluded.source_epoch,fencing_token=excluded.fencing_token,expires_at=excluded.expires_at,updated_at=now()`, digest); err != nil {
		t.Fatal("could not establish test migration lease", err)
	}
	plan, err := compilerInputArchiveRetirementPlan(t.Context(), s.db, objects, backend, true)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CanRetire || plan.ActiveArchiveLeases != 1 || plan.ReclaimableRelationBytes != 0 {
		t.Fatal("active archive migration was not treated as a retirement blocker", plan)
	}
}

func TestCompilerInputArchiveRetirementPlanWorksWithoutArchiveBackend(t *testing.T) {
	s := archiveTestStore(t)
	resetArchiveFenceFixture(t, s)
	if _, err := s.db.ExecContext(t.Context(), `TRUNCATE TABLE fugue_platform_input_chunks RESTRICT`); err != nil {
		t.Fatal("clear disposable legacy chunk fixtures", err)
	}
	for _, name := range []string{
		"FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT", "FUGUE_COMPILER_INPUT_ARCHIVE_REGION",
		"FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET", "FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID", "FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED",
	} {
		t.Setenv(name, "")
	}
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	var rowsBefore int64
	if err := s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM fugue_platform_input_chunks`).Scan(&rowsBefore); err != nil {
		t.Fatal(err)
	}
	plan, err := RunCompilerInputArchiveRetirement(t.Context(), s.databaseURL, CompilerInputArchiveRetirementOptions{})
	if err != nil {
		t.Fatal("read-only plan required an unprovisioned archive target", err)
	}
	var rowsAfter int64
	if err = s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM fugue_platform_input_chunks`).Scan(&rowsAfter); err != nil {
		t.Fatal(err)
	}
	if plan.Applied || plan.CanRetire || plan.RoutingEnabled || plan.BlockedInputCount != 1 || plan.ReclaimableRelationBytes != 0 || rowsAfter != rowsBefore || plan.Inputs[0].Digest != digest {
		t.Fatal("archive-less plan changed data or overstated retirement coverage", plan, rowsBefore, rowsAfter)
	}
}

func TestCompilerInputArchiveStartupRequiresMatchingBackendAfterRetirement(t *testing.T) {
	s := archiveTestStore(t)
	_, _, _, backend := prepareRetirableCompilerInput(t, s)
	resetArchiveFenceFixture(t, s)
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err = installCompilerInputArchiveRetirementGuards(t.Context(), tx); err != nil {
		t.Fatal("install disposable retirement guards", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(t.Context(), `UPDATE fugue_platform_input_archive_control SET retirement_started_at=now() WHERE singleton`); err != nil {
		t.Fatal("mark disposable legacy relation retired", err)
	}
	if _, err := s.db.ExecContext(t.Context(), `TRUNCATE TABLE fugue_platform_input_chunks RESTRICT`); err != nil {
		t.Fatal("retire disposable legacy relation", err)
	}
	for _, name := range []string{
		"FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT", "FUGUE_COMPILER_INPUT_ARCHIVE_REGION",
		"FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET", "FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID", "FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED",
	} {
		t.Setenv(name, "")
	}
	s.compilerInputArchiveObjects = nil
	s.compilerInputArchiveReadLeaseEnabled = false
	if err := s.ConfigureCompilerInputArchiveFromEnv(); !errors.Is(err, ErrConflict) {
		t.Fatal("retired relation allowed startup without its archive backend", err)
	}
	for name, value := range map[string]string{
		"FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT":          "https://archive.example.invalid",
		"FUGUE_COMPILER_INPUT_ARCHIVE_REGION":            "auto",
		"FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET":            "compiler-input-test",
		"FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX":            "compiler-archive-test",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID":     "test-access",
		"FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY": "test-secret",
		"FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED":   "true",
	} {
		t.Setenv(name, value)
	}
	if err := s.ConfigureCompilerInputArchiveFromEnv(); err != nil {
		t.Fatal("retired relation rejected its matching archive backend", err)
	}
	if !s.compilerInputArchiveReadLeaseEnabled || s.compilerInputArchiveObjects == nil {
		t.Fatal("matching archive backend did not enable routed reads and leases")
	}
	if backend == "" {
		t.Fatal("test fixture backend fingerprint is empty")
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET", "another-compiler-input-test")
	if err := s.ConfigureCompilerInputArchiveFromEnv(); !errors.Is(err, ErrConflict) {
		t.Fatal("retired relation accepted a different archive backend", err)
	}
}

func resetArchiveFenceFixture(t *testing.T, s *Store) {
	t.Helper()
	// archiveTestStore permits only an explicitly configured disposable
	// loopback fugue_test database. Never reset another database's control.
	reset := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, table := range []string{"fugue_platform_input_archive_control", "fugue_platform_input_archives", "fugue_platform_input_archive_segments", "fugue_platform_input_archive_leases"} {
			for _, level := range []string{"row", "truncate"} {
				if _, err := s.db.ExecContext(ctx, "DROP TRIGGER IF EXISTS "+table+"_retired_"+level+"_guard ON "+table); err != nil {
					t.Error("reset disposable archive guard", err)
				}
			}
		}
		if _, err := s.db.ExecContext(ctx, `DROP FUNCTION IF EXISTS fugue_platform_input_archive_retirement_guard()`); err != nil {
			t.Error("reset disposable archive guard function", err)
		}
		_, err := s.db.ExecContext(ctx, `DROP TRIGGER IF EXISTS fugue_platform_input_chunks_archive_write_fence ON fugue_platform_input_chunks; DROP FUNCTION IF EXISTS fugue_platform_input_chunks_archive_write_fence(); UPDATE fugue_platform_input_archive_control SET epoch=0,writes_fenced=false,retirement_started_at=NULL,retirement_plan_sha256='',retirement_reader_current='',retirement_reader_rollback='',retirement_archive_policy_sha256='' WHERE singleton`)
		if err != nil {
			t.Error("reset disposable write-fence fixture", err)
		}
	}
	reset()
	t.Cleanup(reset)
}

func TestCompilerInputArchiveRoutedReadRequiresVerifiedBytes(t *testing.T) {
	s := archiveTestStore(t)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	objects := &archiveTestObjects{}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT", "https://archive.example.invalid")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_REGION", "auto")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET", "compiler-input-test")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX", "compiler-archive-test")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID", "test-access")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY", "test-secret")
	cfg, configured, err := inputarchive.ConfigFromEnv()
	if err != nil || !configured {
		t.Fatal("test archive config invalid", err)
	}
	backend, err := cfg.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = copyCompilerInputArchive(t.Context(), s.db, objects, backend, CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true}); err != nil {
		t.Fatal(err)
	}
	s.compilerInputArchiveObjects = objects
	s.compilerInputArchiveReadLeaseEnabled = true
	originalObjects := make(map[string][]byte, len(objects.objects))
	for key, value := range objects.objects {
		originalObjects[key] = bytes.Clone(value)
	}
	for key := range objects.objects {
		objects.objects[key][0] ^= 0xff
		break
	}
	if _, err = routeCompilerInputArchive(t.Context(), s.db, objects, backend, digest); err == nil {
		t.Fatal("corrupt archive was routed")
	}
	var state string
	if err = s.db.QueryRow(`SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&state); err != nil || state != "verified" {
		t.Fatal("failed route changed the archive state", state, err)
	}
	got, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal("unrouted input did not retain its legacy reader", err)
	}
	gotDigest, err := platformconfig.RuntimeSnapshotDigest(got)
	if err != nil || gotDigest != digest {
		t.Fatal("legacy reader changed input identity", err)
	}
	for key, value := range originalObjects {
		objects.objects[key] = value
	}
	route, err := routeCompilerInputArchive(t.Context(), s.db, objects, backend, digest)
	if err != nil || !route.Applied || route.State != "routed" {
		t.Fatal("verified archive was not routed", route, err)
	}
	got, err = s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal("verified archive route was unreadable", err)
	}
	gotDigest, err = platformconfig.RuntimeSnapshotDigest(got)
	if err != nil || gotDigest != digest {
		t.Fatal("archive route changed input identity", err)
	}
	if _, err = routeCompilerInputArchive(t.Context(), s.db, objects, backend, digest); err != nil {
		t.Fatal("repeated route was not idempotent", err)
	}
	var firstChunk []byte
	if err = s.db.QueryRow(`SELECT data FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal LIMIT 1`, digest).Scan(&firstChunk); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_chunks SET data=overlay(data placing decode('00','hex') from 1 for 1) WHERE content_hash=$1 AND ordinal=0`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = unrouteCompilerInputArchive(t.Context(), s.db, digest); err == nil {
		t.Fatal("corrupt legacy source was accepted for unroute")
	}
	if err = s.db.QueryRow(`SELECT state FROM fugue_platform_input_archives WHERE content_hash=$1`, digest).Scan(&state); err != nil || state != "routed" {
		t.Fatal("failed unroute changed the archive state", state, err)
	}
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_chunks SET data=$2 WHERE content_hash=$1 AND ordinal=0`, digest, firstChunk); err != nil {
		t.Fatal(err)
	}
	for key := range objects.objects {
		objects.objects[key][0] ^= 0xff
		break
	}
	if _, err = s.GetPlatformCompilerInput(digest); err == nil {
		t.Fatal("corrupt routed archive was accepted")
	}
	rolledBack, err := unrouteCompilerInputArchive(t.Context(), s.db, digest)
	if err != nil || !rolledBack.Applied || rolledBack.State != "verified" {
		t.Fatal("verified legacy source was not restored", rolledBack, err)
	}
	got, err = s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal("legacy fallback failed after unroute", err)
	}
	gotDigest, err = platformconfig.RuntimeSnapshotDigest(got)
	if err != nil || gotDigest != digest {
		t.Fatal("legacy fallback changed input identity", err)
	}
}

func TestCompilerInputArchiveSourceStreamsMultipleBoundedPages(t *testing.T) {
	s := boundedContentTestStore(t)
	if err := schemamigrate.MigrateCompilerInputArchive(t.Context(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	raw := bytes.Repeat([]byte("stable-source-data-"), 17)
	h := sha256.Sum256(raw)
	digest := "sha256:" + hex.EncodeToString(h[:])
	resetArchiveInputFixture(t, s, digest)
	const chunkSize = 19
	count := (len(raw) + chunkSize - 1) / chunkSize
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,$3,$4,$5,$6)`, digest, inputFormatV1, len(raw), chunkSize, count, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	for ordinal, offset := 0, 0; offset < len(raw); ordinal++ {
		end := min(offset+chunkSize, len(raw))
		chunk := raw[offset:end]
		chunkHash := sha256.Sum256(chunk)
		if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,$2,$3,$4)`, digest, ordinal, "sha256:"+hex.EncodeToString(chunkHash[:]), chunk); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	manifest, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	stream := &archiveInputStream{ctx: t.Context(), db: s.db, m: manifest, hash: sha256.New(), sourceEpoch: 0}
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, raw) || stream.count != count || stream.size != int64(len(raw)) {
		t.Fatalf("archive source reconstruction differs: bytes=%d chunks=%d", len(got), stream.count)
	}
	if stream.err != nil {
		t.Fatal(stream.err)
	}
	// A final read forces the empty keyset page and verifies the whole-input
	// digest; only the caller may accept EOF after this check.
	var extra [1]byte
	if n, err := stream.Read(extra[:]); n != 0 || err != io.EOF {
		t.Fatalf("expected verified EOF, n=%d err=%v", n, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompilerInputArchiveMigrationStreamsBeyondLegacyLimit(t *testing.T) {
	s := archiveTestStore(t)
	const size = int64(128<<20) + 31
	const chunkSize = 256 << 10
	pattern := []byte("stable compiler input evidence\n")
	h := sha256.New()
	for left := size; left > 0; {
		n := int(min(int64(chunkSize), left))
		chunk := make([]byte, n)
		for i := range chunk {
			chunk[i] = pattern[(int(size-left)+i)%len(pattern)]
		}
		h.Write(chunk)
		left -= int64(n)
	}
	digest := "sha256:" + hex.EncodeToString(h.Sum(nil))
	resetArchiveInputFixture(t, s, digest)
	count := int((size + chunkSize - 1) / chunkSize)
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,$3,$4,$5,now())`, digest, inputFormatV1, size, chunkSize, count); err != nil {
		t.Fatal(err)
	}
	for ordinal, offset := 0, int64(0); offset < size; ordinal++ {
		n := int(min(int64(chunkSize), size-offset))
		chunk := make([]byte, n)
		for i := range chunk {
			chunk[i] = pattern[(int(offset)+i)%len(pattern)]
		}
		chunkHash := sha256.Sum256(chunk)
		if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,$2,$3,$4)`, digest, ordinal, "sha256:"+hex.EncodeToString(chunkHash[:]), chunk); err != nil {
			t.Fatal(err)
		}
		offset += int64(n)
	}
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	objects := &archiveTestObjects{}
	backend := "sha256:" + strings.Repeat("e", 64)
	archived, err := copyCompilerInputArchive(t.Context(), s.db, objects, backend, CompilerInputArchiveMigrationOptions{Digest: digest, Apply: true, SegmentBytes: 16 << 20})
	if err != nil || int64(archived.Segments) != (size+(16<<20)-1)/(16<<20) || archived.SourceChunks != count || archived.RawBytes != size {
		t.Fatal("streamed archive dimensions differ", archived, err)
	}
	hot, err := copyCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: size})
	if err != nil || !hot.Applied || hot.Chunks != m.Count || hot.RawBytes != size || hot.StoredBytes != size {
		t.Fatal("hot promotion did not support inputs larger than 128MiB", hot, err)
	}
}

func TestCompilerInputArchiveDryRunIsReadOnlyAndNeedsNoObjectStore(t *testing.T) {
	s := archiveTestStore(t)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	var before int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_archives`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	got, err := copyCompilerInputArchive(t.Context(), s.db, nil, "", CompilerInputArchiveMigrationOptions{Digest: digest, Apply: false})
	if err != nil || got.Applied || got.State != "source-manifest-observed" || got.RawBytes <= 0 || got.SourceChunks < 1 {
		t.Fatal("archive dry-run attempted writes or required object storage", got, err)
	}
	var after int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_archives`).Scan(&after); err != nil || after != before {
		t.Fatal("archive dry-run changed the archive ledger", before, after, err)
	}
}

func TestCompilerInputArchiveSourceRejectsChangedChunkBytes(t *testing.T) {
	s := boundedContentTestStore(t)
	if err := schemamigrate.MigrateCompilerInputArchive(t.Context(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	raw := []byte("a canonical compiler input")
	h := sha256.Sum256(raw)
	digest := "sha256:" + hex.EncodeToString(h[:])
	resetArchiveInputFixture(t, s, digest)
	chunkHash := sha256.Sum256([]byte("different canonical compiler input"))
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,$3,$4,1,$5)`, digest, inputFormatV1, len(raw), len(raw), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,0,$2,$3)`, digest, "sha256:"+hex.EncodeToString(chunkHash[:]), []byte("different canonical compiler input")); err != nil {
		t.Fatal(err)
	}
	manifest, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	stream := &archiveInputStream{ctx: t.Context(), db: s.db, m: manifest, hash: sha256.New()}
	if _, err := io.ReadAll(stream); err == nil {
		t.Fatal("mutated input was accepted")
	}
}

func TestCompilerInputArchiveReaderFallsBackBeforeLedgerMigration(t *testing.T) {
	s := boundedContentTestStore(t)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	s.compilerInputArchiveReadLeaseEnabled = true
	got, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal("configured-but-not-migrated reader broke legacy bytes", err)
	}
	actual, err := platformconfig.RuntimeSnapshotDigest(got)
	if err != nil || actual != digest {
		t.Fatal("legacy input changed before archive DDL", err)
	}
}

func TestCompilerInputArchiveWriterBarrierRetriesOnlyAfterRollback(t *testing.T) {
	s := boundedContentTestStore(t)
	holder, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Rollback()
	if _, err = holder.ExecContext(t.Context(), `SELECT pg_advisory_xact_lock(315609238744290)`); err != nil {
		t.Fatal("could not establish disposable writer contention", err)
	}

	released := make(chan struct{})
	go func() {
		timer := time.NewTimer(400 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			_ = holder.Rollback()
		case <-t.Context().Done():
			_ = holder.Rollback()
		}
		close(released)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	started := time.Now()
	tx, err := beginInputArchiveTx(ctx, s.db)
	if err != nil {
		t.Fatal("bounded barrier retry did not resume after the writer released", err)
	}
	if elapsed := time.Since(started); elapsed < 350*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("barrier retry did not respect bounded wait: %s", elapsed)
	}
	if _, err = tx.ExecContext(ctx, `SELECT 1`); err != nil {
		t.Fatal("barrier retry returned an invalid transaction", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal("barrier retry transaction did not commit", err)
	}
	select {
	case <-released:
	case <-ctx.Done():
		t.Fatal("disposable lock holder did not release")
	}
	var held int
	if err = s.db.QueryRowContext(ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND classid=(315609238744290::bigint>>32)::oid AND objid=(315609238744290::bigint&4294967295)::oid AND granted`).Scan(&held); err != nil || held != 0 {
		t.Fatal("completed archive barrier leaked a lock", held, err)
	}
}

func TestCompilerInputInspectWaitsBehindArchiveReaderFence(t *testing.T) {
	s := archiveTestStore(t)
	snapshot, digest := chunkedInputFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_ENDPOINT", "https://archive.example.invalid")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_REGION", "auto")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_BUCKET", "compiler-input-test")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_PREFIX", "compiler-archive-test")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_ACCESS_KEY_ID", "test-access")
	t.Setenv("FUGUE_COMPILER_INPUT_ARCHIVE_SECRET_ACCESS_KEY", "test-secret")

	conn, err := s.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var acquired bool
	if err = conn.QueryRowContext(t.Context(), `SELECT pg_try_advisory_lock($1)`, compilerInputArchiveReadLeaseLockID).Scan(&acquired); err != nil || !acquired {
		t.Fatal("could not establish the retirement reader fence", err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_, _ = conn.ExecContext(cleanupCtx, `SELECT pg_advisory_unlock_all()`)
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, inspectErr := RunCompilerInputMaintenance(ctx, s.databaseURL, CompilerInputMaintenanceOptions{Action: "inspect", Digest: digest})
		done <- inspectErr
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		// The reader deliberately polls pg_try_advisory_lock_shared rather
		// than queueing an unbounded server-side lock wait. Observe its
		// actual backend query before asserting that it cannot finish.
		var attempts int
		if err = s.db.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND query=$1`, `SELECT pg_try_advisory_lock_shared($1)`).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if attempts > 0 {
			select {
			case err = <-done:
				t.Fatalf("maintenance inspect completed while the reader fence was held: %v", err)
			default:
			}
			break
		}
		select {
		case err = <-done:
			t.Fatalf("maintenance inspect bypassed the archive reader fence: %v", err)
		case <-ctx.Done():
			t.Fatal("maintenance inspect did not enter the reader fence", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("maintenance inspect did not wait for the reader fence")
		}
	}

	var unlocked bool
	if err = conn.QueryRowContext(t.Context(), `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID).Scan(&unlocked); err != nil || !unlocked {
		t.Fatal("could not release the retirement reader fence", err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal("maintenance inspect did not resume after the reader fence", err)
		}
	case <-ctx.Done():
		t.Fatal("maintenance inspect remained blocked after the reader fence", ctx.Err())
	}
}
