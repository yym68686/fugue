package store

import (
	"bytes"
	"context"
	"crypto/sha256"
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
}

func archiveTestStore(t *testing.T) *Store {
	t.Helper()
	s := boundedContentTestStore(t)
	for _, migrate := range []func(context.Context, string) error{schemamigrate.MigrateCompilerInputCodec, schemamigrate.MigrateCompilerInputShared, schemamigrate.MigrateCompilerInputSeparated, schemamigrate.MigrateCompilerInputArchive} {
		if err := migrate(t.Context(), s.databaseURL); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func resetArchiveInputFixture(t *testing.T, s *Store, digest string) {
	t.Helper()
	clear := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
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
	raw, ok := s.objects[key]
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(raw)), nil
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
	stream := &archiveInputStream{ctx: t.Context(), db: s.db, m: m, hash: sha256.New()}
	objects := &archiveTestObjects{}
	plan := inputarchive.Manifest{Format: inputarchive.Format, Digest: digest, RawBytes: size, SegmentBytes: inputarchive.DefaultSegmentBytes, Prefix: "archive/streaming-over-limit"}
	archived, err := inputarchive.Copy(t.Context(), objects, stream, plan, func(inputarchive.Segment) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(archived.Segments)) != (size+int64(plan.SegmentBytes)-1)/int64(plan.SegmentBytes) || stream.count != count || stream.size != size {
		t.Fatalf("streamed archive dimensions differ: segments=%d source_chunks=%d source_bytes=%d", len(archived.Segments), stream.count, stream.size)
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
