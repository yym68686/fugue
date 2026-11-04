package store

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformconfig"
)

type restoreTestObjects struct {
	*archiveTestObjects
	beforeGet func(string) error
}

func (o restoreTestObjects) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if o.beforeGet != nil {
		if err := o.beforeGet(key); err != nil {
			return nil, err
		}
	}
	return o.archiveTestObjects.Get(ctx, key)
}

func assertRestoreNotPublished(t *testing.T, s *Store, digest string, wantRows int) {
	t.Helper()
	var location string
	var count int
	err := s.db.QueryRow(`SELECT storage_location,(SELECT count(*) FROM fugue_platform_input_hot_chunks WHERE content_hash=$1) FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&location, &count)
	if err != nil || location != "legacy" || count != wantRows {
		t.Fatal("failed restore changed the reader route or lost bounded progress", location, count, wantRows, err)
	}
}

func TestCompilerInputArchiveHotRestoreAfterRetirementPreservesImmutableLedger(t *testing.T) {
	for _, codec := range []bool{false, true} {
		t.Run(map[bool]string{false: "v1", true: "v2"}[codec], func(t *testing.T) {
			s := archiveTestStore(t)
			if codec {
				s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: true}
			}
			_, digest, objects, backend := prepareRetirableCompilerInput(t, s)
			plan, err := compilerInputArchiveRetirementPlan(t.Context(), s.db, objects, backend, true)
			if err != nil || !plan.CanRetire {
				t.Fatal("fixture could not produce a verified retirement plan", plan, err)
			}
			options := CompilerInputArchiveRetirementOptions{RoutingEnabled: true, ExpectedPlanSHA256: plan.PlanSHA256, CurrentReaderRelease: strings.Repeat("a", 40), RollbackReaderRelease: strings.Repeat("b", 40), ArchiveRetentionPolicySHA256: strings.Repeat("d", 64), ReaderDrainTimeout: time.Second}
			if got, err := applyCompilerInputArchiveRetirement(t.Context(), s.db, backend, options, plan); err != nil || !got.Applied {
				t.Fatal("test relation did not retire", got, err)
			}
			var before string
			ledger := `SELECT json_build_array((SELECT row_to_json(c) FROM fugue_platform_input_archive_control c WHERE singleton),(SELECT row_to_json(a) FROM fugue_platform_input_archives a WHERE content_hash=$1),(SELECT json_agg(s ORDER BY ordinal) FROM fugue_platform_input_archive_segments s WHERE content_hash=$1),(SELECT row_to_json(l) FROM fugue_platform_input_archive_leases l WHERE content_hash=$1))::text`
			if err = s.db.QueryRow(ledger, digest).Scan(&before); err != nil {
				t.Fatal(err)
			}
			objects.failGet = true
			hotOptions := CompilerInputArchiveHotOptions{Digest: digest, MaxStoredBytes: hotCopyTestBudget}
			preview, err := restoreCompilerInputArchiveHot(t.Context(), s.db, nil, backend, hotOptions)
			if err != nil || preview.Applied || preview.State != "archive-metadata-observed" || preview.StoredBytes != 0 || preview.WorstCaseStoredBytes < preview.RawBytes {
				t.Fatal("restore preview fetched objects, wrote data or claimed a measured size", preview, err)
			}
			assertRestoreNotPublished(t, s, digest, 0)
			hotOptions.Apply = true
			if got, err := restoreCompilerInputArchiveHot(t.Context(), s.db, objects, backend, hotOptions); err == nil || got.Applied {
				t.Fatal("unavailable archive was accepted", got, err)
			}
			assertRestoreNotPublished(t, s, digest, 0)
			objects.failGet = false
			restored, err := restoreCompilerInputArchiveHot(t.Context(), s.db, objects, backend, hotOptions)
			if err != nil || !restored.Applied || restored.State != "hot-verified" || restored.Action != "archive-hot-restore" || restored.StoredBytes < 1 {
				t.Fatal("archive could not restore retired input", restored, err)
			}
			var remaining int
			if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_chunks`).Scan(&remaining); err != nil || remaining != 0 {
				t.Fatal("restore recreated old-table rows", remaining, err)
			}
			var after string
			if err = s.db.QueryRow(ledger, digest).Scan(&after); err != nil || after != before {
				t.Fatal("restore changed immutable recovery metadata", err)
			}
			objects.failGet = true
			got, err := s.GetPlatformCompilerInput(digest)
			if err != nil {
				t.Fatal("restored hot input still depended on R2", err)
			}
			if actual, err := platformconfig.RuntimeSnapshotDigest(got); err != nil || actual != digest {
				t.Fatal("restore changed the original canonical bytes", actual, err)
			}
			if repeated, err := restoreCompilerInputArchiveHot(t.Context(), s.db, objects, backend, hotOptions); err != nil || repeated.Applied || repeated.State != "hot-verified" {
				t.Fatal("idempotent hot verification depended on R2", repeated, err)
			}
		})
	}
}

func TestCompilerInputArchiveHotRestoreInterruptedPageResumes(t *testing.T) {
	s := archiveTestStore(t)
	digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
	options := CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}
	if report, err := restoreCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: 1}); !errors.Is(err, ErrConflict) || report.Applied {
		t.Fatal("stored-byte budget was not enforced", report, err)
	}
	assertRestoreNotPublished(t, s, digest, 0)
	fault := restoreTestObjects{archiveTestObjects: objects, beforeGet: func(key string) error {
		if strings.Contains(key, "/00004-") {
			return errors.New("synthetic interrupted download")
		}
		return nil
	}}
	if report, err := restoreCompilerInputArchiveHot(t.Context(), s.db, fault, backend, options); err == nil || report.Applied {
		t.Fatal("interrupted restore was published", report, err)
	}
	assertRestoreNotPublished(t, s, digest, inputChunkBatchSize)
	if report, err := restoreCompilerInputArchiveHot(t.Context(), s.db, objects, backend, options); err != nil || !report.Applied {
		t.Fatal("exact restore retry did not reuse completed progress", report, err)
	}
}

func TestCompilerInputArchiveHotRestoreRejectsChangedSourceAndActiveOwner(t *testing.T) {
	for _, mutation := range []string{
		`UPDATE fugue_platform_input_archive_control SET epoch=epoch+1 WHERE singleton`,
		`UPDATE fugue_platform_input_archive_leases SET expires_at=now()+interval '1 minute' WHERE content_hash=$1`,
		`UPDATE fugue_platform_input_archives SET state='verified',routed_at=NULL WHERE content_hash=$1`,
	} {
		t.Run(strings.Fields(mutation)[1], func(t *testing.T) {
			s := archiveTestStore(t)
			digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
			fault := restoreTestObjects{archiveTestObjects: objects, beforeGet: func(key string) error {
				if strings.Contains(key, "/00004-") {
					var err error
					if strings.Contains(mutation, "$1") {
						_, err = s.db.Exec(mutation, digest)
					} else {
						_, err = s.db.Exec(mutation)
					}
					return err
				}
				return nil
			}}
			if report, err := restoreCompilerInputArchiveHot(t.Context(), s.db, fault, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}); !errors.Is(err, ErrConflict) || report.Applied {
				t.Fatal("changed source/owner was accepted", report, err)
			}
			assertRestoreNotPublished(t, s, digest, inputChunkBatchSize)
		})
	}
}

func TestCompilerInputArchiveHotRestoreDoesNotOverwriteConflictingStaging(t *testing.T) {
	s := archiveTestStore(t)
	digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
	conflict := []byte("synthetic conflicting stage")
	if _, err := s.db.Exec(`INSERT INTO fugue_platform_input_hot_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,$2,$3,$4)`, digest, inputChunkBatchSize, sharedHash(sha256.Sum256(conflict)), conflict); err != nil {
		t.Fatal(err)
	}
	if got, err := restoreCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}); err == nil || got.Applied {
		t.Fatal("conflicting staging was accepted", got, err)
	}
	assertRestoreNotPublished(t, s, digest, inputChunkBatchSize+1)
	var retained []byte
	if err := s.db.QueryRow(`SELECT data FROM fugue_platform_input_hot_chunks WHERE content_hash=$1 AND ordinal=$2`, digest, inputChunkBatchSize).Scan(&retained); err != nil || !bytes.Equal(retained, conflict) {
		t.Fatal("conflicting bytes were overwritten", err)
	}
}

func TestCompilerInputArchiveHotRestoreResumesAcrossCompressionChanges(t *testing.T) {
	s := archiveTestStore(t)
	s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: true}
	digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
	var original []byte
	if err := s.db.QueryRow(`SELECT data FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal=0`, digest).Scan(&original); err != nil {
		t.Fatal(err)
	}
	m, _, err := verifiedArchiveSource(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := decodeInputChunk(m.Format, original, m.ChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	var alternate bytes.Buffer
	alternate.Write([]byte{1, 0, 0, 0, 0})
	z, err := zlib.NewWriterLevel(&alternate, zlib.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = z.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = z.Close(); err != nil {
		t.Fatal(err)
	}
	stored := alternate.Bytes()
	binary.BigEndian.PutUint32(stored[1:5], uint32(len(raw)))
	if bytes.Equal(stored, original) {
		t.Fatal("fixture did not produce a different valid compression")
	}
	if _, err = s.db.Exec(`INSERT INTO fugue_platform_input_hot_chunks(content_hash,ordinal,chunk_hash,data) VALUES($1,0,$2,$3)`, digest, sharedHash(sha256.Sum256(stored)), stored); err != nil {
		t.Fatal(err)
	}
	if got, err := restoreCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}); err != nil || !got.Applied {
		t.Fatal("valid differently compressed staging was not reusable", got, err)
	}
	var retained []byte
	if err = s.db.QueryRow(`SELECT data FROM fugue_platform_input_hot_chunks WHERE content_hash=$1 AND ordinal=0`, digest).Scan(&retained); err != nil || !bytes.Equal(retained, stored) {
		t.Fatal("valid staged compression was overwritten", err)
	}
}

func TestCompilerInputArchiveHotRestoreRequiresOriginalWholeDigest(t *testing.T) {
	s := archiveTestStore(t)
	digest, objects, backend := prepareVerifiedRoutedArchive(t, s)
	m, _, err := verifiedArchiveSource(t.Context(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	archive, err := compilerInputArchiveManifest(t.Context(), s.db, m, backend, "routed")
	if err != nil {
		t.Fatal(err)
	}
	first := archive.Segments[0]
	z, err := gzip.NewReader(bytes.NewReader(objects.objects[first.Key]))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	if err = z.Close(); err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 1
	var compressed bytes.Buffer
	w := gzip.NewWriter(&compressed)
	if _, err = w.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	stored := compressed.Bytes()
	rawHash, storedHash := sharedHash(sha256.Sum256(raw)), sharedHash(sha256.Sum256(stored))
	key := archive.Prefix + "/00000-" + strings.TrimPrefix(storedHash, "sha256:") + ".gz"
	objects.objects[key] = bytes.Clone(stored)
	// Keep every segment independently self-consistent, but make the full
	// stream differ from the immutable original digest. Segment-only checks
	// must not be sufficient to publish the new local route.
	if _, err = s.db.Exec(`UPDATE fugue_platform_input_archive_segments SET object_key=$2,segment_hash=$3,stored_hash=$4,stored_bytes=$5 WHERE content_hash=$1 AND ordinal=0`, digest, key, rawHash, storedHash, len(stored)); err != nil {
		t.Fatal(err)
	}
	if got, err := restoreCompilerInputArchiveHot(t.Context(), s.db, objects, backend, CompilerInputArchiveHotOptions{Digest: digest, Apply: true, MaxStoredBytes: hotCopyTestBudget}); !errors.Is(err, ErrConflict) || got.Applied {
		t.Fatal("self-consistent segment corruption bypassed the original digest", got, err)
	}
	assertRestoreNotPublished(t, s, digest, m.Count)
}
