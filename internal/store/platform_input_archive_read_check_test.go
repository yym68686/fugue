package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/inputarchive"
)

type archiveReadCheckObjects struct {
	*archiveTestObjects
}

func (o *archiveReadCheckObjects) Put(context.Context, string, []byte) error {
	panic("read check attempted an object write")
}

func TestArchiveReadCheckRejectsMissingBudgetBeforeDatabaseAccess(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, options := range []CompilerInputArchiveReadCheckOptions{
		{Digest: digest}, {Digest: "invalid", MaxRawBytes: 1},
		{Digest: digest, MaxRawBytes: -1}, {Digest: digest, MaxRawBytes: inputarchive.MaxInputBytes + 1},
	} {
		report, err := RunCompilerInputArchiveReadCheck(t.Context(), "invalid", options)
		if !errors.Is(err, ErrInvalidInput) || report.Verified || report.RetirementAuthorized || !report.ReadOnly {
			t.Fatal("invalid read-check intent attempted database access or granted authority", report, err)
		}
	}
}

func TestArchiveReadCheckPostgresVerifiedReceiptAndFullNativeRead(t *testing.T) {
	db, _, _ := hotInventoryTestDatabase(t)
	raw := bytes.Repeat([]byte("neutral original canonical bytes\n"), 8192)
	h := sha256.Sum256(raw)
	digest := "sha256:" + hex.EncodeToString(h[:])
	objects := &archiveTestObjects{}
	manifest, err := inputarchive.Copy(t.Context(), objects, bytes.NewReader(raw), inputarchive.Manifest{
		Format: inputarchive.Format, Digest: digest, RawBytes: int64(len(raw)), SegmentBytes: 64 << 10, Prefix: "neutral-read-check",
	}, func(inputarchive.Segment) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	backend := "sha256:" + strings.Repeat("b", 64)
	if _, err = db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,size_bytes,chunk_size,chunk_count,created_at)
 VALUES($1,$2,$3,$4,$5,now())`, digest, inputFormatV1, len(raw), inputChunkBytes, (len(raw)+inputChunkBytes-1)/inputChunkBytes); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO fugue_platform_input_archives(content_hash,archive_format,source_format,raw_bytes,segment_bytes,segment_count,object_prefix,backend_hash,state,source_epoch,verified_digest,fencing_token,created_at,verified_at,updated_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,'verified',0,$1,0,now(),now(),now())`, digest, manifest.Format, inputFormatV1, manifest.RawBytes, manifest.SegmentBytes, len(manifest.Segments), manifest.Prefix, backend); err != nil {
		t.Fatal(err)
	}
	for _, segment := range manifest.Segments {
		if _, err = db.Exec(`INSERT INTO fugue_platform_input_archive_segments(content_hash,ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash,uploaded_at,verified_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,now(),now())`, digest, segment.Ordinal, segment.Key, segment.RawBytes, segment.StoredBytes, segment.RawHash, segment.StoredHash); err != nil {
			t.Fatal(err)
		}
	}
	options := CompilerInputArchiveReadCheckOptions{Digest: digest, MaxRawBytes: int64(len(raw))}
	tx, err := db.BeginTx(t.Context(), &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := compilerInputArchiveReadCheckManifest(t.Context(), tx, backend, options)
	if err != nil || !reflect.DeepEqual(got, manifest) {
		t.Fatal("read-only receipt lookup changed the exact immutable manifest", got, err)
	}
	options.MaxRawBytes--
	if _, err = compilerInputArchiveReadCheckManifest(t.Context(), tx, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal("explicit raw-byte ceiling was ignored", err)
	}
	options.MaxRawBytes++
	if _, err = compilerInputArchiveReadCheckManifest(t.Context(), tx, "sha256:"+strings.Repeat("c", 64), options); !errors.Is(err, ErrConflict) {
		t.Fatal("different archive backend was accepted", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	reader := &archiveReadCheckObjects{archiveTestObjects: objects}
	report, err := compilerInputArchiveReadCheckBody(t.Context(), reader, got, CompilerInputArchiveReadCheckReport{ReadOnly: true})
	if err != nil || !report.Verified || report.RetirementAuthorized || report.RawBytes != int64(len(raw)) || report.Segments != len(got.Segments) || report.StoredBytes < 1 {
		t.Fatal("full native reconstruction was not verified without destructive authority", report, err)
	}
	// The native reader must reject both stored corruption and an incorrect full
	// digest even if every individual segment is otherwise self-consistent.
	wrongDigest := got
	wrongDigest.Digest = "sha256:" + strings.Repeat("d", 64)
	if report, err = compilerInputArchiveReadCheckBody(t.Context(), reader, wrongDigest, report); !errors.Is(err, inputarchive.ErrIntegrity) || report.Verified {
		t.Fatal("full original digest was not checked", report, err)
	}
	objects.objects[got.Segments[0].Key][0] ^= 1
	if report, err = compilerInputArchiveReadCheckBody(t.Context(), reader, got, report); !errors.Is(err, inputarchive.ErrIntegrity) || report.Verified {
		t.Fatal("corrupt object was accepted", report, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if report, err = compilerInputArchiveReadCheckBody(canceled, reader, got, report); !errors.Is(err, context.Canceled) || report.Verified {
		t.Fatal("canceled read was exposed as a verified result", report, err)
	}
	var chunks, leases int
	if err = db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_input_chunks),(SELECT count(*) FROM fugue_platform_input_archive_leases)`).Scan(&chunks, &leases); err != nil || chunks != 0 || leases != 0 {
		t.Fatal("read check required source chunks or changed migration leases", chunks, leases, err)
	}
	if _, err = db.Exec(`UPDATE fugue_platform_input_archives SET state='uploading',verified_digest='',verified_at=NULL WHERE content_hash=$1`, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = compilerInputArchiveReadCheckManifest(t.Context(), db, backend, options); !errors.Is(err, ErrConflict) {
		t.Fatal("unverified receipt was accepted", err)
	}
}
