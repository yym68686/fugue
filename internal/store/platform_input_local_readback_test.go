package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLocalInputReadbackRequiresExactBoundBeforeDatabaseAccess(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, options := range []CompilerInputLocalReadbackOptions{
		{Digest: digest}, {Digest: "invalid", MaxRawBytes: 1},
		{Digest: digest, MaxRawBytes: -1}, {Digest: digest, MaxRawBytes: inputMaxBytes + 1},
	} {
		report, err := RunCompilerInputLocalReadback(t.Context(), "invalid", options)
		if !errors.Is(err, ErrInvalidInput) || report.Verified || report.RetirementAuthorized || !report.ReadOnly {
			t.Fatal("invalid local readback intent reached the database or granted authority", report, err)
		}
	}
	// An old source is not an independent hot copy, even if it still has every
	// byte locally. A nil queryer proves this refusal precedes any body access.
	m := inputManifest{Digest: digest, Format: inputFormatV1, Location: "legacy", Size: 1, Count: 1, ChunkSize: inputChunkBytes, Created: time.Now()}
	if _, err := verifyCompilerInputLocalRepresentation(t.Context(), nil, m, 1); !errors.Is(err, ErrConflict) {
		t.Fatal("legacy source was accepted as independent hot storage", err)
	}
}

func TestLocalInputReadbackPostgresStreamsBeyond128MiBAndRejectsCorruption(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	const chunks = 129 * 1024 * 1024 / inputChunkBytes
	block := bytes.Repeat([]byte("a"), inputChunkBytes)
	first, last := bytes.Clone(block), bytes.Clone(block)
	copy(first, []byte(`{"facts":{"payload":"`))
	copy(last[len(last)-3:], []byte(`"}}`))
	hash := sha256.New()
	for i := 0; i < chunks; i++ {
		value := block
		if i == 0 {
			value = first
		} else if i == chunks-1 {
			value = last
		}
		hash.Write(value)
	}
	digest := sharedHashBytes(hash.Sum(nil))
	size := int64(chunks * inputChunkBytes)
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_manifests(content_hash,format,storage_location,size_bytes,chunk_size,chunk_count,created_at) VALUES($1,$2,'hot',$3,$4,$5,now())`, digest, inputFormatV1, size, inputChunkBytes, chunks); err != nil {
		t.Fatal(err)
	}
	// The fixture itself also uses fixed-size buffers, not a 129MiB byte slice.
	if _, err := db.Exec(`INSERT INTO fugue_platform_input_hot_chunks(content_hash,ordinal,chunk_hash,data)
 SELECT $1,ordinal,'sha256:'||encode(sha256(data),'hex'),data FROM (
 SELECT ordinal,CASE WHEN ordinal=0 THEN $3::bytea WHEN ordinal=$2-1 THEN $4::bytea ELSE $5::bytea END AS data
 FROM generate_series(0,$2::int-1) ordinal) chunks`, digest, chunks, first, last, block); err != nil {
		t.Fatal(err)
	}
	options := CompilerInputLocalReadbackOptions{Digest: digest, MaxRawBytes: size}
	report, err := RunCompilerInputLocalReadback(t.Context(), address, options)
	if err != nil || !report.Verified || report.RetirementAuthorized || report.ReadBytes != size || report.RawBytes != size || report.Location != "hot" || report.Snapshot == "" || report.ObservedAt.IsZero() {
		t.Fatal("bounded >128MiB local readback was not verified", report, err)
	}
	options.MaxRawBytes--
	if report, err = RunCompilerInputLocalReadback(t.Context(), address, options); !errors.Is(err, ErrConflict) || report.Verified || report.ReadBytes != 0 {
		t.Fatal("raw-byte budget failed to reject before body access", report, err)
	}
	options.MaxRawBytes++
	// A matching per-chunk checksum is insufficient: the original full digest
	// must still fail. No alternate source or R2 configuration exists here.
	if _, err = db.Exec(`UPDATE fugue_platform_input_hot_chunks SET data=set_byte(data,20,98),chunk_hash='sha256:'||encode(sha256(set_byte(data,20,98)),'hex') WHERE content_hash=$1 AND ordinal=$2`, digest, chunks-1); err != nil {
		t.Fatal(err)
	}
	if report, err = RunCompilerInputLocalReadback(t.Context(), address, options); !errors.Is(err, ErrConflict) || report.Verified || report.RetirementAuthorized {
		t.Fatal("self-consistent chunk corruption was accepted", report, err)
	}
	if _, err = db.Exec(`DELETE FROM fugue_platform_input_hot_chunks WHERE content_hash=$1 AND ordinal=$2`, digest, chunks-1); err != nil {
		t.Fatal(err)
	}
	if report, err = RunCompilerInputLocalReadback(t.Context(), address, options); !errors.Is(err, ErrConflict) || report.Verified {
		t.Fatal("missing hot chunk was accepted or fell back", report, err)
	}
	var archiveRows int
	if err = db.QueryRow(`SELECT count(*) FROM fugue_platform_input_archives`).Scan(&archiveRows); err != nil || archiveRows != 0 {
		t.Fatal("local readback changed archive state", archiveRows, err)
	}
}

func TestLocalInputReadbackPostgresBarrierCancellationAndLegacyRefusal(t *testing.T) {
	db, address, _ := hotInventoryTestDatabase(t)
	digest := seedHotInventoryHistory(t, db)
	options := CompilerInputLocalReadbackOptions{Digest: digest, MaxRawBytes: 1024}
	if report, err := RunCompilerInputLocalReadback(t.Context(), address, options); !errors.Is(err, ErrConflict) || report.Verified || report.ReadBytes != 0 || report.Location != "legacy" {
		t.Fatal("old chunks became an independent-hot proof", report, err)
	}
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(t.Context(), `SELECT pg_advisory_lock($1)`, compilerInputArchiveReadLeaseLockID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	if report, err := RunCompilerInputLocalReadback(ctx, address, options); !errors.Is(err, context.DeadlineExceeded) || report.Verified || report.Snapshot != "" {
		t.Fatal("local readback took a snapshot before the reader barrier", report, err)
	}
	if _, err = conn.ExecContext(t.Context(), `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID); err != nil {
		t.Fatal(err)
	}
	// Missing manifests do not silently turn into an inline or R2 read, and the
	// shared lease must be released even on an error.
	options.Digest = "sha256:" + strings.Repeat("d", 64)
	if report, err := RunCompilerInputLocalReadback(t.Context(), address, options); !errors.Is(err, sql.ErrNoRows) || report.Verified {
		t.Fatal("missing local manifest was silently replaced", report, err)
	}
	var acquired bool
	if err = conn.QueryRowContext(t.Context(), `SELECT pg_try_advisory_lock($1)`, compilerInputArchiveReadLeaseLockID).Scan(&acquired); err != nil || !acquired {
		t.Fatal("failed local readback leaked the session reader barrier", acquired, err)
	}
	if _, err = conn.ExecContext(t.Context(), `SELECT pg_advisory_unlock($1)`, compilerInputArchiveReadLeaseLockID); err != nil {
		t.Fatal(err)
	}
}
