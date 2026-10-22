package store

import (
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestExistingCodecInputRechecksPhysicalBytesWithoutBody(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "corrupt_and_claim"}[corrupt], func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s := &Store{databaseURL: "postgres://test", db: db, dbReady: true}
			s.storagePolicy.value = StoragePolicy{Version: 1, CompilerInputChunks: true}
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "intent", PolicyGeneration: "policy"}
			raw, _ := platformconfig.EncodeRuntimeSnapshot(snapshot)
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			encoded := codecTestChunk(t, raw, false)
			physical := sha256.Sum256(encoded)
			m := inputManifest{Digest: digest, Format: inputFormatCodecV2, Size: int64(len(raw)), ChunkSize: len(raw), Count: 1, Created: time.Now()}
			s.compilerDNSInputs.put(&compilerDNSInput{Manifest: m, Hashes: [][sha256.Size]byte{physical}, StoredSizes: []int{len(encoded)}, Bytes: 512})
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(315609238744290)`)).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT format,size_bytes,chunk_size,chunk_count,created_at FROM fugue_platform_input_manifests WHERE content_hash=$1`)).WithArgs(digest).WillReturnRows(sqlmock.NewRows([]string{"format", "size", "chunk_size", "count", "created"}).AddRow(m.Format, m.Size, m.ChunkSize, m.Count, m.Created))
			if corrupt {
				physical[0] ^= 255
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT ordinal,chunk_hash,octet_length(data),sha256(data) FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal`)).WithArgs(digest).WillReturnRows(sqlmock.NewRows([]string{"ordinal", "declared", "size", "actual"}).AddRow(0, "sha256:"+hex.EncodeToString(physical[:]), len(encoded), physical[:]))
			if corrupt {
				mock.ExpectRollback()
			} else {
				mock.ExpectCommit()
			}
			err = s.EnsurePlatformCompilerInput(snapshot, digest)
			if (err != nil) != corrupt {
				t.Fatal("integrity decision", err)
			}
			if err = mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
