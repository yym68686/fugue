package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"github.com/DATA-DOG/go-sqlmock"
)

func TestExistingCompilerInputVerifiesStoredBytesWithoutReturningBody(t *testing.T) {
	for _, scenario := range []string{"valid", "changed_bytes", "changed_bytes_and_claim", "wrong_size", "wrong_order", "missing_chunk", "extra_chunk", "row_error", "query_error", "wrong_manifest_size"} {
		t.Run(scenario, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			state := &Store{databaseURL: "postgres://test", db: db, dbReady: true}
			state.storagePolicy.value = StoragePolicy{Version: 1, CompilerInputChunks: true}
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-test", PolicyGeneration: "policy-test", Origins: []platformconfig.OriginObservation{{Ref: "origin-test", RuntimeID: strings.Repeat("payload", 120)}}}
			raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			// Exercise older valid manifests with a smaller chunk size too.
			chunkSize := 256
			count := (len(raw) + chunkSize - 1) / chunkSize
			size := len(raw)
			if scenario == "wrong_manifest_size" {
				size--
			}
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(315609238744290)`)).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT format,size_bytes,chunk_size,chunk_count,created_at FROM fugue_platform_input_manifests WHERE content_hash=$1`)).WithArgs(digest).WillReturnRows(sqlmock.NewRows([]string{"format", "size_bytes", "chunk_size", "chunk_count", "created_at"}).AddRow("runtime-snapshot-json-v1", size, chunkSize, count, time.Now()))
			if scenario != "wrong_manifest_size" {
				q := mock.ExpectQuery(regexp.QuoteMeta(`SELECT ordinal,chunk_hash,octet_length(data),sha256(data) FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal`)).WithArgs(digest)
				if scenario == "query_error" {
					q.WillReturnError(assertInputReadError{})
				} else {
					rows := sqlmock.NewRows([]string{"ordinal", "chunk_hash", "size", "actual_hash"})
					for i, start := 0, 0; start < len(raw); i, start = i+1, start+chunkSize {
						if scenario == "missing_chunk" && i == 1 {
							continue
						}
						b := raw[start:min(start+chunkSize, len(raw))]
						h := sha256.Sum256(b)
						claim := "sha256:" + hex.EncodeToString(h[:])
						ordinal, n := i, len(b)
						if i == 1 {
							switch scenario {
							case "changed_bytes", "changed_bytes_and_claim":
								h[0] ^= 255
								if scenario == "changed_bytes_and_claim" {
									claim = "sha256:" + hex.EncodeToString(h[:])
								}
							case "wrong_size":
								n--
							case "wrong_order":
								ordinal++
							}
						}
						rows.AddRow(ordinal, claim, n, h[:])
					}
					if scenario == "extra_chunk" {
						h := sha256.Sum256([]byte("extra"))
						rows.AddRow(count, "sha256:"+hex.EncodeToString(h[:]), 5, h[:])
					}
					if scenario == "row_error" {
						rows.RowError(1, assertInputReadError{})
					}
					q.WillReturnRows(rows)
				}
			}
			if scenario == "valid" {
				mock.ExpectCommit()
			} else {
				mock.ExpectRollback()
			}
			err = state.EnsurePlatformCompilerInput(snapshot, digest)
			if (err == nil) != (scenario == "valid") {
				t.Fatalf("scenario=%s err=%v", scenario, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type assertInputReadError struct{}

func (assertInputReadError) Error() string { return "synthetic input verification read failure" }

func TestChunkedCompilerInputRechecksExistingBytes(t *testing.T) {
	state := boundedContentTestStore(t)
	for _, mutation := range []string{"data", "data_and_claim", "claim", "size", "missing", "extra", "order"} {
		t.Run(mutation, func(t *testing.T) {
			snapshot, digest := chunkedInputFixture(t)
			if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { state.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest) })
			query := `UPDATE fugue_platform_input_chunks SET data=set_byte(data,0,33) WHERE content_hash=$1 AND ordinal=0`
			switch mutation {
			case "data_and_claim":
				query = `UPDATE fugue_platform_input_chunks SET data=set_byte(data,0,33),chunk_hash='sha256:'||encode(sha256(set_byte(data,0,33)),'hex') WHERE content_hash=$1 AND ordinal=0`
			case "claim":
				query = `UPDATE fugue_platform_input_chunks SET chunk_hash='sha256:'||repeat('0',64) WHERE content_hash=$1 AND ordinal=0`
			case "size":
				query = `UPDATE fugue_platform_input_chunks SET data=substring(data from 2),chunk_hash='sha256:'||encode(sha256(substring(data from 2)),'hex') WHERE content_hash=$1 AND ordinal=0`
			case "missing":
				query = `DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal=0`
			case "extra":
				query = `INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) SELECT content_hash,99999,chunk_hash,data FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal=0`
			case "order":
				query = `UPDATE fugue_platform_input_chunks SET ordinal=99999 WHERE content_hash=$1 AND ordinal=0`
			}
			if _, err := state.db.Exec(query, digest); err != nil {
				t.Fatal(err)
			}
			if err := state.EnsurePlatformCompilerInput(snapshot, digest); err == nil {
				t.Fatal("existing stored corruption was accepted")
			}
		})
	}
}

func BenchmarkCompilerInputStoredVerification(b *testing.B) {
	state := boundedContentTestStore(b)
	snapshot, digest := chunkedInputFixture(b)
	if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { state.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest) })
	ctx := context.Background()
	m, err := readInputManifest(ctx, state.db, digest)
	if err != nil {
		b.Fatal(err)
	}
	w := &inputChunkWriter{digest: digest, hash: sha256.New(), chunk: make([]byte, 0, m.ChunkSize)}
	if err := platformconfig.WriteRuntimeSnapshot(w, snapshot); err != nil {
		b.Fatal(err)
	}
	if err := w.flush(); err != nil {
		b.Fatal(err)
	}
	for _, mode := range []string{"full_body", "actual_hashes"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if mode == "actual_hashes" {
					if err := verifyInputChunkHashes(ctx, state.db, m, w.expected); err != nil {
						b.Fatal(err)
					}
				} else {
					r, err := openInputChunks(ctx, state.db, m)
					if err != nil {
						b.Fatal(err)
					}
					_, err = io.Copy(io.Discard, r)
					r.rows.Close()
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			payload := m.Size + int64(m.Count*(4+71))
			if mode == "actual_hashes" {
				payload = int64(m.Count * (4 + 71 + 4 + 32))
			}
			b.ReportMetric(float64(payload), "column_bytes/op")
		})
	}
	b.Log(fmt.Sprintf("verified %d bytes in %d chunks", m.Size, m.Count))
}

func TestCompilerInputSmallChunkManifestRetainsBoundedReadback(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			state := &Store{databaseURL: "postgres://test", db: db, dbReady: true}
			state.storagePolicy.value = StoragePolicy{Version: 1, CompilerInputChunks: true}
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: strings.Repeat("x", 5000)}
			raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			mock.ExpectBegin()
			mock.ExpectExec(regexp.QuoteMeta(`SELECT pg_advisory_xact_lock(315609238744290)`)).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT format,size_bytes,chunk_size,chunk_count,created_at FROM fugue_platform_input_manifests WHERE content_hash=$1`)).WithArgs(digest).WillReturnRows(sqlmock.NewRows([]string{"format", "size_bytes", "chunk_size", "chunk_count", "created_at"}).AddRow("runtime-snapshot-json-v1", len(raw), 1, len(raw), time.Now()))
			rows := sqlmock.NewRows([]string{"ordinal", "chunk_hash", "data"})
			for i, v := range raw {
				if corrupt && i == 0 {
					v = '!'
				}
				b := []byte{v}
				h := sha256.Sum256(b)
				rows.AddRow(i, "sha256:"+hex.EncodeToString(h[:]), b)
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT ordinal,chunk_hash,data FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal`)).WithArgs(digest).WillReturnRows(rows)
			if corrupt {
				mock.ExpectRollback()
			} else {
				mock.ExpectCommit()
			}
			err = state.EnsurePlatformCompilerInput(snapshot, digest)
			if (err != nil) != corrupt {
				t.Fatalf("corrupt=%v err=%v", corrupt, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
