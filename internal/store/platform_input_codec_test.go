package store

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"github.com/DATA-DOG/go-sqlmock"
)

const testCodecFormat = "runtime-snapshot-json-chunk-codec-v2"

// Independent wire fixture; production deliberately has no v2 writer yet.
func codecTestChunk(t testing.TB, raw []byte, compressed bool) []byte {
	t.Helper()
	out := make([]byte, 5)
	binary.BigEndian.PutUint32(out[1:], uint32(len(raw)))
	if !compressed {
		return append(out, raw...)
	}
	out[0] = 1
	var b bytes.Buffer
	z := zlib.NewWriter(&b)
	if _, err := z.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err := z.Close(); err != nil {
		t.Fatal(err)
	}
	return append(out, b.Bytes()...)
}

func TestCompilerInputCodecReaderCompatibility(t *testing.T) {
	snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: "intent-test", PolicyGeneration: "policy-test", Facts: map[string]any{"payload": string(bytes.Repeat([]byte("evidence-"), 1000))}}
	raw, _ := platformconfig.EncodeRuntimeSnapshot(snapshot)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	for _, mode := range []string{"v1", "v2_raw", "v2_zlib", "truncated", "trailing", "declared_length", "expanded_length", "wrong_hash", "wrong_digest", "unknown_codec", "unknown_format"} {
		t.Run(mode, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			state := &Store{databaseURL: "postgres://test", db: db, dbReady: true}
			format := testCodecFormat
			stored := codecTestChunk(t, raw, mode != "v2_raw")
			requested := digest
			switch mode {
			case "v1":
				format = "runtime-snapshot-json-v1"
				stored = raw
			case "truncated":
				stored = stored[:len(stored)-1]
			case "trailing":
				stored = append(stored, 0)
			case "declared_length":
				binary.BigEndian.PutUint32(stored[1:], uint32(len(raw)+1))
			case "expanded_length":
				stored = codecTestChunk(t, append(append([]byte(nil), raw...), ' '), true)
				binary.BigEndian.PutUint32(stored[1:], uint32(len(raw)))
			case "wrong_digest":
				requested = "sha256:" + fmt.Sprintf("%064x", 1)
			case "unknown_codec":
				stored[0] = 2
			case "unknown_format":
				format = "runtime-snapshot-unknown"
			}
			mock.ExpectQuery(regexp.QuoteMeta(`SELECT format,size_bytes,chunk_size,chunk_count,created_at FROM fugue_platform_input_manifests WHERE content_hash=$1`)).WithArgs(requested).WillReturnRows(sqlmock.NewRows([]string{"format", "size", "chunk_size", "count", "created"}).AddRow(format, len(raw), len(raw), 1, time.Now()))
			if mode != "unknown_format" {
				h := sha256.Sum256(stored)
				declared := "sha256:" + hex.EncodeToString(h[:])
				if mode == "wrong_hash" {
					declared = digest
				}
				mock.ExpectQuery(regexp.QuoteMeta(`SELECT ordinal,chunk_hash,data FROM fugue_platform_input_chunks WHERE content_hash=$1 ORDER BY ordinal`)).WithArgs(requested).WillReturnRows(sqlmock.NewRows([]string{"ordinal", "hash", "data"}).AddRow(0, declared, stored))
			}
			got, err := state.GetPlatformCompilerInput(requested)
			valid := mode == "v1" || mode == "v2_raw" || mode == "v2_zlib"
			if (err == nil) != valid {
				t.Fatalf("valid=%v err=%v", valid, err)
			}
			if valid {
				actual, _ := platformconfig.RuntimeSnapshotDigest(got)
				if actual != digest {
					t.Fatal("canonical digest changed")
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func codecTestStore(t testing.TB) *Store {
	t.Helper()
	s := boundedContentTestStore(t)
	// Session-local prospective schema. No production schema/policy is changed.
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1)
	_, err := s.db.Exec(`CREATE TEMP TABLE fugue_platform_input_manifests (LIKE public.fugue_platform_input_manifests INCLUDING DEFAULTS); CREATE TEMP TABLE fugue_platform_input_chunks (LIKE public.fugue_platform_input_chunks INCLUDING DEFAULTS)`)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func insertCodecFixture(t testing.TB, s *Store, snapshot platformconfig.RuntimeSnapshot, digest string) {
	t.Helper()
	raw, err := platformconfig.EncodeRuntimeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	size := inputChunkBytes - 5
	count := (len(raw) + size - 1) / size
	if _, err = s.db.Exec(`INSERT INTO fugue_platform_input_manifests VALUES($1,$2,$3,$4,$5,$6)`, digest, testCodecFormat, len(raw), size, count, time.Now()); err != nil {
		t.Fatal(err)
	}
	for n, start := 0, 0; start < len(raw); n, start = n+1, start+size {
		encoded := codecTestChunk(t, raw[start:min(start+size, len(raw))], n%2 == 0)
		h := sha256.Sum256(encoded)
		if _, err = s.db.Exec(`INSERT INTO fugue_platform_input_chunks VALUES($1,$2,$3,$4)`, digest, n, "sha256:"+hex.EncodeToString(h[:]), encoded); err != nil {
			t.Fatal(err)
		}
	}
}
func TestCompilerInputCodecPostgresReadersAndRecheck(t *testing.T) {
	s := codecTestStore(t)
	snapshot, digest := compilerDNSFixture(t)
	insertCodecFixture(t, s, snapshot, digest)
	for _, read := range []string{"full", "content", "selective_cold", "selective_warm", "ensure_existing"} {
		t.Run(read, func(t *testing.T) {
			switch read {
			case "full":
				got, err := s.GetPlatformCompilerInput(digest)
				if err != nil {
					t.Fatal(err)
				}
				actual, _ := platformconfig.RuntimeSnapshotDigest(got)
				if actual != digest {
					t.Fatal("digest changed")
				}
			case "content":
				got, err := s.GetPlatformArtifactContent(digest)
				if err != nil {
					t.Fatal(err)
				}
				actual, _ := platformconfig.RuntimeSnapshotContentDigest(got.Content)
				if actual != digest {
					t.Fatal("content digest changed")
				}
			case "ensure_existing":
				if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
					t.Fatal(err)
				}
			default:
				if read == "selective_cold" {
					s.compilerDNSInputs = compilerDNSInputCache{}
				}
				got, err := s.GetPlatformCompilerInputDNSSelections(digest, func(f CompilerDNSSelectionSummary) bool { return f.Hostname == "service-2.example.test" })
				if err != nil {
					t.Fatal(err)
				}
				a, _ := json.Marshal(got.DNSSelections)
				b, _ := json.Marshal(snapshot.DNSSelections[2:3])
				if !bytes.Equal(a, b) || len(got.Omitted) != 4 {
					t.Fatal("selection changed")
				}
				if s.compilerDNSInputs.get(digest) == nil {
					t.Fatal("verified compressed index not warmed")
				}
			}
		})
	}
	for _, mutation := range []string{"data_and_claim", "missing", "extra", "size"} {
		t.Run(mutation, func(t *testing.T) {
			snapshot, digest := compilerDNSFixture(t)
			insertCodecFixture(t, s, snapshot, digest)
			if _, err := s.GetPlatformCompilerInputDNSSelections(digest, nil); err != nil {
				t.Fatal(err)
			}
			sql := `UPDATE fugue_platform_input_chunks SET data=set_byte(data,octet_length(data)-1,42),chunk_hash='sha256:'||encode(sha256(set_byte(data,octet_length(data)-1,42)),'hex') WHERE content_hash=$1 AND ordinal=(SELECT max(ordinal) FROM fugue_platform_input_chunks WHERE content_hash=$1)`
			if mutation == "missing" {
				sql = `DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal=1`
			}
			if mutation == "extra" {
				sql = `INSERT INTO fugue_platform_input_chunks SELECT content_hash,99999,chunk_hash,data FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal=0`
			}
			if mutation == "size" {
				sql = `UPDATE fugue_platform_input_chunks SET data=substring(data from 2) WHERE content_hash=$1 AND ordinal=0`
			}
			if _, err := s.db.Exec(sql, digest); err != nil {
				t.Fatal(err)
			}
			got, err := s.GetPlatformCompilerInputDNSSelections(digest, func(f CompilerDNSSelectionSummary) bool { return f.Hostname == "service-0.example.test" })
			if err == nil || len(got.DNSSelections) != 0 {
				t.Fatal("corruption authorized partial output")
			}
			if err := s.EnsurePlatformCompilerInput(snapshot, digest); err == nil {
				t.Fatal("existing corrupted input accepted")
			}
		})
	}
	// Reader rollout does not enable compressed production writes.
	snapshot, digest = compilerDNSFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	var format string
	if err := s.db.QueryRow(`SELECT format FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest).Scan(&format); err != nil {
		t.Fatal(err)
	}
	if format != "runtime-snapshot-json-v1" {
		t.Fatal("writer activated before policy")
	}
	// Direct streaming reader checks EOF and decoded digest too.
	m, err := readInputManifest(context.Background(), s.db, digest)
	if err != nil {
		t.Fatal(err)
	}
	r, err := openInputChunks(context.Background(), s.db, m)
	if err != nil {
		t.Fatal(err)
	}
	defer r.rows.Close()
	if _, err = io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
}
