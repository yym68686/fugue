package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

func TestCompilerInputCodecPolicyIndependentAndFailsClosed(t *testing.T) {
	s := &Store{}
	s.storagePolicy.value = StoragePolicy{Version: 1, CompilerInputChunks: true, DatabaseMaxOpen: 24, DatabaseMaxIdle: 8, DatabaseIdleSeconds: 60}
	dir := t.TempDir()
	path := filepath.Join(dir, "compiler-input-codec.json")
	base := s.StoragePolicy()
	for _, item := range []struct {
		raw  string
		want bool
	}{{`{"version":1,"enabled":true}`, true}, {`{"version":1,"enabled":false}`, false}, {`{"version":1,"enabled":true,"unknown":1}`, false}, {`{"version":2,"enabled":true}`, false}, {`{"version":1,"enabled":"true"}`, false}, {``, false}} {
		if err := os.WriteFile(path, []byte(item.raw), 0600); err != nil {
			t.Fatal(err)
		}
		s.loadCompilerInputCodecPolicy(path)
		if s.compilerInputCodecEnabled() != item.want || s.StoragePolicy() != base {
			t.Fatal("independent codec policy changed base policy or failed open")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s.loadCompilerInputCodecPolicy(path)
	if s.compilerInputCodecEnabled() {
		t.Fatal("missing policy enables codec")
	}
}
func TestCompilerInputCodecEncoderBoundsAndFallback(t *testing.T) {
	random := make([]byte, inputChunkBytes-inputCodecHeaderBytes)
	rand.New(rand.NewSource(17)).Read(random)
	e := inputChunkEncoder{}
	for _, raw := range [][]byte{[]byte("x"), bytes.Repeat([]byte("evidence"), 30000), random} {
		got, err := e.encode(raw)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeInputChunk(inputFormatCodecV2, got, len(raw))
		if err != nil || !bytes.Equal(raw, decoded) {
			t.Fatal("codec roundtrip", err)
		}
		if len(got) > inputChunkBytes || len(got) > len(raw)+inputCodecHeaderBytes {
			t.Fatal("stored bounds exceeded")
		}
		if len(raw) == len(random) && got[0] != 0 {
			t.Fatal("incompressible input did not use raw fallback")
		}
	}
	for _, raw := range [][]byte{nil, make([]byte, inputChunkBytes)} {
		if _, err := e.encode(raw); err == nil {
			t.Fatal("invalid logical chunk accepted")
		}
	}
}
func TestCompilerInputCodecProductionWriterPostgres(t *testing.T) {
	s := boundedContentTestStore(t)
	if err := schemamigrate.MigrateCompilerInputCodec(t.Context(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: true}
	snapshot, digest := compilerDNSFixture(t)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.db.Exec(`DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest)
		s.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest)
	})
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil || m.Format != inputFormatCodecV2 {
		t.Fatal("writer did not use codec", m.Format, err)
	}
	var stored int64
	if err = s.db.QueryRow(`SELECT sum(octet_length(data)) FROM fugue_platform_input_chunks WHERE content_hash=$1`, digest).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored >= m.Size/2 {
		t.Fatal("compressible fixture not reduced", stored, m.Size)
	}
	for _, cold := range []bool{false, true} {
		if cold {
			s.compilerDNSInputs = compilerDNSInputCache{}
		}
		got, err := s.GetPlatformCompilerInputDNSSelections(digest, func(f CompilerDNSSelectionSummary) bool { return f.Hostname == "service-2.example.test" })
		if err != nil {
			t.Fatal(err)
		}
		a, _ := json.Marshal(got.DNSSelections)
		b, _ := json.Marshal(snapshot.DNSSelections[2:3])
		if !bytes.Equal(a, b) || len(got.Omitted) != 4 {
			t.Fatal("selective read differs")
		}
	}
	for i := 0; i < 2; i++ {
		if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
			t.Fatal("existing input", err)
		}
	}
	s.storagePolicy.codec.Enabled = false
	got, err := s.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	actual, _ := platformconfig.RuntimeSnapshotDigest(got)
	if actual != digest {
		t.Fatal("disabled writer changed reader")
	}
	snapshot, d2 := chunkedInputFixture(t)
	if err = s.EnsurePlatformCompilerInput(snapshot, d2); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.db.Exec(`DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1`, d2)
		s.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, d2)
	})
	m, err = readInputManifest(t.Context(), s.db, d2)
	if err != nil || m.Format != inputFormatV1 {
		t.Fatal("disabling did not revert future writes")
	}
}
func TestCompilerInputCodecWriterRollsBackChangedStoredBytes(t *testing.T) {
	s := boundedContentTestStore(t)
	if err := schemamigrate.MigrateCompilerInputCodec(t.Context(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: true}
	snapshot, digest := chunkedInputFixture(t)
	name := fmt.Sprintf("codec_corruption_%d", os.Getpid())
	body := fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.content_hash='%s' AND NEW.ordinal=1 THEN NEW.data=set_byte(NEW.data,octet_length(NEW.data)-1,(get_byte(NEW.data,octet_length(NEW.data)-1)+1)%%256);NEW.chunk_hash='sha256:'||encode(sha256(NEW.data),'hex');END IF;RETURN NEW;END $$;CREATE TRIGGER %s BEFORE INSERT ON fugue_platform_input_chunks FOR EACH ROW EXECUTE FUNCTION %s()`, name, digest, name, name)
	if _, err := s.db.Exec(body); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		s.db.Exec("DROP TRIGGER " + name + " ON fugue_platform_input_chunks")
		s.db.Exec("DROP FUNCTION " + name + "()")
	})
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err == nil {
		t.Fatal("stored corruption plus false claim accepted")
	}
	var count int
	if err := s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_input_manifests WHERE content_hash=$1)+(SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash=$1)`, digest).Scan(&count); err != nil || count != 0 {
		t.Fatal("transaction did not roll back", count, err)
	}
}
func BenchmarkCompilerInputCodecEncode(b *testing.B) {
	raw := bytes.Repeat([]byte(`{"evidence":"normalized-signed-network-observation","generation":"abc123"},`), 4000)
	raw = raw[:inputChunkBytes-inputCodecHeaderBytes]
	for _, codec := range []bool{false, true} {
		b.Run(fmt.Sprint(codec), func(b *testing.B) {
			e := inputChunkEncoder{}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				out := raw
				if codec {
					var err error
					out, err = e.encode(raw)
					if err != nil {
						b.Fatal(err)
					}
				}
				sha256.Sum256(out)
			}
		})
	}
}

// Keep v2 storage hashes distinct from logical hashes, including raw fallback.
func TestCompilerInputCodecPhysicalWitness(t *testing.T) {
	e := inputChunkEncoder{}
	raw := []byte(strings.Repeat("payload", 100))
	out, err := e.encode(raw)
	if err != nil {
		t.Fatal(err)
	}
	a := sha256.Sum256(raw)
	b := sha256.Sum256(out)
	if hex.EncodeToString(a[:]) == hex.EncodeToString(b[:]) {
		t.Fatal("physical and logical identities collapsed")
	}
}
