package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/platformconfig"
	"fugue/internal/schemamigrate"
)

func TestSharedInputBlocksAndRecipeBounds(t *testing.T) {
	random := make([]byte, inputChunkBytes)
	rand.New(rand.NewSource(37)).Read(random)
	for _, raw := range [][]byte{[]byte(`"x"`), []byte(fmt.Sprintf("%q", random[:60000])), []byte(fmt.Sprintf("%q", bytes.Repeat([]byte("repeatable-proof-"), 15000)))} {
		var refs []sharedRecipeRef
		var rebuilt []byte
		if err := sharedBlocks(raw, func(b []byte) error {
			if len(b) > sharedBlockMax {
				t.Fatal("unbounded block")
			}
			refs = append(refs, sharedRecipeRef{ID: int64(len(refs) + 1), Size: len(b)})
			rebuilt = append(rebuilt, b...)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, rebuilt) {
			t.Fatal("chunking changed bytes")
		}
		recipe, err := encodeSharedRecipe(refs, len(raw))
		if err != nil {
			t.Fatal(err)
		}
		if len(recipe) > 49181 {
			t.Fatal("unbounded recipe")
		}
		got, err := decodeSharedRecipe(recipe, len(raw))
		if err != nil || len(got) != len(refs) {
			t.Fatal(err)
		}
		for _, bad := range [][]byte{nil, recipe[:len(recipe)-1], append(append([]byte(nil), recipe...), 0)} {
			if _, err := decodeSharedRecipe(bad, len(raw)); err == nil {
				t.Fatal("invalid recipe accepted")
			}
		}
		if _, err := decodeSharedRecipe(recipe, len(raw)+1); err == nil {
			t.Fatal("wrong decoded length accepted")
		}
	}
}

func TestSharedInputPolicyFailsClosed(t *testing.T) {
	s := &Store{}
	path := filepath.Join(t.TempDir(), "shared.json")
	for _, item := range []struct {
		raw     string
		enabled bool
	}{{`{"version":1,"enabled":true}`, true}, {`{"version":1,"enabled":false}`, false}, {`{"version":1,"enabled":true,"unexpected":1}`, false}, {`{"version":2,"enabled":true}`, false}, {`{`, false}} {
		if err := os.WriteFile(path, []byte(item.raw), 0600); err != nil {
			t.Fatal(err)
		}
		s.loadCompilerInputSharedPolicy(path)
		if s.compilerInputSharedEnabled() != item.enabled {
			t.Fatal("policy failed open")
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	s.loadCompilerInputSharedPolicy(path)
	if s.compilerInputSharedEnabled() {
		t.Fatal("missing policy enabled writes")
	}
}

func sharedInputTestStore(t testing.TB) *Store {
	s := boundedContentTestStore(t)
	if err := schemamigrate.MigrateCompilerInputCodec(t.Context(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	if err := schemamigrate.MigrateCompilerInputShared(t.Context(), s.databaseURL); err != nil {
		t.Fatal(err)
	}
	s.storagePolicy.shared = compilerInputCodecPolicy{Version: 1, Enabled: true}
	return s
}
func sharedSnapshot(t testing.TB) platformconfig.RuntimeSnapshot {
	t.Helper()
	s := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("shared-%d", time.Now().UnixNano()), PolicyGeneration: "policy"}
	// Neutral service fixtures repeat byte-identical measurements while each
	// executor retains its own identity, capture time and DNS binding.
	items := []map[string]any{}
	for i := 0; i < 180; i++ {
		items = append(items, map[string]any{"id": fmt.Sprintf("sample-%d", i), "observed_at": "2026-01-01T00:00:00Z", "hostname": "service.example.test", "payload": strings.Repeat(fmt.Sprintf("proof-%d-", i%11), 70)})
	}
	for i := 0; i < 24; i++ {
		evidence, _ := json.Marshal(map[string]any{"snapshot": map[string]any{"network_samples": items, "actual_dns_receipt": map[string]any{"node_id": fmt.Sprintf("dns-%d", i), "captured_at": time.Now().UTC()}}})
		s.DNSSelections = append(s.DNSSelections, platformconfig.DNSSelectionObservation{NodeID: fmt.Sprintf("dns-%d", i), Hostname: "service.example.test", Type: "A", PhysicalEvidence: evidence})
	}
	return s
}
func TestSharedInputPostgresRoundtripReuseAndCompatibility(t *testing.T) {
	s := sharedInputTestStore(t)
	snapshot := sharedSnapshot(t)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil || m.Format != inputFormatSharedV3 {
		t.Fatal(m, err)
	}
	readback, err := RunCompilerInputLocalReadback(t.Context(), s.databaseURL, CompilerInputLocalReadbackOptions{Digest: digest, MaxRawBytes: inputMaxBytes})
	if err != nil || !readback.Verified || readback.RetirementAuthorized || readback.Format != inputFormatSharedV3 {
		t.Fatal("shared local full-stream readback did not verify", readback, err)
	}
	var objects, stored int64
	if err = s.db.QueryRow(`SELECT (SELECT count(*) FROM fugue_platform_input_shared_objects WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_refs WHERE content_hash=$1)),sum(octet_length(data)) FROM fugue_platform_input_shared_packs WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_refs WHERE content_hash=$1)`, digest).Scan(&objects, &stored); err != nil {
		t.Fatal(err)
	}
	t.Logf("logical=%d stored object bytes=%d shared objects=%d", m.Size, stored, objects)
	if stored >= m.Size/3 {
		t.Fatal("repeated proof not shared")
	}
	reader := &Store{databaseURL: s.databaseURL, db: s.db, dbReady: true}
	got, err := reader.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := platformconfig.EncodeRuntimeSnapshot(got)
	want, _ := platformconfig.EncodeRuntimeSnapshot(snapshot)
	if !bytes.Equal(raw, want) {
		t.Fatal("canonical bytes changed")
	}
	content, err := reader.GetPlatformArtifactContent(digest)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := platformconfig.RuntimeSnapshotContentDigest(content.Content)
	if err != nil || hash != digest {
		t.Fatal("content API changed", err)
	}
	selected, err := reader.GetPlatformCompilerInputDNSSelections(digest, func(f CompilerDNSSelectionSummary) bool { return f.NodeID == "dns-2" })
	if err != nil || len(selected.DNSSelections) != 1 || len(selected.Omitted) != 23 {
		t.Fatal("selective compatibility", err)
	}
	if reader.compilerDNSInputs.get(digest) == nil {
		t.Fatal("shared selective index absent")
	}
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	changed := snapshot
	changed.IntentGeneration += "-next"
	next, _ := platformconfig.RuntimeSnapshotDigest(changed)
	if err = s.EnsurePlatformCompilerInput(changed, next); err != nil {
		t.Fatal(err)
	}
	var shared int
	if err = s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_shared_refs a JOIN fugue_platform_input_shared_refs b USING(pack_id) WHERE a.content_hash=$1 AND b.content_hash=$2`, digest, next).Scan(&shared); err != nil || shared < 1 {
		t.Fatal("no cross-snapshot reuse", shared, err)
	}
	// Disabling writers is not disabling readers or reverting old program code.
	s.storagePolicy.shared = compilerInputCodecPolicy{}
	if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
}
func TestSharedInputPostgresCorruptionAndAtomicRollback(t *testing.T) {
	s := sharedInputTestStore(t)
	for _, mode := range []string{"object", "object_and_claim", "missing_object", "recipe", "recipe_and_claim", "missing_recipe", "manifest"} {
		t.Run(mode, func(t *testing.T) {
			snapshot := sharedSnapshot(t)
			digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Fatal(err)
			}
			tx, err := s.db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			// Use an isolated transaction to restore shared objects after corruption.
			var key []byte
			if err = tx.QueryRow(`SELECT object_hash FROM fugue_platform_input_shared_objects WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_refs WHERE content_hash=$1) LIMIT 1`, digest).Scan(&key); err != nil {
				t.Fatal(err)
			}
			sqlText := `UPDATE fugue_platform_input_shared_packs SET data=set_byte(data,5,(get_byte(data,5)+1)%256) WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_objects WHERE object_hash=$1)`
			args := []any{key}
			switch mode {
			case "object_and_claim":
				sqlText = `UPDATE fugue_platform_input_shared_packs SET data=set_byte(data,5,(get_byte(data,5)+1)%256),stored_hash=sha256(set_byte(data,5,(get_byte(data,5)+1)%256)) WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_objects WHERE object_hash=$1)`
			case "missing_object":
				sqlText = `DELETE FROM fugue_platform_input_shared_refs WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_objects WHERE object_hash=$1)`
				if _, err = tx.Exec(sqlText, key); err != nil {
					t.Fatal(err)
				}
				sqlText = `DELETE FROM fugue_platform_input_shared_objects WHERE object_hash=$1`
			case "recipe":
				sqlText = `UPDATE fugue_platform_input_shared_recipes SET recipe=set_byte(recipe,9,(get_byte(recipe,9)+1)%256) WHERE content_hash=$1 AND ordinal=0`
				args = []any{digest}
			case "recipe_and_claim":
				sqlText = `UPDATE fugue_platform_input_shared_recipes SET recipe=set_byte(recipe,9,(get_byte(recipe,9)+1)%256),recipe_hash='sha256:'||encode(sha256(set_byte(recipe,9,(get_byte(recipe,9)+1)%256)),'hex') WHERE content_hash=$1 AND ordinal=0`
				args = []any{digest}
			case "missing_recipe":
				sqlText = `DELETE FROM fugue_platform_input_shared_recipes WHERE content_hash=$1 AND ordinal=0`
				args = []any{digest}
			case "manifest":
				sqlText = `UPDATE fugue_platform_input_manifests SET size_bytes=size_bytes-1 WHERE content_hash=$1`
				args = []any{digest}
			}
			if _, err = tx.Exec(sqlText, args...); err != nil {
				t.Fatal(err)
			}
			m, err := readInputManifest(t.Context(), tx, digest)
			if err == nil {
				var r *sharedInputReader
				r, err = openSharedInput(t.Context(), tx, m)
				if err == nil {
					_, err = io.Copy(io.Discard, r)
				}
			}
			if err == nil {
				t.Fatal("corruption accepted")
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
		})
	}
	snapshot := sharedSnapshot(t)
	wrong := "sha256:" + strings.Repeat("f", 64)
	if err := s.EnsurePlatformCompilerInput(snapshot, wrong); err == nil {
		t.Fatal("wrong digest accepted")
	}
	var leaks int
	if err := s.db.QueryRow(`SELECT count(*) FROM fugue_platform_input_shared_recipes WHERE content_hash=$1`, wrong).Scan(&leaks); err != nil || leaks != 0 {
		t.Fatal("failed write leaked", leaks, err)
	}
}

func TestSharedInputWarmOmissionVerifiesPhysicalBytes(t *testing.T) {
	s := sharedInputTestStore(t)
	snapshot := sharedSnapshot(t)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err := s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetPlatformCompilerInput(digest); err != nil {
		t.Fatal(err)
	}
	c := s.compilerDNSInputs.get(digest)
	if c == nil {
		t.Fatal("missing cache")
	}
	for _, mutation := range []string{
		`UPDATE fugue_platform_input_shared_packs SET data=set_byte(data,5,(get_byte(data,5)+1)%256) WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_refs WHERE content_hash=$1)`,
		`UPDATE fugue_platform_input_shared_packs SET data=set_byte(data,5,(get_byte(data,5)+1)%256),stored_hash=sha256(set_byte(data,5,(get_byte(data,5)+1)%256)) WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_refs WHERE content_hash=$1)`,
		`UPDATE fugue_platform_input_shared_objects SET object_hash=sha256(object_hash) WHERE pack_id IN(SELECT pack_id FROM fugue_platform_input_shared_refs WHERE content_hash=$1)`,
		`DELETE FROM fugue_platform_input_shared_refs WHERE content_hash=$1`,
	} {
		tx, err := s.db.BeginTx(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(mutation, digest); err != nil {
			tx.Rollback()
			t.Fatal(err)
		}
		if _, err = readSharedDNSSelections(t.Context(), tx, c.Manifest, c, func(CompilerDNSSelectionSummary) bool { return false }); err == nil {
			tx.Rollback()
			t.Fatal("omitted corruption accepted")
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSharedInputSplitterAcrossBoundaries(t *testing.T) {
	raw := []byte(`{"records":[{"v":"escaped \\\" [ ] }"},{"v":[{},{}]}]}`)
	for step := 1; step < 20; step++ {
		var s sharedSplitter
		var got []byte
		for start := 0; start < len(raw); start += step {
			if err := s.blocks(raw[start:min(start+step, len(raw))], func(b []byte) error { got = append(got, b...); return nil }); err != nil {
				t.Fatal(err)
			}
		}
		if !bytes.Equal(got, raw) || s.quoted || len(s.stack) != 0 {
			t.Fatal("splitter state lost")
		}
	}
	for _, raw := range []string{"}", "[}", strings.Repeat("[", 10001)} {
		var s sharedSplitter
		if s.blocks([]byte(raw), func([]byte) error { return nil }) == nil {
			t.Fatal("invalid nesting accepted")
		}
	}
}

func TestSharedInputMetadataBudgetAtomicFallback(t *testing.T) {
	s := sharedInputTestStore(t)
	s.storagePolicy.codec = compilerInputCodecPolicy{Version: 1, Enabled: true}
	// Exceed the object budget without a giant fixture. Reuse a writer whose
	// inventory is already at its fixed cap, then exercise the database
	// savepoint/fallback path with a trigger returning the exact quota sentinel
	// through an injected writer helper is deliberately avoided: test the
	// splitter boundary directly and transaction rollback independently.
	w := &sharedInputWriter{hash: sha256.New(), objects: map[[32]byte]sharedObject{}, seen: map[[32]byte]bool{}}
	for i := 0; i < sharedObjectLimit; i++ {
		w.seen[sha256.Sum256([]byte(fmt.Sprint(i)))] = true
	}
	if _, err := w.Write([]byte(strings.Repeat("x", inputChunkBytes))); err != errSharedInputBudget {
		t.Fatal("metadata budget ignored", err)
	}
	tx, err := s.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.Exec(`SAVEPOINT shared_input_budget`); err != nil {
		t.Fatal(err)
	}
	snapshot := sharedSnapshot(t)
	digest, _ := platformconfig.RuntimeSnapshotDigest(snapshot)
	if err = s.pgWriteSharedInput(t.Context(), tx, snapshot, digest); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`ROLLBACK TO SAVEPOINT shared_input_budget`); err != nil {
		t.Fatal(err)
	}
	var exists bool
	if err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM fugue_platform_input_manifests WHERE content_hash=$1)`, digest).Scan(&exists); err != nil || exists {
		t.Fatal("partial shared input remained", err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	s.storagePolicy.shared.Enabled = false
	if err = s.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	m, err := readInputManifest(t.Context(), s.db, digest)
	if err != nil || m.Format != inputFormatCodecV2 {
		t.Fatal("established fallback failed", err)
	}
}

func BenchmarkSharedInputEncode(b *testing.B) {
	snapshot := sharedSnapshot(b)
	raw, _ := platformconfig.EncodeRuntimeSnapshot(snapshot)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoder := inputChunkEncoder{}
		var splitter sharedSplitter
		unique := map[[32]byte]bool{}
		stored := 0
		for offset := 0; offset < len(raw); offset += inputChunkBytes {
			part := raw[offset:min(offset+inputChunkBytes, len(raw))]
			err := splitter.blocks(part, func(p []byte) error {
				h := sha256.Sum256(p)
				if !unique[h] {
					data, err := encoder.encode(p)
					if err != nil {
						return err
					}
					stored += len(data)
					unique[h] = true
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(stored), "object-bytes/op")
	}
}
