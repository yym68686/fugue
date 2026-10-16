package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"fugue/internal/model"
	"fugue/internal/platformconfig"
)

func compilerDNSFixture(t testing.TB) (platformconfig.RuntimeSnapshot, string) {
	t.Helper()
	now := time.Now().In(time.FixedZone("test-zone", 3600)).Truncate(time.Millisecond)
	s := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("intent-%d", time.Now().UnixNano()), PolicyGeneration: "policy-a", Facts: map[string]any{"unrelated": strings.Repeat("padding-", 100000)}}
	for i := 0; i < 5; i++ {
		raw, _ := json.Marshal(map[string]any{"payload": strings.Repeat(fmt.Sprintf("evidence-%d<&中文-", i), 23000)})
		s.DNSSelections = append(s.DNSSelections, platformconfig.DNSSelectionObservation{NodeID: "dns-a", Hostname: fmt.Sprintf("service-%d.example.test", i), Type: "A", PhysicalEvidence: raw, PhysicalSelection: &model.DNSPhysicalSelection{CapturedAt: now.Add(time.Duration(i) * time.Second), OrderedEdgeIDs: []string{"edge-a", "edge-b"}}, Candidates: []platformconfig.DNSSelectionCandidate{{EdgeID: "edge-a", IP: "8.8.8.8", ScoreBreakdown: map[string]float64{"quality": 2.5}}}})
	}
	d, err := platformconfig.RuntimeSnapshotDigest(s)
	if err != nil {
		t.Fatal(err)
	}
	return s, d
}
func TestCompilerDNSInputSelectiveMatchesFullAndOwnsResults(t *testing.T) {
	state := boundedContentTestStore(t)
	snapshot, digest := compilerDNSFixture(t)
	if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest) })
	full, err := state.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	for _, cold := range []bool{false, true} {
		if cold {
			state.compilerDNSInputs = compilerDNSInputCache{}
		}
		for _, wanted := range []int{-1, 0, 2, 4, 99} {
			selectFact := func(s CompilerDNSSelectionSummary) bool {
				return wanted == 99 || s.Hostname == fmt.Sprintf("service-%d.example.test", wanted)
			}
			got, err := state.GetPlatformCompilerInputDNSSelections(digest, selectFact)
			if err != nil {
				t.Fatal(err)
			}
			var expected []platformconfig.DNSSelectionObservation
			for _, f := range full.DNSSelections {
				if selectFact(compilerDNSSelectionSummary(f)) {
					expected = append(expected, f)
				}
			}
			a, _ := json.Marshal(got.DNSSelections)
			b, _ := json.Marshal(expected)
			if !bytes.Equal(a, b) || got.IntentGeneration != full.IntentGeneration || got.PolicyGeneration != full.PolicyGeneration || len(got.Omitted) != len(full.DNSSelections)-len(expected) {
				t.Fatalf("selection differs cold=%v wanted=%d", cold, wanted)
			}
			if len(got.DNSSelections) > 0 {
				got.DNSSelections[0].Candidates[0].ScoreBreakdown["quality"] = 999
				got.DNSSelections[0].PhysicalEvidence[0] = '!'
				got.DNSSelections[0].PhysicalSelection.OrderedEdgeIDs[0] = "changed"
			}
		}
	}
	c := state.compilerDNSInputs.get(digest)
	if c == nil {
		t.Fatal("verified input did not warm the index")
	}
	// Force proof that a warm read uses the index even for omitted observations.
	if len(c.Ranges) != 5 || len(c.Hashes) < 5 {
		t.Fatal("index lacks ranges/chunk witnesses")
	}
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := state.GetPlatformCompilerInputDNSSelections(digest, func(f CompilerDNSSelectionSummary) bool { return f.Hostname == "service-2.example.test" }); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestCompilerDNSInputRejectsCorruptUnselectedChunks(t *testing.T) {
	state := boundedContentTestStore(t)
	for _, mutation := range []string{"data", "data_and_claim", "claim", "size", "missing", "extra", "manifest"} {
		t.Run(mutation, func(t *testing.T) {
			snapshot, digest := compilerDNSFixture(t)
			if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { state.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest) })
			c := state.compilerDNSInputs.get(digest)
			if c == nil {
				t.Fatal("index missing")
			}
			last := c.Manifest.Count - 1
			sql := `UPDATE fugue_platform_input_chunks SET data=set_byte(data,0,33) WHERE content_hash=$1 AND ordinal=$2`
			switch mutation {
			case "data_and_claim":
				sql = `UPDATE fugue_platform_input_chunks SET data=set_byte(data,0,33),chunk_hash='sha256:'||encode(sha256(set_byte(data,0,33)),'hex') WHERE content_hash=$1 AND ordinal=$2`
			case "claim":
				sql = `UPDATE fugue_platform_input_chunks SET chunk_hash='sha256:'||repeat('0',64) WHERE content_hash=$1 AND ordinal=$2`
			case "size":
				sql = `UPDATE fugue_platform_input_chunks SET data=substring(data from 2),chunk_hash='sha256:'||encode(sha256(substring(data from 2)),'hex') WHERE content_hash=$1 AND ordinal=$2`
			case "missing":
				sql = `DELETE FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal=$2`
			case "extra":
				sql = `INSERT INTO fugue_platform_input_chunks(content_hash,ordinal,chunk_hash,data) SELECT content_hash,99999,chunk_hash,data FROM fugue_platform_input_chunks WHERE content_hash=$1 AND ordinal=$2`
			case "manifest":
				sql = `UPDATE fugue_platform_input_manifests SET size_bytes=size_bytes-1 WHERE content_hash=$1 AND chunk_count=$2`
				last = c.Manifest.Count
			}
			if _, err := state.db.Exec(sql, digest, last); err != nil {
				t.Fatal(err)
			}
			got, err := state.GetPlatformCompilerInputDNSSelections(digest, func(f CompilerDNSSelectionSummary) bool { return f.Hostname == "service-0.example.test" })
			if err == nil || len(got.DNSSelections) != 0 {
				t.Fatal("corrupt unused input authorized partial result", err)
			}
		})
	}
}
func TestCompilerDNSCacheBounds(t *testing.T) {
	c := compilerDNSInputCache{}
	for i := 0; i < 20; i++ {
		c.put(&compilerDNSInput{Manifest: inputManifest{Digest: fmt.Sprint(i)}, Bytes: 700000})
	}
	if c.bytes > compilerDNSCacheBytes || len(c.entries) > compilerDNSCacheEntries || c.get("0") != nil || c.get("19") == nil {
		t.Fatal("cache bounds or eviction failed")
	}
	before := len(c.entries)
	c.put(&compilerDNSInput{Manifest: inputManifest{Digest: "large"}, Bytes: compilerDNSCacheBytes + 1})
	if len(c.entries) != before {
		t.Fatal("oversized index retained")
	}
}
func TestCompilerDNSInputFileCompatibility(t *testing.T) {
	state := New(t.TempDir() + "/state.json")
	if err := state.Init(); err != nil {
		t.Fatal(err)
	}
	snapshot, digest := compilerDNSFixture(t)
	if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		t.Fatal(err)
	}
	full, err := state.GetPlatformCompilerInput(digest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := state.GetPlatformCompilerInputDNSSelections(digest, nil)
	if err != nil || !reflect.DeepEqual(got.DNSSelections, full.DNSSelections) {
		t.Fatal("legacy file read changed", err)
	}
}

func BenchmarkCompilerDNSInputRestore(b *testing.B) {
	state := boundedContentTestStore(b)
	snapshot, digest := compilerDNSFixture(b)
	if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { state.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest) })
	for _, mode := range []string{"full_snapshot", "no_new_evidence", "one_selection"} {
		b.Run(mode, func(b *testing.B) {
			c := state.compilerDNSInputs.get(digest)
			var bodyBytes int64
			if mode == "full_snapshot" {
				bodyBytes = c.Manifest.Size
			} else if mode == "one_selection" {
				selected := map[int]bool{}
				for _, r := range c.Ranges {
					if r.Summary.Hostname == "service-2.example.test" {
						for i := int(r.Offset / int64(c.Manifest.ChunkSize)); i <= int((r.Offset+r.Length-1)/int64(c.Manifest.ChunkSize)); i++ {
							selected[i] = true
						}
					}
				}
				for i := range selected {
					bodyBytes += min(int64(c.Manifest.ChunkSize), c.Manifest.Size-int64(i)*int64(c.Manifest.ChunkSize))
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				if mode == "full_snapshot" {
					if _, err := state.GetPlatformCompilerInput(digest); err != nil {
						b.Fatal(err)
					}
				} else {
					_, err := state.GetPlatformCompilerInputDNSSelections(digest, func(s CompilerDNSSelectionSummary) bool {
						return mode == "one_selection" && s.Hostname == "service-2.example.test"
					})
					if err != nil {
						b.Fatal(err)
					}
				}
			}
			b.ReportMetric(float64(bodyBytes), "body_bytes/op")
		})
	}
}

func TestCompilerDNSInputInvalidUTF8KeepsCanonicalFallback(t *testing.T) {
	state := boundedContentTestStore(t)
	for _, field := range []string{"intent", "policy", "node"} {
		t.Run(field, func(t *testing.T) {
			snapshot := platformconfig.RuntimeSnapshot{IntentGeneration: fmt.Sprintf("intent-%d", time.Now().UnixNano()), PolicyGeneration: "policy", DNSSelections: []platformconfig.DNSSelectionObservation{{NodeID: "dns-a", Hostname: "service.example.test", Type: "A"}}}
			switch field {
			case "intent":
				snapshot.IntentGeneration += "\xff"
			case "policy":
				snapshot.PolicyGeneration += "\xff"
			case "node":
				snapshot.DNSSelections[0].NodeID += "\xff"
			}
			digest, err := platformconfig.RuntimeSnapshotDigest(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			if err := state.EnsurePlatformCompilerInput(snapshot, digest); err != nil {
				t.Fatal(err)
			}
			if state.compilerDNSInputs.get(digest) != nil {
				t.Fatal("noncanonical original strings indexed")
			}
			full, err := state.GetPlatformCompilerInput(digest)
			if err != nil {
				t.Fatal(err)
			}
			got, err := state.GetPlatformCompilerInputDNSSelections(digest, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got.IntentGeneration != full.IntentGeneration || got.PolicyGeneration != full.PolicyGeneration || !reflect.DeepEqual(got.DNSSelections, full.DNSSelections) {
				t.Fatal("canonical decoded value changed")
			}
			t.Cleanup(func() { state.db.Exec(`DELETE FROM fugue_platform_input_manifests WHERE content_hash=$1`, digest) })
		})
	}
}
