package dnsserver

import (
	"encoding/json"
	"fmt"
	"fugue/internal/cellpublication"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformsafety"
	"fugue/internal/testfixture/celldns"
	"reflect"
	"runtime"
	"testing"
)

func TestDNSContentCompactionPreservesExactEncoding(t *testing.T) {
	for _, raw := range []string{`{"a":{"x":[1,true,null,"<&>"]},"b":{"x":[1,true,null,"<&>"]},"c":{"x":[2,true,null,"<&>"]}}`, `{"empty":{},"empty2":{},"null":null,"array":[],"array2":[]}`, `{"n":0,"negative":-0,"s":"0"}`} {
		var content map[string]any
		if err := json.Unmarshal([]byte(raw), &content); err != nil {
			t.Fatal(err)
		}
		a := model.PlatformArtifact{ID: "identity", Content: content, Metadata: map[string]string{"signature": "unchanged"}}
		before, _ := json.Marshal(a)
		compactDNSArtifactContent(&a)
		after, _ := json.Marshal(a)
		if string(before) != string(after) {
			t.Fatal("compaction changed signed input")
		}
		if left, ok := a.Content["a"].(map[string]any); ok {
			if reflect.ValueOf(left).Pointer() != reflect.ValueOf(a.Content["b"]).Pointer() {
				t.Fatal("identical tree not shared")
			}
			if reflect.DeepEqual(left, a.Content["c"]) {
				t.Fatal("different values aliased")
			}
		}
	}
}
func TestDNSContentCompactionIndexIsBounded(t *testing.T) {
	c := dnsContentInterner{objects: make(map[[32]byte]any)}
	for i := 0; i < dnsContentInternLimit+100; i++ {
		c.visit(map[string]any{"value": fmt.Sprint(i)}, 0)
	}
	if len(c.objects) != dnsContentInternLimit {
		t.Fatal("index exceeds bound", len(c.objects))
	}
}
func BenchmarkDNSContentCompaction(b *testing.B) {
	rows := make([]any, 4096)
	for i := range rows {
		rows[i] = map[string]any{"name": fmt.Sprintf("host-%d.example.test", i), "policy": map[string]any{"mode": "bounded", "targets": []any{"edge-a", "edge-b"}, "metadata": map[string]any{"owner": "tenant-a", "required": true}}}
	}
	raw, _ := json.Marshal(map[string]any{"records": rows})
	for _, compact := range []bool{false, true} {
		b.Run(fmt.Sprint(compact), func(b *testing.B) {
			b.ReportAllocs()
			var retained uint64
			for i := 0; i < b.N; i++ {
				runtime.GC()
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				var a model.PlatformArtifact
				json.Unmarshal(raw, &a.Content)
				if compact {
					compactDNSArtifactContent(&a)
				}
				runtime.GC()
				runtime.GC()
				runtime.ReadMemStats(&after)
				if after.HeapAlloc > before.HeapAlloc {
					retained += after.HeapAlloc - before.HeapAlloc
				}
				runtime.KeepAlive(a)
			}
			b.ReportMetric(float64(retained)/float64(b.N), "retained-B/op")
		})
	}
}

func TestDNSContentCompactionRetainsSignedTransitionValidation(t *testing.T) {
	a := celldns.Compile(t, celldns.TransitionRequest(t)).DNSArtifact
	before, err := platformconfig.Digest(a.Content)
	if err != nil {
		t.Fatal(err)
	}
	compactDNSArtifactContent(&a)
	after, err := platformconfig.Digest(a.Content)
	if err != nil || before != after {
		t.Fatal("signed digest changed", err)
	}
	if !platformsafety.EvaluateArtifactIntegrity(a, celldns.Keys()).Pass {
		t.Fatal("signature changed")
	}
	if _, err := cellpublication.VerifyDNSArtifact(a, celldns.Keys()); err != nil {
		t.Fatal(err)
	}
}
