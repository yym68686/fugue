package dnsserver

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"runtime"
	"testing"
	"time"

	"fugue/internal/dnsroutesource"
	"fugue/internal/model"
	"fugue/internal/testfixture/celldns"
)

func TestDNSJSONEqualityMatchesDeepEqual(t *testing.T) {
	values := []any{nil, true, false, "", "value", float64(0), math.Copysign(0, -1), float64(1), math.NaN(), map[string]any(nil), map[string]any{}, map[string]any{"a": nil}, map[string]any{"b": nil}, []any(nil), []any{}, []any{nil}, []any{"value", float64(1)}, json.Number("1"), int64(1), []string{"value"}, map[string]int{"a": 1}}
	for _, a := range values {
		for _, b := range values {
			if got, want := equalDNSJSON(a, b, 0), reflect.DeepEqual(a, b); got != want {
				t.Fatalf("equality differs for %T %v / %T %v: %t != %t", a, a, b, b, got, want)
			}
		}
	}
	for _, change := range []bool{false, true} {
		var a, b any = "leaf", "leaf"
		if change {
			b = "changed"
		}
		for i := 0; i < 140; i++ {
			a, b = map[string]any{"nested": []any{a}}, map[string]any{"nested": []any{b}}
		}
		if equalDNSJSON(a, b, 0) != reflect.DeepEqual(a, b) {
			t.Fatal("deep fallback differs", change)
		}
	}
	a, b := map[string]any{}, map[string]any{}
	a["cycle"], b["cycle"] = a, b
	if !equalDNSJSON(a, b, 0) {
		t.Fatal("cyclic non-JSON fallback differs")
	}
	delete(b, "cycle")
	if equalDNSJSON(a, b, 0) {
		t.Fatal("cyclic content mismatch reused")
	}
}

func FuzzDNSJSONEquality(f *testing.F) {
	f.Add([]byte(`{"records":[null,true,false,0,1,"x",{"a":1}],"empty":[]}`), []byte(`{"records":[null,true,false,0,1,"x",{"a":1}],"empty":[]}`))
	f.Add([]byte(`{"a":null}`), []byte(`{"b":null}`))
	f.Add([]byte(`[]`), []byte(`null`))
	f.Fuzz(func(t *testing.T, first, second []byte) {
		if len(first) > 32<<10 || len(second) > 32<<10 {
			t.Skip()
		}
		var a, b any
		if json.Unmarshal(first, &a) != nil || json.Unmarshal(second, &b) != nil {
			return
		}
		if equalDNSJSON(a, b, 0) != reflect.DeepEqual(a, b) {
			t.Fatal("native JSON equality differs")
		}
		var clone any
		if json.Unmarshal(first, &clone) != nil || !equalDNSJSON(a, clone, 0) {
			t.Fatal("equal native JSON not reused")
		}
	})
}

func TestDNSArtifactStorageReuseRequiresExactContentAndRetainsEnvelope(t *testing.T) {
	original := model.PlatformArtifact{ID: "artifact", ContentHash: "claimed", Content: map[string]any{"nested": map[string]any{"value": "original"}}, Status: "validated"}
	for _, change := range []string{"same", "content", "nil", "id", "digest", "envelope"} {
		var next model.PlatformArtifact
		raw, _ := json.Marshal(original)
		json.Unmarshal(raw, &next)
		switch change {
		case "content":
			next.Content["nested"].(map[string]any)["value"] = "changed"
		case "nil":
			next.Content = nil
		case "id":
			next.ID = "foreign"
		case "digest":
			next.ContentHash = "different"
		case "envelope":
			next.Status = "rejected"
		}
		before, _ := json.Marshal(next)
		reuseDNSArtifactContent(&next, original)
		after, _ := json.Marshal(next)
		if string(before) != string(after) {
			t.Fatal("reuse changed fresh response", change)
		}
		shared := reflect.ValueOf(next.Content).Pointer() == reflect.ValueOf(original.Content).Pointer()
		if shared != (change == "same" || change == "envelope") {
			t.Fatal("incorrect content reuse", change)
		}
	}
}

func TestDNSRouteSourceStorageReusePreservesFreshAuthorityAndProofs(t *testing.T) {
	req, policies, activations := celldns.SourceAuthorizedRequest(t, true)
	c := celldns.Compile(t, req)
	now := time.Now().UTC()
	previous := celldns.SourceSnapshot(t, req, c.DNSArtifact, policies, activations, now)
	raw, _ := json.Marshal(previous)
	var next model.PlatformDNSRouteSourceSnapshot
	if err := json.Unmarshal(raw, &next); err != nil {
		t.Fatal(err)
	}
	next.ObservedAt = now.Add(time.Second)
	next.Scopes[0].Publications[0].Release.FencingToken++
	next.Scopes[0].Publications[0].Route.Provenance.Signature = "forged"
	before, _ := json.Marshal(next)
	reuseDNSRouteSourceContent(&next, &dnsroutesource.Context{Snapshot: previous})
	after, _ := json.Marshal(next)
	if string(before) != string(after) {
		t.Fatal("reuse changed current release or trust input")
	}
	if reflect.ValueOf(next.Scopes[0].Publications[0].Route.Content).Pointer() != reflect.ValueOf(previous.Scopes[0].Publications[0].Route.Content).Pointer() {
		t.Fatal("identical source graph retained twice")
	}
	if err := dnsroutesource.VerifySnapshot(next, c.DNSArtifact, celldns.Keys(), now.Add(time.Second)); err == nil {
		t.Fatal("storage reuse bypassed fresh authority checks")
	}
}

// Hold the same old/new generation pair present during a refresh, then measure
// retained heap. JSON download allocations still happen and are not described
// as eliminated; exact content reuse shortens the second graph's lifetime.
func BenchmarkDNSArtifactStorageReuseRetainedHeap(b *testing.B) {
	previous := model.PlatformArtifact{ID: "artifact", ContentHash: "claimed", Content: map[string]any{}}
	rows := make([]any, 4096)
	for i := range rows {
		rows[i] = map[string]any{"id": fmt.Sprintf("record-%d", i), "values": []any{fmt.Sprintf("synthetic-value-%d", i)}, "ttl": float64(60)}
	}
	previous.Content["records"] = rows
	raw, err := json.Marshal(previous)
	if err != nil {
		b.Fatal(err)
	}
	measure := func(reuse bool) uint64 {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		var next model.PlatformArtifact
		if err := json.Unmarshal(raw, &next); err != nil {
			b.Fatal(err)
		}
		if reuse {
			reuseDNSArtifactContent(&next, previous)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(next)
		if after.HeapAlloc < before.HeapAlloc {
			return 0
		}
		return after.HeapAlloc - before.HeapAlloc
	}
	for _, reuse := range []bool{false, true} {
		b.Run(fmt.Sprintf("reuse=%t", reuse), func(b *testing.B) {
			var retained uint64
			for i := 0; i < b.N; i++ {
				retained += measure(reuse)
			}
			b.ReportMetric(float64(retained)/float64(b.N), "retained-B/op")
		})
	}
	runtime.KeepAlive(previous)
}

// The network response is fresh native JSON; compare complete contents rather
// than trusting the claimed digest, without retaining comparison scratch maps.
func BenchmarkDNSArtifactContentEquality(b *testing.B) {
	previous := model.PlatformArtifact{ID: "artifact", ContentHash: "claimed", Content: map[string]any{}}
	rows := make([]any, 4096)
	for i := range rows {
		rows[i] = map[string]any{"id": fmt.Sprintf("record-%d", i), "values": []any{fmt.Sprintf("synthetic-value-%d", i)}, "ttl": float64(60)}
	}
	previous.Content["records"] = rows
	raw, err := json.Marshal(previous)
	if err != nil {
		b.Fatal(err)
	}
	var next model.PlatformArtifact
	if err := json.Unmarshal(raw, &next); err != nil {
		b.Fatal(err)
	}
	content := next.Content
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		next.Content = content
		if !reuseDNSArtifactContent(&next, previous) {
			b.Fatal("identical native JSON not reused")
		}
	}
}
