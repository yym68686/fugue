package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"fugue/internal/model"
)

func TestHotDecodeCostSeparatesJSONAndDependencyRefusals(t *testing.T) {
	a := model.PlatformArtifact{ID: "artifact-neutral", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ContentHash: "sha256:" + strings.Repeat("a", 64)}
	for _, test := range []struct {
		name, content  string
		originalBytes  int64
		wantRejected   bool
		wantDependency bool
	}{
		{"valid", `{}`, 2, false, true},
		{"invalid-json", `{"lineage":`, 11, true, false},
		{"invalid-lineage", `{"lineage":null}`, 16, true, true},
		{"source-bound", `{}`, compilerInputHotArtifactBytes + 1, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var aggregate CompilerInputHotReadCost
			cost := compilerInputHotDecodeCost(&aggregate, a.ArtifactKind)
			_, err := decodeCompilerInputHotBatchArtifactObserved(a, []byte(test.content), []byte(`{}`), test.originalBytes, cost)
			if (err != nil) != test.wantRejected || cost.Attempts != 1 || (cost.Rejected == 1) != test.wantRejected || cost.SourceBytes != test.originalBytes || cost.TransportBytes != int64(len(test.content)+2) {
				t.Fatal("decode work/refusal not represented", cost, err)
			}
			if cost.JSONUS < 0 || cost.DependenciesUS < 0 || !test.wantDependency && cost.DependenciesUS != 0 {
				t.Fatal("failed JSON was attributed to dependency validation", cost)
			}
		})
	}
}

func TestHotDecodeCostUsesOnlySixFixedKindBuckets(t *testing.T) {
	var cost CompilerInputHotReadCost
	for _, kind := range []string{model.PlatformArtifactKindDNSAnswerBundle, model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindPolicySnapshot, model.PlatformArtifactKindReleaseSet} {
		compilerInputHotDecodeCost(&cost, kind).Attempts++
	}
	for i := 0; i < 1000; i++ {
		compilerInputHotDecodeCost(&cost, fmt.Sprintf("untrusted-neutral-kind-%d", i)).Attempts++
	}
	if len(cost.DecodeByKind) != 6 || cost.DecodeByKind["other"].Attempts != 1000 || compilerInputHotDecodeCost(nil, "untrusted-neutral-kind") != nil {
		t.Fatal("diagnostic cardinality or nil observation changed", cost)
	}
	raw, err := json.Marshal(cost)
	if err != nil || strings.Contains(string(raw), "untrusted-neutral") || strings.Contains(string(raw), "artifact-neutral") {
		t.Fatal("arbitrary identity leaked through aggregate kind labels", string(raw), err)
	}
}
