package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/testfixture/celldns"
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
			if cost.SourceBoundRejected+cost.MetadataBoundRejected+cost.JSONRejected+cost.DependencyRejected != cost.Rejected || (cost.DependencyRejected != 0) != (test.wantDependency && test.wantRejected) || cost.DNS != nil {
				t.Fatal("fixed refusal counts changed or non-DNS work mislabeled", cost)
			}
		})
	}
}

func TestHotDecodeCostIncludesDNSStagesWithoutChangingNode(t *testing.T) {
	for _, transition := range []bool{false, true} {
		req, _, _ := celldns.SourceAuthorizedRequest(t, transition)
		a := celldns.Compile(t, req).DNSArtifact
		content, _ := json.Marshal(a.Content)
		metadata, _ := json.Marshal(a.Metadata)
		var cost CompilerInputHotDecodeCost
		got, err := decodeCompilerInputHotBatchArtifactObserved(a, content, metadata, int64(len(content)), &cost)
		want, oldErr := decodeCompilerInputHotBatchArtifact(a, content, metadata, int64(len(content)))
		encodedGot, _ := json.Marshal(got)
		encodedWant, _ := json.Marshal(want)
		if err != nil || oldErr != nil || string(encodedGot) != string(encodedWant) || cost.DNS == nil || cost.DNS.Attempts != 1 || cost.DNS.Rejected != 0 || cost.Rejected != 0 {
			t.Fatal("DNS observation changed the node or lost nested work", cost, err, oldErr)
		}
		raw, _ := json.Marshal(cost)
		if strings.Contains(string(raw), a.ID) || strings.Contains(string(raw), a.ContentHash) || strings.Contains(string(raw), "example.test") {
			t.Fatal("DNS diagnostics exposed artifact identities or content")
		}
	}
}

func TestHotDecodeCostRetainsOriginalRefusalOrder(t *testing.T) {
	a := model.PlatformArtifact{ID: "artifact-neutral", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ContentHash: "sha256:" + strings.Repeat("a", 64)}
	for _, tc := range []struct {
		name, content, metadata string
		bytes                   int64
		source, meta, syntax    int64
	}{
		{"source-first", `{`, strings.Repeat("x", 16385), compilerInputHotArtifactBytes + 1, 1, 0, 0},
		{"metadata-before-json", `{`, strings.Repeat("x", 16385), 1, 0, 1, 0},
		{"metadata-json", `{}`, `{`, 2, 0, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cost CompilerInputHotDecodeCost
			_, err := decodeCompilerInputHotBatchArtifactObserved(a, []byte(tc.content), []byte(tc.metadata), tc.bytes, &cost)
			if err != ErrConflict || cost.Rejected != 1 || cost.SourceBoundRejected != tc.source || cost.MetadataBoundRejected != tc.meta || cost.JSONRejected != tc.syntax || cost.DependencyRejected != 0 || cost.DependenciesUS != 0 || cost.DNS != nil {
				t.Fatal("original short-circuit refusal order changed", cost, err)
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
