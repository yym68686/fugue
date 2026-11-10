package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/testfixture/celldns"
)

func TestHotRouteProjectedValuesOwnTheirBytes(t *testing.T) {
	compiled := compilerInputHotLineageFixture(t, true)
	for _, source := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.TLSArtifact} {
		source.Content["ignored_view"] = strings.Repeat("x", 96<<10)
		content, err := json.Marshal(source.Content)
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := json.Marshal(source.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		source.Content = nil
		if err := decodeCompilerInputHotArtifactJSON(&source, content, metadata); err != nil {
			t.Fatal(err)
		}
		before, err := json.Marshal(source)
		if err != nil {
			t.Fatal(err)
		}
		for i := range content {
			content[i] = 'x'
		}
		for i := range metadata {
			metadata[i] = 'y'
		}
		after, err := json.Marshal(source)
		if err != nil || string(before) != string(after) {
			t.Fatal("retained values alias the caller's mutable source", err)
		}
	}
}

func TestHotRouteLargePolicyPreservesFullObjectBehavior(t *testing.T) {
	req, _, _ := celldns.SourceAuthorizedRequest(t, false)
	for _, source := range []model.PlatformArtifact{req.CellRoutePublications[0].Route, req.CellRoutePublications[0].TLS} {
		// Legacy dependency extraction does not consume this policy. Its
		// representation must not change the resulting retention references.
		source.Content["policy"] = map[string]any{
			"nested":  []any{nil, map[string]any{"value": "retained", "count": 3.0}},
			"padding": strings.Repeat("x", 96<<10),
		}
		content, err := json.Marshal(source.Content)
		if err != nil {
			t.Fatal(err)
		}
		metadata, err := json.Marshal(source.Metadata)
		if err != nil {
			t.Fatal(err)
		}
		source.Content = nil
		assertHotRouteDecodeEquivalent(t, source, content, metadata, int64(len(content)))
		for _, policy := range []string{`null`, `[]`, `42`, `{"nested":1e309}`, `{"n":1e-9999}`, `{"n":9007199254740993}`} {
			// Include a duplicate policy key so even discarded/overwritten
			// fields retain the generic decoder's numeric overflow refusal.
			raw := append(append([]byte(nil), content[:len(content)-1]...), []byte(`,"policy":`+policy+`}`)...)
			assertHotRouteDecodeEquivalent(t, source, raw, metadata, int64(len(raw)))
		}
	}
}

func BenchmarkHotRouteDecodeLargePolicy(b *testing.B) {
	req, _, _ := celldns.SourceAuthorizedRequest(b, false)
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig} {
		b.Run(kind, func(b *testing.B) {
			source := req.CellRoutePublications[0].Route
			source.ArtifactKind = kind
			content := make(map[string]any, len(source.Content))
			for key, value := range source.Content {
				content[key] = value
			}
			rules := make([]any, 1600)
			for i := range rules {
				rules[i] = map[string]any{"hostname": fmt.Sprintf("host-%d.example.test", i),
					"enabled": true, "route_policy": "active", "min_healthy_edge_nodes": 1,
					"edge_group_id": "group-neutral", "excluded_edge_ids": []any{}}
			}
			content["policy"] = map[string]any{"route_constraints": rules}
			raw, err := json.Marshal(content)
			if err != nil {
				b.Fatal(err)
			}
			metadata, err := json.Marshal(source.Metadata)
			if err != nil {
				b.Fatal(err)
			}
			source.Content = nil
			assertHotRouteDecodeEquivalent(b, source, raw, metadata, int64(len(raw)))
			// Retain the exact predecessor's large-object projection as a
			// benchmark baseline, independently of the full-object oracle.
			previous := func(a model.PlatformArtifact, content, metadata []byte, _ int64) (inputarchive.HotNode, error) {
				var fields map[string]json.RawMessage
				if json.Unmarshal(content, &fields) != nil || !compilerInputHotJSONNumbersRepresentable(content) || json.Unmarshal(metadata, &a.Metadata) != nil {
					return inputarchive.HotNode{}, ErrConflict
				}
				a.Content = map[string]any{}
				for _, key := range []string{"lineage", "policy"} {
					if value, present := fields[key]; present {
						var decoded any
						if json.Unmarshal(value, &decoded) != nil {
							return inputarchive.HotNode{}, ErrConflict
						}
						a.Content[key] = decoded
					}
				}
				return compilerInputArtifactHotNode(a)
			}
			for _, implementation := range []struct {
				name   string
				decode func(model.PlatformArtifact, []byte, []byte, int64) (inputarchive.HotNode, error)
			}{{"full-object", referenceHotFullObjectDecode}, {"previous-projection", previous}, {"candidate", decodeCompilerInputHotBatchArtifact}} {
				b.Run(implementation.name, func(b *testing.B) {
					b.SetBytes(int64(len(raw)))
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						if _, err := implementation.decode(source, raw, metadata, int64(len(raw))); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
