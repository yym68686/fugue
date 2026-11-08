package store

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

// Preserve the previous complete-object decoder as an independent oracle.
func referenceHotFullObjectDecode(a model.PlatformArtifact, content, metadata []byte, originalBytes int64) (inputarchive.HotNode, error) {
	if originalBytes > compilerInputHotArtifactBytes || len(content) > compilerInputHotArtifactBytes || len(metadata) > 16<<10 || json.Unmarshal(content, &a.Content) != nil || json.Unmarshal(metadata, &a.Metadata) != nil {
		return inputarchive.HotNode{}, ErrConflict
	}
	return compilerInputArtifactHotNode(a)
}

func assertHotRouteDecodeEquivalent(t testing.TB, a model.PlatformArtifact, content, metadata []byte, originalBytes int64) {
	t.Helper()
	clone := func() model.PlatformArtifact {
		out := a
		if a.Content != nil {
			raw, err := json.Marshal(a.Content)
			if err != nil {
				t.Fatal(err)
			}
			out.Content = nil
			if err := json.Unmarshal(raw, &out.Content); err != nil {
				t.Fatal(err)
			}
		}
		if a.Metadata != nil {
			out.Metadata = make(map[string]string, len(a.Metadata))
			for key, value := range a.Metadata {
				out.Metadata[key] = value
			}
		}
		return out
	}
	want, we := referenceHotFullObjectDecode(clone(), content, metadata, originalBytes)
	got, ge := decodeCompilerInputHotBatchArtifact(clone(), content, metadata, originalBytes)
	if (we == nil) != (ge == nil) || we != nil && we.Error() != ge.Error() || we == nil && !reflect.DeepEqual(want, got) {
		t.Fatalf("kind=%s sourceBytes=%d originalBytes=%d result changed: want=%+v/%v got=%+v/%v", a.ArtifactKind, len(content), originalBytes, want, we, got, ge)
	}
}

func TestHotRouteDecodeMatchesFullObject(t *testing.T) {
	req, _, _ := celldns.SourceAuthorizedRequest(t, false)
	a := req.CellRoutePublications[0].Route
	metadata, err := json.Marshal(a.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	cases := []string{
		`null`, `{}`, `[]`, `42`, `{"lineage":null}`, `{"lineage":{}}`, `{"lineage":[]}`,
		`{"Lineage":{"input_snapshot_digest":"bad"}}`,
		`{"lineage":{"input_snapshot_digest":"sha256:` + strings.Repeat("a", 64) + `"},"ignored":[{"x":1.0,"y":1e2,"z":-0.0}]}`,
		`{"ignored":1e309}`, `{"ignored":{"nested":[-1e309]}}`, `{"ignored":1e-9999}`,
		`{"ignored":"escaped \\\"1e309 not a number"}`, `{"ignored":"1e309"}`,
		`{"ignored":1.7976931348623157e308}`, `{"ignored":1.7976931348623159e308}`,
		`{"lineage":{"unknown":1e309}}`, `{"lineage":{"unknown":"` + strings.Repeat("x", 16384) + `"}}`,
		`{"lineage":null,"lineage":{"input_snapshot_digest":"bad"}}`,
		`{"ignored":1e309,"ignored":0}`, `{"ignored":[[[1.25e-20,9007199254740993]]]}`,
		`{"lineage":{"input_snapshot_digest":"bad"},"lineage":{}}`,
		`{"li\u006eeage":{},"ignored":{"str":"\\\\\\\"-1e309"}}`,
		`{"lineage":{},"trailing":true}garbage`,
	}
	for _, kind := range []string{model.PlatformArtifactKindEdgeRouteBundle, model.PlatformArtifactKindCaddyRouteConfig, model.PlatformArtifactKindPolicySnapshot, "unknown-kind"} {
		a.ArtifactKind = kind
		for _, raw := range cases {
			for _, padding := range []int{0, 64 << 10} {
				content := []byte(strings.Repeat(" ", padding) + raw)
				a.Content = nil
				assertHotRouteDecodeEquivalent(t, a, content, metadata, int64(len(content)))
			}
		}
	}
	// The fallback preserves map merging and JSON null for nonempty destinations.
	a.ArtifactKind = model.PlatformArtifactKindEdgeRouteBundle
	for _, raw := range []string{`{}`, `null`, `{"lineage":{}}`} {
		a.Content = map[string]any{"lineage": map[string]any{"input_snapshot_digest": "bad"}}
		content := []byte(strings.Repeat(" ", 64<<10) + raw)
		assertHotRouteDecodeEquivalent(t, a, content, metadata, int64(len(content)))
	}
	a.Content = nil
	content := []byte(strings.Repeat(" ", 64<<10) + `{}`)
	for _, badMetadata := range []string{`null`, `[]`, `{"n":1}`, `{"n":null}`, strings.Repeat(" ", (16<<10)+1)} {
		assertHotRouteDecodeEquivalent(t, a, content, []byte(badMetadata), int64(len(content)))
	}
	assertHotRouteDecodeEquivalent(t, a, content, metadata, compilerInputHotArtifactBytes+1)
	// The existing JSON decoder still enforces the nesting bound.
	nested := []byte(`{"ignored":` + strings.Repeat("[", 10001) + "0" + strings.Repeat("]", 10001) + "}")
	nested = append([]byte(strings.Repeat(" ", 64<<10)), nested...)
	assertHotRouteDecodeEquivalent(t, a, nested, metadata, int64(len(nested)))
}

func TestHotRouteDecodePreservesConfigurationRuntimePolicyBinding(t *testing.T) {
	compiled, err := platformconfig.Compile(platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent"}, Policy: platformconfig.PolicySnapshot{Generation: "policy", RuntimeInputMode: platformconfig.RuntimeInputConfigurationV1}, RuntimeSnapshot: platformconfig.RuntimeSnapshot{Facts: map[string]any{"audit": true}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range []model.PlatformArtifact{compiled.RouteArtifact, compiled.TLSArtifact} {
		a.ID, a.ContentHash = "runtime-child", "sha256:"+strings.Repeat("a", 64)
		a.Content["unrelated_padding"] = strings.Repeat("x", 70<<10)
		content, _ := json.Marshal(a.Content)
		metadata, _ := json.Marshal(a.Metadata)
		a.Content = nil
		assertHotRouteDecodeEquivalent(t, a, content, metadata, int64(len(content)))
		node, err := decodeCompilerInputHotBatchArtifact(a, content, metadata, int64(len(content)))
		if err != nil || len(node.References) != 2 {
			t.Fatal("large optimized decode lost runtime or original retention binding", node, err)
		}
		seen := map[string]bool{}
		for _, ref := range node.References {
			seen[ref.ID] = true
		}
		if !seen[compiled.Lineage.RuntimeInputDigest] || !seen[compiled.Lineage.InputSnapshotDigest] {
			t.Fatal("retention decoder dropped original provenance", node)
		}
	}
}

func FuzzHotRouteDecodeMatchesFullObject(f *testing.F) {
	for _, seed := range []string{`{}`, `null`, `{"ignored":1e309}`, `{"lineage":null}`, `{"ignored":["1e309",1.5,-0,1e-999]}`} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		a := model.PlatformArtifact{ID: "neutral-route", ArtifactKind: model.PlatformArtifactKindEdgeRouteBundle, ContentHash: "sha256:" + strings.Repeat("a", 64)}
		content := []byte(strings.Repeat(" ", 64<<10) + raw)
		assertHotRouteDecodeEquivalent(t, a, content, []byte(`{}`), int64(len(content)))
	})
}

func BenchmarkHotRouteDecode(b *testing.B) {
	req, _, _ := celldns.SourceAuthorizedRequest(b, false)
	for _, size := range []int{0, 400, 2000} {
		b.Run(fmt.Sprintf("ignored-records-%d", size), func(b *testing.B) {
			a := req.CellRoutePublications[0].Route
			content := make(map[string]any, len(a.Content)+1)
			for k, v := range a.Content {
				content[k] = v
			}
			view := make([]any, size)
			for i := range view {
				view[i] = map[string]any{"host": fmt.Sprintf("host-%d.example.test", i), "ports": []int{80, 443}, "enabled": true, "weight": 1.0, "details": map[string]any{"path": "/", "origin": "http://origin.example.test:8080"}}
			}
			content["ignored_view"] = view
			raw, _ := json.Marshal(content)
			metadata, _ := json.Marshal(a.Metadata)
			a.Content = nil
			for _, candidate := range []bool{false, true} {
				b.Run(fmt.Sprintf("candidate-%t", candidate), func(b *testing.B) {
					b.SetBytes(int64(len(raw)))
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						decode := referenceHotFullObjectDecode
						if candidate {
							decode = decodeCompilerInputHotBatchArtifact
						}
						if _, err := decode(a, raw, metadata, int64(len(raw))); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}

func BenchmarkHotRouteDecodeLargeString(b *testing.B) {
	req, _, _ := celldns.SourceAuthorizedRequest(b, false)
	a := req.CellRoutePublications[0].Route
	content := make(map[string]any, len(a.Content)+1)
	for key, value := range a.Content {
		content[key] = value
	}
	content["ignored_view"] = strings.Repeat("x", 256<<10)
	raw, _ := json.Marshal(content)
	metadata, _ := json.Marshal(a.Metadata)
	a.Content = nil
	for _, candidate := range []bool{false, true} {
		b.Run(fmt.Sprintf("candidate-%t", candidate), func(b *testing.B) {
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				decode := referenceHotFullObjectDecode
				if candidate {
					decode = decodeCompilerInputHotBatchArtifact
				}
				if _, err := decode(a, raw, metadata, int64(len(raw))); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
