package inputarchive

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestHotObligationTargetsRejectAmbiguityAndTrailingData(t *testing.T) {
	for _, raw := range []string{`[{"kind":"artifact","id":"a","id":"b"}]`, `[{"kind":"artifact","id":"a","expected_hash":null}]`} {
		if _, err := DecodeHotObligationTargets(raw); err == nil {
			t.Fatal("ambiguous string identity accepted", raw)
		}
	}
	for _, raw := range []string{`null`, `[]`, `[{"kind":"generation","id":"g"}]`, `[{"kind":"artifact","id":"a","extra":true}]`, `[{"kind":"artifact","id":"a"}] {}`, `[{"kind":"artifact","id":"a"},{"kind":"artifact","id":"a"}]`} {
		if _, err := DecodeHotObligationTargets(raw); err == nil {
			t.Fatal("unsafe target accepted", raw)
		}
	}
	if _, err := DecodeHotObligationTargets(`[{"kind":"generation","id":"g","artifact_kind":"edge_route_bundle","scope_key":"neutral"}]`); err != nil {
		t.Fatal(err)
	}
}

func TestHotObligationPathsKeepSharedCyclicAndUnresolvedParents(t *testing.T) {
	a := HotReference{Kind: "artifact", ID: "a"}
	b := HotReference{Kind: "artifact", ID: "b"}
	missing := HotReference{Kind: "generation", ID: "missing", ArtifactKind: "edge_route_bundle", ScopeKey: "neutral"}
	roots := []HotRoot{{"current", "first", a}, {"rollback", "later", b}}
	nodes := []HotNode{{Identity: a, References: []HotReference{b}}, {Identity: b, References: []HotReference{a, missing}}}
	var paths []HotObligationPath
	emit := func(path HotObligationPath) error { paths = append(paths, path); return nil }
	complete, err := TraceHotObligationPaths(t.Context(), roots, nodes, []HotReference{missing}, 8, emit)
	if err != nil || !complete || len(paths) != 2 || paths[0].RootIndex != 0 || paths[1].RootIndex != 1 || paths[0].DeclaredHashesMatch || len(paths[0].References) != 3 || len(paths[1].References) != 2 {
		t.Fatal("known missing-target parents lost or mistaken for verification", paths, complete, err)
	}
	paths = nil
	complete, err = TraceHotObligationPaths(t.Context(), roots, nodes, []HotReference{missing}, 1, emit)
	if err == nil || complete {
		t.Fatal("binding bound produced a complete diagnostic")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if complete, err = TraceHotObligationPaths(ctx, roots, nodes, []HotReference{missing}, 8, emit); complete || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled trace continued", complete, err)
	}
}

func TestHotClosureDiagnosticPreservesFailedEdgesWithoutChangingProof(t *testing.T) {
	parent := HotReference{Kind: "artifact", ID: "parent"}
	child := HotReference{Kind: "artifact", ID: "child"}
	roots := []HotRoot{{"current", "lane", parent}}
	resolve := func(_ context.Context, refs []HotReference) (map[HotReference]HotNode, error) {
		out := map[HotReference]HotNode{}
		for _, ref := range refs {
			node := HotNode{Identity: ref}
			if ref == parent {
				node.References = []HotReference{child}
			}
			out[ref] = node
		}
		return out, nil
	}
	plain, err := PlanHotClosureBatched(t.Context(), roots, hotFixtureBudget(), resolve)
	if err != nil {
		t.Fatal(err)
	}
	var observed []HotNode
	withDiagnostics, err := PlanHotClosureBatchedDiagnostic(t.Context(), roots, hotFixtureBudget(), resolve, nil, func(node HotNode) error {
		observed = append(observed, node)
		if len(node.References) > 0 {
			node.References[0].ID = "observer-must-not-mutate-proof"
		}
		return nil
	})
	if err != nil || !reflect.DeepEqual(plain, withDiagnostics) || len(observed) != 2 {
		t.Fatal("observer changed proof", err)
	}
	failing := func(ctx context.Context, refs []HotReference) (map[HotReference]HotNode, error) {
		if refs[0] == child {
			return nil, errors.New("private resolver details")
		}
		return resolve(ctx, refs)
	}
	observed = nil
	refused, err := PlanHotClosureBatchedDiagnostic(t.Context(), roots, hotFixtureBudget(), failing, nil, func(node HotNode) error { observed = append(observed, node); return nil })
	if err == nil || refused.MetadataComplete || refused.PlanSHA256 != "" || len(observed) != 1 || observed[0].References[0] != child {
		t.Fatal("refusal lost known edge or yielded proof", refused, observed, err)
	}
}
