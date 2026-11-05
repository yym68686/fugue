package inputarchive

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func hotFixtureBudget() HotClosureBudget {
	return HotClosureBudget{Roots: 16, Nodes: 32, Edges: 64, Bindings: 128}
}

func TestHotClosureProgressSeparatesRefusalFromProofAndPreservesHash(t *testing.T) {
	parent := HotReference{Kind: "artifact", ID: "parent"}
	child := HotReference{Kind: "artifact", ID: "child"}
	roots := []HotRoot{{Source: "current", ID: "lane", Reference: parent}}
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
	var progress HotClosureProgress
	observe := func(p HotClosureProgress) { progress = p }
	budget := hotFixtureBudget()
	budget.Bindings = 1
	refused, err := PlanHotClosureBatchedObserved(t.Context(), roots, budget, resolve, observe)
	if HotClosureFailureReason(err) != "root-binding-budget" || refused.MetadataComplete || refused.PlanSHA256 != "" || progress.Phase != "propagate-roots" || progress.ResolvedNodes != 2 || progress.Edges != 1 || progress.RootBindings != 2 {
		t.Fatal("refusal lost exact phase/counts or granted proof", progress, refused, err)
	}
	plain, err := PlanHotClosureBatched(t.Context(), roots, hotFixtureBudget(), resolve)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := PlanHotClosureBatchedObserved(t.Context(), roots, hotFixtureBudget(), resolve, observe)
	if err != nil || !reflect.DeepEqual(plain, observed) || progress.Phase != "complete" || progress.ResolverCalls != 2 {
		t.Fatal("diagnostics changed deterministic graph/proof", progress, err)
	}
	for _, item := range []struct {
		err  error
		want string
	}{{context.DeadlineExceeded, "deadline"}, {context.Canceled, "canceled"}, {errors.New("private SQL connection token"), "resolver-unavailable"}} {
		if got := HotClosureFailureReason(fmt.Errorf("private wrapper: %w", item.err)); got != item.want {
			t.Fatalf("failure category leaked details or lost wrapper: %q", got)
		}
	}
}

func hotFixtureResolver(nodes map[string]HotNode, calls map[string]int) func(context.Context, HotReference) (HotNode, error) {
	return func(_ context.Context, ref HotReference) (HotNode, error) {
		key := hotReferenceKey(ref)
		calls[key]++
		node, ok := nodes[key]
		if !ok {
			return HotNode{}, errors.New("secret database URL must never enter a report")
		}
		return node, nil
	}
}

func TestHotClosureSharedDescendantRetainsEveryRootAndCountsInputOnce(t *testing.T) {
	a := HotReference{Kind: "artifact", ID: "parent-a"}
	b := HotReference{Kind: "artifact", ID: "parent-b"}
	shared := HotReference{Kind: "artifact", ID: "shared-child"}
	input := HotReference{Kind: "input", ID: "sha256:" + strings.Repeat("a", 64)}
	nodes := map[string]HotNode{}
	for _, n := range []HotNode{
		{Identity: a, References: []HotReference{shared}},
		{Identity: b, References: []HotReference{shared}},
		{Identity: shared, References: []HotReference{input}},
		{Identity: input, ContentHash: input.ID, LogicalBytes: 1000, EncodedBytes: 37, NeedsLocalCopy: true},
	} {
		nodes[hotReferenceKey(n.Identity)] = n
	}
	calls := map[string]int{}
	roots := []HotRoot{{Source: "serving", ID: "lane-a", Reference: a}, {Source: "rollback", ID: "lane-b", Reference: b}}
	report, err := PlanHotClosure(t.Context(), roots, hotFixtureBudget(), hotFixtureResolver(nodes, calls))
	if err != nil || !report.MetadataComplete || report.LocalCopiesVerified || report.RequiredLocalCopyBytes != 37 || len(report.Nodes) != 4 {
		t.Fatal("shared input accounting or metadata/verification distinction lost", report, err)
	}
	for _, node := range report.Nodes {
		if node.Identity == shared || node.Identity == input {
			if len(node.Roots) != 2 {
				t.Fatal("later root did not reach a shared descendant", node)
			}
		}
		if calls[hotReferenceKey(node.Identity)] != 1 {
			t.Fatal("same identity was fetched repeatedly", calls)
		}
	}
	// Permute both root and edge discovery order. The proof remains identical.
	roots[0], roots[1] = roots[1], roots[0]
	again, err := PlanHotClosure(t.Context(), roots, hotFixtureBudget(), hotFixtureResolver(nodes, map[string]int{}))
	if err != nil || !reflect.DeepEqual(report, again) {
		t.Fatal("proof depended on discovery order", report, again, err)
	}
	n := nodes[hotReferenceKey(input)]
	n.LocalVerified = true
	n.NeedsLocalCopy = false
	nodes[hotReferenceKey(input)] = n
	verified, err := PlanHotClosure(t.Context(), roots, hotFixtureBudget(), hotFixtureResolver(nodes, map[string]int{}))
	if err != nil || !verified.LocalCopiesVerified || verified.RequiredLocalCopyBytes != 0 || verified.PlanSHA256 == report.PlanSHA256 {
		t.Fatal("local representation change did not invalidate the proof", verified, err)
	}
}

func TestHotClosureValidatesConvergingHashReferencesAfterDeduplication(t *testing.T) {
	a := HotReference{Kind: "artifact", ID: "parent-a"}
	b := HotReference{Kind: "artifact", ID: "parent-b"}
	child := HotReference{Kind: "artifact", ID: "child"}
	good, bad := child, child
	good.ExpectedHash, bad.ExpectedHash = "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	nodes := map[string]HotNode{
		hotReferenceKey(a):     {Identity: a, References: []HotReference{good}},
		hotReferenceKey(b):     {Identity: b, References: []HotReference{bad}},
		hotReferenceKey(child): {Identity: child, ContentHash: good.ExpectedHash},
	}
	calls := map[string]int{}
	report, err := PlanHotClosure(t.Context(), []HotRoot{{"current", "a", a}, {"rollback", "b", b}}, hotFixtureBudget(), hotFixtureResolver(nodes, calls))
	if err != nil || report.MetadataComplete || report.LocalCopiesVerified || len(report.Blockers) != 1 || report.Blockers[0].Reason != "declared-hash-mismatch" || report.Blockers[0].Reference != bad || calls[hotReferenceKey(child)] != 1 {
		t.Fatal("deduplication hid a mismatched later edge", report, calls, err)
	}
}

func TestHotClosureMissingScopedGenerationCannotBecomeAnInputOrDisappear(t *testing.T) {
	ref := HotReference{Kind: "generation", ID: "bundle-prefix-is-not-a-digest", ArtifactKind: "edge_route_bundle", ScopeKey: "cell-neutral"}
	report, err := PlanHotClosure(t.Context(), []HotRoot{{"consumer-lkg", "consumer-neutral", ref}}, hotFixtureBudget(), hotFixtureResolver(nil, map[string]int{}))
	if err != nil || report.MetadataComplete || report.LocalCopiesVerified || len(report.Blockers) != 1 || report.Blockers[0].Reference != ref || len(report.Nodes) != 0 {
		t.Fatal("unresolved execution identity was discarded or guessed", report, err)
	}
	if strings.Contains(report.Blockers[0].Reason, "secret") {
		t.Fatal("resolver error text leaked")
	}
}

func TestHotClosureCyclesTerminateAndBudgetsRefuseTruncatedProof(t *testing.T) {
	a := HotReference{Kind: "artifact", ID: "a"}
	b := HotReference{Kind: "release", ID: "b"}
	nodes := map[string]HotNode{
		hotReferenceKey(a): {Identity: a, References: []HotReference{b}},
		hotReferenceKey(b): {Identity: b, References: []HotReference{a}},
	}
	roots := []HotRoot{{"lane", "a", a}}
	if got, err := PlanHotClosure(t.Context(), roots, hotFixtureBudget(), hotFixtureResolver(nodes, map[string]int{})); err != nil || !got.MetadataComplete || len(got.Nodes) != 2 {
		t.Fatal("release/artifact back reference did not terminate", got, err)
	}
	for _, field := range []string{"roots", "nodes", "edges", "bindings"} {
		t.Run(field, func(t *testing.T) {
			budget := hotFixtureBudget()
			selected := roots
			switch field {
			case "roots":
				budget.Roots = 1
				selected = append(append([]HotRoot{}, roots...), HotRoot{"lkg", "b", b})
			case "nodes":
				budget.Nodes = 1
			case "edges":
				budget.Edges = 1
			case "bindings":
				budget.Bindings = 1
			}
			got, err := PlanHotClosure(t.Context(), selected, budget, hotFixtureResolver(nodes, map[string]int{}))
			if err == nil || got.MetadataComplete || got.LocalCopiesVerified || got.PlanSHA256 != "" {
				t.Fatal("truncated walk was exposed as a reusable proof", got, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := PlanHotClosure(ctx, roots, hotFixtureBudget(), hotFixtureResolver(nodes, map[string]int{})); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation was ignored", err)
	}
}

func TestHotClosureRejectsAmbiguousIdentitiesAndResolverSubstitution(t *testing.T) {
	a := HotReference{Kind: "artifact", ID: "a"}
	root := HotRoot{"lane", "a", a}
	for _, selected := range [][]HotRoot{{root, root}, {{"lane", "a", HotReference{Kind: "generation", ID: "a"}}}} {
		if _, err := PlanHotClosure(t.Context(), selected, hotFixtureBudget(), hotFixtureResolver(nil, map[string]int{})); err == nil {
			t.Fatal("duplicate root or unscoped generation accepted")
		}
	}
	resolve := func(context.Context, HotReference) (HotNode, error) {
		return HotNode{Identity: HotReference{Kind: "artifact", ID: "different"}}, nil
	}
	if _, err := PlanHotClosure(t.Context(), []HotRoot{root}, hotFixtureBudget(), resolve); err == nil {
		t.Fatal("resolver replaced the requested ledger identity")
	}
}

func TestHotClosureCancellationInsideTheLastResolverCannotReturnProof(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	root := HotRoot{"consumer", "consumer-neutral", HotReference{Kind: "artifact", ID: "artifact-neutral"}}
	resolve := func(context.Context, HotReference) (HotNode, error) {
		cancel()
		return HotNode{}, errors.New("private transport error")
	}
	got, err := PlanHotClosure(ctx, []HotRoot{root}, hotFixtureBudget(), resolve)
	if !errors.Is(err, context.Canceled) || got.PlanSHA256 != "" || got.MetadataComplete || got.LocalCopiesVerified {
		t.Fatal("final canceled lookup produced a reusable proof", got, err)
	}
}

func TestHotClosureBatchesPreserveEveryRootHashAndUnresolvedIdentity(t *testing.T) {
	input := HotReference{Kind: "input", ID: "sha256:" + strings.Repeat("f", 64)}
	missing := HotReference{Kind: "artifact", ID: "unresolved-neutral"}
	nodes := map[string]HotNode{hotReferenceKey(input): {Identity: input, ContentHash: input.ID, LogicalBytes: 11, EncodedBytes: 11, NeedsLocalCopy: true}}
	roots := []HotRoot{}
	for i := 0; i < 65; i++ {
		ref := HotReference{Kind: "artifact", ID: fmt.Sprintf("neutral-%03d", i)}
		nodes[hotReferenceKey(ref)] = HotNode{Identity: ref, References: []HotReference{input, missing}}
		roots = append(roots, HotRoot{"serving", ref.ID, ref})
	}
	budget := HotClosureBudget{Roots: 100, Nodes: 100, Edges: 200, Bindings: 300}
	want, err := PlanHotClosure(t.Context(), roots, budget, hotFixtureResolver(nodes, map[string]int{}))
	if err != nil {
		t.Fatal(err)
	}
	calls, total := 0, 0
	batch := func(_ context.Context, refs []HotReference) (map[HotReference]HotNode, error) {
		calls++
		total += len(refs)
		if len(refs) == 0 || len(refs) > 32 {
			t.Fatal("unbounded transport batch", len(refs))
		}
		out := map[HotReference]HotNode{}
		for _, ref := range refs {
			if ref.ExpectedHash != "" {
				t.Fatal("edge hash changed the resolver identity", ref)
			}
			if node, ok := nodes[hotReferenceKey(ref)]; ok {
				out[ref] = node
			}
		}
		return out, nil
	}
	got, err := PlanHotClosureBatched(t.Context(), roots, budget, batch)
	if err != nil || !reflect.DeepEqual(want, got) || calls > 4 || total != 67 {
		t.Fatal("batching changed the graph/proof or fetched identities repeatedly", got, calls, total, err)
	}
	for _, node := range got.Nodes {
		if node.Identity == input && len(node.Roots) != 65 {
			t.Fatal("batching lost later shared roots", node)
		}
	}
	// A late conflicting hash remains a blocker after its node was cached.
	bad := input
	bad.ExpectedHash = "sha256:" + strings.Repeat("a", 64)
	roots = append(roots, HotRoot{"rollback", "late-hash", bad})
	got, err = PlanHotClosureBatched(t.Context(), roots, budget, batch)
	if err != nil || got.MetadataComplete || len(got.Blockers) != 2 {
		t.Fatal("batched cache hid a late hash conflict", got, err)
	}
}

func TestHotClosureBatchFailureCannotBecomeMissingDependenciesOrProof(t *testing.T) {
	root := HotRoot{"lane", "neutral", HotReference{Kind: "artifact", ID: "neutral"}}
	failure := errors.New("private driver diagnostic")
	got, err := PlanHotClosureBatched(t.Context(), []HotRoot{root}, hotFixtureBudget(), func(context.Context, []HotReference) (map[HotReference]HotNode, error) {
		return nil, failure
	})
	if !errors.Is(err, failure) || got.PlanSHA256 != "" || got.MetadataComplete || got.LocalCopiesVerified || len(got.Blockers) != 0 {
		t.Fatal("batch failure was exposed as a completed graph with missing identities", got, err)
	}
	got, err = PlanHotClosureBatched(t.Context(), []HotRoot{root}, hotFixtureBudget(), func(context.Context, []HotReference) (map[HotReference]HotNode, error) {
		ref := HotReference{Kind: "artifact", ID: "substitute"}
		return map[HotReference]HotNode{ref: {Identity: ref}}, nil
	})
	if err == nil || got.PlanSHA256 != "" {
		t.Fatal("unrequested batched identity was accepted", got, err)
	}
}
