package store

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/platformproducer"
	"fugue/internal/testfixture/celldns"
)

func TestCompilerInputHotDependenciesFollowTypedProducerSources(t *testing.T) {
	req, policies, releases, _, _ := celldns.CompletePublication(t, true, true)
	previous := req.PreviousTrafficPublication
	p := platformproducer.Policy{SchemaVersion: platformproducer.Schema, Generation: "dns-source-policy", PublicationRole: platformconfig.PublicationRoleCellDNS,
		AuthorityCellID: "cell-dns", TargetScope: "authority-cell:cell-dns", Mode: "shadow", InputSource: platformproducer.DNSProducerInputSource,
		IntervalSeconds: 30, RefreshSeconds: 120, DNSSource: &platformproducer.DNSProducerSource{ProducerPolicy: *previous.ProducerPolicy,
			PreviousTopology: req.Intent.RouteAuthorityTransition.PreviousTopology, DNSNodeIDs: []string{"dns-a"}}}
	makeArtifact := func(policy platformproducer.Policy) model.PlatformArtifact {
		raw, err := json.Marshal(policy)
		var content map[string]any
		if err != nil || json.Unmarshal(raw, &content) != nil {
			t.Fatal(err)
		}
		scope, err := platformproducer.PolicyScopeForTarget(policy.TargetScope)
		if err != nil {
			t.Fatal(err)
		}
		return celldns.Sign(t, model.PlatformArtifact{ArtifactKind: model.PlatformArtifactKindPolicySnapshot,
			Scope: model.PlatformArtifactScope{Key: scope}, Generation: policy.Generation, Content: content}, "typed-producer-source", 1)
	}
	node, err := compilerInputArtifactHotNode(makeArtifact(p))
	want := map[inputarchive.HotReference]bool{
		{Kind: "artifact", ID: previous.ProducerPolicy.ArtifactID, ExpectedHash: previous.ProducerPolicy.ContentHash}: true,
		{Kind: "release", ID: previous.ProducerPolicy.ReleaseID}:                                                      true,
	}
	if err != nil || len(node.References) != len(want) {
		t.Fatal("DNS producer omitted its typed source or invented empty static refs", node, err)
	}
	for _, ref := range node.References {
		if !want[ref] {
			t.Fatal("DNS producer changed exact nested source identity", ref)
		}
	}
	var static platformproducer.Policy
	for i, policy := range policies {
		if policy.ID == previous.ProducerPolicy.ArtifactID {
			if releases[i].ID != previous.ProducerPolicy.ReleaseID {
				t.Fatal("fixture source activation mismatch")
			}
			static, err = platformproducer.Decode(policy)
		}
	}
	if err != nil || static.StaticIntentArtifactID == "" {
		t.Fatal("missing source policy", err)
	}
	static.Mode, static.Serving, static.RequireApplicationDomains, static.RequireRouteDefaults, static.RequireDNSQueryPolicy = "shadow", nil, false, false, false
	static.DNSPolicyArtifactID, static.DNSPolicyDigest = "", ""
	node, err = compilerInputArtifactHotNode(makeArtifact(static))
	if err != nil || len(node.References) != 1 || node.References[0].ID != static.StaticIntentArtifactID {
		t.Fatal("optional absent projection hid valid static source", node, err)
	}
	p.DNSSource.ProducerPolicy.ContentHash = "invalid"
	if _, err := compilerInputArtifactHotNode(makeArtifact(p)); err == nil {
		t.Fatal("malformed nested source was ignored")
	}
}

func TestCompilerInputHotDependenciesRetainDNSPublicationAndProducerAncestors(t *testing.T) {
	for _, transition := range []bool{false, true} {
		req, policies, _ := celldns.SourceAuthorizedRequest(t, transition)
		compiled := celldns.Compile(t, req)
		node, err := compilerInputArtifactHotNode(compiled.DNSArtifact)
		if err != nil {
			t.Fatal(err)
		}
		has := func(kind, id, hash string) bool {
			for _, ref := range node.References {
				if ref.Kind == kind && ref.ID == id && ref.ExpectedHash == hash {
					return true
				}
			}
			return false
		}
		for _, publication := range req.CellRoutePublications {
			for _, artifact := range []model.PlatformArtifact{publication.Parent, publication.Route, publication.TLS} {
				if !has("artifact", artifact.ID, artifact.ContentHash) {
					t.Fatal("DNS child lost its exact embedded publication ancestor", artifact.ID, node)
				}
			}
			if !has("release", publication.Reference.ReleaseID, "") || !has("release", publication.ProducerPolicy.ReleaseID, "") {
				t.Fatal("DNS source lost publication/producer release identity", node)
			}
		}
		if previous := req.PreviousTrafficPublication; previous != nil {
			for _, artifact := range []model.PlatformArtifact{previous.Parent, previous.Route, previous.TLS, previous.DNS} {
				if !has("artifact", artifact.ID, artifact.ContentHash) {
					t.Fatal("DNS transition lost the previous complete publication", artifact.ID, node)
				}
			}
		}
		for _, policy := range policies {
			if !has("artifact", policy.ID, policy.ContentHash) {
				t.Fatal("DNS source lost authorized producer policy", policy.ID, node)
			}
			policyNode, err := compilerInputArtifactHotNode(policy)
			if err != nil || len(policyNode.References) < 2 {
				t.Fatal("producer lost its typed immutable input declarations", policyNode, err)
			}
		}
	}
}

func TestCompilerInputHotTaskReferencesInspectNestedActivitiesAndRefuseInvalidReferences(t *testing.T) {
	input := "sha256:" + strings.Repeat("d", 64)
	refs, err := compilerInputHotTaskReferences(map[string]any{"nested": []any{
		map[string]any{"artifact_id": "artifact-neutral"},
		map[string]any{"deeper": map[string]any{"input_snapshot_digest": input}},
	}})
	if err != nil || len(refs) != 2 {
		t.Fatal("nested task dependencies disappeared", refs, err)
	}
	want := map[inputarchive.HotReference]bool{{Kind: "artifact", ID: "artifact-neutral"}: true, {Kind: "input", ID: input}: true}
	got := map[inputarchive.HotReference]bool{}
	for _, ref := range refs {
		got[ref] = true
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("nested dependency identities changed", got)
	}
	for _, payload := range []any{
		map[string]any{"artifact_id": float64(1)},
		map[string]any{"input_snapshot_digest": "generation-is-not-a-digest"},
		map[string]any{"release_id": strings.Repeat("x", 513)},
	} {
		if _, err := compilerInputHotTaskReferences(payload); err == nil {
			t.Fatal("uninterpretable reference was silently omitted", payload)
		}
	}
	var deep any = map[string]any{"artifact_id": "artifact-neutral"}
	for i := 0; i < 33; i++ {
		deep = []any{deep}
	}
	if _, err := compilerInputHotTaskReferences(deep); err == nil {
		t.Fatal("excessive payload depth was treated as a complete scan")
	}
}

func TestCompilerInputHotDependenciesBindCompositionAndRejectConflictingLineage(t *testing.T) {
	input := "sha256:" + strings.Repeat("a", 64)
	artifact := model.PlatformArtifact{ID: "parent-neutral", ArtifactKind: model.PlatformArtifactKindReleaseSet,
		ContentHash: "sha256:" + strings.Repeat("b", 64), Metadata: map[string]string{"input_snapshot_digest": input},
		Content: map[string]any{"artifact_ids": []string{"child-a", "child-b"}, "lineage": platformconfig.Lineage{InputSnapshotDigest: input}}}
	node, err := compilerInputArtifactHotNode(artifact)
	if err != nil || len(node.References) != 3 {
		t.Fatal("release composition/input edges missing", node, err)
	}
	artifact.Content["lineage"] = platformconfig.Lineage{InputSnapshotDigest: "sha256:" + strings.Repeat("c", 64)}
	if _, err = compilerInputArtifactHotNode(artifact); err == nil {
		t.Fatal("conflicting content/metadata lineage accepted")
	}
	artifact.Content["lineage"] = nil
	if _, err = compilerInputArtifactHotNode(artifact); err == nil {
		t.Fatal("explicit null lineage treated as no lineage")
	}
	delete(artifact.Content, "lineage")
	for _, children := range []any{nil, []string{}, []string{"child", "child"}, []string{artifact.ID}, []any{5}, strings.Repeat("x", 100)} {
		artifact.Content["artifact_ids"] = children
		if _, err = compilerInputArtifactHotNode(artifact); err == nil {
			t.Fatal("invalid composition accepted", children)
		}
	}
}

func compilerInputHotLineageFixture(t *testing.T, compact bool) platformconfig.CompileResult {
	t.Helper()
	req := platformconfig.CompileRequest{Intent: platformconfig.PlatformIntent{Generation: "intent-neutral"},
		Policy:          platformconfig.PolicySnapshot{Generation: "policy-neutral"},
		RuntimeSnapshot: platformconfig.RuntimeSnapshot{Facts: map[string]any{"audit": "retained-original"}}}
	if compact {
		req.Policy.RuntimeInputMode = platformconfig.RuntimeInputConfigurationV1
	}
	compiled, err := platformconfig.Compile(req)
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []*model.PlatformArtifact{&compiled.IntentArtifact, &compiled.PolicyArtifact, &compiled.RouteArtifact, &compiled.DNSArtifact, &compiled.TLSArtifact} {
		*artifact, err = normalizePlatformArtifactForStore(*artifact)
		if err != nil {
			t.Fatal(err)
		}
	}
	compiled.ReleaseArtifact, err = normalizePlatformArtifactForStore(platformconfig.BuildReleaseSetArtifact(compiled.ReleaseSet,
		[]string{compiled.RouteArtifact.ID, compiled.DNSArtifact.ID, compiled.TLSArtifact.ID}, compiled.RouteArtifact.CreatedAt))
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestCompilerInputHotRetentionKeepsExactCompilerSources(t *testing.T) {
	for _, compact := range []bool{false, true} {
		compiled := compilerInputHotLineageFixture(t, compact)
		for _, artifact := range []model.PlatformArtifact{compiled.ReleaseArtifact, compiled.RouteArtifact, compiled.DNSArtifact, compiled.TLSArtifact} {
			node, err := compilerInputArtifactHotNode(artifact)
			if err != nil {
				t.Fatal(err)
			}
			for _, source := range []model.PlatformArtifact{compiled.IntentArtifact, compiled.PolicyArtifact} {
				want := inputarchive.HotReference{Kind: "generation", ID: source.Generation,
					ArtifactKind: source.ArtifactKind, ScopeKey: source.ScopeKey}
				found := false
				for _, ref := range node.References {
					found = found || ref == want
				}
				if !found {
					t.Fatal("retention lost the exact original source", compact, artifact.ArtifactKind, want)
				}
			}
			// Operational admission keeps its established dependency contract;
			// historical provenance is not a new serving dependency.
			runtime, err := compilerInputArtifactRuntimeNode(artifact)
			if err != nil {
				t.Fatal(err)
			}
			for _, ref := range runtime.References {
				if ref.Kind == "generation" && ref.ArtifactKind == model.PlatformArtifactKindPlatformIntent {
					t.Fatal("retained source became an operational dependency")
				}
			}
		}
	}
}

func TestCompilerInputHotRetentionRefusesIncompleteOrChangedCompilerSources(t *testing.T) {
	compiled := compilerInputHotLineageFixture(t, false)
	for _, change := range []func(*model.PlatformArtifact){
		func(a *model.PlatformArtifact) { delete(a.Content, "lineage") },
		func(a *model.PlatformArtifact) { a.ScopeKey = "" },
		func(a *model.PlatformArtifact) { delete(a.Metadata, "intent_digest") },
		func(a *model.PlatformArtifact) { a.Metadata["policy_digest"] = "sha256:" + strings.Repeat("a", 64) },
		func(a *model.PlatformArtifact) {
			lineage := compiled.Lineage
			lineage.IntentGeneration = "changed-generation"
			a.Content["lineage"] = lineage
		},
	} {
		raw, err := json.Marshal(compiled.RouteArtifact)
		var artifact model.PlatformArtifact
		if err != nil || json.Unmarshal(raw, &artifact) != nil {
			t.Fatal("fixture copy failed")
		}
		change(&artifact)
		if _, err := compilerInputArtifactHotNode(artifact); err == nil {
			t.Fatal("incomplete or changed compiler source was treated as a complete graph")
		}
	}
}
