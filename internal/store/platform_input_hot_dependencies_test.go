package store

import (
	"reflect"
	"strings"
	"testing"

	"fugue/internal/inputarchive"
	"fugue/internal/model"
	"fugue/internal/platformconfig"
	"fugue/internal/testfixture/celldns"
)

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
