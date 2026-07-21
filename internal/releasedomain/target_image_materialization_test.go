package releasedomain

import (
	"bytes"
	"strings"
	"testing"
)

func TestMaterializeTargetPublishedImageRefsReplacesOnlyExactTargetTags(t *testing.T) {
	target := strings.Repeat("2", 40)
	apiDigest := "sha256:" + strings.Repeat("a", 64)
	edgeDigest := "sha256:" + strings.Repeat("b", 64)
	plan, err := NewBuildArtifactPlan(strings.Repeat("1", 40), target, "sha256:"+strings.Repeat("c", 64), []BuildArtifact{
		{Name: "api", SourceBaseCommit: strings.Repeat("1", 40), ArtifactDigest: apiDigest, ProvenanceDigest: "sha256:" + strings.Repeat("d", 64), PublishedImageRef: "registry.test/api@" + apiDigest},
		{Name: "edge", SourceBaseCommit: strings.Repeat("1", 40), ArtifactDigest: edgeDigest, ProvenanceDigest: "sha256:" + strings.Repeat("d", 64), PublishedImageRef: "registry.test/edge@" + edgeDigest},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest := []byte("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: fugue-api\n  namespace: fugue-system\nspec:\n  selector:\n    matchLabels:\n      app: fugue-api\n  template:\n    metadata:\n      labels:\n        app: fugue-api\n    spec:\n      containers:\n        - name: api\n          image: registry.test/api:" + target + "\n        - name: sidecar\n          image: registry.test/sidecar:stable\n")
	materialized, err := MaterializeTargetPublishedImageRefs(manifest, targetMaterializationOwnership(), "fugue-system", target, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(materialized, []byte("registry.test/api@"+apiDigest)) || bytes.Contains(materialized, []byte("registry.test/api:"+target)) {
		t.Fatalf("target image was not materialized exactly:\n%s", materialized)
	}
	if !bytes.Contains(materialized, []byte("registry.test/sidecar:stable")) || bytes.Contains(materialized, []byte("registry.test/edge@")) {
		t.Fatalf("unrelated or built-only image changed:\n%s", materialized)
	}
	repeated, err := MaterializeTargetPublishedImageRefs(materialized, targetMaterializationOwnership(), "fugue-system", target, plan)
	if err != nil || !bytes.Equal(materialized, repeated) {
		t.Fatalf("materialization is not deterministic: err=%v\nfirst=%s\nsecond=%s", err, materialized, repeated)
	}
}

func TestMaterializeTargetPublishedImageRefsFailsClosedOnMissingOrAmbiguousArtifact(t *testing.T) {
	target := strings.Repeat("2", 40)
	digestA := "sha256:" + strings.Repeat("a", 64)
	digestB := "sha256:" + strings.Repeat("b", 64)
	manifest := []byte("apiVersion: apps/v1\nkind: Deployment\nmetadata:\n  name: fugue-api\n  namespace: fugue-system\nspec:\n  selector:\n    matchLabels:\n      app: fugue-api\n  template:\n    metadata:\n      labels:\n        app: fugue-api\n    spec:\n      containers:\n        - name: api\n          image: registry.test/api:" + target + "\n")
	missing, err := NewBuildArtifactPlan(strings.Repeat("1", 40), target, "sha256:"+strings.Repeat("c", 64), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MaterializeTargetPublishedImageRefs(manifest, targetMaterializationOwnership(), "fugue-system", target, missing); err == nil {
		t.Fatal("target-commit image without a published artifact was accepted")
	}
	ambiguous, err := NewBuildArtifactPlan(strings.Repeat("1", 40), target, "sha256:"+strings.Repeat("c", 64), []BuildArtifact{
		{Name: "api", SourceBaseCommit: strings.Repeat("1", 40), ArtifactDigest: digestA, ProvenanceDigest: "sha256:" + strings.Repeat("d", 64), PublishedImageRef: "registry.test/api@" + digestA},
		{Name: "api_alt", SourceBaseCommit: strings.Repeat("1", 40), ArtifactDigest: digestB, ProvenanceDigest: "sha256:" + strings.Repeat("d", 64), PublishedImageRef: "registry.test/api@" + digestB},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MaterializeTargetPublishedImageRefs(manifest, targetMaterializationOwnership(), "fugue-system", target, ambiguous); err == nil {
		t.Fatal("ambiguous published artifact repository was accepted")
	}
}

func targetMaterializationOwnership() []byte {
	return []byte("apiVersion: release-domain.fugue.dev/v1\nkind: ReleaseDomainOwnership\ndomains:\n  - node-local\n  - authoritative-dns\n  - control-plane\n  - image-cache\n  - backup\nrequiredBindings: []\nfileRules: []\nvalueRules: []\nobjectRules:\n  - id: api\n    domain: control-plane\n    apiGroup: apps\n    version: v1\n    kind: Deployment\n    scope: Namespaced\n    namespace: fugue-system\n    name: fugue-api\n")
}
