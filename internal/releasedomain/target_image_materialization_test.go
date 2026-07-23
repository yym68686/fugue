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

func TestMaterializeLiveRelativeTargetPublishedImageRefsPreservesOnlyBuiltOnlyPublicEdgeChecksum(t *testing.T) {
	baseCommit := strings.Repeat("1", 40)
	targetCommit := strings.Repeat("2", 40)
	oldDigest := "sha256:" + strings.Repeat("a", 64)
	newDigest := "sha256:" + strings.Repeat("b", 64)
	changedDigest := "sha256:" + strings.Repeat("c", 64)
	plan, err := NewBuildArtifactPlan(baseCommit, targetCommit, changedDigest, []BuildArtifact{{
		Name: "edge", SourceBaseCommit: baseCommit, ArtifactDigest: newDigest,
		ProvenanceDigest:  "sha256:" + strings.Repeat("d", 64),
		PublishedImageRef: "registry.test/edge@" + newDigest,
	}})
	if err != nil {
		t.Fatal(err)
	}
	ownership := publicEdgeTargetMaterializationOwnership()
	base := publicEdgeTargetMaterializationManifest("live-checksum", "registry.test/edge@"+oldDigest)
	builtOnlyTarget := publicEdgeTargetMaterializationManifest("target-checksum", "registry.test/edge@"+oldDigest)

	t.Run("built-only edge retains live checksum", func(t *testing.T) {
		releasePlan := publicEdgeTargetMaterializationPlan(t, base, builtOnlyTarget, ownership, changedDigest, DomainControlPlane)
		materialized, err := MaterializeLiveRelativeTargetPublishedImageRefs(
			base, builtOnlyTarget, ownership, "fugue-system", targetCommit, plan, releasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(materialized, []byte("checksum/edge-blue-green-front: live-checksum")) ||
			bytes.Contains(materialized, []byte("checksum/edge-blue-green-front: target-checksum")) {
			t.Fatalf("built-only edge checksum was not preserved:\n%s", materialized)
		}
	})

	t.Run("activated edge retains target checksum", func(t *testing.T) {
		activatedTarget := publicEdgeTargetMaterializationManifest("target-checksum", "registry.test/edge:"+targetCommit)
		releasePlan := publicEdgeTargetMaterializationPlan(t, base, activatedTarget, ownership, changedDigest, DomainAuthoritativeDNS)
		materialized, err := MaterializeLiveRelativeTargetPublishedImageRefs(
			base, activatedTarget, ownership, "fugue-system", targetCommit, plan, releasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(materialized, []byte("checksum/edge-blue-green-front: target-checksum")) ||
			!bytes.Contains(materialized, []byte("registry.test/edge@"+newDigest)) {
			t.Fatalf("activated edge evidence was suppressed:\n%s", materialized)
		}
	})

	t.Run("authoritative dns source change retains target checksum", func(t *testing.T) {
		releasePlan := publicEdgeTargetMaterializationPlan(t, base, builtOnlyTarget, ownership, changedDigest, DomainAuthoritativeDNS)
		materialized, err := MaterializeLiveRelativeTargetPublishedImageRefs(
			base, builtOnlyTarget, ownership, "fugue-system", targetCommit, plan, releasePlan,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Contains(materialized, []byte("checksum/edge-blue-green-front: target-checksum")) {
			t.Fatalf("authoritative-dns checksum change was suppressed:\n%s", materialized)
		}
	})
}

func publicEdgeTargetMaterializationPlan(
	t *testing.T,
	base, target, ownership []byte,
	changedDigest string,
	fileDomain Domain,
) Plan {
	t.Helper()
	spec, err := LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatal(err)
	}
	context, err := NewClassificationContextEvidence(
		"fugue-system",
		map[string]string{"releaseNamespace": "fugue-system"},
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	files := FileClassification{
		Domains:  []Domain{fileDomain},
		Evidence: []Evidence{{Source: "changed-file", Subject: "fixture", Domains: []Domain{fileDomain}}},
	}
	rendered := ClassifyRendered(base, target, spec, RenderedOptions{DefaultNamespace: "fugue-system"})
	return BuildPlan(PlanInput{
		Files: files, Rendered: rendered,
		Digests: DigestEvidence{
			Base: "sha256:" + strings.Repeat("e", 64), Target: "sha256:" + strings.Repeat("f", 64),
			Live:         "sha256:" + strings.Repeat("e", 64),
			BaseManifest: digestBytesSHA256(base), TargetManifest: digestBytesSHA256(target),
			RepeatedTargetManifest: digestBytesSHA256(target), Ownership: digestBytesSHA256(ownership),
			ChangedFiles: changedDigest, ClassificationContext: context,
		},
	})
}

func publicEdgeTargetMaterializationManifest(checksum, image string) []byte {
	return []byte("apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: fugue-edge-front\n  namespace: fugue-system\n  labels:\n    fugue.io/rollout-subsystem: public-data-plane\n    fugue.io/rollout-mode: node-local-blue-green-front\nspec:\n  selector:\n    matchLabels:\n      app: fugue-edge-front\n  template:\n    metadata:\n      annotations:\n        checksum/edge-blue-green-front: " + checksum + "\n      labels:\n        app: fugue-edge-front\n    spec:\n      containers:\n        - name: edge-front\n          image: " + image + "\n")
}

func publicEdgeTargetMaterializationOwnership() []byte {
	return []byte("apiVersion: release-domain.fugue.dev/v1\nkind: ReleaseDomainOwnership\ndomains:\n  - node-local\n  - authoritative-dns\n  - control-plane\n  - image-cache\n  - backup\nrequiredBindings: []\nfileRules: []\nvalueRules: []\nobjectRules:\n  - id: public-edge-front\n    domain: authoritative-dns\n    apiGroup: apps\n    version: v1\n    kind: DaemonSet\n    scope: Namespaced\n    namespace: fugue-system\n    name: fugue-edge-front\n    requiredLabels:\n      fugue.io/rollout-subsystem: public-data-plane\n      fugue.io/rollout-mode: node-local-blue-green-front\n")
}

func targetMaterializationOwnership() []byte {
	return []byte("apiVersion: release-domain.fugue.dev/v1\nkind: ReleaseDomainOwnership\ndomains:\n  - node-local\n  - authoritative-dns\n  - control-plane\n  - image-cache\n  - backup\nrequiredBindings: []\nfileRules: []\nvalueRules: []\nobjectRules:\n  - id: api\n    domain: control-plane\n    apiGroup: apps\n    version: v1\n    kind: Deployment\n    scope: Namespaced\n    namespace: fugue-system\n    name: fugue-api\n")
}
