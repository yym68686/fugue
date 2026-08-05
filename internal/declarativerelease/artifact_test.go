package declarativerelease

import (
	"bytes"
	"strings"
	"testing"
)

func boundAPIPlan(t *testing.T) Plan {
	t.Helper()
	registry := testRegistry()
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{
		"cmd/fugue-api/main.go", "deploy/releases/api/intent.json",
	})
	if err != nil {
		t.Fatal(err)
	}
	intent := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 1, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: testSHA1, ExpectedPreviousManifestSHA: testSHA1, ExpectedPreviousOCIRevision: testSHA1, ExpectedPreviousImageDigest: testDigest, Rollback: "previous-git-lkg"}
	bound, err := BindIntents(registry, plan, map[string]Intent{"api": intent}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}

func TestArtifactReceiptRequiresCompleteImmutableVerification(t *testing.T) {
	plan := boundAPIPlan(t)
	verification := RegistryVerification{
		Image:          "ghcr.io/example/fugue-api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		IndexDigest:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ManifestDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ConfigDigest:   "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		OCIRevision:    testSHA2, Platform: "linux/amd64",
		Verification: "registry_manifest_config_and_layer_get", BlobCount: 3, LayerProbeCount: 2, RequestCount: 8, TotalLayerBytes: 100,
	}
	receipt, err := MaterializeArtifactReceipt(plan, "api", verification)
	if err != nil {
		t.Fatalf("materialize artifact receipt: %v", err)
	}
	if !digestPattern.MatchString(receipt.ReceiptDigest) || receipt.ImmutableRef != verification.Image {
		t.Fatalf("invalid artifact receipt: %+v", receipt)
	}
	encoded, err := CanonicalJSON(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeArtifactReceipt(bytes.NewReader(encoded)); err != nil {
		t.Fatalf("decode materialized receipt: %v", err)
	}
	verification.Verification = "registry_manifest_config_get"
	if _, err := MaterializeArtifactReceipt(plan, "api", verification); err == nil {
		t.Fatal("metadata-only verification was accepted for a new build")
	}
}

func TestArtifactReceiptRejectsWrongRevisionAndRepository(t *testing.T) {
	plan := boundAPIPlan(t)
	verification := RegistryVerification{
		Image:          "ghcr.io/example/other@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ManifestDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ConfigDigest:   "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		OCIRevision:    testSHA2, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 10,
	}
	if _, err := MaterializeArtifactReceipt(plan, "api", verification); err == nil {
		t.Fatal("wrong repository was accepted")
	}
	verification.Image = "ghcr.io/example/fugue-api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	verification.OCIRevision = testSHA1
	if _, err := MaterializeArtifactReceipt(plan, "api", verification); err == nil {
		t.Fatal("wrong OCI revision was accepted")
	}
}

func TestBoundPlanRejectsMoreThanOneProductionAtom(t *testing.T) {
	plan := boundAPIPlan(t)
	plan.Releases = append(plan.Releases, plan.Releases[0])
	if err := plan.ValidateBound(); err == nil || !strings.Contains(err.Error(), "more than one component atom") {
		t.Fatalf("multi-component bound plan was accepted: %v", err)
	}
}
