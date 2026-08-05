package declarativerelease

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRenderManifestsChangesOnlyReleaseIdentityAndSelectedImage(t *testing.T) {
	plan := boundAPIPlan(t)
	verification := RegistryVerification{
		Image:          "ghcr.io/example/fugue-api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		IndexDigest:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ManifestDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ConfigDigest:   "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		OCIRevision:    testSHA2, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 100,
	}
	receipt, err := MaterializeArtifactReceipt(plan, "api", verification)
	if err != nil {
		t.Fatal(err)
	}
	base := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"annotations":{"existing":"keep"},"labels":{"app":"api","app.kubernetes.io/managed-by":"Helm"},"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"selector":{"matchLabels":{"app":"api"}},"strategy":{"type":"RollingUpdate"},"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"old"},"labels":{"app":"api"}},"spec":{"containers":[{"env":[{"name":"KEEP","value":"yes"}],"image":"ghcr.io/example/fugue-api:old","name":"api"},{"image":"example/sidecar@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","name":"sidecar"}]}}}}],"kind":"ComponentResourceSet"}`)
	rendered, err := RenderManifests(plan, "api", receipt, bytes.NewReader(base), bytes.NewReader(base))
	if err != nil {
		t.Fatalf("render manifests: %v", err)
	}
	if !digestPattern.MatchString(rendered.ForwardDigest) || !digestPattern.MatchString(rendered.LKGDigest) || bytes.Equal(rendered.Forward, rendered.LKG) {
		t.Fatalf("invalid rendered identity: %+v", rendered)
	}
	var forward, lkg ResourceSet
	if err := json.Unmarshal(rendered.Forward, &forward); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rendered.LKG, &lkg); err != nil {
		t.Fatal(err)
	}
	forwardContainers := forward.Items[0]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	lkgContainers := lkg.Items[0]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	if forwardContainers[0].(map[string]any)["image"] != verification.Image {
		t.Fatalf("forward image mismatch: %+v", forwardContainers[0])
	}
	if lkgContainers[0].(map[string]any)["image"] != "ghcr.io/example/fugue-api@"+testDigest {
		t.Fatalf("LKG image mismatch: %+v", lkgContainers[0])
	}
	if forwardContainers[1].(map[string]any)["image"] != "example/sidecar@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" || lkgContainers[1].(map[string]any)["image"] != "example/sidecar@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" {
		t.Fatal("non-selected sidecar image changed")
	}
	if forwardContainers[0].(map[string]any)["env"].([]any)[0].(map[string]any)["value"] != "yes" {
		t.Fatal("unrelated workload configuration changed")
	}
	for label, set := range map[string]ResourceSet{"forward": forward, "lkg": lkg} {
		labels := set.Items[0]["metadata"].(map[string]any)["labels"].(map[string]any)
		if labels["app.kubernetes.io/managed-by"] != "Helm" {
			t.Fatalf("%s renderer rewrote declared ownership label: %#v", label, labels)
		}
	}
}

func TestRenderManifestsBindsEveryDeclaredArtifactTarget(t *testing.T) {
	registry := testRegistry()
	registry.Components[0].ArtifactTargets = []ArtifactTarget{
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", ContainerType: "container"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "bootstrap", ContainerType: "init-container"},
		{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api-worker", Container: "api-worker", ContainerType: "container"},
	}
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"cmd/fugue-api/main.go", "deploy/releases/api/intent.json"})
	if err != nil {
		t.Fatal(err)
	}
	intent := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: "api", Generation: 1, ExpectedPreviousPresent: false, Rollback: "previous-git-lkg"}
	plan, err = BindIntents(registry, plan, map[string]Intent{"api": intent}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	verification := RegistryVerification{
		Image: "ghcr.io/example/fugue-api@sha256:" + string(bytes.Repeat([]byte{'b'}, 64)), IndexDigest: "sha256:" + string(bytes.Repeat([]byte{'b'}, 64)),
		ManifestDigest: "sha256:" + string(bytes.Repeat([]byte{'c'}, 64)), ConfigDigest: "sha256:" + string(bytes.Repeat([]byte{'d'}, 64)),
		OCIRevision: testSHA2, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get", BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 10,
	}
	receipt, err := MaterializeArtifactReceipt(plan, "api", verification)
	if err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"image":"placeholder","name":"api"},{"image":"example/sidecar@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","name":"sidecar"}],"initContainers":[{"image":"placeholder","name":"bootstrap"}]}}}},{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api-worker","namespace":"fugue-system"},"spec":{"replicas":1,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"image":"placeholder","name":"api-worker"}]}}}}],"kind":"ComponentResourceSet"}`)
	rendered, err := RenderManifests(plan, "api", receipt, bytes.NewReader(manifest), nil)
	if err != nil {
		t.Fatalf("render multi-workload component: %v", err)
	}
	var forward ResourceSet
	if err := json.Unmarshal(rendered.Forward, &forward); err != nil {
		t.Fatal(err)
	}
	primarySpec := forward.Items[0]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	primaryContainers := primarySpec["containers"].([]any)
	primaryInit := primarySpec["initContainers"].([]any)
	workerContainers := forward.Items[1]["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)
	for label, value := range map[string]any{
		"primary": primaryContainers[0].(map[string]any)["image"],
		"init":    primaryInit[0].(map[string]any)["image"],
		"worker":  workerContainers[0].(map[string]any)["image"],
	} {
		if value != verification.Image {
			t.Fatalf("%s artifact target was not digest-bound: %v", label, value)
		}
	}
	if primaryContainers[1].(map[string]any)["image"] != "example/sidecar@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee" {
		t.Fatal("undeclared sidecar artifact changed")
	}
}

func TestBootstrapLKGPreservesGroupLocalImmutableImages(t *testing.T) {
	group := edgeGroupFixture("gamma", "edge-group-metro-gamma")
	group.Worker.MigrationState = "adopting"
	group.Worker.OwnershipAdoption = &OwnershipAdoption{
		LegacyFieldManager: "helm",
		Resources: []OwnershipAdoptionScope{{
			Identity: ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-front"},
			Fields:   []string{"/spec/template"},
		}},
	}
	registry := Registry{APIVersion: RegistryAPIVersion, Kind: RegistryKind, Components: []Component{group.Worker}}
	plan, err := BuildPlan(registry, testSHA1, testSHA2, []string{"internal/edge/service.go", group.Worker.IntentPath})
	if err != nil {
		t.Fatal(err)
	}
	intent := Intent{APIVersion: IntentAPIVersion, Kind: IntentKind, Component: group.Worker.ID, Generation: 1, ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: testSHA1, ExpectedPreviousManifestSHA: testSHA1, ExpectedPreviousOCIRevision: testSHA1, ExpectedPreviousImageDigest: testDigest, Rollback: "previous-git-lkg"}
	plan, err = BindIntents(registry, plan, map[string]Intent{group.Worker.ID: intent}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := MaterializeArtifactReceipt(plan, group.Worker.ID, RegistryVerification{
		Image: "ghcr.io/example/fugue-edge@sha256:" + strings.Repeat("b", 64), IndexDigest: "sha256:" + strings.Repeat("b", 64),
		ManifestDigest: "sha256:" + strings.Repeat("c", 64), ConfigDigest: "sha256:" + strings.Repeat("d", 64), OCIRevision: testSHA2,
		Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get", BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	forward := edgeWorkerResourceSetFixture("placeholder", "placeholder", testSHA2)
	lkg := edgeWorkerResourceSetFixture("ghcr.io/example/fugue-edge@"+testDigest, "ghcr.io/example/fugue-edge@sha256:"+strings.Repeat("e", 64), testSHA1)
	rendered, err := RenderManifests(plan, group.Worker.ID, receipt, bytes.NewReader(forward), bytes.NewReader(lkg))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(rendered.LKG, []byte("sha256:"+strings.Repeat("e", 64))) {
		t.Fatalf("auxiliary group-local LKG image was overwritten: %s", rendered.LKG)
	}
	if bytes.Count(rendered.Forward, []byte(receipt.ImmutableRef)) != 3 {
		t.Fatalf("forward artifact was not bound across the group: %s", rendered.Forward)
	}
	wrong := bytes.Replace(lkg, []byte(testDigest), []byte("sha256:"+strings.Repeat("f", 64)), 1)
	if _, err := RenderManifests(plan, group.Worker.ID, receipt, bytes.NewReader(forward), bytes.NewReader(wrong)); err == nil {
		t.Fatal("bootstrap LKG with a mismatched primary digest was accepted")
	}
}

func edgeWorkerResourceSetFixture(frontImage, workerImage, source string) []byte {
	return []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"edge-gamma-front","namespace":"fugue-system"},"spec":{"selector":{"matchLabels":{"app":"front"}},"template":{"metadata":{"annotations":{"fugue.pro/oci-revision":"` + source + `","fugue.pro/source-commit":"` + source + `"},"labels":{"app":"front"}},"spec":{"containers":[{"image":"` + frontImage + `","name":"edge-front"}]}},"updateStrategy":{"type":"OnDelete"}}},{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"edge-gamma-worker-a","namespace":"fugue-system"},"spec":{"selector":{"matchLabels":{"app":"worker-a"}},"template":{"metadata":{"annotations":{"fugue.pro/oci-revision":"` + source + `","fugue.pro/source-commit":"` + source + `"},"labels":{"app":"worker-a"}},"spec":{"containers":[{"image":"` + workerImage + `","name":"edge"}]}},"updateStrategy":{"type":"OnDelete"}}},{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"edge-gamma-worker-b","namespace":"fugue-system"},"spec":{"selector":{"matchLabels":{"app":"worker-b"}},"template":{"metadata":{"annotations":{"fugue.pro/oci-revision":"` + source + `","fugue.pro/source-commit":"` + source + `"},"labels":{"app":"worker-b"}},"spec":{"containers":[{"image":"` + workerImage + `","name":"edge"}]}},"updateStrategy":{"type":"OnDelete"}}}],"kind":"ComponentResourceSet"}`)
}

func TestRenderManifestsRejectsMutableOrPlaceholderSidecars(t *testing.T) {
	for _, image := range []string{
		"caddy:latest",
		"registry.invalid/caddy/caddy@sha256:" + strings.Repeat("0", 64),
	} {
		if immutableImageRef(image) {
			t.Fatalf("unsafe sidecar image was accepted: %s", image)
		}
	}
	if !immutableImageRef("docker.io/library/caddy@sha256:" + strings.Repeat("a", 64)) {
		t.Fatal("valid immutable sidecar image was rejected")
	}
}

func TestLKGResourceIdentitiesMayBeAForwardSubsetButNeverASuperset(t *testing.T) {
	deployment := map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "api", "namespace": "fugue-system"}}
	service := map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "api-tls", "namespace": "fugue-system"}}
	forward := ResourceSet{Items: []map[string]any{deployment, service}}
	lkg := ResourceSet{Items: []map[string]any{deployment}}
	if !lkgResourceIdentitiesSubset(forward, lkg) {
		t.Fatal("forward-only created resource was not representable")
	}
	if lkgResourceIdentitiesSubset(lkg, forward) {
		t.Fatal("LKG-only resource was accepted")
	}
	other := ResourceSet{Items: []map[string]any{{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "other", "namespace": "fugue-system"}}}}
	if lkgResourceIdentitiesSubset(forward, other) {
		t.Fatal("unrelated LKG resource was accepted")
	}
}

func TestRenderManifestsRejectsWrongWorkloadAndAmbiguousContainer(t *testing.T) {
	plan := boundAPIPlan(t)
	verification := RegistryVerification{
		Image:          "ghcr.io/example/fugue-api@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		ManifestDigest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		ConfigDigest:   "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		OCIRevision:    testSHA2, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 100,
	}
	receipt, err := MaterializeArtifactReceipt(plan, "api", verification)
	if err != nil {
		t.Fatal(err)
	}
	wrongName := `{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"other","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"name":"api"}]}}}}],"kind":"ComponentResourceSet"}`
	if _, err := RenderManifests(plan, "api", receipt, bytes.NewBufferString(wrongName), nil); err == nil {
		t.Fatal("wrong workload name was accepted")
	}
	ambiguous := `{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"name":"api"},{"name":"api"}]}}}}],"kind":"ComponentResourceSet"}`
	if _, err := RenderManifests(plan, "api", receipt, bytes.NewBufferString(ambiguous), nil); err == nil {
		t.Fatal("ambiguous workload container was accepted")
	}
}

func TestRenderManifestsPreservesExplicitLKGConfigurationAndIdentitySet(t *testing.T) {
	plan := boundAPIPlan(t)
	receipt, err := MaterializeArtifactReceipt(plan, "api", RegistryVerification{
		Image: "ghcr.io/example/fugue-api@sha256:" + string(bytes.Repeat([]byte{'b'}, 64)), IndexDigest: "sha256:" + string(bytes.Repeat([]byte{'b'}, 64)),
		ManifestDigest: "sha256:" + string(bytes.Repeat([]byte{'c'}, 64)), ConfigDigest: "sha256:" + string(bytes.Repeat([]byte{'d'}, 64)),
		OCIRevision: testSHA2, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get", BlobCount: 2, LayerProbeCount: 1, RequestCount: 5, TotalLayerBytes: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	forward := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"env":[{"name":"FORWARD_ONLY","value":"true"}],"image":"placeholder","name":"api"}]}}}}],"kind":"ComponentResourceSet"}`)
	lkg := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{},"spec":{"containers":[{"env":[{"name":"LKG_ONLY","value":"true"}],"image":"placeholder","name":"api"}]}}}}],"kind":"ComponentResourceSet"}`)
	rendered, err := RenderManifests(plan, "api", receipt, bytes.NewReader(forward), bytes.NewReader(lkg))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(rendered.Forward, []byte("FORWARD_ONLY")) || bytes.Contains(rendered.Forward, []byte("LKG_ONLY")) ||
		!bytes.Contains(rendered.LKG, []byte("LKG_ONLY")) || bytes.Contains(rendered.LKG, []byte("FORWARD_ONLY")) {
		t.Fatalf("explicit LKG configuration was not kept independent: forward=%s lkg=%s", rendered.Forward, rendered.LKG)
	}
	wrongLKG := bytes.Replace(lkg, []byte("fugue-fugue-api"), []byte("fugue-other-api"), 1)
	if _, err := RenderManifests(plan, "api", receipt, bytes.NewReader(forward), bytes.NewReader(wrongLKG)); err == nil {
		t.Fatal("LKG with a different resource identity was accepted")
	}
}
