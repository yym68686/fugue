package declarativerelease

import (
	"bytes"
	"testing"
)

func TestResourceSetRequiresOrderedUniqueIdentitiesAndPrimary(t *testing.T) {
	valid := `{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"}}},{"apiVersion":"v1","kind":"Service","metadata":{"name":"fugue-api-tls","namespace":"fugue-system"}}],"kind":"ComponentResourceSet"}`
	set, err := DecodeResourceSet(bytes.NewBufferString(valid))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := set.Primary(Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Replicas: 2, RolloutMode: "rolling"}); err != nil {
		t.Fatal(err)
	}
	reversed := `{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"v1","kind":"Service","metadata":{"name":"fugue-api-tls","namespace":"fugue-system"}},{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"strategy":{"type":"RollingUpdate"}}}],"kind":"ComponentResourceSet"}`
	if _, err := DecodeResourceSet(bytes.NewBufferString(reversed)); err == nil {
		t.Fatal("out-of-order resource set was accepted")
	}
}

func TestResourceDesiredSubsetAllowsServerDefaultsButRejectsDesiredDrift(t *testing.T) {
	desired := map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{"name": "fugue-api-tls", "namespace": "fugue-system"},
		"spec":     map[string]any{"ports": []any{map[string]any{"port": 8443.0}}},
	}
	live := deepCopyMap(desired)
	liveMetadata := live["metadata"].(map[string]any)
	liveMetadata["uid"] = "generated"
	live["spec"].(map[string]any)["clusterIP"] = "10.43.0.1"
	if !ResourceDesiredSubset(desired, live) {
		t.Fatal("server-populated fields made desired subset fail")
	}
	live["spec"].(map[string]any)["ports"].([]any)[0].(map[string]any)["port"] = 9443.0
	if ResourceDesiredSubset(desired, live) {
		t.Fatal("desired port drift was accepted")
	}
}

func TestBindManifestCASBindsPresentResourcesAndAllowsDeclaredCreate(t *testing.T) {
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"template":{"metadata":{},"spec":{"containers":[]}}}},{"apiVersion":"v1","kind":"Service","metadata":{"name":"fugue-api-tls","namespace":"fugue-system"}}],"kind":"ComponentResourceSet"}`)
	observation := stableObservation("api-uid", "50", "ghcr.io/example/fugue-api@"+testDigest, testSHA1)
	observation.Resources = append(observation.Resources, ResourceObservation{
		Identity: ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: "fugue-system", Name: "fugue-api-tls"},
	})
	bound, err := BindManifestCAS(manifest, observation)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Count(bound, []byte(`"uid":"api-uid"`)) != 1 || bytes.Contains(bound, []byte(`"uid":""`)) {
		t.Fatalf("resource-set CAS did not preserve create semantics: %s", bound)
	}
}

func TestRetainOnRollbackIsRestrictedToPVC(t *testing.T) {
	unsafe := `{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"v1","kind":"Service","metadata":{"annotations":{"fugue.pro/release-retain-on-rollback":"true"},"name":"fugue-api-tls","namespace":"fugue-system"}}],"kind":"ComponentResourceSet"}`
	if _, err := DecodeResourceSet(bytes.NewBufferString(unsafe)); err == nil {
		t.Fatal("non-PVC retained resource was accepted")
	}
}

func TestPredecessorConvergenceManifestOnlyDropsReleaseOwnershipMetadata(t *testing.T) {
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"annotations":{"fugue.pro/artifact-receipt-digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","fugue.pro/production-config-sha":"1111111111111111111111111111111111111111","fugue.pro/release-plan-digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","stable.example/key":"keep"},"labels":{"app.kubernetes.io/managed-by":"new-owner","stable.example/key":"keep"},"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"1111111111111111111111111111111111111111"}},"spec":{"containers":[{"image":"ghcr.io/example/fugue-api@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","name":"api"}]}}}}],"kind":"ComponentResourceSet"}`)
	witness, err := PredecessorConvergenceManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, removed := range [][]byte{
		[]byte(`app.kubernetes.io/managed-by`),
		[]byte(`fugue.pro/artifact-receipt-digest`),
		[]byte(`fugue.pro/production-config-sha`),
		[]byte(`fugue.pro/release-plan-digest`),
	} {
		if bytes.Contains(witness, removed) {
			t.Fatalf("release-only metadata %q remains in predecessor witness: %s", removed, witness)
		}
	}
	for _, retained := range [][]byte{
		[]byte(`stable.example/key`),
		[]byte(`fugue.pro/source-commit`),
		[]byte(`"replicas":2`),
		[]byte(`ghcr.io/example/fugue-api@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc`),
	} {
		if !bytes.Contains(witness, retained) {
			t.Fatalf("operational predecessor field %q was dropped: %s", retained, witness)
		}
	}
}
