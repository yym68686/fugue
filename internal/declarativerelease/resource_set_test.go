package declarativerelease

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
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

func TestResourceDesiredSubsetComparesKubernetesMapListsBySchemaKey(t *testing.T) {
	desired := map[string]any{
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{
				"name": "guardian",
				"volumeMounts": []any{
					map[string]any{"name": "tmp", "mountPath": "/tmp"},
					map[string]any{"name": "recovery", "mountPath": "/var/run/recovery", "readOnly": true},
				},
			}},
			"volumes": []any{
				map[string]any{"name": "tmp", "emptyDir": map[string]any{}},
				map[string]any{"name": "recovery", "secret": map[string]any{"secretName": "recovery-key"}},
			},
		}}},
	}
	live := deepCopyMap(desired)
	podSpec := live["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	podSpec["volumes"] = []any{
		map[string]any{"name": "recovery", "secret": map[string]any{"secretName": "recovery-key", "defaultMode": json.Number("420")}},
		map[string]any{"name": "tmp", "emptyDir": map[string]any{}},
	}
	container := podSpec["containers"].([]any)[0].(map[string]any)
	container["volumeMounts"] = []any{
		map[string]any{"name": "recovery", "mountPath": "/var/run/recovery", "readOnly": true},
		map[string]any{"name": "tmp", "mountPath": "/tmp"},
	}
	if !ResourceDesiredSubset(desired, live) {
		t.Fatal("Kubernetes map-list reordering was treated as resource drift")
	}

	drifted := deepCopyMap(live)
	driftedSpec := drifted["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	driftedSpec["volumes"].([]any)[0].(map[string]any)["secret"].(map[string]any)["secretName"] = "other-key"
	if ResourceDesiredSubset(desired, drifted) {
		t.Fatal("Kubernetes map-list value drift was accepted")
	}

	positionalDesired := map[string]any{"items": []any{map[string]any{"name": "first"}, map[string]any{"name": "second"}}}
	positionalLive := map[string]any{"items": []any{map[string]any{"name": "second"}, map[string]any{"name": "first"}}}
	if ResourceDesiredSubset(positionalDesired, positionalLive) {
		t.Fatal("an undeclared positional list was treated as a Kubernetes map list")
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
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"annotations":{"fugue.pro/artifact-receipt-digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","fugue.pro/production-config-sha":"1111111111111111111111111111111111111111","fugue.pro/release-plan-digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","stable.example/key":"keep"},"labels":{"app.kubernetes.io/managed-by":"new-owner","stable.example/key":"keep"},"name":"fugue-fugue-api","namespace":"fugue-system"},"spec":{"replicas":2,"template":{"metadata":{"annotations":{"fugue.pro/oci-revision":"1111111111111111111111111111111111111111","fugue.pro/production-config-sha":"1111111111111111111111111111111111111111","fugue.pro/source-commit":"1111111111111111111111111111111111111111"}},"spec":{"containers":[{"image":"ghcr.io/example/fugue-api@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","name":"api"}]}}}}],"kind":"ComponentResourceSet"}`)
	witness, err := PredecessorConvergenceManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, removed := range [][]byte{
		[]byte(`app.kubernetes.io/managed-by`),
		[]byte(`fugue.pro/artifact-receipt-digest`),
		[]byte(`fugue.pro/oci-revision`),
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

func TestBootstrapPredecessorAllowsOnlyAbsentForwardInitContainer(t *testing.T) {
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"edge-worker-a","namespace":"fugue-system"},"spec":{"template":{"metadata":{"annotations":{"fugue.pro/source-commit":"1111111111111111111111111111111111111111","stable.example/key":"keep"}},"spec":{"containers":[{"image":"ghcr.io/example/fugue-edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"edge"},{"image":"docker.io/library/caddy@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","name":"caddy"}]}}}}],"kind":"ComponentResourceSet"}`)
	release := PlanRelease{ArtifactTargets: []ArtifactTarget{
		{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-worker-a", Container: "edge", ContainerType: "container"},
		{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-worker-a", Container: "edge-workload-identity", ContainerType: "init-container"},
	}}
	witness, err := BootstrapPredecessorConvergenceManifest(manifest, release)
	if err != nil {
		t.Fatalf("forward-only init container made bootstrap LKG invalid: %v", err)
	}
	if bytes.Contains(witness, []byte("ghcr.io/example/fugue-edge")) {
		t.Fatal("bootstrap convergence witness retained primary image identity")
	}
	if bytes.Contains(witness, []byte("docker.io/library/caddy")) {
		t.Fatal("bootstrap convergence witness retained sidecar image identity")
	}
	if bytes.Contains(witness, []byte("fugue.pro/source-commit")) || !bytes.Contains(witness, []byte("stable.example/key")) {
		t.Fatalf("bootstrap convergence witness did not isolate renderer source identity: %s", witness)
	}
	invalid := bytes.Replace(manifest, []byte(`"containers":[`), []byte(`"containers":"invalid","ignored":[`), 1)
	if _, err := BootstrapPredecessorConvergenceManifest(invalid, release); err == nil {
		t.Fatal("invalid primary container list was accepted")
	}
}

func TestRetryPredecessorConvergenceManifestDropsOnlyRenderedTargetIdentity(t *testing.T) {
	release := PlanRelease{
		Workload: Workload{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "telemetry", Container: "telemetry-agent", FieldManager: "fugue-telemetry-declarative"},
	}
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"annotations":{"fugue.pro/artifact-receipt-digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","fugue.pro/production-config-sha":"1111111111111111111111111111111111111111","fugue.pro/release-plan-digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","stable.example/key":"keep"},"labels":{"app.kubernetes.io/managed-by":"fugue-telemetry-declarative"},"name":"telemetry","namespace":"fugue-system"},"spec":{"replicas":1,"strategy":{"type":"RollingUpdate"},"template":{"metadata":{"annotations":{"fugue.pro/oci-revision":"1111111111111111111111111111111111111111","fugue.pro/production-config-sha":"1111111111111111111111111111111111111111","fugue.pro/source-commit":"1111111111111111111111111111111111111111"}},"spec":{"containers":[{"env":[{"name":"LIMIT","value":"16"}],"image":"ghcr.io/example/telemetry@sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc","name":"telemetry-agent"}]}}}}],"kind":"ComponentResourceSet"}`)
	witnessRaw, err := RetryPredecessorConvergenceManifest(manifest, release)
	if err != nil {
		t.Fatal(err)
	}
	for _, removed := range []string{"fugue.pro/artifact-receipt-digest", "fugue.pro/production-config-sha", "fugue.pro/release-plan-digest", "fugue.pro/oci-revision", "fugue.pro/source-commit", "ghcr.io/example/telemetry@"} {
		if bytes.Contains(witnessRaw, []byte(removed)) {
			t.Fatalf("rendered identity %q remains in retry witness: %s", removed, witnessRaw)
		}
	}
	for _, retained := range []string{"app.kubernetes.io/managed-by", "fugue-telemetry-declarative", "stable.example/key", `"name":"LIMIT"`, `"value":"16"`, `"replicas":1`} {
		if !bytes.Contains(witnessRaw, []byte(retained)) {
			t.Fatalf("operational field %q was dropped: %s", retained, witnessRaw)
		}
	}
	witness, err := DecodeResourceSet(bytes.NewReader(witnessRaw))
	if err != nil {
		t.Fatal(err)
	}
	live := deepCopyMap(witness.Items[0])
	container := live["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)
	container["image"] = "ghcr.io/example/telemetry@sha256:" + strings.Repeat("9", 64)
	live["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"] = map[string]any{"fugue.pro/source-commit": strings.Repeat("9", 40)}
	if !ResourceDesiredSubset(witness.Items[0], live) {
		t.Fatal("exact operational predecessor with different rendered identity was rejected")
	}
	for name, mutate := range map[string]func(map[string]any){
		"manager": func(value map[string]any) {
			value["metadata"].(map[string]any)["labels"].(map[string]any)["app.kubernetes.io/managed-by"] = "other"
		},
		"env": func(value map[string]any) {
			value["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["env"].([]any)[0].(map[string]any)["value"] = "64"
		},
		"replicas": func(value map[string]any) { value["spec"].(map[string]any)["replicas"] = 2 },
	} {
		drifted := deepCopyMap(live)
		mutate(drifted)
		if ResourceDesiredSubset(witness.Items[0], drifted) {
			t.Fatalf("%s drift was accepted", name)
		}
	}
}

func TestPredecessorConvergenceAcceptsLegacyTelemetryWithoutRendererAnnotations(t *testing.T) {
	const source = "5a3b09c571601993367c50561b257dd6b9e743ca"
	const image = "ghcr.io/yym68686/fugue-telemetry-agent@sha256:6773f84b084d6808e147cea643becc90ece225972032cf6f7fd8b58988570700"
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"annotations":{"fugue.pro/artifact-receipt-digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","fugue.pro/production-config-sha":"dbbe33daee539f0c38f94965177c2524889adaf5","fugue.pro/release-plan-digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","fugue.pro/telemetry-ownership":"declarative"},"labels":{"app.kubernetes.io/component":"telemetry-agent","app.kubernetes.io/instance":"fugue","app.kubernetes.io/managed-by":"fugue-telemetry-declarative","app.kubernetes.io/name":"fugue"},"name":"fugue-fugue-telemetry-agent","namespace":"fugue-system"},"spec":{"replicas":1,"revisionHistoryLimit":3,"selector":{"matchLabels":{"app.kubernetes.io/component":"telemetry-agent","app.kubernetes.io/instance":"fugue","app.kubernetes.io/name":"fugue"}},"strategy":{"rollingUpdate":{"maxSurge":"25%","maxUnavailable":"25%"},"type":"RollingUpdate"},"template":{"metadata":{"annotations":{"fugue.pro/oci-revision":"` + source + `","fugue.pro/production-config-sha":"dbbe33daee539f0c38f94965177c2524889adaf5","fugue.pro/source-commit":"` + source + `"},"labels":{"app.kubernetes.io/component":"telemetry-agent","app.kubernetes.io/instance":"fugue","app.kubernetes.io/name":"fugue"}},"spec":{"containers":[{"env":[{"name":"FUGUE_OBSERVABILITY_BATCH_SIZE","value":"512"},{"name":"FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES","value":"134217728"}],"image":"` + image + `","imagePullPolicy":"IfNotPresent","name":"telemetry-agent"}],"serviceAccountName":"fugue-fugue-sa","terminationGracePeriodSeconds":20}}}}],"kind":"ComponentResourceSet"}`)
	live := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"annotations": map[string]any{"fugue.pro/telemetry-ownership": "declarative", "helm.sh/resource-policy": "keep"},
			"labels":      map[string]any{"app.kubernetes.io/component": "telemetry-agent", "app.kubernetes.io/instance": "fugue", "app.kubernetes.io/managed-by": "fugue-telemetry-declarative", "app.kubernetes.io/name": "fugue", "helm.sh/chart": "fugue-0.1.0"},
			"name":        "fugue-fugue-telemetry-agent", "namespace": "fugue-system", "uid": "c456dfa2-f54a-4053-b832-4b205d1dcfcd", "resourceVersion": "68861966", "generation": 423,
		},
		"spec": map[string]any{
			"replicas": 1.0, "revisionHistoryLimit": 3.0,
			"selector": map[string]any{"matchLabels": map[string]any{"app.kubernetes.io/component": "telemetry-agent", "app.kubernetes.io/instance": "fugue", "app.kubernetes.io/name": "fugue"}},
			"strategy": map[string]any{"rollingUpdate": map[string]any{"maxSurge": "25%", "maxUnavailable": "25%"}, "type": "RollingUpdate"},
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": source}, "labels": map[string]any{"app.kubernetes.io/component": "telemetry-agent", "app.kubernetes.io/instance": "fugue", "app.kubernetes.io/name": "fugue"}},
				"spec":     map[string]any{"containers": []any{map[string]any{"env": []any{map[string]any{"name": "FUGUE_OBSERVABILITY_BATCH_SIZE", "value": "512"}, map[string]any{"name": "FUGUE_OBSERVABILITY_MEMORY_LIMIT_BYTES", "value": "134217728"}}, "image": image, "imagePullPolicy": "IfNotPresent", "name": "telemetry-agent"}}, "serviceAccountName": "fugue-fugue-sa", "terminationGracePeriodSeconds": 20.0},
			},
		},
	}
	liveRaw, err := CanonicalJSON(live)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(liveRaw))
	decoder.UseNumber()
	if err := decoder.Decode(&live); err != nil {
		t.Fatal(err)
	}
	witnessRaw, err := PredecessorConvergenceManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	witnessSet, err := DecodeResourceSet(bytes.NewReader(witnessRaw))
	if err != nil {
		t.Fatal(err)
	}
	witness := witnessSet.Items[0]
	if !ResourceDesiredSubset(witness, live) {
		t.Fatal("real legacy Telemetry LKG did not satisfy the normalized predecessor witness")
	}
	withRendererAnnotation := deepCopyMap(witness)
	withRendererAnnotation["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)["fugue.pro/oci-revision"] = source
	if ResourceDesiredSubset(withRendererAnnotation, live) {
		t.Fatal("gen7 renderer-owned OCI annotation unexpectedly matched the legacy LKG")
	}
	for name, mutate := range map[string]func(map[string]any){
		"image": func(value map[string]any) {
			value["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["image"] = "ghcr.io/yym68686/fugue-telemetry-agent@sha256:" + string(bytes.Repeat([]byte{'0'}, 64))
		},
		"source": func(value map[string]any) {
			value["spec"].(map[string]any)["template"].(map[string]any)["metadata"].(map[string]any)["annotations"].(map[string]any)["fugue.pro/source-commit"] = string(bytes.Repeat([]byte{'0'}, 40))
		},
		"replicas": func(value map[string]any) { value["spec"].(map[string]any)["replicas"] = 2.0 },
		"env": func(value map[string]any) {
			value["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)["containers"].([]any)[0].(map[string]any)["env"].([]any)[0].(map[string]any)["value"] = "64"
		},
	} {
		drifted := deepCopyMap(live)
		mutate(drifted)
		if ResourceDesiredSubset(witness, drifted) {
			t.Fatalf("%s drift was accepted by predecessor convergence", name)
		}
	}
}

func TestReferencedRequiredSecretsReturnsNamesOnlyAndSkipsOptionalRefs(t *testing.T) {
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","items":[{"apiVersion":"apps/v1","kind":"DaemonSet","metadata":{"name":"edge-worker","namespace":"fugue-system"},"spec":{"selector":{"matchLabels":{"app":"edge"}},"template":{"metadata":{"labels":{"app":"edge"}},"spec":{"containers":[{"env":[{"name":"TOKEN","valueFrom":{"secretKeyRef":{"key":"token","name":"edge-api"}}},{"name":"OPTIONAL","valueFrom":{"secretKeyRef":{"key":"value","name":"optional-config","optional":true}}}],"envFrom":[{"secretRef":{"name":"edge-env"}}],"image":"ghcr.io/example/edge@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"edge"}],"imagePullSecrets":[{"name":"registry-auth"}],"volumes":[{"name":"reader","secret":{"secretName":"edge-reader"}}]}},"updateStrategy":{"type":"OnDelete"}}}],"kind":"ComponentResourceSet"}`)
	names, err := ReferencedRequiredSecrets(manifest)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"edge-api", "edge-env", "edge-reader", "registry-auth"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("required Secret names=%v want=%v", names, want)
	}
	if bytes.Contains([]byte(strings.Join(names, "\n")), []byte("token")) {
		t.Fatal("Secret key/value material escaped the name-only inventory")
	}
}
