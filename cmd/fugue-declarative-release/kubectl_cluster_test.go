package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestVerifyTargetUsesFixedMetadataOnlyRegistryEvidence(t *testing.T) {
	image := "ghcr.io/example/fugue-telemetry-agent@sha256:" + strings.Repeat("b", 64)
	revision := strings.Repeat("a", 40)
	script := filepath.Join(t.TempDir(), "verify.py")
	program := `import json, sys
image = "ghcr.io/example/fugue-telemetry-agent@sha256:" + "b" * 64
revision = "a" * 40
expected = ["--image", image, "--platform", "linux/amd64", "--expected-revision", revision, "--metadata-only", "--timeout-seconds", "18", "--request-timeout-seconds", "5", "--max-attempts", "2", "--retry-delay-seconds", "0.1"]
if sys.argv[1:] != expected:
    raise SystemExit(2)
print(json.dumps({"image": image, "index_digest": "sha256:" + "b" * 64, "manifest_digest": "sha256:" + "c" * 64, "config_digest": "sha256:" + "d" * 64, "oci_revision": revision, "platform": "linux/amd64", "verification": "registry_manifest_config_get", "blob_count": 0, "layer_get_probe_count": 0, "request_count": 3, "total_layer_bytes": 0}, separators=(",", ":")))
`
	if err := os.WriteFile(script, []byte(program), 0o600); err != nil {
		t.Fatal(err)
	}
	cluster := &kubectlCluster{verifier: script, timeout: time.Second}
	if err := cluster.VerifyTarget(context.Background(), declarativerelease.TargetIdentity{Present: true, ImageRef: image, OCIRevision: revision}); err != nil {
		t.Fatalf("verify exact immutable predecessor: %v", err)
	}
}

func TestSelectorFromWorkloadNarrowsSharedSelectorWithPodComponent(t *testing.T) {
	raw := []byte(`{"spec":{"selector":{"matchLabels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/name":"fugue"}},"template":{"metadata":{"labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/name":"fugue","app.kubernetes.io/component":"api"}}}}}`)
	selector, err := selectorFromWorkload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if selector != "app.kubernetes.io/component=api,app.kubernetes.io/instance=fugue,app.kubernetes.io/name=fugue" {
		t.Fatalf("selector = %q", selector)
	}

	conflicting := []byte(`{"spec":{"selector":{"matchLabels":{"app.kubernetes.io/component":"controller"}},"template":{"metadata":{"labels":{"app.kubernetes.io/component":"api"}}}}}`)
	if _, err := selectorFromWorkload(conflicting); err == nil {
		t.Fatal("conflicting selector and Pod component label was accepted")
	}
}

func TestObserveTreatsKubectlJSONNullAsAbsentFirstInstall(t *testing.T) {
	kubectl := filepath.Join(t.TempDir(), "kubectl")
	if err := os.WriteFile(kubectl, []byte("#!/bin/sh\nprintf 'null\\n'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	release := declarativerelease.PlanRelease{
		ComponentID: "edge-control-us",
		Workload: declarativerelease.Workload{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-us",
			Container: "edge-control", FieldManager: "fugue-edge-control-us-declarative", Replicas: 1,
		},
	}
	manifest := []byte(`{"apiVersion":"release.fugue.dev/v2","kind":"ComponentResourceSet","items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"edge-control-us","namespace":"fugue-system"}},{"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"annotations":{"fugue.pro/release-retain-on-rollback":"true"},"name":"edge-control-us-state","namespace":"fugue-system"}}]}`)
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	observation, err := cluster.Observe(context.Background(), release, declarativerelease.TargetIdentity{Present: false}, manifest)
	if err != nil || observation.Present || observation.Primary.Name != "edge-control-us" || len(observation.Resources) != 2 {
		t.Fatalf("JSON null was not treated as absent: observation=%+v err=%v", observation, err)
	}
	if observation.Resources[0].Present || observation.Resources[1].Present || observation.Resources[0].Identity == observation.Resources[1].Identity {
		t.Fatalf("absent first-install resources were not fully witnessed: %+v", observation.Resources)
	}
	if !observation.Resources[1].RetainOnRollback {
		t.Fatalf("absent retain-on-rollback marker was lost: %+v", observation.Resources)
	}
	cas, err := cluster.ObserveCAS(context.Background(), release, manifest)
	if err != nil || !cas.SameResourceCAS(observation) {
		t.Fatalf("JSON null changed between full and CAS observations: observation=%+v cas=%+v err=%v", observation, cas, err)
	}
}

func TestPublicRouteCanaryDialsExactEdgeWithTLSHostAndExpectedBody(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Host != "example.com" || request.URL.Path != "/healthz" {
			t.Fatalf("unexpected canary request: host=%q path=%q", request.Host, request.URL.Path)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	probe := declarativerelease.HealthProbe{
		Type: "public-route-http", Name: "edge-group-test", Address: server.Listener.Addr().String(),
		Host: "example.com", Path: "/healthz", Expected: "ok",
	}
	body, err := readPublicRouteCanaryWithRoots(context.Background(), probe, roots)
	if err != nil || string(body) != "ok" {
		t.Fatalf("public route canary failed: body=%q err=%v", body, err)
	}
	probe.Expected = "different"
	if _, err := readPublicRouteCanaryWithRoots(context.Background(), probe, roots); err == nil {
		t.Fatal("public route canary accepted the wrong response")
	}
}

func TestSanitizeObservedResourceDropsDaemonSetControllerGenerationAnnotation(t *testing.T) {
	resource := map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{
			"deprecated.daemonset.template.generation": "109",
			"fugue.pro/source-commit":                  strings.Repeat("a", 40),
		}},
		"spec":   map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "edge", "image": "ghcr.io/example/edge@sha256:" + strings.Repeat("b", 64)}}}}},
		"status": map[string]any{"numberReady": 1},
	}
	clean := sanitizeObservedResource(resource)
	annotations := mapField(clean["metadata"].(map[string]any), "annotations")
	if _, ok := annotations["deprecated.daemonset.template.generation"]; ok {
		t.Fatal("controller-owned generation annotation remained in CAS digest")
	}
	if annotations["fugue.pro/source-commit"] != strings.Repeat("a", 40) {
		t.Fatal("declarative source identity was removed")
	}
	if _, ok := clean["status"]; ok {
		t.Fatal("status remained in CAS digest")
	}
}

func TestParseObservationRequiresOneStableImmutableCohort(t *testing.T) {
	release := declarativerelease.PlanRelease{
		ComponentID: "api",
		Workload:    declarativerelease.Workload{Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api", FieldManager: "fugue-api-declarative", Replicas: 2},
	}
	workload := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "fugue-fugue-api", "namespace": "fugue-system", "uid": "api-uid", "resourceVersion": "42", "generation": 7,
			"annotations":   map[string]any{"fugue.pro/production-config-sha": strings.Repeat("1", 40)},
			"managedFields": []any{map[string]any{"manager": "fugue-api-declarative"}},
		},
		"spec": map[string]any{
			"replicas": 2,
			"selector": map[string]any{"matchLabels": map[string]any{"app": "api"}},
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": strings.Repeat("1", 40)}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "api", "image": "ghcr.io/example/fugue-api@sha256:" + strings.Repeat("a", 64)}}},
			},
		},
		"status": map[string]any{"observedGeneration": 7, "updatedReplicas": 2, "readyReplicas": 2, "availableReplicas": 2, "unavailableReplicas": 0},
	}
	pods := map[string]any{"items": []any{
		podFixture("api-1", "uid-1", strings.Repeat("1", 40), strings.Repeat("b", 64)),
		podFixture("api-2", "uid-2", strings.Repeat("1", 40), strings.Repeat("b", 64)),
	}}
	observation, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release)
	if err != nil {
		t.Fatalf("parse observation: %v", err)
	}
	if observation.UID != "api-uid" || observation.ResourceVersion != "42" || observation.Desired != 2 || observation.Ready != 2 ||
		observation.ImageID != "sha256:"+strings.Repeat("b", 64) || observation.ConfigSHA != strings.Repeat("1", 40) ||
		len(observation.FieldManagers) != 1 || observation.FieldManagers[0] != "fugue-api-declarative" {
		t.Fatalf("unexpected observation: %+v", observation)
	}
	pods["items"].([]any)[1] = podFixture("api-2", "uid-2", strings.Repeat("1", 40), strings.Repeat("c", 64))
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release); err == nil || !strings.Contains(err.Error(), "mixed image IDs") || errors.Is(err, declarativerelease.ErrDegradedPredecessorHealth) {
		t.Fatalf("mixed cohort was accepted: %v", err)
	}
	pods["items"] = pods["items"].([]any)[:1]
	if _, err := parseObservation(mustJSON(t, workload), mustJSON(t, pods), release); err == nil || !strings.Contains(err.Error(), "ready workload pod count mismatch: got=1 want=2") || !errors.Is(err, declarativerelease.ErrDegradedPredecessorHealth) {
		t.Fatalf("insufficient ready cohort was not classified as degraded predecessor health: %v", err)
	}
}

func TestTypedHealthShortCircuitIsRestrictedToTheExactPrewritePredecessor(t *testing.T) {
	revision := strings.Repeat("1", 40)
	digest := "sha256:" + strings.Repeat("a", 64)
	release := declarativerelease.PlanRelease{
		Artifact:                declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-edge"},
		ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: revision, ExpectedPreviousManifestSHA: revision,
		ExpectedPreviousOCIRevision: revision, ExpectedPreviousImageDigest: digest,
	}
	predecessor := declarativerelease.TargetIdentity{
		Present: true, ImageRef: release.Artifact.Repository + "@" + digest,
		ConfigSHA: revision, ManifestSHA: revision, OCIRevision: revision,
	}
	marked := declarativerelease.WithPrewritePredecessorHealthWait(context.Background())
	typed := fmt.Errorf("%w: ready workload pod count mismatch", declarativerelease.ErrDegradedPredecessorHealth)
	forward := predecessor
	forward.ConfigSHA = strings.Repeat("2", 40)
	for _, test := range []struct {
		name   string
		ctx    context.Context
		target declarativerelease.TargetIdentity
		err    error
		want   bool
	}{
		{name: "exact typed predecessor", ctx: marked, target: predecessor, err: typed, want: true},
		{name: "forward typed zero ready", ctx: marked, target: forward, err: typed},
		{name: "unmarked compensation predecessor", ctx: context.Background(), target: predecessor, err: typed},
		{name: "recoverable non-typed predecessor", ctx: marked, target: predecessor, err: errors.New("temporarily unavailable")},
		{name: "unknown context predecessor", ctx: marked, target: predecessor, err: context.DeadlineExceeded},
		{name: "healthy predecessor", ctx: marked, target: predecessor},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldReturnTypedPrewritePredecessorHealth(test.ctx, release, test.target, test.err); got != test.want {
				t.Fatalf("short-circuit=%v, want %v", got, test.want)
			}
		})
	}
}

func TestParseDegradedObservationKeepsOwnedIdentityWithoutPodHealth(t *testing.T) {
	revision := strings.Repeat("9", 40)
	release := declarativerelease.PlanRelease{
		ComponentID: "telemetry",
		Workload: declarativerelease.Workload{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "telemetry",
			Container: "telemetry-agent", FieldManager: "fugue-telemetry-declarative", Replicas: 1,
		},
	}
	workload := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "telemetry", "namespace": "fugue-system", "uid": "telemetry-uid", "resourceVersion": "42", "generation": 7,
			"annotations":   map[string]any{"fugue.pro/production-config-sha": revision},
			"managedFields": []any{map[string]any{"manager": "fugue-telemetry-declarative"}},
		},
		"spec": map[string]any{
			"replicas": 1,
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": revision, "fugue.pro/oci-revision": revision}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "telemetry-agent", "image": "ghcr.io/example/telemetry@sha256:" + strings.Repeat("a", 64)}}},
			},
		},
		"status": map[string]any{"observedGeneration": 7, "updatedReplicas": 1, "readyReplicas": 0, "availableReplicas": 0, "unavailableReplicas": 1},
	}
	observation, err := parseDegradedObservation(mustJSON(t, workload), release)
	if err != nil {
		t.Fatal(err)
	}
	if observation.ConfigSHA != revision || observation.ManifestSHA != revision || observation.OCIRevision != revision ||
		observation.Ready != 0 || observation.HealthDigest != "" || len(observation.FieldManagers) != 1 {
		t.Fatalf("unexpected degraded observation: %+v", observation)
	}
}

func TestApplyResourceSetConsumesDependentCASBeforeStartingTheWorkload(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "order.log")
	kubectl := filepath.Join(directory, "kubectl")
	program := `#!/bin/sh
set -eu
python3 -c 'import json,os,sys; value=json.load(sys.stdin); open(os.environ["ORDER_LOG"], "a").write(value["kind"]+"/"+value["metadata"]["name"]+"\n")'
printf '{}\n'
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ORDER_LOG", logPath)
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{
		APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de",
		FieldManager: "fugue-edge-control-de-declarative",
	}}
	manifest := mustJSON(t, map[string]any{
		"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet", "items": []any{
			map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system"}},
			map[string]any{"apiVersion": "policy/v1", "kind": "PodDisruptionBudget", "metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system"}},
			map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system"}},
		},
	})
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	if err := cluster.applyResourceSet(context.Background(), release, manifest, false); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(raw), "PodDisruptionBudget/edge-control-de\nService/edge-control-de\nDeployment/edge-control-de\n"; got != want {
		t.Fatalf("apply order=%q want=%q", got, want)
	}
}

func TestEmergencyOwnershipConvergenceIsExactAndCASBound(t *testing.T) {
	release := declarativerelease.PlanRelease{
		Workload: declarativerelease.Workload{FieldManager: "fugue-edge-control-de-declarative"},
		ArtifactTargets: []declarativerelease.ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de", Container: "edge-control", ContainerType: "container"},
			{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de", Container: "state-permissions", ContainerType: "init-container"},
		},
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de"}
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "edge-control-de", "namespace": "fugue-system", "uid": "control-uid", "resourceVersion": "42",
			"annotations": map[string]any{
				"fugue.pro/artifact-receipt-digest": "sha256:" + strings.Repeat("a", 64),
				"fugue.pro/production-config-sha":   strings.Repeat("1", 40),
				"fugue.pro/release-plan-digest":     "sha256:" + strings.Repeat("b", 64),
			},
		},
		"spec": map[string]any{"template": map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{
				"fugue.pro/oci-revision":          strings.Repeat("1", 40),
				"fugue.pro/production-config-sha": strings.Repeat("1", 40),
				"fugue.pro/source-commit":         strings.Repeat("1", 40),
			}},
			"spec": map[string]any{
				"containers":     []any{map[string]any{"name": "edge-control", "image": "ghcr.io/example/control@sha256:" + strings.Repeat("c", 64)}},
				"initContainers": []any{map[string]any{"name": "state-permissions", "image": "ghcr.io/example/control@sha256:" + strings.Repeat("c", 64)}},
			},
		}},
	}
	allowed := emergencyOwnershipPointers(release, identity, desired)
	if len(allowed) != 8 {
		t.Fatalf("allowlist=%v", allowed)
	}
	live := deepCopyJSONMap(t, desired)
	metadata := mapField(live, "metadata")
	metadata["managedFields"] = []any{
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, allowed)},
		map[string]any{"manager": "kubectl", "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, allowed)},
	}
	conflictLines := make([]string, 0, len(allowed)+1)
	conflictLines = append(conflictLines, fmt.Sprintf("Apply failed with %d conflicts: conflicts with \"kubectl\" using apps/v1:", len(allowed)))
	for _, pointer := range allowed {
		conflictLines = append(conflictLines, "- "+ssaFieldForPointer(pointer))
	}
	applyErr := errors.New(strings.Join(conflictLines, "\n"))
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, applyErr); err != nil {
		t.Fatalf("exact emergency conflict rejected: %v", err)
	}
	patch, found, err := nextEmergencyOwnershipPatch(live, release.Workload.FieldManager, allowed, true)
	if err != nil || !found || len(patch) != 4 || patch[0]["path"] != "/metadata/uid" || patch[1]["path"] != "/metadata/resourceVersion" || patch[3]["path"] != "/metadata/managedFields/1" {
		t.Fatalf("unexpected cleanup patch: patch=%v found=%v err=%v", patch, found, err)
	}

	extra := append(append([]string(nil), allowed...), "/spec/replicas")
	metadata["managedFields"].([]any)[1].(map[string]any)["fieldsV1"] = managedFieldsTree(t, extra)
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, applyErr); err == nil || !strings.Contains(err.Error(), "expands beyond") {
		t.Fatalf("expanded emergency manager was accepted: %v", err)
	}
	if _, _, err := nextEmergencyOwnershipPatch(live, release.Workload.FieldManager, allowed, true); err == nil || !strings.Contains(err.Error(), "unreviewed") {
		t.Fatalf("cleanup accepted expanded ownership: %v", err)
	}
}

func TestOwnershipCleanupRebindsOnlyFreshResourceVersion(t *testing.T) {
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "edge-control-us", "namespace": "fugue-system", "uid": "control-uid", "resourceVersion": "42"},
		"spec":     map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "edge-control", "image": "target"}}}}},
	}
	before := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "edge-control-us", "namespace": "fugue-system", "uid": "control-uid", "resourceVersion": "42", "generation": json.Number("8"),
			"managedFields": []any{map[string]any{"manager": "kubectl", "operation": "Update"}},
		},
		"spec":   map[string]any{"replicas": 1, "template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "edge-control", "image": "lkg"}}}}},
		"status": map[string]any{"readyReplicas": 1},
	}
	fresh := deepCopyJSONMap(t, before)
	freshMetadata := mapField(fresh, "metadata")
	freshMetadata["resourceVersion"] = "43"
	freshMetadata["managedFields"] = []any{map[string]any{"manager": "fugue-edge-control-us-declarative", "operation": "Apply"}}
	mapField(fresh, "status")["readyReplicas"] = 0

	reboundRaw, err := rebindDesiredResourceVersionAfterOwnershipCleanup(desired, before, fresh)
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := decodeJSONObject(reboundRaw)
	if err != nil {
		t.Fatal(err)
	}
	if got := stringValue(mapField(rebound, "metadata")["resourceVersion"]); got != "43" {
		t.Fatalf("rebound resourceVersion=%q", got)
	}
	containers := anySlice(mapField(mapField(mapField(rebound, "spec"), "template"), "spec")["containers"])
	if got := stringValue(containers[0].(map[string]any)["image"]); got != "target" {
		t.Fatalf("target image changed during rebind: %q", got)
	}

	for name, mutate := range map[string]func(map[string]any){
		"uid":          func(value map[string]any) { mapField(value, "metadata")["uid"] = "other" },
		"generation":   func(value map[string]any) { mapField(value, "metadata")["generation"] = json.Number("9") },
		"spec":         func(value map[string]any) { mapField(value, "spec")["replicas"] = 2 },
		"unchanged-rv": func(value map[string]any) { mapField(value, "metadata")["resourceVersion"] = "42" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := deepCopyJSONMap(t, fresh)
			mutate(changed)
			if _, err := rebindDesiredResourceVersionAfterOwnershipCleanup(desired, before, changed); err == nil {
				t.Fatal("unsafe post-cleanup state was accepted")
			}
		})
	}
}

func TestEmergencyOwnershipRejectsUnknownManagerAndField(t *testing.T) {
	allowed := []string{"/spec/template/spec/containers[name=edge-control]/image"}
	for _, failure := range []error{
		errors.New(`Apply failed with 1 conflict: conflict with "helm" using apps/v1: .spec.template.spec.containers[name="edge-control"].image`),
		errors.New(`Apply failed with 1 conflict: conflict with "kubectl" using apps/v1: .spec.replicas`),
	} {
		desired := map[string]any{"metadata": map[string]any{"uid": "u", "resourceVersion": "1"}}
		live := map[string]any{"metadata": map[string]any{"uid": "u", "resourceVersion": "1", "managedFields": []any{}}}
		if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, failure); err == nil || !strings.Contains(err.Error(), "outside the exact allowlist") {
			t.Fatalf("unknown ownership conflict was accepted: %v", err)
		}
	}
}

func TestEmergencyOwnershipVerificationAcceptsOpaqueKubernetesListSelectors(t *testing.T) {
	allowed := []string{"/spec/template/spec/containers[name=edge]/image"}
	metadata := map[string]any{"managedFields": []any{
		map[string]any{"manager": "fugue-edge-worker-de-declarative", "operation": "Apply", "fieldsV1": managedFieldsTree(t, allowed)},
		map[string]any{"manager": "kubectl-patch", "operation": "Update", "fieldsV1": map[string]any{
			"f:spec": map[string]any{"f:template": map[string]any{"f:spec": map[string]any{"f:containers": map[string]any{
				`k:{"name":"edge"}`: map[string]any{"f:ports": map[string]any{
					`k:{"containerPort":7832,"protocol":"TCP"}`: map[string]any{"f:name": map[string]any{}},
				}},
			}}}},
		}},
	}}
	pointers, err := managedFieldsEntryPointers(mapField(metadata["managedFields"].([]any)[1].(map[string]any), "fieldsV1"))
	if err != nil || len(pointers) != 1 || !strings.Contains(pointers[0], "/ports[selector=") || !strings.HasSuffix(pointers[0], "]/name") {
		t.Fatalf("opaque associative selector was not preserved safely: pointers=%v err=%v", pointers, err)
	}
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{FieldManager: "fugue-edge-worker-de-declarative"}, ArtifactTargets: []declarativerelease.ArtifactTarget{{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a", Container: "edge", ContainerType: "container",
	}}}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a"}
	desired := map[string]any{"metadata": map[string]any{}, "spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "edge", "image": "target"}}}}}}
	live := map[string]any{"metadata": metadata}
	if err := verifyNoEmergencyOwnership(release, identity, desired, live); err != nil {
		t.Fatalf("unrelated Kubernetes list selector blocked exact runtime ownership verification: %v", err)
	}
}

func deepCopyJSONMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	raw := mustJSON(t, value)
	var copy map[string]any
	if err := json.Unmarshal(raw, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func managedFieldsTree(t *testing.T, pointers []string) map[string]any {
	t.Helper()
	root := map[string]any{}
	for _, pointer := range pointers {
		current := root
		for _, encoded := range strings.Split(strings.TrimPrefix(pointer, "/"), "/") {
			token := strings.ReplaceAll(strings.ReplaceAll(encoded, "~1", "/"), "~0", "~")
			field, selector := token, ""
			if open := strings.Index(token, "[name="); open > 0 && strings.HasSuffix(token, "]") {
				field, selector = token[:open], token[open+len("[name="):len(token)-1]
			}
			next, _ := current["f:"+field].(map[string]any)
			if next == nil {
				next = map[string]any{}
				current["f:"+field] = next
			}
			current = next
			if selector != "" {
				key := `k:{"name":` + strconv.Quote(selector) + `}`
				next, _ = current[key].(map[string]any)
				if next == nil {
					next = map[string]any{}
					current[key] = next
				}
				current = next
			}
		}
	}
	return root
}

func TestParseLeaderLeaseRequiresTypedRenewTime(t *testing.T) {
	raw := []byte(`{"spec":{"holderIdentity":"controller-1","renewTime":"2026-08-05T01:02:03.123456Z"}}`)
	holder, renew, err := parseLeaderLease(raw)
	if err != nil || holder != "controller-1" || renew.IsZero() {
		t.Fatalf("parse lease: holder=%q renew=%s err=%v", holder, renew, err)
	}
	if _, _, err := parseLeaderLease([]byte(`{"spec":{"holderIdentity":"","renewTime":"bad"}}`)); err == nil {
		t.Fatal("invalid leader lease was accepted")
	}
}

func TestResourceDeletionAllowlistContainsOnlyOwnedComponentKinds(t *testing.T) {
	for _, identity := range []declarativerelease.ResourceIdentity{
		{APIVersion: "apps/v1", Kind: "Deployment"},
		{APIVersion: "apps/v1", Kind: "DaemonSet"},
		{APIVersion: "batch/v1", Kind: "Job"},
		{APIVersion: "v1", Kind: "Service"},
		{APIVersion: "v1", Kind: "ServiceAccount"},
		{APIVersion: "v1", Kind: "PersistentVolumeClaim"},
		{APIVersion: "policy/v1", Kind: "PodDisruptionBudget"},
		{APIVersion: "networking.k8s.io/v1", Kind: "NetworkPolicy"},
	} {
		if _, err := resourceGVR(identity); err != nil {
			t.Fatalf("owned kind is missing from deletion allowlist: %+v: %v", identity, err)
		}
	}
	if _, err := resourceGVR(declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Secret"}); err == nil {
		t.Fatal("Secret unexpectedly entered release-owned deletion allowlist")
	}
}

func TestCreatedResourceDeletionsSelectOnlyAbsentToPresentNonRetainedObjects(t *testing.T) {
	deployment := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "api"}
	service := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "Service", Namespace: "fugue-system", Name: "api-tls"}
	pvc := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: "fugue-system", Name: "state"}
	identities := []declarativerelease.ResourceIdentity{deployment, pvc, service}
	before := declarativerelease.Observation{Resources: []declarativerelease.ResourceObservation{{Identity: deployment, Present: true}, {Identity: pvc}, {Identity: service}}}
	after := declarativerelease.Observation{Resources: []declarativerelease.ResourceObservation{
		{Identity: deployment, Present: true, UID: "api-uid", ResourceVersion: "10"},
		{Identity: pvc, Present: true, RetainOnRollback: true, UID: "pvc-uid", ResourceVersion: "11"},
		{Identity: service, Present: true, UID: "service-uid", ResourceVersion: "12"},
	}}
	deletions, err := createdResourceDeletions(identities, before, after)
	if err != nil || len(deletions) != 1 || deletions[0].Identity != service || deletions[0].UID != "service-uid" {
		t.Fatalf("created-resource deletions=%+v err=%v", deletions, err)
	}
	after.Resources = after.Resources[:2]
	if _, err := createdResourceDeletions(identities, before, after); err == nil {
		t.Fatal("missing rollback identity was accepted")
	}
}

func TestFreshDeletionPreconditionsRefreshStatusOnlyResourceVersion(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "policy", Version: "v1", Resource: "poddisruptionbudgets"}
	object := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "policy/v1", "kind": "PodDisruptionBudget",
		"metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system", "uid": "pdb-uid", "resourceVersion": "22", "generation": int64(1)},
	}}
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), object)
	resource := client.Resource(gvr).Namespace("fugue-system")
	expected := declarativerelease.ResourceObservation{
		Identity: declarativerelease.ResourceIdentity{APIVersion: "policy/v1", Kind: "PodDisruptionBudget", Namespace: "fugue-system", Name: "edge-control-de"},
		Present:  true, UID: "pdb-uid", ResourceVersion: "10", Generation: 1,
	}
	uid, rv, present, err := freshDeletionPreconditions(context.Background(), resource, expected)
	if err != nil || !present || uid != types.UID("pdb-uid") || rv != "22" {
		t.Fatalf("fresh delete preconditions: uid=%q rv=%q present=%t err=%v", uid, rv, present, err)
	}
	expected.UID = "replacement-uid"
	if _, _, _, err := freshDeletionPreconditions(context.Background(), resource, expected); err == nil {
		t.Fatal("replacement object entered created-resource rollback")
	}
	if err := client.Resource(gvr).Namespace("fugue-system").Delete(context.Background(), "edge-control-de", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	expected.UID = "pdb-uid"
	if _, _, present, err := freshDeletionPreconditions(context.Background(), resource, expected); err != nil || present {
		t.Fatalf("already absent object did not reconcile: present=%t err=%v", present, err)
	}
}

func TestParseSucceededJobPodRequiresOneZeroExitImmutableExecution(t *testing.T) {
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{Container: "schema-migrate"}}
	source := strings.Repeat("a", 40)
	digest := strings.Repeat("b", 64)
	pods := map[string]any{"items": []any{map[string]any{
		"metadata": map[string]any{"uid": "job-pod", "annotations": map[string]any{"fugue.pro/source-commit": source}},
		"status": map[string]any{"phase": "Succeeded", "containerStatuses": []any{map[string]any{
			"name": "schema-migrate", "restartCount": 0, "imageID": "docker-pullable://ghcr.io/example/schema@sha256:" + digest,
			"state": map[string]any{"terminated": map[string]any{"exitCode": 0, "reason": "Completed"}},
		}}},
	}}}
	imageID, health, err := parseSucceededJobPod(mustJSON(t, pods), release, source)
	if err != nil || imageID != "sha256:"+digest || !strings.HasPrefix(health, "sha256:") {
		t.Fatalf("parse succeeded Job pod: image=%q health=%q err=%v", imageID, health, err)
	}
	pods["items"].([]any)[0].(map[string]any)["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["state"].(map[string]any)["terminated"].(map[string]any)["exitCode"] = 1
	if _, _, err := parseSucceededJobPod(mustJSON(t, pods), release, source); err == nil {
		t.Fatal("failed migration Job was accepted")
	}
}

func TestDeclaredArtifactImageIDsAcceptVerifiedPlatformManifest(t *testing.T) {
	topDigest := "sha256:" + strings.Repeat("a", 64)
	platformDigest := "sha256:" + strings.Repeat("b", 64)
	image := "ghcr.io/example/edge-control@" + topDigest
	release := declarativerelease.PlanRelease{
		Workload: declarativerelease.Workload{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de", Container: "edge-control",
		},
		ArtifactTargets: []declarativerelease.ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de", ContainerType: "container", Container: "edge-control"},
			{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de", ContainerType: "init-container", Container: "state-permissions"},
		},
	}
	manifest := mustJSON(t, map[string]any{
		"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet",
		"items": []any{map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system"},
			"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
				"containers":     []any{map[string]any{"name": "edge-control", "image": image}},
				"initContainers": []any{map[string]any{"name": "state-permissions", "image": image}},
			}}},
		}},
	})
	pods := map[string]any{"items": []any{map[string]any{
		"metadata": map[string]any{"name": "edge-control-de-1"},
		"status": map[string]any{
			"containerStatuses":     []any{map[string]any{"name": "edge-control", "imageID": "containerd://" + platformDigest}},
			"initContainerStatuses": []any{map[string]any{"name": "state-permissions", "imageID": "docker-pullable://ghcr.io/example/edge-control@" + platformDigest}},
		},
	}}}
	if err := verifyDeclaredArtifactImageIDs(mustJSON(t, pods), manifest, release, image, platformDigest); err != nil {
		t.Fatalf("verified platform manifest was rejected: %v", err)
	}
	pods["items"].([]any)[0].(map[string]any)["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["imageID"] = "containerd://sha256:" + strings.Repeat("c", 64)
	if err := verifyDeclaredArtifactImageIDs(mustJSON(t, pods), manifest, release, image, platformDigest); err == nil {
		t.Fatal("unverified runtime digest was accepted")
	}
}

func TestWorkloadFromDeclaredResourceDerivesOnlyTypedRolloutShapes(t *testing.T) {
	identity := declarativerelease.ResourceIdentity{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-worker-de-a",
	}
	desired := map[string]any{"spec": map[string]any{"updateStrategy": map[string]any{"type": "OnDelete"}}}
	workload, err := workloadFromDeclaredResource(desired, identity, "edge", "fugue-edge-worker-de-declarative")
	if err != nil {
		t.Fatal(err)
	}
	if workload.Kind != "DaemonSet" || workload.RolloutMode != "on-delete" || workload.Container != "edge" || workload.Replicas != 0 {
		t.Fatalf("unexpected derived workload: %+v", workload)
	}

	desired["spec"].(map[string]any)["updateStrategy"] = map[string]any{"type": "Unknown"}
	if _, err := workloadFromDeclaredResource(desired, identity, "edge", "fugue-edge-worker-de-declarative"); err == nil {
		t.Fatal("unsupported auxiliary rollout strategy was accepted")
	}
}

func TestHealthSoakTrackerRequiresOneContinuousWindow(t *testing.T) {
	start := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	tracker := healthSoakTracker{required: 180 * time.Second}
	if tracker.observe(start, true) || tracker.observe(start.Add(179*time.Second), true) {
		t.Fatal("health soak completed before its continuous window")
	}
	if tracker.observe(start.Add(179*time.Second), false) {
		t.Fatal("unhealthy observation completed health soak")
	}
	if tracker.observe(start.Add(200*time.Second), true) || !tracker.observe(start.Add(380*time.Second), true) {
		t.Fatal("health soak did not reset and complete after a new continuous window")
	}
}

func TestWaitHealthyTerminalErrorPreservesLastObservation(t *testing.T) {
	lastFailure := errors.New("live registry identity mismatch")
	err := waitHealthyTerminalError(context.DeadlineExceeded, lastFailure)
	if !errors.Is(err, lastFailure) || !strings.Contains(err.Error(), "context deadline exceeded; last health observation: live registry identity mismatch") {
		t.Fatalf("terminal health error lost its concrete observation: %v", err)
	}
	if err := waitHealthyTerminalError(context.DeadlineExceeded, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty last observation changed the context error: %v", err)
	}
}

func TestPodHTTPUsesBoundReadyPodIPAndNamedPort(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/readyz" {
			t.Fatalf("unexpected health path: %s", request.URL.Path)
		}
		_, _ = response.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()
	host, portRaw, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portRaw)
	if err != nil {
		t.Fatal(err)
	}
	pod := podFixture("edge-front-1", "pod-uid", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["spec"] = map[string]any{"containers": []any{map[string]any{"name": "edge-front", "ports": []any{map[string]any{"name": "health", "containerPort": port}}}}}
	pod["status"].(map[string]any)["podIP"] = host
	endpoints, err := podHTTPEndpointsFromJSON(mustJSON(t, map[string]any{"items": []any{pod}}), "edge-front", "health")
	if err != nil || len(endpoints) != 1 || endpoints[0].Name != "edge-front-1" || endpoints[0].IP != host || endpoints[0].Port != port {
		t.Fatalf("parse bound pod endpoint: endpoints=%+v err=%v", endpoints, err)
	}
	body, err := readPodHTTP(context.Background(), endpoints[0], "/readyz")
	if err != nil || string(body) != `{"status":"ok"}` {
		t.Fatalf("direct pod health: body=%q err=%v", body, err)
	}
	pod["status"].(map[string]any)["podIP"] = "not-an-ip"
	if _, err := podHTTPEndpointsFromJSON(mustJSON(t, map[string]any{"items": []any{pod}}), "edge-front", "health"); err == nil {
		t.Fatal("invalid pod IP was accepted")
	}
}

func TestEdgeWorkerHealthUsesCanonicalNamedPort(t *testing.T) {
	pod := podFixture("edge-worker-a-1", "pod-uid", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["status"].(map[string]any)["podIP"] = "10.42.0.17"
	pod["spec"] = map[string]any{"containers": []any{map[string]any{
		"name":  "edge",
		"ports": []any{map[string]any{"name": "health", "containerPort": 7832}},
	}}}
	raw := mustJSON(t, map[string]any{"items": []any{pod}})
	if endpoints, err := podHTTPEndpointsFromJSON(raw, "edge", "health"); err != nil || len(endpoints) != 1 || endpoints[0].Port != 7832 {
		t.Fatalf("canonical worker health port was not accepted: endpoints=%+v err=%v", endpoints, err)
	}
	if _, err := podHTTPEndpointsFromJSON(raw, "edge", "http"); err == nil {
		t.Fatal("non-canonical worker health port was accepted")
	}
}

func TestHealthWorkloadContainerUsesTransitionBoundEdgeWorker(t *testing.T) {
	release := declarativerelease.PlanRelease{
		Workload: declarativerelease.Workload{Namespace: "fugue-system"},
		Transition: &declarativerelease.Transition{EdgeGroupAB: &declarativerelease.EdgeGroupABTransition{
			WorkerAName: "edge-gamma-worker-a", WorkerBName: "edge-gamma-worker-b", WorkerContainer: "edge",
		}},
		ArtifactTargets: []declarativerelease.ArtifactTarget{
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-worker-a", ContainerType: "container", Container: "edge"},
			{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-gamma-worker-a", ContainerType: "container", Container: "caddy"},
		},
	}
	container, err := healthWorkloadContainer(release, "apps/v1", "DaemonSet", "edge-gamma-worker-a")
	if err != nil || container != "edge" {
		t.Fatalf("select transition-bound worker: container=%q err=%v", container, err)
	}
	release.Transition.EdgeGroupAB.WorkerContainer = "missing"
	if _, err := healthWorkloadContainer(release, "apps/v1", "DaemonSet", "edge-gamma-worker-a"); err == nil {
		t.Fatal("missing transition-bound worker container was accepted")
	}
	release.Transition = nil
	if _, err := healthWorkloadContainer(release, "apps/v1", "DaemonSet", "edge-gamma-worker-a"); err == nil {
		t.Fatal("unbound multi-container health workload was accepted")
	}
}

func podFixture(name, uid, source, imageDigest string) map[string]any {
	return map[string]any{
		"metadata": map[string]any{"name": name, "uid": uid, "annotations": map[string]any{"fugue.pro/source-commit": source}},
		"status": map[string]any{
			"conditions":        []any{map[string]any{"type": "Ready", "status": "True"}},
			"containerStatuses": []any{map[string]any{"name": "api", "imageID": "docker-pullable://ghcr.io/example/fugue-api@sha256:" + imageDigest, "restartCount": 0}},
		},
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
