package main

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	clienttesting "k8s.io/client-go/testing"
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

func TestTrustedCurrentArtifactBypassesRegistryOnlyForExactIdentity(t *testing.T) {
	revision := strings.Repeat("a", 40)
	repository := "ghcr.io/example/edge-worker"
	artifact := declarativerelease.ArtifactReceipt{
		APIVersion: declarativerelease.ArtifactReceiptAPIVersion, Kind: declarativerelease.ArtifactReceiptKind,
		Component: "edge-worker-de", ConfigSHA: revision, SourceSHA: revision, SourceTag: revision,
		Repository: repository, TopDigest: "sha256:" + strings.Repeat("b", 64),
		PlatformManifestDigest: "sha256:" + strings.Repeat("c", 64), ConfigDigest: "sha256:" + strings.Repeat("d", 64),
		OCIRevision: revision, Platform: "linux/amd64", Verification: "registry_manifest_config_and_layer_get",
		PlanDigest: "sha256:" + strings.Repeat("e", 64), IntentDigest: "sha256:" + strings.Repeat("f", 64),
	}
	artifact.ImmutableRef = repository + "@" + artifact.TopDigest
	unsigned, err := declarativerelease.CanonicalJSON(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifact.ReceiptDigest = fmt.Sprintf("sha256:%x", sha256.Sum256(unsigned))
	raw, err := declarativerelease.CanonicalJSON(artifact)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(trustedCurrentArtifactEnv, string(raw))
	trusted, err := trustedCurrentArtifactFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "registry-called")
	verifier := filepath.Join(t.TempDir(), "verify.py")
	if err := os.WriteFile(verifier, []byte("import os\nopen(os.environ['MARKER'], 'w').close()\nraise SystemExit(19)\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MARKER", marker)
	cluster := &kubectlCluster{verifier: verifier, trustedCurrent: trusted}
	verification, err := cluster.verifyRuntimeArtifact(context.Background(), artifact.ImmutableRef, revision)
	if err != nil || verification.ManifestDigest != artifact.PlatformManifestDigest || verification.Verification != "sealed_current_artifact_receipt" {
		t.Fatalf("trusted verification=%+v err=%v", verification, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("exact trusted identity called registry verifier: %v", err)
	}
	if _, err := cluster.verifyRuntimeArtifact(context.Background(), artifact.ImmutableRef, strings.Repeat("9", 40)); err == nil {
		t.Fatal("mismatched revision bypassed registry verification")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("mismatched identity did not call registry verifier: %v", err)
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

func TestKubectlGetRetriesReadsWithoutRetryingWrites(t *testing.T) {
	directory := t.TempDir()
	kubectl := filepath.Join(directory, "kubectl")
	getCount := filepath.Join(directory, "get-count")
	writeCount := filepath.Join(directory, "write-count")
	program := `#!/bin/sh
set -eu
case "${1:-}" in
  get)
    count=0
    if test -f "$GET_COUNT"; then read -r count <"$GET_COUNT"; fi
    count=$((count + 1))
    printf '%s\n' "$count" >"$GET_COUNT"
	if test "$count" -eq 1; then exit 42; fi
    printf '%s\n' '{"items":[]}'
    ;;
  *)
    count=0
    if test -f "$WRITE_COUNT"; then read -r count <"$WRITE_COUNT"; fi
    printf '%s\n' "$((count + 1))" >"$WRITE_COUNT"
    exit 41
    ;;
esac
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GET_COUNT", getCount)
	t.Setenv("WRITE_COUNT", writeCount)
	cluster := &kubectlCluster{
		kubectl: kubectl, readTimeout: 2 * time.Second, readAttempts: 2, readRetryDelay: time.Millisecond,
	}
	output, err := cluster.kubectlRun(context.Background(), nil, "get", "pods", "--output", "json")
	if err != nil || string(output) != "{\"items\":[]}\n" {
		t.Fatalf("read retry output=%q err=%v", output, err)
	}
	if raw, err := os.ReadFile(getCount); err != nil || strings.TrimSpace(string(raw)) != "2" {
		t.Fatalf("read attempts=%q err=%v", raw, err)
	}
	if _, err := cluster.kubectlRun(context.Background(), nil, "delete", "pod", "example"); err == nil {
		t.Fatal("failed mutating kubectl command was accepted")
	}
	if raw, err := os.ReadFile(writeCount); err != nil || strings.TrimSpace(string(raw)) != "1" {
		t.Fatalf("mutating attempts=%q err=%v", raw, err)
	}
}

func TestKubectlGetTimeoutIsBounded(t *testing.T) {
	directory := t.TempDir()
	kubectl := filepath.Join(directory, "kubectl")
	if err := os.WriteFile(kubectl, []byte("#!/bin/sh\nexec sleep 30\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cluster := &kubectlCluster{kubectl: kubectl, readTimeout: 250 * time.Millisecond, readAttempts: 1}
	started := time.Now()
	_, err := cluster.kubectlRun(context.Background(), nil, "get", "pods", "--output", "json")
	if err == nil || time.Since(started) >= 2*time.Second {
		t.Fatalf("bounded read timeout elapsed=%s err=%v", time.Since(started), err)
	}
}

func TestGetResourceUsesRetriedNativeKubernetesRead(t *testing.T) {
	resource := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "DaemonSet",
		"metadata": map[string]any{
			"name": "edge-worker", "namespace": "fugue-system", "uid": "uid-1", "resourceVersion": "42",
			"managedFields": []any{map[string]any{"manager": "fugue", "operation": "Apply"}},
		},
	}}
	client := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), resource)
	reads := 0
	client.PrependReactor("get", "daemonsets", func(action clienttesting.Action) (bool, runtime.Object, error) {
		reads++
		if reads == 1 {
			return true, nil, errors.New("transient read failure")
		}
		return false, nil, nil
	})
	cluster := &kubectlCluster{resources: client, readTimeout: time.Second, readAttempts: 2, readRetryDelay: time.Millisecond}
	raw, err := cluster.getResource(context.Background(), declarativerelease.ResourceIdentity{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-worker",
	})
	if err != nil || reads != 2 {
		t.Fatalf("native read raw=%s reads=%d err=%v", raw, reads, err)
	}
	value, err := decodeJSONObject(raw)
	if err != nil || stringValue(mapField(value, "metadata")["resourceVersion"]) != "42" || len(anySlice(mapField(value, "metadata")["managedFields"])) != 1 {
		t.Fatalf("native read lost resource evidence: value=%+v err=%v", value, err)
	}

	missing, err := cluster.getResource(context.Background(), declarativerelease.ResourceIdentity{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "missing",
	})
	if err != nil || string(missing) != "null" {
		t.Fatalf("native not-found raw=%q err=%v", missing, err)
	}
}

func TestApplyReconcilesCommittedResponseLossButRejectsUnprovenState(t *testing.T) {
	directory := t.TempDir()
	kubectl := filepath.Join(directory, "kubectl")
	program := `#!/bin/sh
set -eu
case "${1:-}" in
  apply)
    cat >/dev/null
    exit 42
    ;;
  get)
    printf '%s\n' "$LIVE_RESOURCE"
    ;;
  *)
    exit 43
    ;;
esac
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	revision := strings.Repeat("a", 40)
	image := "ghcr.io/example/edge@sha256:" + strings.Repeat("b", 64)
	manager := "fugue-edge-worker-de-declarative"
	release := declarativerelease.PlanRelease{
		ComponentID: "edge-worker-de",
		Workload: declarativerelease.Workload{
			APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-front",
			Container: "edge-front", FieldManager: manager, RolloutMode: "on-delete",
		},
		ArtifactTargets: []declarativerelease.ArtifactTarget{{
			APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-front",
			Container: "edge-front", ContainerType: "container",
		}},
	}
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{
			"name": "edge-front", "namespace": "fugue-system", "uid": "uid-1", "resourceVersion": "41",
			"annotations": map[string]any{"fugue.pro/production-config-sha": revision},
		},
		"spec": map[string]any{"template": map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{
				"fugue.pro/source-commit": revision, "fugue.pro/oci-revision": revision,
			}},
			"spec": map[string]any{"containers": []any{map[string]any{"name": "edge-front", "image": image}}},
		}},
	}
	live := deepCopyJSONMap(t, desired)
	metadata := mapField(live, "metadata")
	metadata["resourceVersion"] = "42"
	metadata["generation"] = 8
	metadata["managedFields"] = []any{map[string]any{
		"manager": manager, "operation": "Apply", "apiVersion": "apps/v1", "fieldsType": "FieldsV1",
		"fieldsV1": map[string]any{
			"f:metadata": map[string]any{"f:annotations": map[string]any{".": map[string]any{}, "f:fugue.pro/production-config-sha": map[string]any{}}},
			"f:spec": map[string]any{"f:template": map[string]any{
				"f:metadata": map[string]any{"f:annotations": map[string]any{
					".": map[string]any{}, "f:fugue.pro/source-commit": map[string]any{}, "f:fugue.pro/oci-revision": map[string]any{},
				}},
				"f:spec": map[string]any{"f:containers": map[string]any{
					`k:{"name":"edge-front"}`: map[string]any{".": map[string]any{}, "f:image": map[string]any{}, "f:name": map[string]any{}},
				}},
			}},
		},
	}}
	liveRaw, err := declarativerelease.CanonicalJSON(live)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LIVE_RESOURCE", string(liveRaw))
	encoded, err := declarativerelease.CanonicalJSON(desired)
	if err != nil {
		t.Fatal(err)
	}
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-front"}
	if err := cluster.applyResourceWithOwnershipConvergence(context.Background(), release, identity, desired, encoded, false); err != nil {
		t.Fatalf("committed response loss was not reconciled: %v", err)
	}

	drifted := deepCopyJSONMap(t, live)
	mapField(mapField(mapField(drifted, "spec"), "template"), "spec")["containers"] = []any{map[string]any{"name": "edge-front", "image": "ghcr.io/example/edge@sha256:" + strings.Repeat("c", 64)}}
	driftedRaw, err := declarativerelease.CanonicalJSON(drifted)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("LIVE_RESOURCE", string(driftedRaw))
	if err := cluster.applyResourceWithOwnershipConvergence(context.Background(), release, identity, desired, encoded, false); err == nil {
		t.Fatal("response loss with live image drift was accepted")
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

func TestMonitorConvergenceIgnoresOnlyRendererEvidenceAnnotations(t *testing.T) {
	desired := map[string]any{
		"apiVersion": "v1", "kind": "Service",
		"metadata": map[string]any{
			"name": "fugue-api-tls", "namespace": "fugue-system",
			"annotations": map[string]any{
				"fugue.pro/artifact-receipt-digest": "sha256:" + strings.Repeat("a", 64),
				"fugue.pro/production-config-sha":   strings.Repeat("1", 40),
				"fugue.pro/release-plan-digest":     "sha256:" + strings.Repeat("b", 64),
				"fugue.pro/ownership":               "declarative",
			},
		},
		"spec": map[string]any{"selector": map[string]any{"app": "api"}},
	}
	live := deepCopyJSONMap(t, desired)
	annotations := mapField(mapField(live, "metadata"), "annotations")
	annotations["fugue.pro/artifact-receipt-digest"] = "sha256:" + strings.Repeat("c", 64)
	annotations["fugue.pro/production-config-sha"] = strings.Repeat("2", 40)
	annotations["fugue.pro/release-plan-digest"] = "sha256:" + strings.Repeat("d", 64)

	stripMonitorReleaseEvidence(desired)
	stripMonitorReleaseEvidence(live)
	if !declarativerelease.ResourceDesiredSubset(desired, live) {
		t.Fatal("renderer evidence annotations caused false monitor drift")
	}
	annotations["fugue.pro/ownership"] = "legacy"
	if declarativerelease.ResourceDesiredSubset(desired, live) {
		t.Fatal("non-evidence annotation drift was ignored")
	}
	annotations["fugue.pro/ownership"] = "declarative"
	mapField(live, "spec")["selector"] = map[string]any{"app": "controller"}
	if declarativerelease.ResourceDesiredSubset(desired, live) {
		t.Fatal("runtime spec drift was ignored")
	}
}

func TestWorkloadOriginatedServiceProbeUsesEveryReadySourcePod(t *testing.T) {
	directory := t.TempDir()
	kubectl := filepath.Join(directory, "kubectl")
	logPath := filepath.Join(directory, "kubectl.log")
	program := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KUBECTL_LOG"
case "$*" in
  "get deployment fugue-fugue-api --namespace fugue-system --output json --show-managed-fields --ignore-not-found")
    printf '%s\n' '{"spec":{"selector":{"matchLabels":{"app":"api"}},"template":{"metadata":{"labels":{"app":"api"}}}}}'
    ;;
  "get pods --namespace fugue-system --selector app=api --output json")
    printf '%s\n' '{"items":[{"metadata":{"name":"api-b"},"spec":{"containers":[{"name":"api"}]},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"api-a"},"spec":{"containers":[{"name":"api"}]},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"api-not-ready"},"spec":{"containers":[{"name":"api"}]},"status":{"conditions":[{"type":"Ready","status":"False"}]}}]}'
    ;;
  "get service edge-control-de --namespace fugue-system --output json --show-managed-fields --ignore-not-found")
    printf '%s\n' '{"spec":{"ports":[{"name":"http","port":8092}]}}'
    ;;
  "exec --namespace fugue-system api-a --container api -- wget -qO- -T 5 http://edge-control-de:8092/v1/authority/groups/edge-pool-a/readyz"|\
  "exec --namespace fugue-system api-b --container api -- wget -qO- -T 5 http://edge-control-de:8092/v1/authority/groups/edge-pool-a/readyz")
    printf '%s\n' '{"ready":true}'
    ;;
  *)
    exit 41
    ;;
esac
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECTL_LOG", logPath)
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	probe := declarativerelease.HealthProbe{
		Type: "service-http-via-workload", Name: "edge-control-de", Port: "http",
		Path: "/v1/authority/groups/edge-pool-a/readyz", Expected: "\"ready\":true",
		SourceWorkload: "fugue-fugue-api", SourceContainer: "api",
	}
	evidence, err := cluster.readServiceHTTPViaWorkload(context.Background(), "fugue-system", probe)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(evidence), "\n")
	if len(lines) != 2 || !strings.HasPrefix(lines[0], "api-a:sha256:") || !strings.HasPrefix(lines[1], "api-b:sha256:") {
		t.Fatalf("unexpected workload-originated evidence: %q", evidence)
	}
	logRaw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(logRaw), "wget -qO- -T 5 http://edge-control-de:8092/v1/authority/groups/edge-pool-a/readyz") != 2 {
		t.Fatalf("probe did not execute from every ready source Pod: %s", logRaw)
	}
}

func TestServiceHTTPProbeUsesInClusterDataPlaneAndKeepsExternalPrepareProxy(t *testing.T) {
	directory := t.TempDir()
	kubectl := filepath.Join(directory, "kubectl")
	logPath := filepath.Join(directory, "kubectl.log")
	program := `#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$KUBECTL_LOG"
case "$*" in
  "get service fugue-fugue --namespace fugue-system --output json --show-managed-fields --ignore-not-found")
    printf '%s\n' '{"spec":{"ports":[{"name":"http","port":80}]}}'
    ;;
  "get --raw /api/v1/namespaces/fugue-system/services/fugue-fugue:http/proxy/healthz")
    printf '%s\n' '{"status":"proxy-ok"}'
    ;;
  *) exit 41 ;;
esac
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECTL_LOG", logPath)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/healthz" {
			t.Fatalf("path = %q", request.URL.Path)
		}
		_, _ = writer.Write([]byte(`{"status":"direct-ok"}`))
	}))
	defer server.Close()
	probe := declarativerelease.HealthProbe{Type: "service-http", Name: "fugue-fugue", Port: "http", Path: "/healthz", Expected: "ok"}
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second, serviceHTTPURL: func(namespace, name string, port int) string {
		if namespace != "fugue-system" || name != "fugue-fugue" || port != 80 {
			t.Fatalf("service identity = %s/%s:%d", namespace, name, port)
		}
		return server.URL
	}}
	body, err := cluster.readServiceHTTP(context.Background(), "fugue-system", probe)
	if err != nil || string(body) != `{"status":"direct-ok"}` {
		t.Fatalf("direct body=%q err=%v", body, err)
	}
	logRaw, err := os.ReadFile(logPath)
	if err != nil || strings.Contains(string(logRaw), "get --raw") {
		t.Fatalf("in-cluster probe used API proxy: %q err=%v", logRaw, err)
	}
	cluster.serviceHTTPURL = nil
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.43.0.1")
	if got := cluster.serviceHTTPDataPlaneURL("fugue-system", "fugue-fugue", 80); got != "http://fugue-fugue.fugue-system.svc:80" {
		t.Fatalf("in-cluster data-plane URL = %q", got)
	}

	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	body, err = cluster.readServiceHTTP(context.Background(), "fugue-system", probe)
	if err != nil || string(body) != "{\"status\":\"proxy-ok\"}\n" {
		t.Fatalf("prepare proxy body=%q err=%v", body, err)
	}
}

func TestWorkloadOriginatedServiceProbeParsersAreExact(t *testing.T) {
	service := []byte(`{"spec":{"ports":[{"name":"http","port":8092},{"name":"metrics","port":9090}]}}`)
	if port, err := servicePortByName(service, "http"); err != nil || port != 8092 {
		t.Fatalf("service port=%d err=%v", port, err)
	}
	if _, err := servicePortByName(service, "missing"); err == nil {
		t.Fatal("missing service port was accepted")
	}
	pods := []byte(`{"items":[{"metadata":{"name":"api-b"},"spec":{"containers":[{"name":"api"}]},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"api-a"},"spec":{"containers":[{"name":"api"}]},"status":{"conditions":[{"type":"Ready","status":"True"}]}},{"metadata":{"name":"ignored"},"spec":{"containers":[{"name":"api"}]},"status":{"conditions":[{"type":"Ready","status":"False"}]}}]}`)
	ready, err := readyWorkloadPods(pods, "api")
	if err != nil || strings.Join(ready, ",") != "api-a,api-b" {
		t.Fatalf("ready pods=%v err=%v", ready, err)
	}
	if _, err := readyWorkloadPods(pods, "controller"); err == nil {
		t.Fatal("missing source container was accepted")
	}
}

func TestMonitorAcceptsDeclarativeOwnershipWithoutDeletingManagedFields(t *testing.T) {
	release := declarativerelease.PlanRelease{
		Workload: declarativerelease.Workload{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api",
			Container: "api", FieldManager: "fugue-api-declarative",
		},
		ArtifactTargets: []declarativerelease.ArtifactTarget{{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api",
			Container: "api", ContainerType: "container",
		}},
	}
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "fugue-fugue-api", "namespace": "fugue-system", "uid": "api-uid", "resourceVersion": "42",
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
			"spec": map[string]any{"containers": []any{map[string]any{
				"name": "api", "image": "ghcr.io/example/api@sha256:" + strings.Repeat("c", 64),
			}}},
		}},
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"}
	allowed := ownershipConvergencePointers(release, identity, desired)
	live := deepCopyJSONMap(t, desired)
	metadata := mapField(live, "metadata")
	metadata["uid"], metadata["resourceVersion"], metadata["generation"] = "api-uid", "42", json.Number("7")
	declarativeEntry := map[string]any{"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, allowed)}
	emergencyPointer := "/spec/template/spec/containers[name=api]/image"
	emergencyEntry := map[string]any{"manager": "kubectl", "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{emergencyPointer})}
	metadata["managedFields"] = []any{declarativeEntry, emergencyEntry}
	if err := verifyNoEmergencyOwnership(release, identity, desired, live); err != nil {
		t.Fatalf("declarative ownership with harmless Update history was rejected: %v", err)
	}
	metadata["managedFields"] = []any{emergencyEntry}
	if err := verifyNoEmergencyOwnership(release, identity, desired, live); err == nil || !strings.Contains(err.Error(), "lacks declarative ownership") {
		t.Fatalf("missing declarative ownership was accepted: %v", err)
	}
}

func TestEmergencyOwnershipAllowlistIncludesPrimaryWorkloadImageWithoutArtifactTargets(t *testing.T) {
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{
		APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api",
		Container: "api", FieldManager: "fugue-api-declarative",
	}}
	desired := map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/production-config-sha": strings.Repeat("a", 40)}},
		"spec": map[string]any{"template": map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": strings.Repeat("a", 40)}},
			"spec": map[string]any{"containers": []any{map[string]any{
				"name": "api", "image": "ghcr.io/example/api@sha256:" + strings.Repeat("b", 64),
			}}},
		}},
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api"}
	allowed := emergencyOwnershipPointers(release, identity, desired)
	if !stringSubset([]string{"/spec/template/spec/containers[name=api]/image"}, allowed) {
		t.Fatalf("primary workload image is absent from emergency ownership allowlist: %v", allowed)
	}
	other := identity
	other.Name = "fugue-fugue-controller"
	if got := emergencyOwnershipPointers(release, other, desired); stringSubset([]string{"/spec/template/spec/containers[name=api]/image"}, got) {
		t.Fatalf("primary workload image leaked into a different resource allowlist: %v", got)
	}
}

func TestOwnershipConvergenceAllowlistIncludesOnlyReviewedRuntimeScalars(t *testing.T) {
	release := declarativerelease.PlanRelease{
		Workload: declarativerelease.Workload{
			APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-worker",
			Container: "edge", FieldManager: "fugue-edge-worker-declarative",
		},
		ArtifactTargets: []declarativerelease.ArtifactTarget{{
			APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-worker",
			Container: "edge", ContainerType: "container",
		}},
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "edge-worker"}
	desired := map[string]any{
		"metadata": map[string]any{"annotations": map[string]any{"fugue.pro/source-commit": strings.Repeat("a", 40)}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{
				"name": "edge", "image": "ghcr.io/example/edge@sha256:" + strings.Repeat("b", 64),
				"env": []any{
					map[string]any{"name": "FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "value": "true"},
					map[string]any{"name": "FUGUE_DNS_TOKEN", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "dns-token", "key": "token"}}},
					map[string]any{"name": "INVALID-NAME", "value": "false"},
					map[string]any{"name": "DUPLICATE", "value": "first"},
					map[string]any{"name": "DUPLICATE", "value": "second"},
				},
				"resources": map[string]any{
					"limits":   map[string]any{"cpu": "2", "memory": "512Mi"},
					"requests": map[string]any{"cpu": "100m", "memory": "128Mi"},
				},
				"livenessProbe":  map[string]any{"httpGet": map[string]any{"path": "/livez", "port": "health"}, "periodSeconds": json.Number("10")},
				"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/readyz", "port": "health"}, "timeoutSeconds": json.Number("3")},
				"startupProbe":   map[string]any{"httpGet": map[string]any{"path": "/healthz", "port": "health"}},
			}},
		}}},
	}
	allowed := ownershipConvergencePointers(release, identity, desired)
	envPointer := "/spec/template/spec/containers[name=edge]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED]/value"
	if stringSubset([]string{envPointer}, emergencyOwnershipPointers(release, identity, desired)) {
		t.Fatalf("environment value entered the emergency rollback allowlist: %s", envPointer)
	}
	want := []string{
		"/spec/template/spec/containers[name=edge]/image",
		"/spec/template/spec/containers[name=edge]/resources/limits/cpu",
		"/spec/template/spec/containers[name=edge]/resources/requests/cpu",
		"/spec/template/spec/containers[name=edge]/livenessProbe/httpGet/path",
		"/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/path",
		"/spec/template/spec/containers[name=edge]/startupProbe/httpGet/path",
		envPointer,
	}
	if !stringSubset(want, allowed) {
		t.Fatalf("reviewed runtime scalars missing from ownership convergence allowlist: allowed=%v want=%v", allowed, want)
	}
	cleanupAllowed := ownershipCleanupPointers(allowed)
	for _, structural := range []string{
		"/spec/template/spec/containers[name=edge]/name",
		"/spec/template/spec/containers[name=edge]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED]/name",
	} {
		if !stringSubset([]string{structural}, cleanupAllowed) {
			t.Fatalf("associative-list identity leaf missing from cleanup allowlist: %s in %v", structural, cleanupAllowed)
		}
	}
	for _, forbidden := range []string{
		"/spec/template/spec/containers[name=edge]/env[name=FUGUE_DNS_TOKEN]/name",
		"/spec/template/spec/containers[name=other]/name",
		"/spec/template/spec/containers[name=edge]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED]/valueFrom",
	} {
		if stringSubset([]string{forbidden}, cleanupAllowed) {
			t.Fatalf("unreviewed structural field entered ownership cleanup allowlist: %s in %v", forbidden, cleanupAllowed)
		}
	}
	for _, forbidden := range []string{
		"/spec/template/spec/containers[name=edge]/livenessProbe/periodSeconds",
		"/spec/template/spec/containers[name=edge]/readinessProbe/timeoutSeconds",
		"/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/port",
		"/spec/template/spec/containers[name=edge]/resources/limits/memory",
		"/spec/template/spec/containers[name=edge]/resources/requests/memory",
		"/spec/template/spec/containers[name=edge]/env[name=FUGUE_DNS_TOKEN]/value",
		"/spec/template/spec/containers[name=edge]/env[name=INVALID-NAME]/value",
		"/spec/template/spec/containers[name=edge]/env[name=DUPLICATE]/value",
	} {
		if stringSubset([]string{forbidden}, allowed) {
			t.Fatalf("unreviewed workload field entered ownership convergence allowlist: %s in %v", forbidden, allowed)
		}
	}
	live := deepCopyJSONMap(t, desired)
	if value, ok := emergencyRuntimePointerValue(live, "/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/path"); !ok || value != "/readyz" {
		t.Fatalf("probe path pointer read failed: value=%q ok=%v", value, ok)
	}
	if !setEmergencyRuntimePointerValue(live, "/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/path", "/livez") {
		t.Fatal("probe path pointer write failed")
	}
	if value, ok := emergencyRuntimePointerValue(live, "/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/path"); !ok || value != "/livez" {
		t.Fatalf("probe path pointer write was not observable: value=%q ok=%v", value, ok)
	}
	cpuPointer := "/spec/template/spec/containers[name=edge]/resources/limits/cpu"
	if value, ok := emergencyRuntimePointerValue(live, cpuPointer); !ok || value != "2" {
		t.Fatalf("CPU resource pointer read failed: value=%q ok=%v", value, ok)
	}
	if !setEmergencyRuntimePointerValue(live, cpuPointer, "1500m") {
		t.Fatal("CPU resource pointer write failed")
	}
	if value, ok := emergencyRuntimePointerValue(live, cpuPointer); !ok || value != "1500m" {
		t.Fatalf("CPU resource pointer write was not observable: value=%q ok=%v", value, ok)
	}
	if value, ok := emergencyRuntimePointerValue(live, envPointer); !ok || value != "true" {
		t.Fatalf("environment value pointer read failed: value=%q ok=%v", value, ok)
	}
	if !setEmergencyRuntimePointerValue(live, envPointer, "false") {
		t.Fatal("environment value pointer write failed")
	}
	if value, ok := emergencyRuntimePointerValue(live, envPointer); !ok || value != "false" {
		t.Fatalf("environment value pointer write was not observable: value=%q ok=%v", value, ok)
	}
	missingResources := deepCopyJSONMap(t, live)
	delete(anySlice(mapField(mapField(mapField(missingResources, "spec"), "template"), "spec")["containers"])[0].(map[string]any), "resources")
	if setEmergencyRuntimePointerValue(missingResources, cpuPointer, "1") {
		t.Fatal("CPU resource pointer write created an undeclared resource map")
	}
}

func TestLegacyOwnershipTransferPatchMovesOnlyExactProbePaths(t *testing.T) {
	liveness := "/spec/template/spec/containers[name=edge]/livenessProbe/httpGet/path"
	readiness := "/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/path"
	image := "/spec/template/spec/containers[name=edge]/image"
	port := "/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/port"
	desired := map[string]any{
		"metadata": map[string]any{"uid": "worker-uid", "resourceVersion": "42"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "edge", "image": "target",
			"livenessProbe":  map[string]any{"httpGet": map[string]any{"path": "/livez", "port": "health"}},
			"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/readyz", "port": "health"}},
		}}}}},
	}
	entry := map[string]any{
		"manager": "helm", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{liveness, readiness, image, port}),
	}
	live := deepCopyJSONMap(t, desired)
	liveMetadata := mapField(live, "metadata")
	liveMetadata["managedFields"] = []any{
		map[string]any{"manager": "declarative", "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{liveness, readiness})},
		entry,
	}
	edge := anySlice(mapField(mapField(mapField(live, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	mapField(mapField(edge, "livenessProbe"), "httpGet")["path"] = "/healthz"
	mapField(mapField(edge, "readinessProbe"), "httpGet")["path"] = "/healthz"
	allowed := []string{liveness, readiness, image}
	applyErr := errors.New(strings.Join([]string{
		"Apply failed with 2 conflicts: conflicts with \"helm\" using apps/v1:",
		"- " + ssaFieldForPointer(liveness),
		"- " + ssaFieldForPointer(readiness),
	}, "\n"))
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "", applyErr); err != nil {
		t.Fatalf("exact legacy probe conflicts were rejected: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, allowed, "", applyErr)
	if err != nil || !found {
		t.Fatalf("legacy probe value patch was not produced: patch=%v found=%v err=%v", patch, found, err)
	}
	if len(patch) != 7 || patch[0]["path"] != "/metadata/uid" || patch[1]["path"] != "/metadata/resourceVersion" ||
		patch[2]["path"] != "/spec/template/spec/containers/0/name" {
		t.Fatalf("legacy probe value patch lacks exact CAS tests: %v", patch)
	}
	replacements := make(map[string]any)
	for _, operation := range patch {
		path := fmt.Sprint(operation["path"])
		if strings.Contains(path, "/managedFields/") {
			t.Fatalf("legacy probe patch mutates managedFields internals: %v", patch)
		}
		if operation["op"] == "replace" {
			replacements[path] = operation["value"]
		}
	}
	if replacements["/spec/template/spec/containers/0/livenessProbe/httpGet/path"] != "/livez" ||
		replacements["/spec/template/spec/containers/0/readinessProbe/httpGet/path"] != "/readyz" {
		t.Fatalf("legacy probe patch moved unexpected values: %v", patch)
	}
	mixed := errors.New(strings.Join([]string{
		"Apply failed with 2 conflicts:",
		`conflict with "helm" using apps/v1: ` + ssaFieldForPointer(liveness),
		`conflict with "kubectl" using apps/v1: ` + ssaFieldForPointer(image),
	}, "\n"))
	if patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, allowed, "", mixed); err != nil || !found || len(patch) == 0 {
		t.Fatalf("mixed reviewed scalar ownership transfer was rejected: patch=%v found=%v err=%v", patch, found, err)
	}
}

func TestEmergencyOwnershipWitnessAcceptsStatusOnlyResourceVersionAdvance(t *testing.T) {
	pointer := "/spec/template/spec/containers[name=edge]/image"
	desired := map[string]any{
		"metadata": map[string]any{"uid": "worker-uid", "resourceVersion": "42"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "edge", "image": "target",
		}}}}},
	}
	live := deepCopyJSONMap(t, desired)
	liveMetadata := mapField(live, "metadata")
	liveMetadata["resourceVersion"] = "57"
	liveMetadata["managedFields"] = []any{map[string]any{
		"manager": "kubectl-set", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{pointer}),
	}}
	container := anySlice(mapField(mapField(mapField(live, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	container["image"] = "old"
	applyErr := errors.New(`Apply failed with 1 conflict: conflict with "kubectl-set" using apps/v1: ` + ssaFieldForPointer(pointer))

	if err := validateEmergencyOwnershipConflictEvidence(desired, live, []string{pointer}, "declarative", applyErr); err != nil {
		t.Fatalf("status-only resourceVersion advance was rejected: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, []string{pointer}, "declarative", applyErr)
	if err != nil || !found || len(patch) < 2 || patch[1]["path"] != "/metadata/resourceVersion" || patch[1]["value"] != "57" {
		t.Fatalf("ownership transfer was not bound to the latest live RV: patch=%v found=%v err=%v", patch, found, err)
	}

	wrongUID := deepCopyJSONMap(t, live)
	mapField(wrongUID, "metadata")["uid"] = "replacement-uid"
	if err := validateEmergencyOwnershipConflictEvidence(desired, wrongUID, []string{pointer}, "declarative", applyErr); err == nil || !strings.Contains(err.Error(), "UID changed") {
		t.Fatalf("replacement UID was accepted: %v", err)
	}
	invalidRV := deepCopyJSONMap(t, live)
	mapField(invalidRV, "metadata")["resourceVersion"] = "057"
	if err := validateEmergencyOwnershipConflictEvidence(desired, invalidRV, []string{pointer}, "declarative", applyErr); err == nil || !strings.Contains(err.Error(), "valid resourceVersion") {
		t.Fatalf("non-canonical live RV was accepted: %v", err)
	}
}

func TestLegacyOwnershipTransferPatchMovesOnlyExactEnvironmentValues(t *testing.T) {
	enabled := "/spec/template/spec/containers[name=dns]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED]/value"
	timeout := "/spec/template/spec/containers[name=dns]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_TIMEOUT]/value"
	unreviewed := "/spec/template/spec/containers[name=dns]/env[name=FUGUE_DNS_MAX_STALE]/value"
	desired := map[string]any{
		"metadata": map[string]any{"uid": "dns-uid", "resourceVersion": "42", "generation": json.Number("7")},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "dns",
			"env": []any{
				map[string]any{"name": "FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "value": "true"},
				map[string]any{"name": "FUGUE_DNS_EDGE_HEALTH_PROBE_TIMEOUT", "value": "2s"},
				map[string]any{"name": "FUGUE_DNS_MAX_STALE", "value": "24h"},
			},
		}}}}},
	}
	live := deepCopyJSONMap(t, desired)
	liveMetadata := mapField(live, "metadata")
	liveMetadata["managedFields"] = []any{map[string]any{
		"manager": "helm", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{enabled, timeout, unreviewed}),
	}}
	dns := anySlice(mapField(mapField(mapField(live, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	anySlice(dns["env"])[0].(map[string]any)["value"] = "false"
	anySlice(dns["env"])[1].(map[string]any)["value"] = "250ms"
	allowed := []string{enabled, timeout}
	applyErr := errors.New(strings.Join([]string{
		"Apply failed with 2 conflicts: conflicts with \"helm\" using apps/v1:",
		"- " + ssaFieldForPointer(enabled),
		"- " + ssaFieldForPointer(timeout),
	}, "\n"))
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "", applyErr); err != nil {
		t.Fatalf("exact legacy environment conflicts were rejected: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, allowed, "", applyErr)
	if err != nil || !found {
		t.Fatalf("legacy environment value patch was not produced: patch=%v found=%v err=%v", patch, found, err)
	}
	if len(patch) != 9 || patch[0]["path"] != "/metadata/uid" || patch[1]["path"] != "/metadata/resourceVersion" ||
		patch[2]["path"] != "/spec/template/spec/containers/0/name" ||
		patch[3]["path"] != "/spec/template/spec/containers/0/env/0/name" ||
		patch[6]["path"] != "/spec/template/spec/containers/0/env/1/name" {
		t.Fatalf("legacy environment patch lacks exact identity tests: %v", patch)
	}
	replacements := make(map[string]any)
	for _, operation := range patch {
		path := fmt.Sprint(operation["path"])
		if strings.Contains(path, "/managedFields/") {
			t.Fatalf("legacy environment patch mutates managedFields internals: %v", patch)
		}
		if operation["op"] == "replace" {
			replacements[path] = operation["value"]
		}
	}
	if replacements["/spec/template/spec/containers/0/env/0/value"] != "true" ||
		replacements["/spec/template/spec/containers/0/env/1/value"] != "2s" {
		t.Fatalf("legacy environment patch moved unexpected values: %v", patch)
	}
	fresh := deepCopyJSONMap(t, desired)
	mapField(fresh, "metadata")["resourceVersion"] = "43"
	mapField(fresh, "metadata")["generation"] = json.Number("8")
	expected, err := expectedStateAfterLegacyOwnershipTransfer(desired, live, fresh, allowed, applyErr)
	if err != nil {
		t.Fatalf("legacy environment expected state failed: %v", err)
	}
	if value, ok := emergencyRuntimePointerValue(expected, enabled); !ok || value != "true" {
		t.Fatalf("expected environment state did not contain enabled probe: value=%q ok=%v", value, ok)
	}

	ambiguous := deepCopyJSONMap(t, live)
	ambiguousDNS := anySlice(mapField(mapField(mapField(ambiguous, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	ambiguousDNS["env"] = append(anySlice(ambiguousDNS["env"]), map[string]any{
		"name": "FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "value": "false",
	})
	if _, _, err := nextLegacyOwnershipTransferPatch(desired, ambiguous, allowed, "", applyErr); err == nil {
		t.Fatalf("ambiguous environment identity was accepted: %v", err)
	}
}

func TestExistingBridgeOwnershipTransfersOnlyReviewedAssociativeValue(t *testing.T) {
	enabled := "/spec/template/spec/containers[name=dns]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED]/value"
	containerName := "/spec/template/spec/containers[name=dns]/name"
	envName := "/spec/template/spec/containers[name=dns]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED]/name"
	desired := map[string]any{
		"metadata": map[string]any{"uid": "dns-uid", "resourceVersion": "43"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "dns", "env": []any{map[string]any{
				"name": "FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "value": "false",
			}},
		}}}}},
	}
	live := deepCopyJSONMap(t, desired)
	mapField(live, "metadata")["managedFields"] = []any{map[string]any{
		"manager": "fugue-probe-bridge-0123456789abcdef", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{containerName, envName, enabled}),
	}}
	applyErr := errors.New("Apply failed with 1 conflict: conflict with \"fugue-probe-bridge-0123456789abcdef\" using apps/v1: " + ssaFieldForPointer(enabled))
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, []string{enabled}, "", applyErr); err != nil {
		t.Fatalf("reviewed associative identity conflict was rejected: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, []string{enabled}, "", applyErr)
	if err != nil || !found {
		t.Fatalf("reviewed associative value transfer failed: patch=%v found=%v err=%v", patch, found, err)
	}
	encoded := string(mustJSON(t, patch))
	if strings.Contains(encoded, "/managedFields/") || !strings.Contains(encoded, `"path":"/spec/template/spec/containers/0/env/0/value"`) {
		t.Fatalf("associative transfer was not an exact value patch: %s", encoded)
	}

	expanded := deepCopyJSONMap(t, live)
	entry := anySlice(mapField(expanded, "metadata")["managedFields"])[0].(map[string]any)
	entry["fieldsV1"] = managedFieldsTree(t, []string{
		containerName, envName, enabled,
		"/spec/template/spec/containers[name=dns]/env[name=FUGUE_DNS_MAX_STALE]/name",
	})
	if err := validateEmergencyOwnershipConflictEvidence(desired, expanded, []string{enabled}, "", applyErr); err == nil || !strings.Contains(err.Error(), "expands beyond") {
		t.Fatalf("unreviewed associative ownership was accepted: %v", err)
	}
}

func TestProbeOwnershipTransferAcceptsAlreadyDesiredBroadEmergencyManager(t *testing.T) {
	liveness := "/spec/template/spec/containers[name=edge]/livenessProbe/httpGet/path"
	readiness := "/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/path"
	unreviewed := "/spec/template/spec/containers[name=edge]/env[name=EXTRA]/value"
	desired := map[string]any{
		"metadata": map[string]any{"uid": "worker-uid", "resourceVersion": "43"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name":           "edge",
			"livenessProbe":  map[string]any{"httpGet": map[string]any{"path": "/livez"}},
			"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/readyz"}},
		}}}}},
	}
	live := deepCopyJSONMap(t, desired)
	mapField(live, "metadata")["managedFields"] = []any{map[string]any{
		"manager": "kubectl-patch", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{liveness, readiness, unreviewed}),
	}}
	allowed := []string{liveness, readiness}
	applyErr := errors.New(strings.Join([]string{
		"Apply failed with 2 conflicts: conflicts with \"kubectl-patch\" using apps/v1:",
		"- " + ssaFieldForPointer(liveness),
		"- " + ssaFieldForPointer(readiness),
	}, "\n"))
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "", applyErr); err != nil {
		t.Fatalf("broad emergency probe witness was rejected before exact transfer: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, allowed, "", applyErr)
	if err != nil || !found {
		t.Fatalf("already-desired probe transfer patch=%v found=%v err=%v", patch, found, err)
	}
	replaces := 0
	for _, operation := range patch {
		if operation["op"] == "replace" {
			replaces++
			if value := operation["value"]; value != "/livez" && value != "/readyz" {
				t.Fatalf("already-desired transfer changed an unexpected value: %v", patch)
			}
		}
	}
	if replaces != 2 {
		t.Fatalf("already-desired transfer did not rewrite both exact paths: %v", patch)
	}
}

func TestExistingProbeBridgeManagerNamesRemainRecognized(t *testing.T) {
	if !emergencyOwnershipManager(probeBridgeManagerPrefix + "0123456789abcdef") {
		t.Fatal("existing transaction bridge manager was not recognized for migration")
	}
	for _, invalid := range []string{probeBridgeManagerPrefix + "1234", probeBridgeManagerPrefix + "0123456789abcdeg", probeBridgeManagerPrefix + "0123456789ABCDEf"} {
		if emergencyOwnershipManager(invalid) {
			t.Fatalf("invalid probe bridge manager was accepted: %q", invalid)
		}
	}
}

func TestLegacyProbeOwnershipConvergenceUsesCASBoundValueMove(t *testing.T) {
	directory := t.TempDir()
	kubectl := filepath.Join(directory, "kubectl")
	livePath := filepath.Join(directory, "live.json")
	transferredPath := filepath.Join(directory, "transferred.json")
	transferredMarker := filepath.Join(directory, "transferred")
	applyInputPath := filepath.Join(directory, "apply-input.json")
	argumentsPath := filepath.Join(directory, "apply-arguments.log")
	patchesPath := filepath.Join(directory, "patches.log")
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a",
		Container: "edge", FieldManager: "fugue-edge-worker-de-declarative",
	}}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a"}
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "worker-a", "namespace": "fugue-system", "uid": "worker-uid", "resourceVersion": "42"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "edge", "image": "ghcr.io/example/edge@sha256:" + strings.Repeat("a", 64),
			"livenessProbe":  map[string]any{"httpGet": map[string]any{"path": "/livez", "port": "health"}},
			"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/readyz", "port": "health"}},
		}}}}},
	}
	liveness := "/spec/template/spec/containers[name=edge]/livenessProbe/httpGet/path"
	readiness := "/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/path"
	allowed := emergencyOwnershipPointers(release, identity, desired)
	live := deepCopyJSONMap(t, desired)
	liveMetadata := mapField(live, "metadata")
	liveMetadata["generation"] = json.Number("7")
	containers := anySlice(mapField(mapField(mapField(live, "spec"), "template"), "spec")["containers"])
	edge := containers[0].(map[string]any)
	mapField(mapField(edge, "livenessProbe"), "httpGet")["path"] = "/healthz"
	mapField(mapField(edge, "readinessProbe"), "httpGet")["path"] = "/healthz"
	liveMetadata["managedFields"] = []any{
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, allowed)},
		map[string]any{"manager": "helm", "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{liveness, readiness})},
	}
	transferred := deepCopyJSONMap(t, desired)
	transferredMetadata := mapField(transferred, "metadata")
	transferredMetadata["resourceVersion"] = "43"
	transferredMetadata["generation"] = json.Number("8")
	transferredMetadata["managedFields"] = []any{
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, allowed)},
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{liveness, readiness})},
	}
	if err := os.WriteFile(livePath, mustJSON(t, live), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(transferredPath, mustJSON(t, transferred), 0o600); err != nil {
		t.Fatal(err)
	}
	program := `#!/bin/sh
set -eu
case "$1" in
  apply)
    printf '%s\n' "$*" >>"$APPLY_ARGUMENTS"
	if test -e "$TRANSFERRED"; then
	  cat >"$APPLY_INPUT"
	  cat "$TRANSFERRED_JSON"
	else
	  cat >/dev/null
	  printf '%s\n' 'Apply failed with 2 conflicts: conflicts with "helm" using apps/v1:' >&2
	  printf '%s\n' '- .spec.template.spec.containers[name="edge"].livenessProbe.httpGet.path' >&2
	  printf '%s\n' '- .spec.template.spec.containers[name="edge"].readinessProbe.httpGet.path' >&2
	  exit 1
	fi
    ;;
  get)
	if test -e "$TRANSFERRED"; then cat "$TRANSFERRED_JSON"; else cat "$LIVE_JSON"; fi
	;;
  patch)
	printf '%s\n' "$*" >>"$PATCHES"
	case " $* " in *" --field-manager fugue-edge-worker-de-declarative "*) ;; *) exit 53 ;; esac
	: >"$TRANSFERRED"
	cat "$TRANSFERRED_JSON"
    ;;
  *) exit 52 ;;
esac
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LIVE_JSON", livePath)
	t.Setenv("TRANSFERRED_JSON", transferredPath)
	t.Setenv("TRANSFERRED", transferredMarker)
	t.Setenv("APPLY_INPUT", applyInputPath)
	t.Setenv("APPLY_ARGUMENTS", argumentsPath)
	t.Setenv("PATCHES", patchesPath)
	encoded := mustJSON(t, desired)
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	if err := cluster.applyResourceWithOwnershipConvergence(context.Background(), release, identity, desired, encoded, false); err != nil {
		t.Fatalf("exact legacy probe ownership transfer failed: %v", err)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(arguments)), "\n")
	if len(lines) != 2 {
		t.Fatalf("unexpected apply sequence: %q", arguments)
	}
	patches, err := os.ReadFile(patchesPath)
	if err != nil {
		t.Fatal(err)
	}
	patchLines := strings.Split(strings.TrimSpace(string(patches)), "\n")
	if len(patchLines) != 1 || !strings.Contains(patchLines[0], "--field-manager "+release.Workload.FieldManager) ||
		!strings.Contains(patchLines[0], `"path":"/metadata/uid"`) ||
		!strings.Contains(patchLines[0], `"op":"replace"`) || strings.Contains(patchLines[0], "/managedFields/") {
		t.Fatalf("unexpected same-manager scalar transfer patch: %q", patches)
	}
	applyInput, err := os.ReadFile(applyInputPath)
	if err != nil {
		t.Fatal(err)
	}
	var rebound map[string]any
	if err := json.Unmarshal(applyInput, &rebound); err != nil {
		t.Fatal(err)
	}
	metadata := mapField(rebound, "metadata")
	if stringValue(metadata["uid"]) != "worker-uid" || stringValue(metadata["resourceVersion"]) != "43" {
		t.Fatalf("ordinary apply was not rebound to the exact post-transfer CAS: %s", applyInput)
	}
}

func TestScalarOwnershipApplyRetriesStatusOnlyResourceVersionConflict(t *testing.T) {
	directory := t.TempDir()
	kubectl := filepath.Join(directory, "kubectl")
	livePath := filepath.Join(directory, "live.json")
	transferredPath := filepath.Join(directory, "transferred.json")
	racedPath := filepath.Join(directory, "raced.json")
	applyInputPath := filepath.Join(directory, "apply-input.json")
	secondApplyInputPath := filepath.Join(directory, "second-apply-input.json")
	applyCountPath := filepath.Join(directory, "apply-count")
	patchesPath := filepath.Join(directory, "patches.log")
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a",
		Container: "edge", FieldManager: "fugue-edge-worker-de-declarative",
	}}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a"}
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "worker-a", "namespace": "fugue-system", "uid": "worker-uid", "resourceVersion": "42"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "edge", "image": "ghcr.io/example/edge@sha256:" + strings.Repeat("a", 64),
		}}}}},
	}
	image := "/spec/template/spec/containers[name=edge]/image"
	live := deepCopyJSONMap(t, desired)
	liveMetadata := mapField(live, "metadata")
	liveMetadata["generation"] = json.Number("7")
	liveContainers := anySlice(mapField(mapField(mapField(live, "spec"), "template"), "spec")["containers"])
	liveContainers[0].(map[string]any)["image"] = "ghcr.io/example/edge@sha256:" + strings.Repeat("b", 64)
	liveMetadata["managedFields"] = []any{
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{image})},
		map[string]any{"manager": "helm", "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{image})},
	}
	transferred := deepCopyJSONMap(t, desired)
	transferredMetadata := mapField(transferred, "metadata")
	transferredMetadata["resourceVersion"] = "43"
	transferredMetadata["generation"] = json.Number("8")
	transferredMetadata["managedFields"] = []any{map[string]any{
		"manager": release.Workload.FieldManager, "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{image}),
	}}
	raced := deepCopyJSONMap(t, transferred)
	racedMetadata := mapField(raced, "metadata")
	racedMetadata["resourceVersion"] = "44"
	racedMetadata["managedFields"] = []any{map[string]any{
		"manager": release.Workload.FieldManager, "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{image}),
	}}
	for filename, value := range map[string]map[string]any{livePath: live, transferredPath: transferred, racedPath: raced} {
		if err := os.WriteFile(filename, mustJSON(t, value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	program := `#!/bin/sh
set -eu
case "$1" in
  apply)
    count=0
    if test -f "$APPLY_COUNT"; then count=$(cat "$APPLY_COUNT"); fi
    count=$((count + 1))
    printf '%s' "$count" >"$APPLY_COUNT"
    case "$count" in
      1) cat >/dev/null; printf '%s\n' 'Apply failed with 1 conflict: conflict with "helm" using apps/v1: .spec.template.spec.containers[name="edge"].image' >&2; exit 1 ;;
      2) cat >"$APPLY_INPUT"; printf '%s\n' 'The request is invalid: Operation cannot be fulfilled on daemonsets.apps "worker-a": the object has been modified; please apply your changes to the latest version and try again' >&2; exit 1 ;;
      3) cat >"$SECOND_APPLY_INPUT"; cat "$RACED_JSON" ;;
      *) exit 52 ;;
    esac
    ;;
  get)
    if test -f "$TRANSFERRED"; then
      count=0
      if test -f "$APPLY_COUNT"; then count=$(cat "$APPLY_COUNT"); fi
      if test "$count" -ge 2; then cat "$RACED_JSON"; else cat "$TRANSFERRED_JSON"; fi
    else
      cat "$LIVE_JSON"
    fi
    ;;
  patch)
    printf '%s\n' "$*" >>"$PATCHES"
    : >"$TRANSFERRED"
    cat "$TRANSFERRED_JSON"
    ;;
  *) exit 52 ;;
esac
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	transferredMarker := filepath.Join(directory, "transferred")
	t.Setenv("LIVE_JSON", livePath)
	t.Setenv("TRANSFERRED_JSON", transferredPath)
	t.Setenv("TRANSFERRED", transferredMarker)
	t.Setenv("RACED_JSON", racedPath)
	t.Setenv("APPLY_INPUT", applyInputPath)
	t.Setenv("SECOND_APPLY_INPUT", secondApplyInputPath)
	t.Setenv("APPLY_COUNT", applyCountPath)
	t.Setenv("PATCHES", patchesPath)
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	if err := cluster.applyResourceWithOwnershipConvergence(context.Background(), release, identity, desired, mustJSON(t, desired), false); err != nil {
		t.Fatalf("status-only resourceVersion conflict was not retried: %v", err)
	}
	firstRetry, err := os.ReadFile(applyInputPath)
	if err != nil {
		t.Fatal(err)
	}
	secondRetry, err := os.ReadFile(secondApplyInputPath)
	if err != nil {
		t.Fatal(err)
	}
	var first, second map[string]any
	if err := json.Unmarshal(firstRetry, &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(secondRetry, &second); err != nil {
		t.Fatal(err)
	}
	if got := stringValue(mapField(first, "metadata")["resourceVersion"]); got != "43" {
		t.Fatalf("first retry resourceVersion=%q, want 43", got)
	}
	if got := stringValue(mapField(second, "metadata")["resourceVersion"]); got != "44" {
		t.Fatalf("second retry resourceVersion=%q, want 44", got)
	}
}

func TestManagedFieldsOwnershipMaySpanDeclarativeApplyAndUpdateEntries(t *testing.T) {
	manager := "fugue-edge-client-us-declarative"
	pointers := []string{
		"/spec/template/spec/containers[name=edge]/image",
		"/spec/template/spec/containers[name=edge]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED]/value",
	}
	metadata := map[string]any{"managedFields": []any{
		map[string]any{"manager": manager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, pointers[:1])},
		map[string]any{"manager": manager, "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, pointers[1:])},
	}}
	if !managedFieldsOwnPointers(metadata, manager, pointers) {
		t.Fatalf("same declarative manager ownership was not aggregated across Apply and Update entries: %v", pointers)
	}
	metadata["managedFields"].([]any)[1].(map[string]any)["manager"] = "helm"
	if managedFieldsOwnPointers(metadata, manager, pointers) {
		t.Fatal("legacy manager ownership was incorrectly accepted as declarative")
	}
}

func TestDeclarativeUpdateOwnershipTransfersOnlyExactEnvironmentValue(t *testing.T) {
	manager := "fugue-edge-client-us-declarative"
	enabled := "/spec/template/spec/containers[name=dns]/env[name=FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED]/value"
	desired := map[string]any{
		"metadata": map[string]any{"uid": "dns-uid", "resourceVersion": "42", "generation": json.Number("7")},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "dns", "env": []any{map[string]any{
				"name": "FUGUE_DNS_EDGE_HEALTH_PROBE_ENABLED", "value": "true",
			}},
		}}}}},
	}
	live := deepCopyJSONMap(t, desired)
	mapField(live, "metadata")["managedFields"] = []any{map[string]any{
		"manager": manager, "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{enabled}),
	}}
	dns := anySlice(mapField(mapField(mapField(live, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	anySlice(dns["env"])[0].(map[string]any)["value"] = "false"
	applyErr := errors.New(`Apply failed with 1 conflict: conflict with "` + manager + `" using apps/v1: ` + ssaFieldForPointer(enabled))

	if err := validateEmergencyOwnershipConflictEvidence(desired, live, []string{enabled}, manager, applyErr); err != nil {
		t.Fatalf("exact declarative Update ownership was rejected: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, []string{enabled}, manager, applyErr)
	if err != nil || !found {
		t.Fatalf("exact declarative Update ownership did not produce a scalar transfer: patch=%v found=%v err=%v", patch, found, err)
	}
	encoded := string(mustJSON(t, patch))
	if !strings.Contains(encoded, `"path":"/spec/template/spec/containers/0/env/0/value"`) ||
		!strings.Contains(encoded, `"value":"true"`) || strings.Contains(encoded, "/managedFields/") {
		t.Fatalf("declarative Update ownership transfer was not an exact value patch: %s", encoded)
	}

	expanded := deepCopyJSONMap(t, live)
	entry := anySlice(mapField(expanded, "metadata")["managedFields"])[0].(map[string]any)
	entry["fieldsV1"] = managedFieldsTree(t, []string{
		enabled,
		"/spec/template/spec/containers[name=dns]/env[name=FUGUE_DNS_MAX_STALE]/name",
	})
	if err := validateEmergencyOwnershipConflictEvidence(desired, expanded, []string{enabled}, manager, applyErr); err == nil || !strings.Contains(err.Error(), "expands beyond") {
		t.Fatalf("expanded declarative Update ownership was accepted: %v", err)
	}
}

func TestProbeOwnershipConvergenceRecoversBroadKubectlPatchIntermediate(t *testing.T) {
	directory := t.TempDir()
	kubectl := filepath.Join(directory, "kubectl")
	livePath := filepath.Join(directory, "live.json")
	transferredPath := filepath.Join(directory, "transferred.json")
	transferredMarker := filepath.Join(directory, "transferred")
	applyInputPath := filepath.Join(directory, "apply-input.json")
	patchesPath := filepath.Join(directory, "patches.log")
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a",
		Container: "edge", FieldManager: "fugue-edge-worker-de-declarative",
	}}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a"}
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "worker-a", "namespace": "fugue-system", "uid": "worker-uid", "resourceVersion": "43"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []any{map[string]any{
			"name": "edge", "image": "ghcr.io/example/edge@sha256:" + strings.Repeat("a", 64),
			"livenessProbe":  map[string]any{"httpGet": map[string]any{"path": "/livez", "port": "health"}},
			"readinessProbe": map[string]any{"httpGet": map[string]any{"path": "/readyz", "port": "health"}},
		}}}}},
	}
	liveness := "/spec/template/spec/containers[name=edge]/livenessProbe/httpGet/path"
	readiness := "/spec/template/spec/containers[name=edge]/readinessProbe/httpGet/path"
	unreviewed := "/spec/template/spec/containers[name=edge]/env[name=EXTRA]/value"
	image := "/spec/template/spec/containers[name=edge]/image"
	live := deepCopyJSONMap(t, desired)
	liveMetadata := mapField(live, "metadata")
	liveMetadata["generation"] = json.Number("8")
	liveMetadata["managedFields"] = []any{
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{image})},
		map[string]any{"manager": "kubectl-patch", "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{liveness, readiness, unreviewed})},
	}
	transferred := deepCopyJSONMap(t, desired)
	transferredMetadata := mapField(transferred, "metadata")
	transferredMetadata["resourceVersion"] = "44"
	transferredMetadata["generation"] = json.Number("8")
	transferredMetadata["managedFields"] = []any{
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{image})},
		map[string]any{"manager": "kubectl-patch", "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{unreviewed})},
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{liveness, readiness})},
	}
	for filename, value := range map[string]map[string]any{livePath: live, transferredPath: transferred} {
		if err := os.WriteFile(filename, mustJSON(t, value), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	program := `#!/bin/sh
set -eu
case "$1" in
  apply)
	if test -e "$TRANSFERRED"; then
	  cat >"$APPLY_INPUT"
	  cat "$TRANSFERRED_JSON"
	else
	  cat >/dev/null
	  printf '%s\n' 'Apply failed with 2 conflicts: conflicts with "kubectl-patch" using apps/v1:' >&2
	  printf '%s\n' '- .spec.template.spec.containers[name="edge"].livenessProbe.httpGet.path' >&2
	  printf '%s\n' '- .spec.template.spec.containers[name="edge"].readinessProbe.httpGet.path' >&2
	  exit 1
	fi
    ;;
  get)
	if test -e "$TRANSFERRED"; then cat "$TRANSFERRED_JSON"; else cat "$LIVE_JSON"; fi
	;;
  patch)
	printf '%s\n' "$*" >>"$PATCHES"
	case " $* " in *" --field-manager fugue-edge-worker-de-declarative "*) ;; *) exit 53 ;; esac
	: >"$TRANSFERRED"
	cat "$TRANSFERRED_JSON"
    ;;
  *) exit 52 ;;
esac
`
	if err := os.WriteFile(kubectl, []byte(program), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LIVE_JSON", livePath)
	t.Setenv("TRANSFERRED_JSON", transferredPath)
	t.Setenv("TRANSFERRED", transferredMarker)
	t.Setenv("APPLY_INPUT", applyInputPath)
	t.Setenv("PATCHES", patchesPath)
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	if err := cluster.applyResourceWithOwnershipConvergence(context.Background(), release, identity, desired, mustJSON(t, desired), false); err != nil {
		t.Fatalf("broad kubectl-patch intermediate was not recovered: %v", err)
	}
	patches, err := os.ReadFile(patchesPath)
	if err != nil {
		t.Fatal(err)
	}
	patchLines := strings.Split(strings.TrimSpace(string(patches)), "\n")
	if len(patchLines) != 1 || !strings.Contains(patchLines[0], "--field-manager "+release.Workload.FieldManager) ||
		strings.Contains(patchLines[0], "/managedFields/") {
		t.Fatalf("unexpected broad-manager scalar transfer patch: %q", patches)
	}
	applyInput, err := os.ReadFile(applyInputPath)
	if err != nil {
		t.Fatal(err)
	}
	var rebound map[string]any
	if err := json.Unmarshal(applyInput, &rebound); err != nil {
		t.Fatal(err)
	}
	metadata := mapField(rebound, "metadata")
	if stringValue(metadata["uid"]) != "worker-uid" || stringValue(metadata["resourceVersion"]) != "44" ||
		int64Value(metadata["generation"]) != 0 {
		t.Fatalf("ordinary apply was not rebound to the exact recovered CAS: %s", applyInput)
	}
}

func TestValidateEmergencyRollbackDriftAllowsOnlyExactOwnedRuntimePointer(t *testing.T) {
	release := declarativerelease.PlanRelease{
		ComponentID: "edge-control-de",
		Workload: declarativerelease.Workload{
			APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de",
			Container: "edge-control", FieldManager: "fugue-edge-control-de-declarative",
		},
	}
	desiredImage := "ghcr.io/example/edge-control@sha256:" + strings.Repeat("a", 64)
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{
			"name": "edge-control-de", "namespace": "fugue-system",
			"annotations": map[string]any{"fugue.pro/production-config-sha": strings.Repeat("1", 40)},
		},
		"spec": map[string]any{
			"replicas": json.Number("1"),
			"template": map[string]any{
				"metadata": map[string]any{"annotations": map[string]any{
					"fugue.pro/oci-revision": strings.Repeat("1", 40), "fugue.pro/source-commit": strings.Repeat("1", 40),
				}},
				"spec": map[string]any{"containers": []any{map[string]any{
					"name": "edge-control", "image": desiredImage,
					"env": []any{map[string]any{"name": "MODE", "value": "safe"}},
				}}},
			},
		},
	}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "edge-control-de"}
	serviceAccountIdentity := declarativerelease.ResourceIdentity{APIVersion: "v1", Kind: "ServiceAccount", Namespace: "fugue-system", Name: "edge-control-de"}
	desiredServiceAccount := map[string]any{
		"apiVersion": "v1", "kind": "ServiceAccount",
		"metadata": map[string]any{"name": "edge-control-de", "namespace": "fugue-system"},
	}
	imagePointer := "/spec/template/spec/containers[name=edge-control]/image"
	live := deepCopyJSONMap(t, desired)
	metadata := mapField(live, "metadata")
	metadata["uid"], metadata["resourceVersion"], metadata["generation"] = "edge-uid", "44", json.Number("9")
	metadata["managedFields"] = []any{
		// Kubernetes transfers the image leaf out of the original Apply entry
		// when kubectl-patch claims it through Update. The verified monitor record
		// is the durable witness that the declarative manager owned it before.
		map[string]any{"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{"/spec/replicas"})},
		map[string]any{"manager": "kubectl-patch", "operation": "Update", "fieldsType": "FieldsV1", "fieldsV1": managedFieldsTree(t, []string{imagePointer})},
	}
	container := anySlice(mapField(mapField(mapField(live, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	container["image"] = "ghcr.io/example/edge-control@sha256:" + strings.Repeat("0", 64)

	directory := t.TempDir()
	livePath := filepath.Join(directory, "live.json")
	serviceAccountPath := filepath.Join(directory, "service-account.json")
	kubectl := filepath.Join(directory, "kubectl")
	liveServiceAccount := deepCopyJSONMap(t, desiredServiceAccount)
	serviceAccountMetadata := mapField(liveServiceAccount, "metadata")
	serviceAccountMetadata["uid"], serviceAccountMetadata["resourceVersion"] = "service-account-uid", "20"
	serviceAccountMetadata["managedFields"] = []any{map[string]any{
		"manager": release.Workload.FieldManager, "operation": "Apply", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{"/metadata/name"}),
	}}
	if err := os.WriteFile(serviceAccountPath, mustJSON(t, liveServiceAccount), 0o600); err != nil {
		t.Fatal(err)
	}
	writeLive := func(value map[string]any) declarativerelease.Observation {
		t.Helper()
		if err := os.WriteFile(livePath, mustJSON(t, value), 0o600); err != nil {
			t.Fatal(err)
		}
		resource := declarativerelease.ResourceObservation{
			Identity: identity, Present: true, UID: "edge-uid", ResourceVersion: "44", Generation: 9,
			ObjectDigest: digestJSON(sanitizeObservedResource(value)),
		}
		serviceAccountResource := declarativerelease.ResourceObservation{
			Identity: serviceAccountIdentity, Present: true, UID: "service-account-uid", ResourceVersion: "20",
			ObjectDigest: digestJSON(sanitizeObservedResource(liveServiceAccount)),
		}
		return declarativerelease.Observation{Present: true, Primary: identity, UID: "edge-uid", ResourceVersion: "44", Generation: 9, Resources: []declarativerelease.ResourceObservation{resource, serviceAccountResource}}
	}
	if err := os.WriteFile(kubectl, []byte("#!/bin/sh\nset -eu\ntest \"$1\" = get\ncase \"$2/$3\" in\n  deployment/edge-control-de) cat \"$LIVE_JSON\" ;;\n  serviceaccount/edge-control-de) cat \"$LIVE_SERVICE_ACCOUNT_JSON\" ;;\n  *) exit 51 ;;\nesac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LIVE_JSON", livePath)
	t.Setenv("LIVE_SERVICE_ACCOUNT_JSON", serviceAccountPath)
	manifest := mustJSON(t, map[string]any{"apiVersion": "release.fugue.dev/v2", "kind": "ComponentResourceSet", "items": []any{desired, desiredServiceAccount}})
	cluster := &kubectlCluster{kubectl: kubectl, timeout: time.Second}
	observation := writeLive(live)
	observation.ResourceVersion = "43"
	observation.Resources[0].ResourceVersion = "43"
	validated, err := cluster.ValidateEmergencyRollbackDrift(context.Background(), release, manifest, observation)
	if err != nil {
		t.Fatalf("exact emergency image drift was rejected: %v", err)
	}
	if validated.ResourceVersion != "44" || validated.Resources[0].ResourceVersion != "44" || validated.Resources[1].Generation != 0 || validated.Resources[1].ResourceVersion != "20" {
		t.Fatalf("status-only RV movement was not freshly rebound: %+v", validated)
	}

	drifted := deepCopyJSONMap(t, live)
	driftedContainer := anySlice(mapField(mapField(mapField(drifted, "spec"), "template"), "spec")["containers"])[0].(map[string]any)
	anySlice(driftedContainer["env"])[0].(map[string]any)["value"] = "unsafe"
	observation = writeLive(drifted)
	if _, err := cluster.ValidateEmergencyRollbackDrift(context.Background(), release, manifest, observation); err == nil || !strings.Contains(err.Error(), "beyond the exact allowlist") {
		t.Fatalf("unreviewed environment drift was accepted: %v", err)
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
				"metadata": map[string]any{"annotations": map[string]any{
					"fugue.pro/source-commit": strings.Repeat("1", 40),
					"fugue.pro/oci-revision":  strings.Repeat("1", 40),
				}},
				"spec": map[string]any{"containers": []any{map[string]any{"name": "api", "image": "ghcr.io/example/fugue-api@sha256:" + strings.Repeat("a", 64)}}},
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
		observation.OCIRevision != strings.Repeat("1", 40) ||
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
	serviceDegraded := fmt.Errorf("%w: source Pod did not observe the expected service response", errWorkloadOriginatedServiceHealth)
	serviceProxyDegraded := fmt.Errorf("%w: read-only kubectl get failed after 2 attempts", errServiceHTTPHealth)
	publicRouteDegraded := fmt.Errorf("%w: public route canary response is invalid", errPublicRouteHTTPHealth)
	forward := predecessor
	forward.ConfigSHA = strings.Repeat("2", 40)
	for _, test := range []struct {
		name      string
		ctx       context.Context
		target    declarativerelease.TargetIdentity
		err       error
		want      bool
		wantRoute bool
	}{
		{name: "exact typed predecessor", ctx: marked, target: predecessor, err: typed, want: true},
		{name: "exact workload service predecessor", ctx: marked, target: predecessor, err: serviceDegraded, want: true},
		{name: "exact service proxy predecessor", ctx: marked, target: predecessor, err: serviceProxyDegraded, want: true},
		{name: "exact public route predecessor", ctx: marked, target: predecessor, err: publicRouteDegraded, want: true, wantRoute: true},
		{name: "forward typed zero ready", ctx: marked, target: forward, err: typed},
		{name: "forward workload service failure", ctx: marked, target: forward, err: serviceDegraded},
		{name: "forward service proxy failure", ctx: marked, target: forward, err: serviceProxyDegraded},
		{name: "forward public route failure", ctx: marked, target: forward, err: publicRouteDegraded},
		{name: "unmarked compensation predecessor", ctx: context.Background(), target: predecessor, err: typed},
		{name: "unmarked workload service predecessor", ctx: context.Background(), target: predecessor, err: serviceDegraded},
		{name: "unmarked public route predecessor", ctx: context.Background(), target: predecessor, err: publicRouteDegraded},
		{name: "recoverable non-typed predecessor", ctx: marked, target: predecessor, err: errors.New("temporarily unavailable")},
		{name: "unknown context predecessor", ctx: marked, target: predecessor, err: context.DeadlineExceeded},
		{name: "healthy predecessor", ctx: marked, target: predecessor},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := shouldReturnTypedPrewritePredecessorHealth(test.ctx, release, test.target, test.err); got != test.want {
				t.Fatalf("short-circuit=%v, want %v", got, test.want)
			}
			typedErr := typedPrewritePredecessorHealth(test.ctx, release, test.target, test.err)
			if (typedErr != nil) != test.want || (test.want && !errors.Is(typedErr, declarativerelease.ErrDegradedPredecessorHealth)) {
				t.Fatalf("typed predecessor error=%v, want classified=%v", typedErr, test.want)
			}
			if got := errors.Is(typedErr, declarativerelease.ErrPublicRouteHealth); got != test.wantRoute {
				t.Fatalf("public route classification=%v, want %v: %v", got, test.wantRoute, typedErr)
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
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "", applyErr); err != nil {
		t.Fatalf("exact emergency conflict rejected: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, allowed, "", applyErr)
	if err != nil || !found || patch[0]["path"] != "/metadata/uid" || patch[1]["path"] != "/metadata/resourceVersion" ||
		strings.Contains(string(mustJSON(t, patch)), "/managedFields/") {
		t.Fatalf("unexpected exact scalar transfer patch: patch=%v found=%v err=%v", patch, found, err)
	}

	extra := append(append([]string(nil), allowed...), "/spec/replicas")
	metadata["managedFields"].([]any)[1].(map[string]any)["fieldsV1"] = managedFieldsTree(t, extra)
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "", applyErr); err == nil || !strings.Contains(err.Error(), "expands beyond") {
		t.Fatalf("expanded emergency manager was accepted: %v", err)
	}
}

func TestScalarTransferRebindsOnlyFreshResourceVersion(t *testing.T) {
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

	reboundRaw, err := rebindDesiredResourceVersionAfterScalarTransfer(desired, before, fresh)
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
			if _, err := rebindDesiredResourceVersionAfterScalarTransfer(desired, before, changed); err == nil {
				t.Fatal("unsafe post-cleanup state was accepted")
			}
		})
	}
}

func TestEmergencyOwnershipRejectsUnknownManagerAndField(t *testing.T) {
	allowed := []string{"/spec/template/spec/containers[name=edge-control]/image"}
	for _, failure := range []error{
		errors.New(`Apply failed with 1 conflict: conflict with "controller" using apps/v1: .spec.template.spec.containers[name="edge-control"].image`),
		errors.New(`Apply failed with 1 conflict: conflict with "kubectl" using apps/v1: .spec.replicas`),
	} {
		desired := map[string]any{"metadata": map[string]any{"uid": "u", "resourceVersion": "1"}}
		live := map[string]any{"metadata": map[string]any{"uid": "u", "resourceVersion": "1", "managedFields": []any{}}}
		if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "", failure); err == nil || !strings.Contains(err.Error(), "outside the exact allowlist") {
			t.Fatalf("unknown ownership conflict was accepted: %v", err)
		}
	}
}

func TestArtifactIdentityOwnershipTransfersFromBroadKubectlPatch(t *testing.T) {
	desired := map[string]any{
		"apiVersion": "apps/v1", "kind": "DaemonSet",
		"metadata": map[string]any{"name": "worker-a", "namespace": "fugue-system", "uid": "worker-uid", "resourceVersion": "42"},
		"spec": map[string]any{"template": map[string]any{
			"metadata": map[string]any{"annotations": map[string]any{
				"fugue.pro/artifact-image":          "ghcr.io/example/edge@sha256:" + strings.Repeat("a", 64),
				"fugue.pro/artifact-receipt-digest": "sha256:" + strings.Repeat("b", 64),
				"fugue.pro/oci-revision":            strings.Repeat("c", 40),
				"fugue.pro/production-config-sha":   strings.Repeat("c", 40),
				"fugue.pro/release-plan-digest":     "sha256:" + strings.Repeat("d", 64),
				"fugue.pro/source-commit":           strings.Repeat("c", 40),
			}},
			"spec": map[string]any{"containers": []any{map[string]any{
				"name": "edge", "image": "ghcr.io/example/edge@sha256:" + strings.Repeat("a", 64),
			}}},
		}},
	}
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{
		APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a",
		Container: "edge", FieldManager: "fugue-edge-worker-us-declarative",
	}}
	identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "fugue-system", Name: "worker-a"}
	allowed := ownershipConvergencePointers(release, identity, desired)
	pointer := "/spec/template/metadata/annotations/fugue.pro~1artifact-image"
	if !stringSubset([]string{pointer}, allowed) {
		t.Fatalf("artifact image annotation missing from ownership allowlist: %v", allowed)
	}
	live := deepCopyJSONMap(t, desired)
	liveAnnotations := mapField(mapField(mapField(live, "spec"), "template"), "metadata")
	liveAnnotations = mapField(liveAnnotations, "annotations")
	liveAnnotations["fugue.pro/artifact-image"] = "ghcr.io/example/edge@sha256:" + strings.Repeat("e", 64)
	mapField(live, "metadata")["managedFields"] = []any{map[string]any{
		"manager": "kubectl-patch", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{pointer, "/spec/template/spec/nodeSelector"}),
	}}
	applyErr := errors.New(`Apply failed with 1 conflict: conflict with "kubectl-patch" using apps/v1: ` + ssaFieldForPointer(pointer))
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, release.Workload.FieldManager, applyErr); err != nil {
		t.Fatalf("exact artifact identity transfer was blocked by unrelated kubectl-patch ownership: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, allowed, release.Workload.FieldManager, applyErr)
	if err != nil || !found || len(patch) != 4 || patch[3]["path"] != pointer {
		t.Fatalf("artifact identity transfer patch=%v found=%v err=%v", patch, found, err)
	}
	imagePointer := "/spec/template/spec/containers[name=edge]/image"
	mapField(live, "metadata")["managedFields"] = []any{map[string]any{
		"manager": "kubectl-patch", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{imagePointer, "/spec/template/spec/nodeSelector"}),
	}}
	imageConflict := errors.New(`Apply failed with 1 conflict: conflict with "kubectl-patch" using apps/v1: ` + ssaFieldForPointer(imagePointer))
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, release.Workload.FieldManager, imageConflict); err == nil || !strings.Contains(err.Error(), "expands beyond") {
		t.Fatalf("broad kubectl-patch ownership escaped the artifact annotation boundary: %v", err)
	}
}

func TestBeforeFirstApplyOwnershipTransfersOnlyAllowlistedScalars(t *testing.T) {
	pointer := "/metadata/annotations/fugue.pro~1artifact-receipt-digest"
	desired := map[string]any{"metadata": map[string]any{
		"uid": "control-uid", "resourceVersion": "42",
		"annotations": map[string]any{"fugue.pro/artifact-receipt-digest": "sha256:" + strings.Repeat("a", 64)},
	}}
	live := deepCopyJSONMap(t, desired)
	metadata := mapField(live, "metadata")
	metadata["managedFields"] = []any{map[string]any{
		"manager": "before-first-apply", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{pointer, "/spec/replicas"}),
	}}
	mapField(metadata, "annotations")["fugue.pro/artifact-receipt-digest"] = "sha256:" + strings.Repeat("b", 64)
	applyErr := errors.New(`Apply failed with 1 conflict: conflict with "before-first-apply" using apps/v1: ` + ssaFieldForPointer(pointer))
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, []string{pointer}, "", applyErr); err != nil {
		t.Fatalf("synthetic pre-SSA ownership blocked an exact release scalar: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, []string{pointer}, "", applyErr)
	if err != nil || !found || len(patch) != 4 || patch[3]["path"] != pointer {
		t.Fatalf("synthetic pre-SSA scalar transfer is invalid: patch=%v found=%v err=%v", patch, found, err)
	}
	outside := errors.New(`Apply failed with 1 conflict: conflict with "before-first-apply" using apps/v1: .spec.replicas`)
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, []string{pointer}, "", outside); err == nil || !strings.Contains(err.Error(), "outside the exact allowlist") {
		t.Fatalf("synthetic pre-SSA ownership expanded beyond the allowlist: %v", err)
	}
}

func TestGuardianRoleRulesOwnershipTransfersByExactCAS(t *testing.T) {
	desired := map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
		"metadata": map[string]any{"name": "fugue-release-guardian", "namespace": "fugue-system", "uid": "role-uid", "resourceVersion": "42"},
		"rules": []any{
			map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get", "update"}},
			map[string]any{"apiGroups": []any{""}, "resourceNames": []any{"transition-us"}, "resources": []any{"configmaps"}, "verbs": []any{"delete"}},
		},
	}
	live := deepCopyJSONMap(t, desired)
	live["rules"] = []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get", "update"}}}
	mapField(live, "metadata")["managedFields"] = []any{map[string]any{
		"manager": "kubectl-patch", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{"/rules"}),
	}}
	identity := declarativerelease.ResourceIdentity{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role", Namespace: "fugue-system", Name: "fugue-release-guardian"}
	allowed := ownershipConvergencePointers(declarativerelease.PlanRelease{}, identity, desired)
	if len(allowed) != 1 || allowed[0] != "/rules" {
		t.Fatalf("Role rules ownership allowlist=%v", allowed)
	}
	applyErr := errors.New(`Apply failed with 1 conflict: conflict with "kubectl-patch" using rbac.authorization.k8s.io/v1: .rules`)
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "fugue-release-guardian-declarative", applyErr); err != nil {
		t.Fatalf("exact Role rules conflict rejected: %v", err)
	}
	patch, found, err := nextLegacyOwnershipTransferPatch(desired, live, allowed, "fugue-release-guardian-declarative", applyErr)
	if err != nil || !found || len(patch) != 4 || patch[2]["path"] != "/rules" || patch[3]["path"] != "/rules" {
		t.Fatalf("Role rules transfer patch=%v found=%v err=%v", patch, found, err)
	}

	fresh := deepCopyJSONMap(t, desired)
	mapField(fresh, "metadata")["resourceVersion"] = "43"
	mapField(fresh, "metadata")["managedFields"] = []any{map[string]any{
		"manager": "fugue-release-guardian-declarative", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{"/rules"}),
	}}
	expected, err := expectedStateAfterLegacyOwnershipTransfer(desired, live, fresh, allowed, applyErr)
	if err != nil {
		t.Fatal(err)
	}
	rebound, err := rebindDesiredResourceVersionAfterScalarTransfer(desired, expected, fresh)
	if err != nil {
		t.Fatal(err)
	}
	reboundObject, err := decodeJSONObject(rebound)
	if err != nil || stringValue(mapField(reboundObject, "metadata")["resourceVersion"]) != "43" {
		t.Fatalf("Role rules transfer did not bind fresh RV: object=%v err=%v", reboundObject, err)
	}

	expanded := deepCopyJSONMap(t, live)
	mapField(expanded, "metadata")["managedFields"] = []any{map[string]any{
		"manager": "kubectl-patch", "operation": "Update", "fieldsType": "FieldsV1",
		"fieldsV1": managedFieldsTree(t, []string{"/rules", "/metadata/labels/emergency"}),
	}}
	if err := validateEmergencyOwnershipConflictEvidence(desired, expanded, allowed, "fugue-release-guardian-declarative", applyErr); err == nil || !strings.Contains(err.Error(), "expands beyond") {
		t.Fatalf("expanded Role ownership was accepted: %v", err)
	}
	helmErr := errors.New(`Apply failed with 1 conflict: conflict with "helm" using rbac.authorization.k8s.io/v1: .rules`)
	if err := validateEmergencyOwnershipConflictEvidence(desired, live, allowed, "fugue-release-guardian-declarative", helmErr); err == nil || !strings.Contains(err.Error(), "scalar transfer") {
		t.Fatalf("legacy manager received structural Role takeover: %v", err)
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
		current["."] = map[string]any{}
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

func TestObservedFailedAtomImageIDsRequireVerifiedLiveWorkloadAndPods(t *testing.T) {
	topDigest := "sha256:" + strings.Repeat("a", 64)
	platformDigest := "sha256:" + strings.Repeat("b", 64)
	image := "ghcr.io/example/fugue-api@" + topDigest
	release := declarativerelease.PlanRelease{Workload: declarativerelease.Workload{
		APIVersion: "apps/v1", Kind: "Deployment", Namespace: "fugue-system", Name: "fugue-fugue-api", Container: "api",
	}}
	workload := mustJSON(t, map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "fugue-fugue-api", "namespace": "fugue-system"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{"name": "api", "image": image}},
		}}},
	})
	pods := map[string]any{"items": []any{map[string]any{
		"metadata": map[string]any{"name": "api-1"},
		"status": map[string]any{"containerStatuses": []any{map[string]any{
			"name": "api", "imageID": "containerd://" + platformDigest,
		}}},
	}}}
	if err := verifyObservedArtifactImageIDs(mustJSON(t, pods), workload, release, image, platformDigest); err != nil {
		t.Fatalf("verified failed atom workload was rejected: %v", err)
	}
	pods["items"].([]any)[0].(map[string]any)["status"].(map[string]any)["containerStatuses"].([]any)[0].(map[string]any)["imageID"] = "containerd://sha256:" + strings.Repeat("c", 64)
	if err := verifyObservedArtifactImageIDs(mustJSON(t, pods), workload, release, image, platformDigest); err == nil {
		t.Fatal("failed atom Pod with an unverified imageID was accepted")
	}
	driftedWorkload := bytes.Replace(workload, []byte(image), []byte("ghcr.io/example/fugue-api@sha256:"+strings.Repeat("d", 64)), 1)
	if err := verifyObservedArtifactImageIDs(mustJSON(t, pods), driftedWorkload, release, image, platformDigest); err == nil {
		t.Fatal("failed atom workload with a different immutable image was accepted")
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

func TestHealthSoakTrackerBoundsInventoryHeartbeatGap(t *testing.T) {
	start := time.Date(2026, 8, 25, 0, 0, 0, 0, time.UTC)
	tracker := healthSoakTracker{required: 180 * time.Second}
	if tracker.observe(start, true) {
		t.Fatal("health soak completed at its first observation")
	}
	tracker.observeTransient(start.Add(170*time.Second), 30*time.Second)
	if !tracker.observe(start.Add(180*time.Second), true) {
		t.Fatal("bounded inventory heartbeat gap cleared verified soak history")
	}

	tracker = healthSoakTracker{required: 180 * time.Second}
	tracker.observe(start, true)
	tracker.observeTransient(start.Add(100*time.Second), 30*time.Second)
	tracker.observeTransient(start.Add(131*time.Second), 30*time.Second)
	if tracker.observe(start.Add(180*time.Second), true) || !tracker.observe(start.Add(360*time.Second), true) {
		t.Fatal("inventory heartbeat gap beyond the grace budget did not reset the soak")
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

func TestPodHTTPSnapshotUsesUnreadyPodIdentityAndNamedPort(t *testing.T) {
	pod := podFixture("edge-front-1", "pod-uid", strings.Repeat("1", 40), strings.Repeat("a", 64))
	pod["spec"] = map[string]any{"containers": []any{map[string]any{
		"name":  "edge-front",
		"ports": []any{map[string]any{"name": "health", "containerPort": 7835}},
	}}}
	pod["status"].(map[string]any)["podIP"] = "10.42.0.17"
	pod["status"].(map[string]any)["conditions"] = []any{map[string]any{"type": "Ready", "status": "False"}}
	raw := mustJSON(t, map[string]any{"items": []any{pod}})
	if _, err := podHTTPEndpointsFromJSON(raw, "edge-front", "health"); err == nil {
		t.Fatal("strict endpoint parsing accepted an unready Front")
	}
	endpoints, err := podHTTPEndpointsFromJSONWithReadiness(raw, "edge-front", "health", false)
	if err != nil || len(endpoints) != 1 || endpoints[0].Name != "edge-front-1" || endpoints[0].IP != "10.42.0.17" || endpoints[0].Port != 7835 {
		t.Fatalf("snapshot endpoint parsing lost unready Front identity: endpoints=%+v err=%v", endpoints, err)
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

func TestTypedPrewriteAllowsOnlyExactLKGGroupAuthorityHealth(t *testing.T) {
	release := declarativerelease.PlanRelease{ExpectedPreviousPresent: true, ExpectedPreviousConfigSHA: strings.Repeat("1", 40),
		ExpectedPreviousManifestSHA: strings.Repeat("1", 40), ExpectedPreviousOCIRevision: strings.Repeat("1", 40),
		ExpectedPreviousImageDigest: "sha256:" + strings.Repeat("a", 64), Artifact: declarativerelease.Artifact{Repository: "ghcr.io/example/fugue-edge"}}
	target := declarativerelease.TargetIdentity{Present: true, ConfigSHA: release.ExpectedPreviousConfigSHA,
		ManifestSHA: release.ExpectedPreviousManifestSHA, OCIRevision: release.ExpectedPreviousOCIRevision,
		ImageRef: release.Artifact.Repository + "@" + release.ExpectedPreviousImageDigest}
	healthErr := fmt.Errorf("%w: inventory is not current", errEdgeGroupAuthorityHealth)
	ctx := declarativerelease.WithPrewritePredecessorHealthWait(context.Background())
	if err := typedPrewritePredecessorHealth(ctx, release, target, healthErr); !errors.Is(err, declarativerelease.ErrDegradedPredecessorHealth) {
		t.Fatalf("exact LKG group health was not typed: %v", err)
	}
	if err := typedPrewritePredecessorHealth(context.Background(), release, target, healthErr); err != nil {
		t.Fatalf("ordinary health check gained degraded authority: %v", err)
	}
	target.ConfigSHA = strings.Repeat("2", 40)
	if err := typedPrewritePredecessorHealth(ctx, release, target, healthErr); err != nil {
		t.Fatalf("identity-drifted target gained degraded authority: %v", err)
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
