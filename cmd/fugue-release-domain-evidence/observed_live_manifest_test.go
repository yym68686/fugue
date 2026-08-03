package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"fugue/internal/releasedomain"
)

func TestRunObservedLiveManifestProjectsKubectlListImages(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	ownership := []byte(`apiVersion: release-domain.fugue.dev/v1
kind: ReleaseDomainOwnership
domains: [node-local, authoritative-dns, control-plane, image-cache, backup]
requiredBindings: []
fileRules: []
valueRules: []
objectRules:
  - id: dns
    domain: authoritative-dns
    apiGroup: apps
    version: v1
    kind: DaemonSet
    scope: Namespaced
    namespace: fugue-system
    name: fugue-dns
`)
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatal(err)
	}
	base, err := releasedomain.CanonicalizeRenderedManifest(
		[]byte(observedLiveTestDaemonSet("registry.test/edge:helm-old")), spec, "fugue-system",
	)
	if err != nil {
		t.Fatal(err)
	}
	liveObject := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "DaemonSet",
		"metadata": map[string]any{
			"name": "fugue-dns", "namespace": "fugue-system",
			"generateName":  "api-server-default-must-be-ignored-",
			"managedFields": []any{map[string]any{"manager": "k3s", "fieldsV1": map[string]any{"f:spec": map[string]any{}}}},
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "fugue-dns"}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": "fugue-dns"}},
				"spec": map[string]any{"containers": []any{map[string]any{
					"name": "dns", "image": "registry.test/edge@" + activationTestDigest("a"),
					"resources": map[string]any{"requests": map[string]any{"cpu": "10m"}},
				}}},
			},
		},
		"status": map[string]any{"numberReady": 1},
	}
	liveList, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "List",
		"metadata":   map[string]any{"resourceVersion": "123"},
		"items":      []any{liveObject},
	})
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string][]byte{
		"ownership.yaml": ownership,
		"base.yaml":      base,
		"live.json":      liveList,
	}
	for name, data := range paths {
		if err := os.WriteFile(filepath.Join(root, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(root, "observed.yaml")
	var stderr bytes.Buffer
	exit := runObservedLiveManifest([]string{
		"--base-manifest", filepath.Join(root, "base.yaml"),
		"--live-workloads", filepath.Join(root, "live.json"),
		"--ownership", filepath.Join(root, "ownership.yaml"),
		"--namespace", "fugue-system",
		"--output", output,
	}, ioDiscard{}, &stderr)
	if exit != 0 {
		t.Fatalf("exit=%d stderr=%q", exit, stderr.String())
	}
	observed, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(observed, []byte("registry.test/edge@"+activationTestDigest("a"))) ||
		bytes.Contains(observed, []byte("helm-old")) {
		t.Fatalf("observed live output did not project the digest:\n%s", observed)
	}
	if err := releasedomain.VerifyObservedLiveImageManifest(base, observed, ownership, "fugue-system"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(output)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("output mode=%o, want 600", info.Mode().Perm())
	}
}

func TestRunObservedLiveManifestBindsConvergedDisabledDynamicWorker(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	name := "fugue-fugue-edge-dynamic-worker-b"
	ownership := []byte(`apiVersion: release-domain.fugue.dev/v1
kind: ReleaseDomainOwnership
domains: [node-local, authoritative-dns, control-plane, image-cache, backup]
requiredBindings: []
fileRules: []
valueRules: []
objectRules:
  - id: dynamic-worker-b
    domain: authoritative-dns
    apiGroup: apps
    version: v1
    kind: DaemonSet
    scope: Namespaced
    namespace: fugue-system
    name: fugue-fugue-edge-dynamic-worker-b
`)
	spec, err := releasedomain.LoadOwnership(bytes.NewReader(ownership))
	if err != nil {
		t.Fatal(err)
	}
	baseRaw := observedLiveDisabledDynamicWorker(name, "registry.test/edge:helm-live")
	base, err := releasedomain.CanonicalizeRenderedManifest([]byte(baseRaw), spec, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	var live map[string]any
	if err := json.Unmarshal([]byte(observedLiveDisabledDynamicWorkerJSON(name, "registry.test/edge:api-live")), &live); err != nil {
		t.Fatal(err)
	}
	liveList, err := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "List", "metadata": map[string]any{}, "items": []any{live},
	})
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string][]byte{"ownership.yaml": ownership, "base.yaml": base, "live.json": liveList}
	for name, data := range paths {
		if err := os.WriteFile(filepath.Join(root, name), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	output := filepath.Join(root, "observed.yaml")
	var stderr bytes.Buffer
	exit := runObservedLiveManifest([]string{
		"--base-manifest", filepath.Join(root, "base.yaml"),
		"--live-workloads", filepath.Join(root, "live.json"),
		"--ownership", filepath.Join(root, "ownership.yaml"),
		"--namespace", "fugue-system", "--output", output,
	}, ioDiscard{}, &stderr)
	if exit != 0 {
		t.Fatalf("exit=%d stderr=%q", exit, stderr.String())
	}
	observed, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(observed, []byte(releasedomain.DisabledPublicEdgeWorkerObservationAnnotation)) ||
		!bytes.Contains(observed, []byte("registry.test/edge:api-live")) {
		t.Fatalf("disabled worker observation was not bound:\n%s", observed)
	}
	if err := releasedomain.VerifyObservedLiveImageManifest(base, observed, ownership, "fugue-system"); err != nil {
		t.Fatal(err)
	}

	live["status"].(map[string]any)["numberAvailable"] = float64(1)
	drifted, err := json.Marshal(map[string]any{
		"apiVersion": "v1", "kind": "List", "metadata": map[string]any{}, "items": []any{live},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := expandObservedKubernetesList(drifted, "fugue-system"); err == nil {
		t.Fatal("non-zero disabled worker observation was accepted")
	}
}

func observedLiveDisabledDynamicWorker(name, image string) string {
	return "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: " + name + "\n  namespace: fugue-system\n  labels:\n    app.kubernetes.io/instance: fugue\n    app.kubernetes.io/component: edge-dynamic-worker-b\n    fugue.io/rollout-subsystem: public-data-plane\n    fugue.io/rollout-mode: node-local-blue-green-worker\n    fugue.io/downtime-class: online-required\n    fugue.io/edge-slot: b\nspec:\n  revisionHistoryLimit: 2\n  updateStrategy:\n    type: OnDelete\n  selector:\n    matchLabels:\n      app: edge-dynamic-worker-b\n  template:\n    metadata:\n      labels:\n        app: edge-dynamic-worker-b\n    spec:\n      nodeSelector:\n        fugue.io/edge-workload: dynamic\n      initContainers:\n        - name: edge-workload-identity\n          image: " + image + "\n      containers:\n        - name: edge\n          image: " + image + "\n        - name: caddy\n          image: caddy:2.10.2-alpine\n"
}

func observedLiveDisabledDynamicWorkerJSON(name, image string) string {
	return `{
  "apiVersion":"apps/v1","kind":"DaemonSet",
  "metadata":{"name":"` + name + `","namespace":"fugue-system","uid":"uid-b","resourceVersion":"9182","generation":11,"annotations":{"deprecated.daemonset.template.generation":"11","meta.helm.sh/release-name":"fugue","meta.helm.sh/release-namespace":"fugue-system"},
    "labels":{"app.kubernetes.io/instance":"fugue","app.kubernetes.io/component":"edge-dynamic-worker-b","fugue.io/rollout-subsystem":"public-data-plane","fugue.io/rollout-mode":"node-local-blue-green-worker","fugue.io/downtime-class":"online-required","fugue.io/edge-slot":"b"}},
  "spec":{"revisionHistoryLimit":2,"updateStrategy":{"type":"OnDelete"},"selector":{"matchLabels":{"app":"edge-dynamic-worker-b"}},"template":{"metadata":{"labels":{"app":"edge-dynamic-worker-b"},"creationTimestamp":null,"annotations":{"fugue.io/public-data-plane-release-id":"pdp-20260731T132527Z-d2844418b0464a9bd32d3a147841e99b46140b39","fugue.io/public-data-plane-release-mode":"node-local-blue-green-worker"}},"spec":{"nodeSelector":{"fugue.io/edge-workload":"dynamic"},"dnsPolicy":"ClusterFirst","restartPolicy":"Always","schedulerName":"default-scheduler","securityContext":{},"terminationGracePeriodSeconds":30,"initContainers":[{"name":"edge-workload-identity","image":"` + image + `","terminationMessagePath":"/dev/termination-log","terminationMessagePolicy":"File"}],"containers":[{"name":"edge","image":"` + image + `","terminationMessagePath":"/dev/termination-log","terminationMessagePolicy":"File"},{"name":"caddy","image":"caddy:2.10.2-alpine","terminationMessagePath":"/dev/termination-log","terminationMessagePolicy":"File"}]}}},
  "status":{"observedGeneration":11}
}`
}

func observedLiveTestDaemonSet(image string) string {
	return "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: fugue-dns\n  namespace: fugue-system\nspec:\n  selector:\n    matchLabels:\n      app: fugue-dns\n  template:\n    metadata:\n      labels:\n        app: fugue-dns\n    spec:\n      containers:\n        - name: dns\n          image: " + image + "\n"
}
