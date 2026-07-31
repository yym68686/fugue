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
		},
		"spec": map[string]any{
			"selector": map[string]any{"matchLabels": map[string]any{"app": "fugue-dns"}},
			"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"app": "fugue-dns"}},
				"spec": map[string]any{"containers": []any{map[string]any{
					"name": "dns", "image": "registry.test/edge@" + activationTestDigest("a"),
				}}},
			},
		},
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

func observedLiveTestDaemonSet(image string) string {
	return "apiVersion: apps/v1\nkind: DaemonSet\nmetadata:\n  name: fugue-dns\n  namespace: fugue-system\nspec:\n  selector:\n    matchLabels:\n      app: fugue-dns\n  template:\n    metadata:\n      labels:\n        app: fugue-dns\n    spec:\n      containers:\n        - name: dns\n          image: " + image + "\n"
}
