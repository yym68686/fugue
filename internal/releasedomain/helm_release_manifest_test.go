package releasedomain

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestExtractHelmReleaseManifestBindsIdentityAndIncludesHooks(t *testing.T) {
	mainManifest := "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: main}\n"
	firstHook := "apiVersion: v1\nkind: Secret\nmetadata:\n  name: hook-a\n  annotations: {helm.sh/hook: pre-upgrade}\n"
	secondHook := "apiVersion: batch/v1\nkind: Job\nmetadata:\n  name: hook-b\n  annotations: {helm.sh/hook: post-upgrade}\n"
	payload, err := json.Marshal(map[string]any{
		"apply_method": "server_side",
		"chart":        map[string]any{"metadata": map[string]any{"name": "fixture"}},
		"config":       map[string]any{"fixture": "override"},
		"hooks": []any{
			map[string]any{
				"events": []string{"pre-upgrade"}, "kind": "Secret", "last_run": map[string]any{}, "manifest": firstHook,
				"name": "hook-a", "path": "templates/a.yaml", "weight": 10,
				"delete_policies": []string{"before-hook-creation"}, "output_log_policies": []string{"hook-failed"},
			},
			map[string]any{"events": []string{"post-upgrade"}, "kind": "Job", "last_run": map[string]any{}, "manifest": secondHook, "name": "hook-b", "path": "templates/b.yaml"},
		},
		"info":      map[string]any{"status": "pending-upgrade", "last_deployed": "volatile"},
		"manifest":  mainManifest,
		"name":      "fugue",
		"namespace": "fugue-system",
		"version":   18,
	})
	if err != nil {
		t.Fatal(err)
	}
	extracted, err := ExtractHelmReleaseManifest(payload, "fugue", "fugue-system", 18)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{mainManifest, firstHook, secondHook} {
		if count := bytes.Count(extracted, []byte(part)); count != 1 {
			t.Fatalf("manifest part count = %d, want 1:\n%s", count, extracted)
		}
	}
	canonical, err := CanonicalizeRenderedManifest(extracted, &OwnershipSpec{}, "fugue-system")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"main", "hook-a", "hook-b"} {
		if !bytes.Contains(canonical, []byte("name: "+name)) {
			t.Fatalf("canonical output omitted %s:\n%s", name, canonical)
		}
	}
}

func TestExtractHelmReleaseManifestRejectsInvalidEnvelope(t *testing.T) {
	valid := `{"apply_method":"server_side","chart":{},"hooks":[],"info":{},"manifest":"","name":"fugue","namespace":"fugue-system","version":18}`
	for _, test := range []struct {
		name    string
		payload string
	}{
		{name: "duplicate manifest", payload: `{"hooks":[],"manifest":"one","manifest":"two","name":"fugue","namespace":"fugue-system","version":18}`},
		{name: "nested duplicate", payload: `{"chart":{"name":"one","name":"two"},"hooks":[],"manifest":"","name":"fugue","namespace":"fugue-system","version":18}`},
		{name: "null hooks", payload: `{"hooks":null,"manifest":"","name":"fugue","namespace":"fugue-system","version":18}`},
		{name: "fractional version", payload: `{"hooks":[],"manifest":"","name":"fugue","namespace":"fugue-system","version":18.0}`},
		{name: "unknown top field", payload: `{"hooks":[],"manifest":"","name":"fugue","namespace":"fugue-system","unknown":true,"version":18}`},
		{name: "hook missing manifest", payload: `{"hooks":[{"name":"hook"}],"manifest":"","name":"fugue","namespace":"fugue-system","version":18}`},
		{name: "hook unknown field", payload: `{"hooks":[{"manifest":"","unknown":true}],"manifest":"","name":"fugue","namespace":"fugue-system","version":18}`},
		{name: "trailing JSON", payload: valid + `{}`},
		{name: "isolated surrogate", payload: strings.Replace(valid, `"fugue"`, `"\ud800"`, 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if output, err := ExtractHelmReleaseManifest([]byte(test.payload), "fugue", "fugue-system", 18); err == nil {
				t.Fatalf("invalid envelope unexpectedly succeeded: %q", output)
			}
		})
	}
	withoutHooks := `{"config":{"fixture":true},"manifest":"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: fixture}\n","name":"fugue","namespace":"fugue-system","version":18}`
	if _, err := ExtractHelmReleaseManifest([]byte(withoutHooks), "fugue", "fugue-system", 18); err != nil {
		t.Fatalf("Helm release without hooks failed: %v", err)
	}
	hookOnly := `{"hooks":[{"manifest":"apiVersion: v1\nkind: Secret\nmetadata: {name: hook-only}\n"}],"name":"fugue","namespace":"fugue-system","version":18}`
	extracted, err := ExtractHelmReleaseManifest([]byte(hookOnly), "fugue", "fugue-system", 18)
	if err != nil {
		t.Fatalf("Helm release without main manifest failed: %v", err)
	}
	if !bytes.Contains(extracted, []byte("name: hook-only")) {
		t.Fatalf("hook-only Helm release lost hook manifest: %s", extracted)
	}
	for _, test := range []struct {
		name      string
		expected  string
		namespace string
		version   uint64
	}{
		{name: "name drift", expected: "other", namespace: "fugue-system", version: 18},
		{name: "namespace drift", expected: "fugue", namespace: "other", version: 18},
		{name: "version drift", expected: "fugue", namespace: "fugue-system", version: 19},
	} {
		t.Run(test.name, func(t *testing.T) {
			if output, err := ExtractHelmReleaseManifest([]byte(valid), test.expected, test.namespace, test.version); err == nil {
				t.Fatalf("identity drift unexpectedly succeeded: %q", output)
			}
		})
	}
}
