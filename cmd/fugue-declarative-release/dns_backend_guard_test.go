package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fugue/internal/declarativerelease"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestDNSCodeReplacementRequiresTransportHandoff(t *testing.T) {
	for _, scenario := range []string{"selected", "unselected", "same template", "bootstrap", "transport unavailable", "truncated services", "internal service"} {
		t.Run(scenario, func(t *testing.T) {
			identity := declarativerelease.ResourceIdentity{APIVersion: "apps/v1", Kind: "DaemonSet", Namespace: "test", Name: "dns-a"}
			workload := map[string]any{"apiVersion": "apps/v1", "kind": "DaemonSet", "metadata": map[string]any{"namespace": "test", "name": "dns-a"}, "spec": map[string]any{"template": map[string]any{
				"metadata": map[string]any{"labels": map[string]any{"slot": "a"}, "annotations": map[string]any{"fugue.pro/consumer-identity": `{"component":"dns-server"}`}},
				"spec":     map[string]any{"containers": []any{map[string]any{"name": "dns", "image": "registry.example/dns@sha256:old"}}},
			}}}
			current := &unstructured.Unstructured{Object: workload}
			objects := []runtime.Object{current.DeepCopy()}
			if scenario == "bootstrap" {
				objects = nil
			}
			if scenario != "same template" {
				mapField(mapField(workload, "spec"), "template")["spec"] = map[string]any{"containers": []any{map[string]any{"name": "dns", "image": "registry.example/dns@sha256:new"}}}
			}
			selected := "a"
			if scenario == "unselected" {
				selected = "b"
			}
			service := map[string]any{"metadata": map[string]any{"name": "public-dns"}, "spec": map[string]any{"externalIPs": []any{"8.8.8.8"}, "selector": map[string]any{"slot": selected}, "ports": []any{map[string]any{"port": 53, "protocol": "UDP"}}}}
			if scenario == "internal service" {
				delete(mapField(service, "spec"), "externalIPs")
			}
			list := map[string]any{"metadata": map[string]any{}, "items": []any{service}}
			if scenario == "truncated services" {
				list["metadata"] = map[string]any{"continue": "next"}
			}
			raw, _ := json.Marshal(list)
			dir := t.TempDir()
			script := filepath.Join(dir, "kubectl")
			program := "#!/bin/sh\ncat <<'JSON'\n" + string(raw) + "\nJSON\n"
			if scenario == "transport unavailable" {
				program = "#!/bin/sh\nexit 1\n"
			}
			if err := os.WriteFile(script, []byte(program), 0700); err != nil {
				t.Fatal(err)
			}
			cluster := &kubectlCluster{kubectl: script, resources: dynamicfake.NewSimpleDynamicClient(runtime.NewScheme(), objects...), timeout: time.Second, readAttempts: 1}
			err := cluster.requireUnselectedDNSBackend(context.Background(), identity, workload)
			blocked := scenario == "selected" || scenario == "transport unavailable" || scenario == "truncated services"
			if (err != nil) != blocked {
				t.Fatalf("blocked=%v: %v", blocked, err)
			}
			if scenario == "selected" && !strings.Contains(err.Error(), "handoff") {
				t.Fatal("missing concrete recovery instruction", err)
			}
			for _, action := range cluster.resources.(*dynamicfake.FakeDynamicClient).Actions() {
				if action.GetVerb() != "get" {
					t.Fatal("preflight wrote cluster state", action)
				}
			}
		})
	}
}
