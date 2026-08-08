package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSchemaMigratorUsesRollingLKGDeployment(t *testing.T) {
	manifestPath := filepath.Join("..", "..", "deploy", "releases", "schema", "deployment.json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read schema deployment: %v", err)
	}
	var resourceSet map[string]any
	if err := json.Unmarshal(raw, &resourceSet); err != nil {
		t.Fatalf("decode schema deployment: %v", err)
	}
	items, ok := resourceSet["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("schema resource count = %d, want one Deployment", len(items))
	}
	deployment := items[0].(map[string]any)
	if deployment["apiVersion"] != "apps/v1" || deployment["kind"] != "Deployment" {
		t.Fatalf("schema workload is not an apps/v1 Deployment: %+v", deployment)
	}
	spec := deployment["spec"].(map[string]any)
	if spec["replicas"] != float64(1) {
		t.Fatalf("schema replicas = %v", spec["replicas"])
	}
	strategy := spec["strategy"].(map[string]any)
	rolling := strategy["rollingUpdate"].(map[string]any)
	if strategy["type"] != "RollingUpdate" || rolling["maxUnavailable"] != float64(0) || rolling["maxSurge"] != float64(1) {
		t.Fatalf("unsafe schema rollout strategy: %+v", strategy)
	}
	pod := spec["template"].(map[string]any)["spec"].(map[string]any)
	if pod["automountServiceAccountToken"] != false || pod["restartPolicy"] != "Always" {
		t.Fatalf("unsafe schema pod identity/restart policy: %+v", pod)
	}
	container := pod["containers"].([]any)[0].(map[string]any)
	for _, probeName := range []string{"startupProbe", "readinessProbe", "livenessProbe"} {
		probe := container[probeName].(map[string]any)
		httpGet := probe["httpGet"].(map[string]any)
		if httpGet["path"] != "/healthz" || httpGet["port"] != "health" {
			t.Fatalf("%s is not bound to post-migration health: %+v", probeName, probe)
		}
	}
	startup := container["startupProbe"].(map[string]any)
	startupWindow := startup["failureThreshold"].(float64) * startup["periodSeconds"].(float64)
	if startupWindow < schemaMigrationTimeout.Seconds() {
		t.Fatalf("startup probe window %.0fs is shorter than migration timeout %s", startupWindow, schemaMigrationTimeout)
	}
	if _, exists := container["args"]; exists {
		t.Fatal("schema Deployment must not select migrations with container args")
	}
	if _, exists := container["command"]; exists {
		t.Fatal("schema Deployment must use the fixed image entrypoint")
	}
}
