package api

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPodCapacityPolicyPreservesConfigAndRollsBackFailedReload(t *testing.T) {
	for _, tc := range []struct {
		name, dry, allow, fail string
		wantChange             bool
	}{
		{"dry-run", "true", "false", "false", false},
		{"apply", "false", "true", "false", true},
		{"restart-failure", "false", "true", "true", false},
		{"missing-authorization", "false", "false", "false", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			config := filepath.Join(dir, "config.yaml")
			initial := "server: \"https://control.example.test:6443\"\ntoken: \"test-secret\"\nkubelet-arg:\n  - \"system-reserved=memory=1Gi\"\n  - \"max-pods=110\"\nnode-name: \"worker-a\"\n"
			if err := os.WriteFile(config, []byte(initial), 0600); err != nil {
				t.Fatal(err)
			}
			script := `set -eu
truthy() { [ "$1" = true ]; }
repair_guard() { return 0; }
repair_record_success() { :; }
repair_record_failure() { :; }
restart_k3s_agent() { [ "${FAIL_RELOAD}" = false ]; }
` + podCapacityShellLibrary() + "\nreconcile_pod_capacity_task\n"
			cmd := exec.Command("bash")
			cmd.Stdin = strings.NewReader(script)
			cmd.Env = append(os.Environ(), "FUGUE_NODE_UPDATER_K3S_CONFIG_FILE="+config, "FUGUE_NODE_UPDATE_TASK_POD_CAPACITY_MODE=resources", "FUGUE_NODE_UPDATE_TASK_DRY_RUN="+tc.dry, "FUGUE_NODE_UPDATE_TASK_ALLOW_RESTART="+tc.allow, "FAIL_RELOAD="+tc.fail)
			output, err := cmd.CombinedOutput()
			if (tc.fail == "true" || (tc.dry == "false" && tc.allow == "false")) != (err != nil) {
				t.Fatalf("unexpected result: %s %v", output, err)
			}
			got, err := os.ReadFile(config)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.wantChange {
				if string(got) != initial {
					t.Fatal("dry-run/refused/failed task changed config")
				}
				return
			}
			for _, want := range []string{"max-pods=2147483647", "pods-per-core=0", "system-reserved=memory=1Gi", "test-secret", "worker-a"} {
				if !strings.Contains(string(got), want) {
					t.Fatalf("lost %s", want)
				}
			}
			if strings.Count(string(got), "max-pods=") != 1 {
				t.Fatal("duplicate max-pods")
			}
			cmd = exec.Command("bash")
			cmd.Stdin = strings.NewReader(script)
			cmd.Env = append(os.Environ(), "FUGUE_NODE_UPDATER_K3S_CONFIG_FILE="+config, "FUGUE_NODE_UPDATE_TASK_POD_CAPACITY_MODE=resources", "FUGUE_NODE_UPDATE_TASK_DRY_RUN=false", "FUGUE_NODE_UPDATE_TASK_ALLOW_RESTART=true", "FAIL_RELOAD=true")
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("idempotent task restarted k3s: %s %v", output, err)
			}
		})
	}
}
