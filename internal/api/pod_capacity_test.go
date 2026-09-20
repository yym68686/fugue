package api

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
` + podCapacityShellLibrary() + "\nreload_pod_capacity_k3s() { [ \"${FAIL_RELOAD}\" = false ]; }\nreconcile_pod_capacity_task\n"
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
			for _, want := range []string{"max-pods=65535", "pods-per-core=0", "system-reserved=memory=1Gi", "test-secret", "worker-a"} {
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

func TestPodUIDCapacityHonorsKubeletNamespaceAllocation(t *testing.T) {
	script := podCapacityUIDPython() + `
assert uid_capacity(65536, 2**32 - 65536, 65536) == 65535
assert uid_capacity(131072, 128 * 65536, 65536) == 128
assert uid_capacity(131072, 2**32 - 131072, 131072) == 32767
for values in [(0, 65536, 65536), (65536, 0, 65536), (65536, 65537, 65536), (65536, 2**32, 65536)]:
    try:
        uid_capacity(*values)
    except ValueError:
        continue
    raise AssertionError('unsafe UID range accepted: %r' % (values,))
`
	cmd := exec.Command("python3", "-")
	cmd.Stdin = strings.NewReader(script)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("UID capacity validation: %s %v", output, err)
	}
}

func TestPodCapacityReloadReturnsImmediatelyOnSystemdFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "systemctl"), []byte("#!/bin/sh\nexit 1\n"), 0755); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "bash")
	cmd.Stdin = strings.NewReader(podCapacityShellLibrary() + "\nreload_pod_capacity_k3s\n")
	cmd.Env = append(os.Environ(), "PATH="+dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	output, err := cmd.CombinedOutput()
	if err == nil || ctx.Err() != nil || !strings.Contains(string(output), "capacity reload failed") {
		t.Fatalf("systemd failure was masked or waited for health: %s %v", output, err)
	}
}
