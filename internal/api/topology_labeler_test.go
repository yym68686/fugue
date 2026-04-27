package api

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func TestTopologyLabelerTemplateDoesNotManageSharedPoolMembership(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "deploy", "helm", "fugue", "templates", "topology-labeler-daemonset.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read topology labeler template: %v", err)
	}
	if strings.Contains(string(data), "fugue.io/shared-pool") {
		t.Fatalf("expected topology labeler template to leave shared-pool membership to policy/runtime reconciliation: %s", path)
	}
}

func TestUpgradeScriptDoesNotReapplySharedPoolLabels(t *testing.T) {
	t.Parallel()

	path := filepath.Join(repoRoot(t), "scripts", "upgrade_fugue_control_plane.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read upgrade script: %v", err)
	}
	if strings.Contains(string(data), "fugue.io/shared-pool=internal") {
		t.Fatalf("expected upgrade script to avoid reapplying shared-pool labels: %s", path)
	}
	for _, want := range []string{
		"fugue.install/role=primary",
		"fugue.io/shared-pool-",
		"fugue.io/build-",
	} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("expected upgrade script to remove primary pool label %q: %s", want, path)
		}
	}
}
