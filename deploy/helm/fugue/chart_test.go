package fuguechart_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestNodeJanitorDefaultsToSystemNodeCritical(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	if !strings.Contains(manifest, "name: fugue-fugue-node-janitor") {
		t.Fatalf("rendered manifest missing node-janitor daemonset:\n%s", manifest)
	}
	if !strings.Contains(manifest, "priorityClassName: \"system-node-critical\"") {
		t.Fatalf("node-janitor should render with system-node-critical priority:\n%s", manifest)
	}
}

func TestControlPlaneRBACCoversDiagnosableWorkloads(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm not installed")
	}

	chartDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	cmd := exec.Command("helm", "template", "fugue", chartDir)
	cmd.Dir = chartDir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, output)
	}

	manifest := string(output)
	want := `resources: ["deployments", "replicasets", "daemonsets", "statefulsets"]`
	if !strings.Contains(manifest, want) {
		t.Fatalf("control plane RBAC should cover diagnosable apps workloads %s:\n%s", want, manifest)
	}
}
