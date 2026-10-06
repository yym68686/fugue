package api

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"fugue/internal/model"
	"fugue/internal/storagerecovery"
)

func mustLocalPVUnitProgramPath(t *testing.T) string {
	t.Helper()
	path, err := filepath.Abs("localpv_unit.py")
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLocalPVUnitRecoveryAndConvergence(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is unavailable")
	}
	cmd := exec.Command("python3", "-m", "unittest", "-v", "test_localpv_unit")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("LocalPV boot unit tests failed: %v\n%s", err, output)
	}
}

func TestNodeUpdaterIncludesLocalPVUnitConvergenceAndFreshEvidence(t *testing.T) {
	if nodeUpdaterScriptVersion != storagerecovery.NodeUpdaterVersion {
		t.Fatalf("API script generation %s differs from optional recovery target %s", nodeUpdaterScriptVersion, storagerecovery.NodeUpdaterVersion)
	}
	if model.NodeUpdaterCurrentVersion == storagerecovery.NodeUpdaterVersion {
		t.Fatal("optional recovery release must not advance the global rollout baseline")
	}
	script := (&Server{}).nodeUpdaterInstallScript("https://control.example.test")
	for _, want := range []string{
		localPVUnitProgram,
		"  reconcile_localpv_boot_unit\n",
		`FUGUE_HEARTBEAT_LOCALPV_BOOT_UNIT="$(localpv_boot_unit_observation observe)"`,
		`"localpv_boot_unit", "storage", status`,
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("updater omits LocalPV convergence or observation %q", want)
		}
	}
	if strings.Contains(script, "__FUGUE_LOCALPV_UNIT_LIBRARY__") {
		t.Fatal("LocalPV library was not embedded")
	}
	cmd := exec.Command("bash", "-n")
	cmd.Stdin = strings.NewReader(script)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("updater script is invalid: %v\n%s", err, output)
	}
}
