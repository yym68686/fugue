package api

import (
	"os/exec"
	"strings"
	"testing"

	"fugue/internal/noderesourcepolicy"
)

func TestNodeUpdaterIncludesLocalPVUnitConvergenceAndFreshEvidence(t *testing.T) {
	if nodeUpdaterScriptVersion != noderesourcepolicy.UpdaterVersion {
		t.Fatalf("API script generation %s differs from node diagnostics rollout target %s", nodeUpdaterScriptVersion, noderesourcepolicy.UpdaterVersion)
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
