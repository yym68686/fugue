package host

import (
	"os/exec"
	"testing"
)

func TestLocalPVUnitRecoveryAndConvergence(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is unavailable")
	}
	cmd := exec.Command("python3", "-B", "-m", "unittest", "-v", "test_localpv_unit")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("LocalPV boot unit tests failed: %v\n%s", err, output)
	}
}
