package host

import (
	_ "embed"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

//go:embed test_localpv_unit.py
var localPVUnitTests string

func TestLocalPVUnitRecoveryAndConvergence(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is unavailable")
	}
	dir := t.TempDir()
	// Run exactly the embedded renderer and test suite. Both are declared
	// Go inputs, so Python-only test edits invalidate this test's cache
	// without selecting API or any runtime build.
	if err := os.WriteFile(filepath.Join(dir, "localpv_unit.py"), []byte(localPVUnitProgram), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("python3", "-B", "-", "-v")
	cmd.Dir = dir
	cmd.Stdin = strings.NewReader(localPVUnitTests)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("LocalPV boot unit tests failed: %v\n%s", err, output)
	}
}
