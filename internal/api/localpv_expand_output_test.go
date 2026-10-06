package api

import (
	"os/exec"
	"strings"
	"testing"
)

func TestLocalPVTaskPersistsHostFailureAndDryRunNeverRefreshesInventory(t *testing.T) {
	delivered := (&Server{}).nodeUpdaterInstallScript("https://control.example.test")
	start := strings.Index(delivered, "expand_lvm_localpv() {")
	if start < 0 {
		t.Fatal("missing executor")
	}
	body := delivered[start:]
	end := strings.Index(body, "\n}\n")
	if end < 0 {
		t.Fatal("missing executor end")
	}
	body = body[:end+3]
	for _, failure := range []bool{false, true} {
		mock := `python3() { printf '%s\n' '{"allocation_bytes":100,"host_reserve_bytes":20}'; }
`
		if failure {
			mock = `python3() { printf '%s\n' 'ValueError: insufficient host filesystem headroom: available_bytes=100 growth_bytes=90 reserve_bytes=20'; return 29; }
`
		}
		script := "set -euo pipefail\n" + mock + `
log_task() { printf 'LOG:%s\n' "$1"; }
report_lvm_localpv_inventory() { echo UNEXPECTED_INVENTORY_MUTATION; return 77; }
FUGUE_NODE_UPDATE_TASK_DRY_RUN=true
` + body + `
rc=0
expand_lvm_localpv || rc=$?
printf 'RC=%s\nERROR=%s\nRESULT=%s\n' "$rc" "${FUGUE_NODE_UPDATE_TASK_ERROR_MESSAGE:-}" "${FUGUE_NODE_UPDATE_TASK_RESULT_MESSAGE:-}"
`
		cmd := exec.Command("bash", "-s")
		cmd.Stdin = strings.NewReader(script)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("shell failed: %v %s", err, out)
		}
		text := string(out)
		if strings.Contains(text, "UNEXPECTED_INVENTORY_MUTATION") {
			t.Fatal(text)
		}
		if failure {
			if !strings.Contains(text, "RC=29") || !strings.Contains(text, "ERROR=LocalPV capacity check or expansion failed: ValueError: insufficient host filesystem headroom: available_bytes=100 growth_bytes=90 reserve_bytes=20") || !strings.Contains(text, "LOG:ValueError:") {
				t.Fatal(text)
			}
		} else if !strings.Contains(text, "RC=0") || !strings.Contains(text, "no pool mutation performed") || !strings.Contains(text, `LOG:{"allocation_bytes":100`) {
			t.Fatal(text)
		}
	}
}
