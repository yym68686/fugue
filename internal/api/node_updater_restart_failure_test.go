package api

import (
	"os/exec"
	"strings"
	"testing"
)

func TestNodeUpdaterRestartFailureNotMasked(t *testing.T) {
	var server Server
	script := server.nodeUpdaterInstallScript("https://api.invalid")
	for _, name := range []string{"restart_k3s_agent", "restart_k3s_agent_for_config_reload"} {
		_, tail, found := strings.Cut(script, "\n"+name+"() {\n")
		if !found {
			t.Fatalf("missing function %s", name)
		}
		body, _, found := strings.Cut(tail, "\n}\n")
		if !found {
			t.Fatalf("missing body for %s", name)
		}
		for _, failure := range []string{"restart", "readiness"} {
			t.Run(name+"/"+failure, func(t *testing.T) {
				harness := `
set +e
log_task() { printf '%s\n' "$*"; }
log() { printf '%s\n' "$*"; }
systemctl() { [ "$1" != restart ] || [ "$FAILURE" != restart ]; }
wait_for_unit_active() { [ "$FAILURE" != readiness ]; }
` + name + "() {\n" + body + "\n}\nif " + name + "; then exit 99; fi\n"
				cmd := exec.Command("bash", "-c", harness)
				cmd.Env = []string{"FAILURE=" + failure}
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("restart failure was masked: %v\n%s", err, out)
				}
			})
		}
	}
}
