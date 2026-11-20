package api

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNodeUpdaterReportsHeartbeatTransportFailure(t *testing.T) {
	var server Server
	script := server.nodeUpdaterInstallScript("https://api.invalid")
	_, tail, found := strings.Cut(script, "\nheartbeat() {\n")
	if !found {
		t.Fatal("missing node updater heartbeat")
	}
	body, _, found := strings.Cut(tail, "\n}\n")
	if !found {
		t.Fatal("missing node updater heartbeat body")
	}
	harness := `
set -e
k3s_version() { printf v1.34.0; }
command() { [ "$*" = "-v python3" ]; }
node_deep_health_heartbeat_json() {
  [ "$*" = "v1.34.0 reconcile-observe" ] || return 70
  printf '{}'
}
api_json() { [ "$*" = "POST /v1/node-updater/heartbeat {}" ] && return 22; return 71; }
heartbeat() {
` + body + `
}
if heartbeat reconcile-observe; then exit 99; else exit "$?"; fi
`
	cmd := exec.Command("bash", "-c", harness)
	out, err := cmd.CombinedOutput()
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ExitCode() != 22 {
		t.Fatalf("heartbeat did not preserve transport failure: %v\n%s", err, out)
	}
}

func TestNodeUpdaterSelfUpgradePreservesInstalledScriptOnFailure(t *testing.T) {
	var server Server
	script := server.nodeUpdaterInstallScript("https://api.invalid")
	_, tail, found := strings.Cut(script, "\nupgrade_node_updater() (\n")
	if !found {
		t.Fatal("missing atomic self-upgrade function")
	}
	body, _, found := strings.Cut(tail, "\n)\n")
	if !found {
		t.Fatal("missing self-upgrade function body")
	}
	for _, name := range []string{"download_failure", "empty_response", "invalid_shell", "wrong_version", "downgrade", "rename_failure", "success"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			destination := filepath.Join(dir, "fugue-node-updater")
			if err := os.WriteFile(destination, []byte("original updater\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			harness := `
set +e
log_task() { printf '%s\n' "$*"; }
curl() {
  local output=""
  while [ "$#" -gt 0 ]; do
    if [ "$1" = -o ]; then output="$2"; break; fi
    shift
  done
  case "$CASE" in
    download_failure) return 22 ;;
    empty_response) : > "$output" ;;
    invalid_shell) printf '%s\n' 'if' > "$output" ;;
    wrong_version) printf '%s\n' 'FUGUE_NODE_UPDATER_SCRIPT_VERSION="v49"' > "$output" ;;
    downgrade) printf '%s\n' 'FUGUE_NODE_UPDATER_SCRIPT_VERSION="v46"' > "$output" ;;
    *) printf '%s\n' 'FUGUE_NODE_UPDATER_SCRIPT_VERSION="v48"' > "$output" ;;
  esac
}
mv() { [ "$CASE" != rename_failure ] && command mv "$@"; }
FUGUE_NODE_UPDATER_SCRIPT_VERSION=v47
FUGUE_NODE_UPDATE_TASK_TARGET_VERSION=v48
FUGUE_API_BASE=https://api.invalid
upgrade_node_updater() (
` + strings.ReplaceAll(body, "/usr/local/bin", dir) + "\n)\nif upgrade_node_updater; then exit 0; else exit 1; fi\n"
			cmd := exec.Command("bash", "-c", harness)
			cmd.Env = append(os.Environ(), "CASE="+name)
			out, err := cmd.CombinedOutput()
			if (err == nil) != (name == "success") {
				t.Fatalf("unexpected self-upgrade status: %v\n%s", err, out)
			}
			installed, err := os.ReadFile(destination)
			if err != nil {
				t.Fatal(err)
			}
			if name != "success" && string(installed) != "original updater\n" {
				t.Fatal("self-upgrade failure damaged the installed updater")
			}
			if name == "success" && !strings.Contains(string(installed), `FUGUE_NODE_UPDATER_SCRIPT_VERSION="v48"`) {
				t.Fatal("the validated updater was not installed")
			}
		})
	}
}

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
