package api

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// The dispatcher runs with errexit disabled. Assert the exact reached failure
// phase, not just a nonzero exit caused by a missing host utility on macOS.
func TestK3SAgentUpgradeFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name, target, phase, reached string
		wantSuccess                  bool
	}{
		{"missing_target", "", "validate_versions", "", false},
		{"minor_jump", "v1.37.2+k3s1", "validate_versions", "", false},
		{"downgrade", "v1.34.5+k3s1", "validate_versions", "", false},
		{"bad_kill_mode", "v1.36.6+k3s1", "preflight", "kill_mode", false},
		{"server_older_than_target", "v1.36.6+k3s1", "preflight", "server_version", false},
		{"download_failure", "v1.36.6+k3s1", "download_verify", "checksums", false},
		{"checksum_mismatch", "v1.36.6+k3s1", "download_verify", "candidate", false},
		{"restart_failure", "v1.36.6+k3s1", "restart", "restart", false},
		{"version_did_not_converge", "v1.36.6+k3s1", "convergence", "restart", false},
		{"container_disappeared", "v1.36.6+k3s1", "container_continuity", "restart", false},
		{"successful_upgrade", "v1.36.6+k3s1", "", "restart", true},
		{"idempotent", "v1.35.4+k3s1", "", "server_version", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			harness := guardedK3SUpgradeShell + "\n" + `
set +e
TRACE="${FIXTURE}/trace"
trace() { printf '%s\n' "$1" >> "${TRACE}"; }
log_task() { printf '%s\n' "$*"; }
k3s_version() { printf '%s\n' 'k3s version v1.35.4+k3s1'; }
node_deep_health_cluster_node_name() { printf '%s\n' fixture-node; }
k3s_upgrade_node_uid() {
  local actual=v1.35.4+k3s1
  if [ "${RESTARTED:-0}" = 1 ]; then actual=v1.36.6+k3s1; fi
  [ "$1" = "${actual}" ] || return 1
  printf '%s\n' stable-uid
}
k3s_upgrade_process_version() {
  [ "${CASE}" != version_did_not_converge ] || [ "${RESTARTED:-0}" != 1 ]
}
k3s_upgrade_container_ids() {
  if [ "${CASE}" = container_disappeared ] && [ "${RESTARTED:-0}" = 1 ]; then return 0; fi
  printf '%s\n' existing-container
}
k3s() {
  trace server_version
  if [ "${CASE}" = server_older_than_target ]; then
    printf '%s\n' '{"serverVersion":{"minor":"35"}}'
  else printf '%s\n' '{"serverVersion":{"minor":"36"}}'; fi
}
systemctl() {
  case "$*" in
    'is-active --quiet k3s-agent') return 0 ;;
    'is-active --quiet k3s') return 1 ;;
    'show k3s-agent -p KillMode --value')
      trace kill_mode
      if [ "${CASE}" = bad_kill_mode ]; then printf '%s\n' control-group; else printf '%s\n' process; fi ;;
    'show k3s-agent -p MainPID --value') printf '%s\n' 1234 ;;
    '--no-block restart k3s-agent')
      trace restart
      RESTARTED=1
      [ "${CASE}" != restart_failure ] ;;
    *) printf '%s\n' "unexpected systemctl: $*" >&2; return 99 ;;
  esac
}
readlink() { printf '%s\n' "${FIXTURE}/k3s"; }
flock() { return 0; }
uname() { printf '%s\n' x86_64; }
sha256sum() { printf '%064d  %s\n' 0 "$1"; }
curl() {
  local output=""
  while [ "$#" -gt 0 ]; do
    if [ "$1" = -o ]; then output="$2"; break; fi
    shift
  done
  case "${output}" in
    */checksums)
      trace checksums
      [ "${CASE}" != download_failure ] || return 22
      if [ "${CASE}" = checksum_mismatch ]; then
        printf '%064d  k3s\n' 1 > "${output}"
      else printf '%064d  k3s\n' 0 > "${output}"; fi ;;
    */candidate)
      trace candidate
      printf '#!/bin/sh\nprintf "k3s version v1.36.6+k3s1\\n"\n' > "${output}" ;;
    *) return 98 ;;
  esac
}
date() {
  local n=0
  [ ! -f "${FIXTURE}/clock" ] || read -r n < "${FIXTURE}/clock"
  printf '%s\n' "$((n+1))" > "${FIXTURE}/clock"
  if [ "$n" -gt 1 ]; then printf '%s\n' 1000; else printf '%s\n' "$n"; fi
}
sleep() { return 0; }
FUGUE_NODE_UPDATER_WORK_DIR="${FIXTURE}/work"
FUGUE_NODE_UPDATER_KUBELET_KUBECONFIG="${FIXTURE}/kubeconfig"
if upgrade_k3s_agent; then exit 0; else exit 1; fi
`
			for name, data := range map[string]string{"upgrade-test.sh": harness, "k3s": "original binary\n", "kubeconfig": "fixture\n"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			cmd := exec.Command("bash", filepath.Join(dir, "upgrade-test.sh"))
			cmd.Env = append(os.Environ(), "FIXTURE="+dir, "CASE="+tc.name, "FUGUE_NODE_UPDATE_TASK_TARGET_K3S_VERSION="+tc.target)
			out, err := cmd.CombinedOutput()
			if (err == nil) != tc.wantSuccess {
				t.Fatalf("success=%t want=%t: %v\n%s", err == nil, tc.wantSuccess, err, out)
			}
			if !tc.wantSuccess && !strings.Contains(string(out), "failed at "+tc.phase+";") {
				t.Fatalf("expected failure at %s, got:\n%s", tc.phase, out)
			}
			if tc.reached != "" {
				trace, _ := os.ReadFile(filepath.Join(dir, "trace"))
				if !strings.Contains(string(trace), tc.reached+"\n") {
					t.Fatalf("did not reach %s: %s\n%s", tc.reached, trace, out)
				}
			}
			if tc.phase == "validate_versions" || tc.phase == "preflight" || tc.phase == "download_verify" {
				binary, _ := os.ReadFile(filepath.Join(dir, "k3s"))
				if string(binary) != "original binary\n" {
					t.Fatal("preflight or download failure changed the installed binary")
				}
			}
		})
	}
}
