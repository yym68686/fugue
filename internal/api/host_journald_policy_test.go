package api

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestHostJournaldPolicyShellLibraryAppliesIdempotently(t *testing.T) {
	t.Parallel()

	fixture := newHostJournaldPolicyShellFixture(t)
	out, err := fixture.run(nil)
	if err != nil {
		t.Fatalf("first reconcile failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "state=applied") || !strings.Contains(out, "changed=true") {
		t.Fatalf("unexpected first result: %s", out)
	}
	wantPolicy := "# Managed by Fugue. Local edits will be replaced.\n[Journal]\nMaxRetentionSec=30day\nSystemMaxUse=1G\n"
	if got := mustReadHostJournaldTestFile(t, fixture.policyFile); got != wantPolicy {
		t.Fatalf("unexpected policy:\n%s", got)
	}

	out, err = fixture.run(nil)
	if err != nil {
		t.Fatalf("second reconcile failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "state=applied") || !strings.Contains(out, "changed=false") {
		t.Fatalf("unexpected second result: %s", out)
	}
	calls := mustReadHostJournaldTestFile(t, fixture.callsFile)
	if got := strings.Count(calls, "systemctl restart systemd-journald.service"); got != 1 {
		t.Fatalf("expected exactly one restart, got %d calls:\n%s", got, calls)
	}
	if got := strings.Count(calls, "journalctl --rotate"); got != 2 {
		t.Fatalf("expected a rotation on every reconciliation, got %d calls:\n%s", got, calls)
	}
	if got := strings.Count(calls, "journalctl --vacuum-time=30day --vacuum-size=1G"); got != 2 {
		t.Fatalf("expected a bounded vacuum on every reconciliation, got %d calls:\n%s", got, calls)
	}
}

func TestHostJournaldPolicyShellLibraryDryRunDoesNotMutate(t *testing.T) {
	t.Parallel()

	fixture := newHostJournaldPolicyShellFixture(t)
	out, err := fixture.run(map[string]string{"FUGUE_JOURNALD_DRY_RUN": "true"})
	if err != nil {
		t.Fatalf("dry-run failed: %v\n%s", err, out)
	}
	if !strings.Contains(out, "state=dry-run") || !strings.Contains(out, "changed=true") {
		t.Fatalf("unexpected dry-run result: %s", out)
	}
	if _, err := os.Stat(fixture.policyFile); !os.IsNotExist(err) {
		t.Fatalf("dry-run unexpectedly created policy file: %v", err)
	}
	calls := mustReadHostJournaldTestFile(t, fixture.callsFile)
	for _, forbidden := range []string{"systemctl restart", "journalctl --rotate", "journalctl --vacuum"} {
		if strings.Contains(calls, forbidden) {
			t.Fatalf("dry-run executed %q:\n%s", forbidden, calls)
		}
	}
}

func TestHostJournaldPolicyShellLibraryRejectsInvalidIntent(t *testing.T) {
	t.Parallel()

	fixture := newHostJournaldPolicyShellFixture(t)
	out, err := fixture.run(map[string]string{"FUGUE_JOURNALD_MAX_RETENTION_SEC": "30day;reboot"})
	if err == nil {
		t.Fatalf("expected invalid intent to fail: %s", out)
	}
	if !strings.Contains(out, "state=refused") || !strings.Contains(out, "invalid MaxRetentionSec") {
		t.Fatalf("unexpected invalid intent result: %s", out)
	}
	if _, statErr := os.Stat(fixture.policyFile); !os.IsNotExist(statErr) {
		t.Fatalf("invalid intent unexpectedly created policy file: %v", statErr)
	}
}

func TestHostJournaldPolicyShellLibraryRollsBackRestartFailure(t *testing.T) {
	t.Parallel()

	fixture := newHostJournaldPolicyShellFixture(t)
	oldPolicy := "[Journal]\nMaxRetentionSec=7day\nSystemMaxUse=512M\n"
	if err := os.MkdirAll(filepath.Dir(fixture.policyFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fixture.policyFile, []byte(oldPolicy), 0o644); err != nil {
		t.Fatal(err)
	}
	out, err := fixture.run(map[string]string{"FUGUE_TEST_FAIL_RESTART": "true"})
	if err == nil {
		t.Fatalf("expected restart failure: %s", out)
	}
	if !strings.Contains(out, "state=rolled-back") {
		t.Fatalf("unexpected restart failure result: %s", out)
	}
	if got := mustReadHostJournaldTestFile(t, fixture.policyFile); got != oldPolicy {
		t.Fatalf("previous policy was not restored:\n%s", got)
	}
}

func TestHostJournaldPolicyShellLibraryRollsBackIneffectivePolicy(t *testing.T) {
	t.Parallel()

	fixture := newHostJournaldPolicyShellFixture(t)
	out, err := fixture.run(map[string]string{"FUGUE_TEST_EFFECTIVE_OVERRIDE": "true"})
	if err == nil {
		t.Fatalf("expected overridden policy to fail: %s", out)
	}
	if !strings.Contains(out, "state=rolled-back") || !strings.Contains(out, "effective journald policy differs") {
		t.Fatalf("unexpected override result: %s", out)
	}
	if _, statErr := os.Stat(fixture.policyFile); !os.IsNotExist(statErr) {
		t.Fatalf("ineffective policy was not rolled back: %v", statErr)
	}
}

type hostJournaldPolicyShellFixture struct {
	t          *testing.T
	dir        string
	binDir     string
	policyFile string
	callsFile  string
}

func newHostJournaldPolicyShellFixture(t *testing.T) hostJournaldPolicyShellFixture {
	t.Helper()
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	fixture := hostJournaldPolicyShellFixture{
		t:          t,
		dir:        dir,
		binDir:     binDir,
		policyFile: filepath.Join(dir, "etc", "systemd", "journald.conf.d", "90-fugue-retention.conf"),
		callsFile:  filepath.Join(dir, "calls.log"),
	}
	fixture.writeCommand("systemctl", `
printf 'systemctl %s\n' "$*" >>"${FUGUE_TEST_CALLS_FILE}"
if [ "${1:-}" = "restart" ] && [ "${FUGUE_TEST_FAIL_RESTART:-false}" = "true" ]; then
  exit 1
fi
exit 0
`)
	fixture.writeCommand("journalctl", `
printf 'journalctl %s\n' "$*" >>"${FUGUE_TEST_CALLS_FILE}"
if [ "${1:-}" = "--disk-usage" ]; then
  printf 'Archived and active journals take up 1.0G in the file system.\n'
fi
exit 0
`)
	fixture.writeCommand("systemd-analyze", `
cat "${FUGUE_JOURNALD_POLICY_FILE}"
if [ "${FUGUE_TEST_EFFECTIVE_OVERRIDE:-false}" = "true" ]; then
  printf '[Journal]\nMaxRetentionSec=90day\nSystemMaxUse=4G\n'
fi
`)
	if err := os.WriteFile(fixture.callsFile, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func (fixture hostJournaldPolicyShellFixture) writeCommand(name, body string) {
	fixture.t.Helper()
	content := "#!/usr/bin/env bash\nset -euo pipefail\n" + body
	if err := os.WriteFile(filepath.Join(fixture.binDir, name), []byte(content), 0o755); err != nil {
		fixture.t.Fatal(err)
	}
}

func (fixture hostJournaldPolicyShellFixture) run(overrides map[string]string) (string, error) {
	fixture.t.Helper()
	script := "set -uo pipefail\n" + hostJournaldPolicyShellLibrary() + `
fugue_journald_policy_reconcile
rc=$?
printf 'state=%s\nreason=%s\nchanged=%s\nbefore=%s\nafter=%s\n' \
  "${FUGUE_JOURNALD_POLICY_STATE}" \
  "${FUGUE_JOURNALD_POLICY_REASON}" \
  "${FUGUE_JOURNALD_POLICY_CHANGED}" \
  "${FUGUE_JOURNALD_POLICY_BEFORE_USAGE}" \
  "${FUGUE_JOURNALD_POLICY_AFTER_USAGE}"
exit "${rc}"
`
	cmd := exec.Command("bash", "-c", script)
	env := append(os.Environ(),
		"PATH="+fixture.binDir+":"+os.Getenv("PATH"),
		"FUGUE_JOURNALD_POLICY_FILE="+fixture.policyFile,
		"FUGUE_JOURNALD_MAX_RETENTION_SEC=30day",
		"FUGUE_JOURNALD_SYSTEM_MAX_USE=1G",
		"FUGUE_JOURNALD_DRY_RUN=false",
		"FUGUE_TEST_CALLS_FILE="+fixture.callsFile,
	)
	for key, value := range overrides {
		env = append(env, key+"="+value)
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func mustReadHostJournaldTestFile(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}
