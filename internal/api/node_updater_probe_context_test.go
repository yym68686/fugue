package api

import (
	"os/exec"
	"strings"
	"testing"
)

func TestNodeUpdaterCRIStatusRequiresExplicitRuntimeEvidence(t *testing.T) {
	var server Server
	script := server.nodeUpdaterInstallScript("https://api.invalid")
	const start = "def observe_cri_status():"
	_, tail, found := strings.Cut(script, start)
	if !found {
		t.Fatal("missing CRI status observer")
	}
	source, _, found := strings.Cut(tail, "\nchecks.append(observe_cri_status())")
	if !found {
		t.Fatal("missing CRI status observer boundary")
	}
	harness := `
import json
import subprocess
from types import SimpleNamespace

shutil = SimpleNamespace(which=lambda _: True)
def check(name, category, status, detail, expected, hard=False, repair_action="", evidence=None):
    assert name == "pod_sandbox_creation" and category == "cri"
    return status, hard, repair_action, evidence or {}
def command_env():
    return {}
` + start + source + `
def check_case(response, exit_code, outcome, hard=False, hint="", timeout=False):
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        assert kwargs["timeout"] == 5 and kwargs["capture_output"] is True
        if timeout:
            raise subprocess.TimeoutExpired(argv, 5)
        return SimpleNamespace(stdout=response, stderr="must not affect status JSON", returncode=exit_code)
    subprocess.run = run
    actual = observe_cri_status()
    assert calls == [["crictl", "info", "--quiet"]], calls
    assert actual == (outcome, hard, hint, {"observation_state": "not_observed"} if outcome == "warning" else {}), actual

def status(*entries):
    return json.dumps({"status": {"conditions": [
        {"type": name, "status": ready} for name, ready in entries
    ]}})

ready = status(("NetworkReady", True), ("RuntimeReady", True))
check_case(ready, 0, "pass")
check_case(status(("RuntimeReady", False), ("NetworkReady", True)), 0, "fail", True, "restart-k3s-agent")
check_case(status(("RuntimeReady", True), ("NetworkReady", False)), 0, "fail", True)
check_case(ready, 1, "warning")
check_case("", 0, "warning", timeout=True)
check_case("not JSON", 0, "warning")
check_case(status(("RuntimeReady", True)), 0, "warning")
check_case(status(("RuntimeReady", "true"), ("NetworkReady", True)), 0, "warning")
check_case(status(("RuntimeReady", False), ("RuntimeReady", True), ("NetworkReady", True)), 0, "warning")
`
	if out, err := exec.Command("python3", "-c", harness).CombinedOutput(); err != nil {
		t.Fatalf("release-installed CRI status observer: %v\n%s", err, out)
	}
}

func TestNodeUpdaterProbesOnlyUntilFirstEligibleSandboxWithService(t *testing.T) {
	var server Server
	script := server.nodeUpdaterInstallScript("https://api.invalid")
	between := func(start, end string) string {
		t.Helper()
		_, tail, ok := strings.Cut(script, "\n"+start)
		if !ok {
			t.Fatalf("missing start boundary %q", start)
		}
		body, _, ok := strings.Cut(tail, "\n"+end)
		if !ok {
			t.Fatalf("missing end boundary %q", end)
		}
		return start + body + "\n"
	}
	source := between("def cni_netns_path(", "def pod_netns_run(") +
		between("def pod_context_probe_eligible(", "pod_ctx, same_service, pod_context_reason =")
	harness := `
import json
from types import SimpleNamespace

shutil = SimpleNamespace(which=lambda _cmd: True)
os = SimpleNamespace(path=SimpleNamespace(exists=lambda path: path == "/proc/123/ns/net"))

` + source + `

def run_case(ids, services, expected_inspected, expected_selected, expected_service, expected_reason="", limit=30):
    inspected = []
    invoked = []
    def capture(cmd, timeout):
        assert timeout == 5
        if cmd == ["crictl", "pods", "--state", "Ready", "--quiet"]:
            return True, "\n".join(ids)
        assert cmd[:2] == ["crictl", "inspectp"]
        sandbox = cmd[2]
        inspected.append(sandbox)
        if sandbox == "gone":
            return False, "sandbox disappeared"
        result = {"pid": 123, "config": {
            "metadata": {"namespace": sandbox, "name": sandbox},
            "labels": {"app.kubernetes.io/component": "edge-control" if sandbox == "control" else "app"},
            "dns_config": {"servers": ["10.43.0.10"]},
        }, "cniResult": {"Interfaces": {"eth0": {"Sandbox": "/test/netns"}}}}
        return True, json.dumps({"info": result})
    def service(ctx):
        assert ctx["pod"] != "control"
        assert ctx["dns_servers"] == ["10.43.0.10"]
        invoked.append(ctx["pod"])
        return services.get(ctx["pod"]), "no service in " + ctx["namespace"]
    globals()["run_capture"] = capture
    globals()["first_same_namespace_service"] = service
    ctx, selected_service, reason = select_pod_probe_context(limit)
    assert inspected == expected_inspected, inspected
    assert invoked == [pod for pod in expected_inspected if pod not in ("control", "gone")], invoked
    assert (ctx["pod"] if ctx else None) == expected_selected
    assert selected_service == expected_service
    assert reason == expected_reason, reason

# The first eligible Service still wins, even if TCP cannot be checked there.
dns_only = {"tcp_backend_observed": False}
run_case(["control", "first", "winner", "later"], {"winner": dns_only, "later": {"tcp_backend_observed": True}},
         ["control", "first", "winner"], "winner", dns_only)
# No eligible Service: inspect the bounded list and use the original first valid namespace.
run_case(["gone", "first", "second", "later"], {"later": dns_only}, ["gone", "first", "second"],
         "first", None, "no service in first", limit=3)
# Excluded and gone sandboxes must not become a successful health observation.
run_case(["control"], {"control": dns_only}, ["control"], None, None)
run_case(["gone"], {}, ["gone"], None, None, "no Ready pod sandbox with CNI network namespace found: sandbox disappeared")

def assert_inventory_failure(message, unsupported=False):
    commands = []
    def capture(cmd, timeout):
        commands.append(cmd)
        assert timeout == 5
        if cmd == ["crictl", "pods", "--quiet"]:
            return False, "unfiltered listing failed"
        return False, message
    globals()["run_capture"] = capture
    ctx, service, reason = select_pod_probe_context()
    assert ctx is None and service is None
    assert commands == [["crictl", "pods", "--state", "Ready", "--quiet"]] + ([["crictl", "pods", "--quiet"]] if unsupported else [])
    assert reason == ("unfiltered listing failed" if unsupported else message)

assert_inventory_failure("Command '['crictl', 'pods', '--state', 'Ready', '--quiet']' timed out after 5 seconds")
assert_inventory_failure("unknown flag: --state", unsupported=True)
`
	if out, err := exec.Command("python3", "-c", harness).CombinedOutput(); err != nil {
		t.Fatalf("release-installed probe discovery: %v\n%s", err, out)
	}
}
