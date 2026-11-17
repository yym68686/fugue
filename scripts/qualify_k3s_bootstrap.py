#!/usr/bin/env python3
"""Destructive ONLY on an explicitly acknowledged, empty disposable Linux VM.

This is a qualification test, never a production upgrade entrypoint. It has no
production credentials or remote-node targets. Preserve raw evidence on failure.
"""
import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import threading
import time
import urllib.request

VERSIONS = ("v1.35.10+k3s1", "v1.36.6+k3s1", "v1.37.2+k3s1")
ROOT = Path(__file__).resolve().parent.parent


def run(*args, check=True, timeout=180, **kwargs):
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout, **kwargs)
    if check and result.returncode:
        raise RuntimeError(f"{args!r}: {result.returncode}\n{result.stdout}\n{result.stderr}")
    return result


def kubectl(*args, check=True):
    return run("k3s", "kubectl", "--request-timeout=15s", *args, check=check, timeout=30)


def pod():
    return json.loads(kubectl("-n", "upgrade-qualification", "get", "pod",
                              "bootstrap-floor-fixture", "-o", "json").stdout)


def memory_bytes(value):
    match = re.fullmatch(r"([0-9.]+)([KMGTPE]i|[kMGTPE]|m)?", str(value))
    assert match, value
    number, suffix = match.groups()
    units = {None: 1, "m": Decimal("0.001"), "k": 1000}
    for index, unit in enumerate("KMGTPE", 1):
        units[unit + "i"] = 1024 ** index
        if unit != "K":
            units[unit] = 1000 ** index
    return int(Decimal(number) * units[suffix])


def schedule_capacity_probe(version, evidence, overflow):
    """Prove scheduler accounting, not just a successful API patch.

    This sleeping fixture reserves (but does NOT allocate) slightly more than
    the currently available requests headroom. Only the init downsize can make
    it schedulable; no production node or workload is involved.
    """
    nodes = json.loads(kubectl("get", "nodes", "-o", "json").stdout)["items"]
    assert len(nodes) == 1
    node = nodes[0]
    pods = json.loads(kubectl("get", "pods", "-A", "-o", "json").stdout)["items"]
    total = 0
    for item in pods:
        spec = item["spec"]
        if spec.get("nodeName") != node["metadata"]["name"] or item["status"].get("phase") in {"Succeeded", "Failed"}:
            continue
        assert not spec.get("resources"), "unexpected pod-level resource accounting"
        assert not any(c.get("restartPolicy") == "Always" for c in spec.get("initContainers", []))
        requests = lambda c: memory_bytes(c.get("resources", {}).get("requests", {}).get("memory", "0"))
        total += max(sum(map(requests, spec["containers"])),
                     max(map(requests, spec.get("initContainers", [])), default=0))
        total += memory_bytes(spec.get("overhead", {}).get("memory", "0"))
    allocatable = memory_bytes(node["status"]["allocatable"]["memory"])
    request = allocatable - total + overflow
    assert request > 0
    manifest = {"apiVersion": "v1", "kind": "Pod",
                "metadata": {"name": "capacity-probe", "namespace": "upgrade-qualification"},
                "spec": {"automountServiceAccountToken": False, "terminationGracePeriodSeconds": 1, "containers": [
                    {"name": "reserve-only", "image": "busybox:1.37.0",
                     "command": ["/bin/sleep", "100000"],
                     "resources": {"requests": {"cpu": "1m", "memory": str(request)}}}]}}
    path = evidence / (version + "-capacity-probe.json")
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    kubectl("create", "-f", str(path))
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        probe = json.loads(kubectl("-n", "upgrade-qualification", "get", "pod", "capacity-probe", "-o", "json").stdout)
        assert not probe["spec"].get("nodeName"), "capacity probe unexpectedly scheduled before resize"
        conditions = probe["status"].get("conditions", [])
        if any(c["type"] == "PodScheduled" and c["status"] == "False" and
               "Insufficient memory" in c.get("message", "") for c in conditions):
            return {"allocatable": allocatable, "requests_before": total, "probe_request": request}
        time.sleep(2)
    raise RuntimeError("scheduler did not confirm Insufficient memory before resize")


def download(version, directory, evidence):
    release = "https://github.com/k3s-io/k3s/releases/download/" + version.replace("+", "%2B")
    checksums = run("curl", "-fsSL", "--retry", "3", "--max-time", "300",
                    release + "/sha256sum-amd64.txt").stdout
    expected = [line.split()[0] for line in checksums.splitlines()
                if line.split()[-1].lstrip("*") == "k3s"]
    assert len(expected) == 1
    candidate = directory / version
    run("curl", "-fsSL", "--retry", "3", "--max-time", "300",
        release + "/k3s", "-o", str(candidate), timeout=1000)
    assert hashlib.sha256(candidate.read_bytes()).hexdigest() == expected[0]
    (evidence / (version + "-artifact.json")).write_text(json.dumps(
        {"version": version, "source": release, "sha256": expected[0],
         "size": candidate.stat().st_size}, indent=2) + "\n")
    candidate.chmod(0o755)
    assert run(str(candidate), "--version").stdout.split()[2] == version
    return candidate


def wait_ready(version):
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        result = kubectl("get", "nodes", "-o", "json", check=False)
        if not result.returncode:
            nodes = json.loads(result.stdout)["items"]
            if len(nodes) == 1 and nodes[0]["status"]["nodeInfo"]["kubeletVersion"] == version:
                if any(c["type"] == "Ready" and c["status"] == "True"
                       for c in nodes[0]["status"]["conditions"]):
                    return nodes[0]["metadata"]["uid"]
        time.sleep(2)
    raise RuntimeError("node failed to reach Ready at " + version)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--disposable-host", action="store_true", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--with-storage", action="store_true")
    args = parser.parse_args()
    assert platform.system() == "Linux" and platform.machine() == "x86_64"
    assert os.geteuid() == 0
    assert not Path("/var/lib/rancher/k3s").exists(), "refusing an existing cluster"
    assert not Path("/etc/rancher/k3s").exists(), "refusing existing cluster configuration"
    assert not Path("/usr/local/bin/k3s").exists(), "refusing an existing installation"
    virtualization = run("systemd-detect-virt", check=False).stdout.strip()
    assert virtualization not in {"docker", "lxc", "podman", "systemd-nspawn"}, virtualization
    evidence = args.evidence.resolve()
    evidence.mkdir(parents=True, exist_ok=False)
    binaries = evidence.parent / (evidence.name + "-binaries")
    binaries.mkdir(parents=True, exist_ok=False)
    report = {"versions": list(VERSIONS), "virtualization": virtualization,
              "kernel": platform.release(), "results": [], "passed": False}
    failures, samples = [], []
    stop = threading.Event()

    def write_json(name, value):
        (evidence / name).write_text(json.dumps(value, indent=2) + "\n")

    def observe():
        while not stop.is_set():
            started = time.monotonic()
            try:
                with urllib.request.urlopen("http://127.0.0.1:30080", timeout=2) as response:
                    assert response.status == 200
                    assert response.read() == b"upgrade-fixture-ok\n"
                samples.append({"at": time.time(), "latency": time.monotonic() - started})
            except Exception as exc:
                failures.append({"at": time.time(), "error": str(exc)})
            stop.wait(0.1)

    observer = None
    storage = None
    try:
        installer = evidence / "install.sh"
        installer.write_text(run("curl", "-fsSL", "https://get.k3s.io").stdout)
        binary = download(VERSIONS[0], binaries, evidence)
        run("install", "-m", "0755", str(binary), "/usr/local/bin/k3s")
        env = dict(os.environ, INSTALL_K3S_SKIP_DOWNLOAD="true",
                   INSTALL_K3S_EXEC="server --cluster-init --disable traefik --disable servicelb --node-name qualification-control")
        run("sh", str(installer), env=env, timeout=600)
        node_uid = wait_ready(VERSIONS[0])
        assert run("systemctl", "show", "k3s", "-p", "KillMode", "--value").stdout.strip() == "process"
        kubectl("create", "namespace", "upgrade-qualification", check=False)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            result = kubectl("-n", "upgrade-qualification", "get", "serviceaccount", "default", "-o", "json", check=False)
            if not result.returncode:
                break
            time.sleep(2)
        else:
            raise RuntimeError("default service account controller did not converge")
        kubectl("apply", "-f", str(ROOT / "tests/k3s-upgrade/bootstrap-floor.yaml"))
        run("k3s", "kubectl", "-n", "upgrade-qualification", "wait", "--for=condition=Ready",
            "pod/bootstrap-floor-fixture", "--timeout=180s", timeout=200)
        run("k3s", "kubectl", "-n", "kube-system", "wait", "--for=condition=Ready",
            "pod", "--all", "--timeout=180s", timeout=200)
        before = pod()
        write_json("baseline-pod.json", before)
        assert before["status"]["initContainerStatuses"][0]["state"]["terminated"]["exitCode"] == 0
        container_id = before["status"]["containerStatuses"][0]["containerID"]
        pod_uid = before["metadata"]["uid"]
        if args.with_storage:
            sys.path.insert(0, str(ROOT))
            from scripts.qualify_k3s_storage import StorageQualification
            storage = StorageQualification(evidence)
            storage.setup()
            storage.observe()
        observer = threading.Thread(target=observe)
        observer.start()
        for index, version in enumerate(VERSIONS):
            if index:
                if index == 1 and storage:
                    storage.upgrade_longhorn()
                binary = download(version, binaries, evidence)
                # Atomic binary replacement preserves every installer-managed setting.
                run("install", "-m", "0755", str(binary), "/usr/local/bin/k3s.next")
                os.replace("/usr/local/bin/k3s.next", "/usr/local/bin/k3s")
                run("systemctl", "restart", "k3s", timeout=300)
                assert wait_ready(version) == node_uid
            if storage:
                storage.checkpoint(version)
            initial = pod()
            assert initial["metadata"]["uid"] == pod_uid
            assert initial["status"]["containerStatuses"][0]["containerID"] == container_id
            # Only resize an ALREADY COMPLETED ordinary init container. Main
            # container memory, executable inode, UID and connections stay intact.
            target = "128Mi" if index < 2 else "96Mi"
            capacity = None if storage else schedule_capacity_probe(version, evidence, (128 if index < 2 else 16) * 1024 ** 2)
            patch = json.dumps({"spec": {"initContainers": [{"name": "bootstrap",
                                  "resources": {"requests": {"memory": target},
                                                "limits": {"memory": target}}}]}})
            result = kubectl("-n", "upgrade-qualification", "patch", "pod",
                             "bootstrap-floor-fixture", "--subresource=resize", "--type=strategic",
                             "-p", patch, check=False)
            time.sleep(15)
            after = pod()
            write_json(version + "-pod.json", after)
            (evidence / (version + "-node.txt")).write_text(kubectl("describe", "node").stdout)
            entry = {"version": version, "patch_exit": result.returncode,
                     "patch_stdout": result.stdout, "patch_stderr": result.stderr,
                     "init_request": after["spec"]["initContainers"][0]["resources"]["requests"]["memory"],
                     "main_request": after["spec"]["containers"][0]["resources"]["requests"]["memory"],
                     "container_preserved": after["status"]["containerStatuses"][0]["containerID"] == container_id,
                     "http_failures": len(failures), "scheduler_capacity": capacity}
            report["results"].append(entry)
            write_json("report.json", report)
            print(json.dumps(entry), flush=True)
            assert entry["container_preserved"] and entry["main_request"] == "64Mi"
            if index == 0:
                assert result.returncode != 0 and entry["init_request"] == "512Mi"
            else:
                assert result.returncode == 0, "completed-init resize failed; bootstrap benefit NOT proven"
                assert entry["init_request"] == target
                if not storage:
                    run("k3s", "kubectl", "-n", "upgrade-qualification", "wait", "--for=condition=PodScheduled",
                        "pod/capacity-probe", "--timeout=90s", timeout=100)
                    entry["scheduler_capacity_released"] = True
                write_json("report.json", report)
            if not storage:
                kubectl("-n", "upgrade-qualification", "delete", "pod", "capacity-probe", "--wait=false")
                run("k3s", "kubectl", "-n", "upgrade-qualification", "wait", "--for=delete",
                    "pod/capacity-probe", "--timeout=60s", timeout=70)
        assert not failures, "HTTP continuity failed"
        report["passed"] = True
    finally:
        stop.set()
        storage_error = None
        if storage:
            try:
                storage.finish()
            except Exception as exc:
                storage_error = exc
                report["passed"] = False
        if observer:
            observer.join(timeout=5)
        write_json("http.json", {"samples": samples, "failures": failures})
        write_json("report.json", report)
        logs = run("journalctl", "-u", "k3s", "--no-pager", "-n", "1000", check=False)
        (evidence / "journal.txt").write_text(logs.stdout)
        result = kubectl("get", "pods", "-A", "-o", "json", check=False)
        (evidence / "final-pods.json").write_text(result.stdout)
        result = kubectl("-n", "upgrade-qualification", "logs", "bootstrap-floor-fixture", "--all-containers", "--tail=100", check=False)
        (evidence / "fixture-logs.txt").write_text(result.stdout + result.stderr)
        result = kubectl("get", "events", "-A", "-o", "json", check=False)
        (evidence / "events.json").write_text(result.stdout)
        if storage_error:
            raise storage_error


if __name__ == "__main__":
    main()
