#!/usr/bin/env python3
"""Destructive ONLY on an explicitly acknowledged, empty disposable Linux VM.

This is a qualification test, never a production upgrade entrypoint. It has no
production credentials or remote-node targets. Preserve raw evidence on failure.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
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


def download(version, directory):
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
    try:
        installer = evidence / "install.sh"
        installer.write_text(run("curl", "-fsSL", "https://get.k3s.io").stdout)
        binary = download(VERSIONS[0], evidence)
        run("install", "-m", "0755", str(binary), "/usr/local/bin/k3s")
        env = dict(os.environ, INSTALL_K3S_SKIP_DOWNLOAD="true",
                   INSTALL_K3S_EXEC="server --cluster-init --disable traefik --disable servicelb --node-name qualification-control")
        run("sh", str(installer), env=env, timeout=600)
        node_uid = wait_ready(VERSIONS[0])
        assert run("systemctl", "show", "k3s", "-p", "KillMode", "--value").stdout.strip() == "process"
        kubectl("apply", "-f", str(ROOT / "tests/k3s-upgrade/bootstrap-floor.yaml"))
        run("k3s", "kubectl", "-n", "upgrade-qualification", "wait", "--for=condition=Ready",
            "pod/bootstrap-floor-fixture", "--timeout=180s", timeout=200)
        before = pod()
        write_json("baseline-pod.json", before)
        assert before["status"]["initContainerStatuses"][0]["state"]["terminated"]["exitCode"] == 0
        container_id = before["status"]["containerStatuses"][0]["containerID"]
        pod_uid = before["metadata"]["uid"]
        observer = threading.Thread(target=observe)
        observer.start()
        for index, version in enumerate(VERSIONS):
            if index:
                binary = download(version, evidence)
                # Atomic binary replacement preserves every installer-managed setting.
                run("install", "-m", "0755", str(binary), "/usr/local/bin/k3s.next")
                os.replace("/usr/local/bin/k3s.next", "/usr/local/bin/k3s")
                run("systemctl", "restart", "k3s", timeout=300)
                assert wait_ready(version) == node_uid
            initial = pod()
            assert initial["metadata"]["uid"] == pod_uid
            assert initial["status"]["containerStatuses"][0]["containerID"] == container_id
            # Only resize an ALREADY COMPLETED ordinary init container. Main
            # container memory, executable inode, UID and connections stay intact.
            target = "128Mi" if index < 2 else "96Mi"
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
                     "http_failures": len(failures)}
            report["results"].append(entry)
            write_json("report.json", report)
            print(json.dumps(entry), flush=True)
            assert entry["container_preserved"] and entry["main_request"] == "64Mi"
            if index == 0:
                assert result.returncode != 0 and entry["init_request"] == "512Mi"
            else:
                assert result.returncode == 0, "completed-init resize failed; bootstrap benefit NOT proven"
                assert entry["init_request"] == target
        assert not failures, "HTTP continuity failed"
        report["passed"] = True
    finally:
        stop.set()
        if observer:
            observer.join(timeout=5)
        write_json("http.json", {"samples": samples, "failures": failures})
        write_json("report.json", report)
        logs = run("journalctl", "-u", "k3s", "--no-pager", "-n", "1000", check=False)
        (evidence / "journal.txt").write_text(logs.stdout)
        result = kubectl("get", "pods", "-A", "-o", "json", check=False)
        (evidence / "final-pods.json").write_text(result.stdout)


if __name__ == "__main__":
    main()
