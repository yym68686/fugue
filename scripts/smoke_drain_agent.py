#!/usr/bin/env python3
"""Exercise a pinned drain observer image against real host TCP connections."""

import json
import os
import re
import secrets
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request


def run(image, source):
    if not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("smoke test requires an immutable image digest")
    if not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("smoke test requires the source commit")
    subprocess.run(["docker", "pull", image], check=True, stdout=sys.stderr)
    metadata = json.loads(subprocess.check_output(["docker", "image", "inspect", image]))[0]
    if metadata["Config"]["Labels"]["org.opencontainers.image.revision"] != source:
        raise ValueError("image source differs from release commit")
    name = "fugue-drain-smoke-" + str(os.getpid())
    with socket.socket() as listener, socket.socket() as reserved:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        app_port = listener.getsockname()[1]
        reserved.bind(("127.0.0.1", 0))
        agent_port = reserved.getsockname()[1]
        reserved.close()
        try:
            subprocess.run(["docker", "run", "--detach", "--rm", "--network", "host", "--name", name,
                            "-e", f"FUGUE_DRAIN_AGENT_BIND_ADDR=127.0.0.1:{agent_port}",
                            "-e", f"FUGUE_DRAIN_APP_PORTS={app_port}", "-e", "POD_NAME=probe-agent",
                            "-e", "POD_NAMESPACE=probe-namespace", image], check=True, stdout=sys.stderr)
            base = f"http://127.0.0.1:{agent_port}"

            def observe():
                nonce = secrets.token_hex(16)
                with urllib.request.urlopen(base + "/drain/observe?nonce=" + nonce, timeout=2) as response:
                    result = json.load(response)
                    assert response.headers["Cache-Control"] == "no-store"
                assert result["api_version"] == "fugue.drain-observation/v1"
                assert result["nonce"] == nonce and result["pod"] == "probe-agent"
                assert result["namespace"] == "probe-namespace" and result["app_ports"] == [app_port]
                assert isinstance(result["active_connections"], int) and result["active_connections"] >= 0
                return result

            deadline = time.monotonic() + 30
            while True:
                try:
                    first = observe()
                    break
                except (OSError, urllib.error.URLError):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.2)
            assert first["active_connections"] == 0
            with socket.create_connection(listener.getsockname(), timeout=2) as connection:
                accepted, _ = listener.accept()
                with accepted:
                    busy = observe()
                    assert busy["active_connections"] >= 1
                    connection.sendall(b"still-open")
                    assert accepted.recv(64) == b"still-open"
            deadline = time.monotonic() + 5
            while True:
                idle = observe()
                if idle["active_connections"] == 0:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("closed connection did not become idle")
                time.sleep(0.2)
            with urllib.request.urlopen(base + "/metrics", timeout=2) as response:
                metrics = response.read().decode()
            assert "fugue_app_drain_prestop_requests_total 0" in metrics
            return {"image": image, "source_commit": source, "idle_before": first,
                    "busy": busy, "idle_after": idle, "prestop_unchanged": True}
        finally:
            subprocess.run(["docker", "rm", "--force", name], stdout=sys.stderr, stderr=sys.stderr, check=False)


if __name__ == "__main__":
    print(json.dumps(run(sys.argv[1], sys.argv[2]), indent=2))
