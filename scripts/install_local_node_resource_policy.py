#!/usr/bin/env python3
"""CI-only executor installation on the declared control-plane host."""
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys


def main():
    config_path = Path(sys.argv[1]).resolve()
    config = json.loads(config_path.read_text())
    if os.geteuid() != 0 or socket.gethostname() != config["local_node"]:
        raise ValueError("local resource policy installer must run as root on the declared host")
    source = Path(__file__).resolve().parents[1] / "internal/api/node_resource_policy.py"
    spec = importlib.util.spec_from_file_location("resource_executor", source)
    executor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(executor)
    policy = executor.validate(json.loads((config_path.parent / config["policy_file"]).read_text()))
    plan = executor.reconcile(policy, dry_run=True)
    print(json.dumps({"node": socket.gethostname(), "plan": plan}))
    if "--apply" not in sys.argv:
        return
    # Publish validated configuration independently of future executor releases.
    result = executor.reconcile(policy, persist=True)
    destination = Path("/usr/local/lib/fugue/node-resource-policy.py")
    executor.atomic_write(destination, source.read_text())
    executor.atomic_write(Path("/etc/systemd/system/fugue-node-resource-policy.service"), """[Unit]
Description=Reconcile persisted Fugue node resource policy
After=local-fs.target
[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /usr/local/lib/fugue/node-resource-policy.py reconcile
TimeoutStartSec=45
""")
    executor.atomic_write(Path("/etc/systemd/system/fugue-node-resource-policy.timer"), """[Unit]
Description=Reconcile node resource policy without control-plane connectivity
[Timer]
OnBootSec=60s
OnUnitActiveSec=5min
[Install]
WantedBy=timers.target
""")
    subprocess.run(["systemctl", "daemon-reload"], check=True, timeout=30)
    subprocess.run(["systemctl", "enable", "--now", "fugue-node-resource-policy.timer"], check=True, timeout=30)
    print(json.dumps({"node": socket.gethostname(), "result": result, "observation": executor.observation()}))


if __name__ == "__main__":
    main()
