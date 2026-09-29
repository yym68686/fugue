"""Create-only independent generation installer, embedded in Fugue CLI.

No stop/restart/reload, DNS write, connection flush or timeout-based retirement.
All paths and bytes are pinned by a private immutable deployment receipt.
"""
import base64
import hashlib
import fcntl
import json
import os
import pathlib
import pwd
import re
import socket
import subprocess
import sys


def require(ok, message):
    if not ok:
        raise ValueError(message)


def command(*args):
    p = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=40)
    require(p.returncode == 0, "local prerequisite or candidate validation failed")
    return p.stdout.decode().strip()


def sha(data):
    return hashlib.sha256(data).hexdigest()


def no_links(path):
    for p in [path, *path.parents]:
        require(not p.is_symlink(), "symlink in generation path")


def put(path, raw, uid, gid, mode):
    no_links(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    if path.exists():
        require(path.is_file() and path.read_bytes() == raw, "existing generation asset differs")
        return
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, mode)
    with os.fdopen(fd, "wb") as f:
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    os.chown(path, uid, gid)


def fence(plan):
    for f in plan.get("preserve") or []:
        require(command("systemctl", "show", f["unit"], "--property=MainPID", "--value") == str(f["pid"]), "predecessor process changed")
        require(sha(pathlib.Path(f["config"]).read_bytes()) == f["sha256"], "predecessor configuration changed")


def unit_text(root, binary, config, uid, memory, cpu):
    extra = " run" if binary == "caddy" else ""
    cap = "AmbientCapabilities=CAP_NET_BIND_SERVICE\nCapabilityBoundingSet=CAP_NET_BIND_SERVICE\n" if binary == "caddy" else "CapabilityBoundingSet=\n"
    return ("[Unit]\nDescription=Independent static serving generation\nAfter=network-online.target\nWants=network-online.target\n\n"
            "[Service]\nType=simple\nUser=" + uid + "\nGroup=" + uid + "\n"
            "ExecStart=" + str(root / binary) + extra + " --config " + str(root / config) + "\n"
            "Restart=on-failure\nRestartSec=5s\nTimeoutStopSec=infinity\nSendSIGKILL=no\n"
            "NoNewPrivileges=true\nPrivateTmp=true\nProtectSystem=full\nProtectHome=true\n"
            "MemoryMax=" + memory + "\nCPUQuota=" + cpu + "\nTasksMax=256\nNice=5\n"
            "Environment=GOMAXPROCS=2\nEnvironment=HOME=" + str(root / "storage") + "\n"
            + cap + "\n[Install]\nWantedBy=multi-user.target\n").encode()


def install(v):
    p = v["plan"]
    require(os.getuid() == 0 and sys.platform == "linux", "root Linux target required")
    require(re.fullmatch(r"[a-z][a-z0-9-]{0,31}", p["id"]), "invalid generation id")
    root = pathlib.Path("/opt/fugue-static-generations") / p["id"]
    require(v["root"] == str(root), "generation root mismatch")
    no_links(root)
    try:
        account = pwd.getpwnam("caddy")
    except KeyError:
        command("useradd", "--system", "--home", "/var/lib/caddy", "--create-home", "caddy")
        account = pwd.getpwnam("caddy")
    fence(p)
    declared = sorted(p["listeners"])
    servers = v["caddy"].get("apps", {}).get("http", {}).get("servers", {})
    actual = sorted(a for s in servers.values() for a in s.get("listen", []))
    require(actual == declared, "listeners differ from reviewed plan")
    require(v["caddy"]["apps"]["http"].get("grace_period", 0) == 0, "new generation must use unlimited drain grace")
    require(all(s.get("automatic_https", {}).get("disable") or s.get("automatic_https", {}).get("disable_redirects") for s in servers.values()), "implicit redirect listener is not permitted")
    units = [("observer", "fugue-static-edge-observer", "observer.json", "caddy", "192M", "20%"),
             ("caddy", "caddy", "caddy.json", "caddy", "512M", "100%"),
             ("manager", "fugue-static-edge-manager", "manager.json", "root", "192M", "20%")]
    unit_names = ["fugue-sg-" + p["id"] + "-" + role + ".service" for role, *_ in units]
    receipt = root / "install.json"
    fingerprint = sha(json.dumps(v, sort_keys=True, separators=(",", ":")).encode())
    new = not receipt.exists()
    if not new:
        require(json.loads(receipt.read_text())["input_sha256"] == fingerprint, "generation intent differs; use another id")
    if new:
        require(not root.exists(), "unowned generation directory exists")
        for unit in unit_names:
            require(not pathlib.Path("/etc/systemd/system", unit).exists(), "unit already owned")
        # Test TCP and UDP binds, without SO_REUSEPORT. Wildcard conflicts fail.
        for address in declared + [p["management_listen"]]:
            host, port = address.rsplit(":", 1)
            host = host.strip("[]")
            family = socket.AF_INET6 if ":" in host else socket.AF_INET
            for kind in [socket.SOCK_STREAM, socket.SOCK_DGRAM]:
                with socket.socket(family, kind) as s:
                    s.bind((host, int(port)))
        root.mkdir(parents=True, mode=0o750)
        os.chown(root, 0, account.pw_gid)
        put(receipt, (json.dumps({"input_sha256": fingerprint, "source_commit": p["source_commit"]}) + "\n").encode(), 0, 0, 0o600)
    for directory in ["run", "evidence", "storage"]:
        d = root / directory
        no_links(d)
        d.mkdir(exist_ok=True, mode=0o700)
        os.chown(d, account.pw_uid, account.pw_gid)
    (root / "manager-state").mkdir(exist_ok=True, mode=0o700)
    require(set(v["files"]) == {"caddy", "fugue-static-edge-manager", "fugue-static-edge-observer", "provenance.json"}, "unexpected executables")
    proof = json.loads(base64.b64decode(v["files"]["provenance.json"], validate=True))
    require(proof["source_commit"] == p["source_commit"], "source provenance differs")
    for name, encoded in v["files"].items():
        raw = base64.b64decode(encoded, validate=True)
        require(name == "provenance.json" or sha(raw) == proof["files"][name], "binary digest differs")
        put(root / name, raw, 0, account.pw_gid, 0o750)
    for name, encoded in v["assets"].items():
        require(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_./-]{0,100}", name) and ".." not in name, "invalid asset path")
        raw = base64.b64decode(encoded, validate=True)
        require(len(raw) <= 4 * 1024 * 1024, "asset too large")
        certificate_seed = name.startswith("storage/certificates/")
        require(name.split("/")[0] not in ["run", "manager-state", "evidence"] and (name.split("/")[0]!="storage" or certificate_seed) and name not in v["files"] and name not in ["install.json", "manager.json", "observer.json", "caddy.json", "manager-rpc"], "reserved asset path")
        management = name.startswith("management/")
        put(root / name, raw, account.pw_uid if certificate_seed else 0, 0 if management else account.pw_gid, 0o600 if certificate_seed or management else 0o640)
        # The service identity must traverse each private asset directory.
        for parent in (root / name).parents:
            if parent == root: break
            os.chown(parent, account.pw_uid if certificate_seed else 0, 0 if management else account.pw_gid)
            if management: os.chmod(parent, 0o700)
    for name in ["caddy", "manager", "observer"]:
        put(root / (name + ".json"), (json.dumps(v[name], separators=(",", ":")) + "\n").encode(), 0, account.pw_gid, 0o640 if name != "manager" else 0o600)
    wrapper = '#!/bin/sh\nset -eu\n[ "$#" = 1 ] && [ "$1" = ssh-rpc ]\nexec ' + str(root / "fugue-static-edge-manager") + ' ssh-rpc --socket ' + str(root / "run/manager.sock") + '\n'
    put(root / "manager-rpc", wrapper.encode(), 0, 0, 0o700)
    command("runuser", "-u", "caddy", "--", str(root / "caddy"), "validate", "--config", str(root / "caddy.json"))
    for unit, (_, binary, config, user, memory, cpu) in zip(unit_names, units):
        put(pathlib.Path("/etc/systemd/system", unit), unit_text(root, binary, config, user, memory, cpu), 0, 0, 0o644)
    command("systemctl", "daemon-reload")
    for unit in unit_names:
        # start is idempotent for an already running generation; never restart.
        command("systemctl", "enable", "--now", unit)
        require(command("systemctl", "show", unit, "--property=ActiveState", "--value") == "active", "new generation service is not active")
    fence(p)
    return {"phase": "generation_started_old_retained", "id": p["id"], "source_commit": p["source_commit"], "input_sha256": fingerprint, "units": unit_names, "root": str(root), "dns_changed": False, "predecessors_retained": True, "health_verified": False}


if __name__ == "__main__":
    try:
        # Serializes all generations on this host, including distinct ids.
        fd = os.open("/run/fugue-static-generation.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raw = sys.stdin.buffer.read(128 * 1024 * 1024 + 1)
        require(len(raw) <= 128 * 1024 * 1024, "envelope too large")
        print(json.dumps(install(json.loads(raw))))
    except Exception as error:
        # Never emit validation stderr, configs, asset data, or credentials.
        print(json.dumps({"error": type(error).__name__, "phase": "generation_staging_incomplete", "detail": str(error) if isinstance(error, ValueError) else "local prerequisite failed", "predecessors_retained": True}))
        sys.exit(1)
