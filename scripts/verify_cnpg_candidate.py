#!/usr/bin/env python3
"""Verify an unpublished-to-production controller artifact on an isolated CI host."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile


BASE = "ghcr.io/cloudnative-pg/cloudnative-pg@sha256:68074486205a33ed41928761e22ad48278c690feebe8316727a1c6b3380f9e5e"
EXPECTED_MANAGERS = {
    "manager_amd64": "b77f8f908fdc909122fe24fa164f8247c0689c4ca8c01ccef07fc50e7be8f6f1",
    "manager_arm64": "97fa01dc1c6abcd7f64a51d16f2e4ef9c7bb8d4d842e01adc9d07060e6b84157",
}


def output(*args):
    return subprocess.check_output(args, timeout=180).decode().strip()


def copied_hashes(image, root):
    cid = output("docker", "create", "--network=none", image)
    try:
        subprocess.run(["docker", "cp", cid + ":/operator", str(root)], check=True, timeout=120)
        files = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in root.glob("manager_*")}
        return files
    finally:
        subprocess.run(["docker", "rm", cid], check=True, stdout=sys.stderr, timeout=30)


def verify(image, source):
    if not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image) or not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("candidate requires a full immutable image digest and Git revision")
    for ref in (BASE, image):
        subprocess.run(["docker", "pull", ref], check=True, stdout=sys.stderr, timeout=180)
    metadata = json.loads(output("docker", "image", "inspect", image))[0]
    if metadata["Config"].get("Labels", {}).get("org.opencontainers.image.revision") != source:
        raise ValueError("candidate provenance mismatch")
    if metadata["Config"]["Entrypoint"] != ["/fugue-cnpg-controller"] or metadata["Config"]["User"] != "65532:65532":
        raise ValueError("candidate command or user mismatch")
    with tempfile.TemporaryDirectory(prefix="cnpg-candidate-") as tmp:
        original = copied_hashes(BASE, Path(tmp) / "original")
        candidate = copied_hashes(image, Path(tmp) / "candidate")
    if original != EXPECTED_MANAGERS or candidate != original:
        raise ValueError("instance-manager binary identity changed")
    help_text = output("docker", "run", "--rm", "--network=none", "--read-only", "--cap-drop=ALL", image, "controller", "--help")
    if "--leader-elect" not in help_text or "--max-concurrent-reconciles" not in help_text:
        raise ValueError("candidate controller command unavailable")
    return {"candidate_image": image, "source_revision": source, "base_image": BASE,
            "instance_manager_sha256": candidate, "controller_command_verified": True,
            "production_deployed": False}


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: verify_cnpg_candidate.py IMAGE@DIGEST GIT_REVISION")
    print(json.dumps(verify(*sys.argv[1:]), sort_keys=True))
