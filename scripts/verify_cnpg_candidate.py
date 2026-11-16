#!/usr/bin/env python3
"""Verify an unpublished-to-production controller artifact on an isolated CI host."""

import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile

try:
    from . import baked_postgres_contract as baked
except ImportError:
    import baked_postgres_contract as baked


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


def verify(image, source, catalog_path=None, baked_proof_path=None, smoke_path=None):
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
    receipt = {"candidate_image": image, "source_revision": source, "base_image": BASE,
               "instance_manager_sha256": candidate, "controller_command_verified": True,
               "production_deployed": False}
    if catalog_path:
        expected = baked.validate(json.loads(Path(catalog_path).read_text()))
        embedded = json.loads(output("docker", "run", "--rm", "--network=none", "--read-only", "--cap-drop=ALL", image,
                                     "fugue-instance-manager-contract"))
        if embedded != expected or embedded["managerSHA256"] != candidate:
            raise ValueError("controller does not embed the independently verified image catalog")
        receipt["baked_postgres_contract"] = embedded
    if baked_proof_path:
        proof = json.loads(Path(baked_proof_path).read_text())
        baked.build_catalog(proof, receipt)
        if proof["source_revision"] != source or proof["image"] != receipt["baked_postgres_contract"]["defaultImage"]:
            raise ValueError("baked operand is not bound to this source and catalog")
        receipt["baked_postgres_verification"] = proof
    if smoke_path:
        smoke = json.loads(Path(smoke_path).read_text())
        if (smoke.get("candidate_image") != image or smoke.get("source_revision") != source or
                smoke.get("baked_image") != receipt["baked_postgres_contract"]["defaultImage"] or
                smoke.get("checks") != ["default_initdb_without_bootstrap", "sql_read_write", "container_restart", "pod_recreation", "legacy_unchanged"]):
            raise ValueError("isolated cluster proof is incomplete or belongs to a different artifact")
        receipt["isolated_cluster_verification"] = smoke
    return receipt


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4, 5, 6):
        raise SystemExit("usage: verify_cnpg_candidate.py IMAGE@DIGEST GIT_REVISION [CATALOG [BAKED_PROOF [SMOKE_PROOF]]]")
    print(json.dumps(verify(*sys.argv[1:]), sort_keys=True))
