#!/usr/bin/env python3
"""Verify both operand architectures, then approve only the immutable digest."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

try:
    from . import baked_postgres_contract as contract
except ImportError:
    import baked_postgres_contract as contract


def output(*args):
    return subprocess.check_output(args, timeout=300).decode().strip()


def run(*args):
    subprocess.run(args, check=True, timeout=300, stdout=sys.stderr)


def verify_startup(image):
    with tempfile.TemporaryDirectory(prefix="baked-manager-startup-") as tmp:
        os.chmod(tmp, 0o777)
        common = ["docker", "run", "--rm", "--network=none", "--read-only", "--cap-drop=ALL",
                  "--security-opt=no-new-privileges", "--mount", f"type=bind,src={tmp},dst=/controller"]
        entry = "/usr/local/bin/fugue-cnpg-manager"
        help_text = output(*common, "--entrypoint", entry, image, "--help")
        if "instance" not in help_text:
            raise ValueError("baked manager cannot execute")
        linked = output(*common, "--entrypoint", "/bin/sh", image, "-ec",
                        'test -L /controller/manager; readlink /controller/manager')
        if linked != "/usr/local/libexec/cnpg-manager":
            raise ValueError("startup copied or selected a different manager")
        # Emulate CNPG's already hash-verified atomic replacement. The native
        # download/hash-check/rename/re-exec implementation is unchanged.
        run(*common, "--entrypoint", "/bin/sh", image, "-ec",
            'cp /usr/local/libexec/cnpg-manager /controller/manager.new; chmod 755 /controller/manager.new; mv -f /controller/manager.new /controller/manager')
        output(*common, "--entrypoint", entry, image, "--help")
        output(*common, "--entrypoint", "/bin/sh", image, "-ec",
               'test ! -L /controller/manager; test -x /controller/manager')


def verify(image, revision):
    if not contract.immutable_image(image) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("baked image needs an immutable tagged digest and full revision")
    hashes = {}
    # Pull amd64 last so the isolated native execution cannot select arm64.
    for arch in ("arm64", "amd64"):
        platform = "linux/" + arch
        for ref in (contract.POSTGRES_IMAGE, image):
            run("docker", "pull", "--platform", platform, ref)
        original = json.loads(output("docker", "image", "inspect", contract.POSTGRES_IMAGE))[0]
        metadata = json.loads(output("docker", "image", "inspect", image))[0]
        if metadata["Architecture"] != arch or metadata["Os"] != "linux":
            raise ValueError("operand architecture mismatch")
        config = metadata["Config"]
        if config.get("Labels", {}).get("org.opencontainers.image.revision") != revision:
            raise ValueError("operand provenance mismatch")
        for field in ("User", "Entrypoint", "Cmd", "Env", "WorkingDir", "ExposedPorts", "Volumes", "Healthcheck"):
            if config.get(field) != original["Config"].get(field):
                raise ValueError("upstream PostgreSQL configuration changed: " + field)
        cid = output("docker", "create", "--platform", platform, "--network=none", image)
        try:
            with tempfile.TemporaryDirectory(prefix="baked-manager-binary-") as tmp:
                binary, entry = Path(tmp) / "manager", Path(tmp) / "entry"
                run("docker", "cp", cid + ":/usr/local/libexec/cnpg-manager", str(binary))
                run("docker", "cp", cid + ":/usr/local/bin/fugue-cnpg-manager", str(entry))
                hashes["manager_" + arch] = hashlib.sha256(binary.read_bytes()).hexdigest()
                if entry.read_bytes() != Path(__file__).with_name("postgres_baked_manager.sh").read_bytes():
                    raise ValueError("startup entrypoint identity differs")
        finally:
            run("docker", "rm", cid)
    if hashes != contract.MANAGER_SHA256:
        raise ValueError("preinstalled managers differ from the operator's original binaries")
    verify_startup(image)
    return {"image": image, "source_revision": revision, "postgres_base": contract.POSTGRES_IMAGE,
            "manager_image": contract.MANAGER_IMAGE, "manager_sha256": hashes,
            "platforms": ["linux/amd64", "linux/arm64"], "readonly_startup_verified": True, "upgrade_path_verified": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("image")
    parser.add_argument("revision")
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--previous-release", type=Path, required=True)
    args = parser.parse_args()
    proof = verify(args.image, args.revision)
    previous = json.loads(args.previous_release.read_text())["candidateReceipt"]
    catalog = contract.build_catalog(proof, previous)
    args.catalog.write_text(json.dumps(catalog, sort_keys=True, indent=2) + "\n")
    print(json.dumps(proof, sort_keys=True))
