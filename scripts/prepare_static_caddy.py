#!/usr/bin/env python3
"""Reproduce patched dependencies from Go checksum-verified module sources.

Only files below static-caddy/.build are generated. No production operations.
"""
import json
import os
import pathlib
import shutil
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
DEST = ROOT / "static-caddy" / ".build"
SUMS = {"caddy": "h1:XKxkMTgNSizEvKG6QHue6cAsFOteU2qA61w2tKkCWi0=", "net": "h1:bcvxaJn3e1U6InsFWt1JUq1aSjnRxLzT2rtD2KfkDF8="}

def run(*args, **kw):
    return subprocess.check_output(args, text=True, **kw)

def apply_patches(target, patchdir):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("GIT_")}
    # A ceiling at cwd itself does not stop Git's upward discovery. Its parent
    # does, and copied Go dependency trees contain no local .git directory.
    env["GIT_CEILING_DIRECTORIES"] = str(target.resolve().parent)
    for patch in sorted(patchdir.glob("*.patch")):
        # Dependencies live inside this Git worktree. Repository-aware apply
        # can silently skip Git-format paths outside the current subdirectory.
        # Treat each copied dependency as a standalone tree and reject escapes.
        subprocess.run(["git", "apply", "--no-index", str(patch.resolve())],
                       cwd=target, env=env, check=True)

def dependency(module, version, name):
    # Ignore the local module replacements while downloading canonical inputs.
    env = dict(os.environ, GOTOOLCHAIN="go1.26.3", GO111MODULE="on")
    d = json.loads(run("go", "mod", "download", "-json", module + "@" + version,
                       cwd="/tmp", env=env))
    if d.get("Sum") != SUMS[name]:
        raise RuntimeError("dependency checksum mismatch: " + name)
    target = DEST / name
    if target.exists():
        shutil.rmtree(target)
    shutil.copytree(d["Dir"], target)
    for p in target.rglob("*"):
        p.chmod(0o755 if p.is_dir() else 0o644)
    target.chmod(0o755)
    return {"module": module, "version": version, "sum": d["Sum"], "go_mod_sum": d["GoModSum"]}

def main():
    DEST.mkdir(parents=True, exist_ok=True)
    provenance = [dependency("github.com/caddyserver/caddy/v2", "v2.11.4", "caddy"),
                  dependency("golang.org/x/net", "v0.55.0", "net")]
    for name in ("caddy", "net"):
        patchdir = ROOT / "static-caddy" / "patches" / name
        apply_patches(DEST/name, patchdir)
    (DEST / "provenance.json").write_text(json.dumps(provenance, indent=2)+"\n")
    print(json.dumps(provenance))

if __name__ == "__main__":
    main()
