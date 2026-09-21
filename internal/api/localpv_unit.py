"""Reconcile Fugue's boot implementation without operating on live storage."""

import argparse
import configparser
import datetime
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import stat
import subprocess
import tempfile


UNIT = "fugue-lvm-localpv-loop.service"
REVISION = "localpv-boot-v1"


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def validate_intent(image, vg):
    # These parameters are literal systemd/shell arguments, never shell code.
    if not re.fullmatch(r"/[A-Za-z0-9_./-]+", image) or ".." in Path(image).parts:
        raise ValueError("unsupported storage image path")
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.+-]*", vg):
        raise ValueError("unsupported volume group name")


def boot_command(image, vg):
    validate_intent(image, vg)
    return (
        f'image={image}; association=$(losetup -j "$image"); '
        'if [ -z "$association" ]; then '
        'losetup --find --show --nooverlap "$image" >/dev/null; fi; '
        f"vgchange -ay {vg}"
    )


def render(image, vg, legacy=None):
    # systemd expands dollars before invoking the shell; retain shell variables.
    command = boot_command(image, vg).replace("$", "$$")
    if legacy is not None:
        query = ">/dev/null" if legacy else "| grep -q ."
        command = f"losetup -j {image} {query} || losetup --find --show {image} >/dev/null; vgchange -ay {vg}"
    return (
        "[Unit]\n"
        "Description=Attach Fugue LVM LocalPV loopback volume group\n"
        "DefaultDependencies=no\n"
        "After=local-fs.target\n"
        "Before=k3s.service k3s-agent.service kubelet.service\n\n"
        "[Service]\n"
        "Type=oneshot\n"
        "RemainAfterExit=yes\n"
        f"ExecStart=/bin/sh -ec '{command}'\n"
        f"ExecStop=/bin/sh -ec 'vgchange -an {vg} || true'\n\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )


def parse_unit(raw):
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    parser.optionxform = str
    try:
        parser.read_file(io.StringIO(raw))
    except configparser.Error:
        raise ValueError("invalid or ambiguous systemd unit format") from None
    if parser.defaults():
        raise ValueError("unit defaults are not supported")
    return {section: dict(parser.items(section)) for section in parser.sections()}


def retained_intent(raw):
    parsed = parse_unit(raw)
    args = shlex.split(parsed.get("Service", {}).get("ExecStart", ""))
    if len(args) != 3 or args[:2] != ["/bin/sh", "-ec"]:
        raise ValueError("unrecognized boot command")
    command = args[2]
    match = re.fullmatch(r"losetup -j (\S+) .+; vgchange -ay (\S+)", command)
    if match is None:
        match = re.fullmatch(r"image=(\S+); association=.+; vgchange -ay (\S+)", command)
    if match is None:
        raise ValueError("storage intent cannot be extracted from known unit")
    image, vg = match.groups()
    validate_intent(image, vg)
    if parsed not in [parse_unit(render(image, vg, old)) for old in (None, True, False)]:
        raise ValueError("unit contains unrecognized customizations")
    return image, vg


def run(argv):
    result = subprocess.run(argv, text=True, capture_output=True, timeout=15)
    if result.returncode:
        raise RuntimeError(f"{argv[0]} {argv[1]} failed (exit {result.returncode})")
    return result.stdout.strip()


def properties(unit):
    keys = ["LoadState", "FragmentPath", "DropInPaths", "NeedDaemonReload", "ExecStart", "UnitFileState", "ActiveState", "Result"]
    raw = run(["systemctl", "show", unit, "--no-pager", "--property=" + ",".join(keys)])
    props = dict(line.split("=", 1) for line in raw.splitlines() if "=" in line)
    if any(key not in props for key in keys):
        raise ValueError("systemd observation is incomplete")
    return props


def effective_command(props):
    # systemctl exposes an ExecCommand record, including volatile process facts.
    # Compare the single argv record, not timestamps or an arbitrary substring.
    match = re.fullmatch(r"\{ path=/bin/sh ; argv\[\]=/bin/sh -ec (.*?) ; ignore_errors=no ; [^{}]*\}", props["ExecStart"])
    if match is None:
        raise ValueError("systemd loaded an unrecognized ExecStart")
    return match.group(1)


def read_regular(path):
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o022:
        raise ValueError("unit must be a regular file without group/world write access")
    return path.read_text(), metadata


def atomic_write(path, raw, mode=0o644):
    fd, name = tempfile.mkstemp(prefix=".fugue-localpv-", suffix=".service", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as target:
            os.fchmod(target.fileno(), mode)
            target.write(raw)
            target.flush()
            os.fsync(target.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def snapshot_evidence(props, raw, wanted):
    return {
        "revision": REVISION,
        "desired_sha256": digest(wanted),
        "file_sha256": digest(raw),
        "effective_sha256": digest(effective_command(props)),
        "drop_in_paths": props["DropInPaths"],
        "unit_file_state": props["UnitFileState"],
        "active_state": props["ActiveState"],
        "service_result": props["Result"],
        "need_daemon_reload": props["NeedDaemonReload"],
    }


def reconcile(path, mode):
    result = {"state": "unknown", "changed": "false", "revision": REVISION}
    if mode not in ("apply", "observe"):
        raise ValueError("LocalPV unit mode must be apply or observe")
    if not path.exists() and not path.is_symlink():
        result.update(state="not_applicable", reason="no installed LocalPV loop unit")
        return result
    raw, metadata = read_regular(path)
    image, vg = retained_intent(raw)
    wanted = render(image, vg)
    props = properties(path.name)
    if props["LoadState"] != "loaded" or props["FragmentPath"] != str(path):
        raise ValueError("unit is masked or systemd loaded a different fragment")
    result.update(snapshot_evidence(props, raw, wanted), image_path=image, vg_name=vg)
    if props["DropInPaths"]:
        result.update(state="override_conflict", reason="systemd drop-ins require explicit operator review; preserved")
        return result
    needs_update = raw != wanted
    expected_command = boot_command(image, vg).replace("$", "$$")
    loaded_matches = effective_command(props) == expected_command
    needs_reload = props["NeedDaemonReload"] != "no" or not loaded_matches
    if mode == "observe" and (needs_update or needs_reload):
        result.update(state="drift", reason="observe mode preserves installed and loaded configuration")
        return result
    if needs_update or needs_reload:
        fd, candidate = tempfile.mkstemp(prefix=".fugue-localpv-candidate-", suffix=".service", dir=path.parent)
        replaced = False
        try:
            with os.fdopen(fd, "w") as target:
                os.fchmod(target.fileno(), 0o644)
                target.write(wanted)
                target.flush()
                os.fsync(target.fileno())
            run(["systemd-analyze", "verify", candidate])
            current, current_metadata = read_regular(path)
            latest = properties(path.name)
            if current != raw or current_metadata.st_ino != metadata.st_ino or latest != props:
                raise ValueError("unit changed during reconciliation; retry on next observation")
            if needs_update:
                atomic_write(path.with_name(path.name + ".fugue-previous"), raw, stat.S_IMODE(metadata.st_mode))
                os.replace(candidate, path)
                replaced = True
            run(["systemctl", "daemon-reload"])
            loaded = properties(path.name)
            if (loaded["LoadState"] != "loaded" or loaded["FragmentPath"] != str(path)
                    or loaded["DropInPaths"] or loaded["NeedDaemonReload"] != "no"
                    or effective_command(loaded) != expected_command
                    or read_regular(path)[0] != wanted):
                raise RuntimeError("installed LocalPV unit did not become effective")
            props = loaded
            raw = wanted
            result["changed"] = "true"
        except Exception:
            if replaced:
                # Do not overwrite an operator edit made after our atomic install.
                if read_regular(path)[0] != wanted:
                    raise RuntimeError("verification failed; concurrent edit prevents rollback")
                atomic_write(path, raw, stat.S_IMODE(metadata.st_mode))
                run(["systemctl", "daemon-reload"])
            raise
        finally:
            if os.path.exists(candidate):
                os.unlink(candidate)
    result.update(snapshot_evidence(props, raw, wanted))
    healthy = props["UnitFileState"] == "enabled" and props["ActiveState"] == "active" and props["Result"] == "success"
    result.update(
        state="converged" if healthy else "runtime_unready",
        reason="boot implementation verified; storage was not restarted" if healthy
        else "unit content converged but enablement or runtime state is not healthy; storage was not started",
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    rendering = sub.add_parser("render")
    rendering.add_argument("image")
    rendering.add_argument("vg")
    reconciling = sub.add_parser("reconcile")
    reconciling.add_argument("--unit-path", default="/etc/systemd/system/" + UNIT)
    reconciling.add_argument("--mode", default="apply")
    reconciling.add_argument("--lock-path", default="/run/lock/fugue-localpv-unit.lock")
    args = parser.parse_args()
    if args.command == "render":
        print(render(args.image, args.vg), end="")
        return
    result = {"state": "failed", "changed": "false", "revision": REVISION}
    try:
        with open(args.lock_path, "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            result = reconcile(Path(args.unit_path), args.mode)
    except BlockingIOError:
        result.update(state="busy", reason="another LocalPV unit reconciliation holds the lock")
    except Exception as exc:
        result.update(reason=str(exc))
    result["observed_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
