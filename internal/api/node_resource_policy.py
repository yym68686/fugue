"""Node-local resource policy execution. No serving configuration or restarts."""
import ctypes
import json
import os
from pathlib import Path
import subprocess
import tempfile

POLICY_PATH = Path("/etc/fugue/node-resource-policy.json")
SYSCTL_PATH = Path("/etc/sysctl.d/90-fugue-inotify.conf")
PROC = Path("/proc")
FIELDS = {"schema", "enabled", "min_instances", "max_instances", "instances_per_container", "base_instances", "headroom_percent", "min_watches", "max_watches", "watches_per_container", "warning_percent"}


def validate(policy):
    if set(policy) != FIELDS or policy["schema"] != "fugue.node-resource-policy/v1" or type(policy["enabled"]) is not bool:
        raise ValueError("invalid policy schema or fields")
    for key in FIELDS - {"schema", "enabled"}:
        if type(policy[key]) is not int:
            raise ValueError("policy field %s must be an integer" % key)
    bounds = {"min_instances": (128, 65536), "max_instances": (policy["min_instances"], 65536), "instances_per_container": (1, 64), "base_instances": (64, 4096), "headroom_percent": (25, 400), "min_watches": (8192, 16777216), "max_watches": (policy["min_watches"], 16777216), "watches_per_container": (1, 65536), "warning_percent": (50, 95)}
    if any(not low <= policy[key] <= high for key, (low, high) in bounds.items()):
        raise ValueError("node resource policy outside allowed bounds")
    return policy


def allocation_probe():
    """Prove both a fresh instance and a filesystem watch can be allocated."""
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        create = libc.inotify_init1
        create.argtypes, create.restype = [ctypes.c_int], ctypes.c_int
        watch = libc.inotify_add_watch
        watch.argtypes, watch.restype = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32], ctypes.c_int
        fd = create(os.O_CLOEXEC | os.O_NONBLOCK)
        if fd < 0:
            code = ctypes.get_errno()
            return {"status": "fail", "errno": code, "error": os.strerror(code)}
        try:
            with tempfile.TemporaryDirectory(prefix="fugue-inotify-") as directory:
                if watch(fd, os.fsencode(directory), 0x100) < 0:  # IN_CREATE
                    code = ctypes.get_errno()
                    return {"status": "fail", "errno": code, "error": os.strerror(code)}
        finally:
            os.close(fd)
        return {"status": "pass", "uid": os.getuid(), "instance_created": True, "watch_created": True}
    except (OSError, AttributeError) as exc:
        return {"status": "warning", "observation_state": "not_observed", "error": str(exc)}


def inventory(proc=PROC):
    by_uid, unreadable = {}, 0
    for process in proc.iterdir():
        if not process.name.isdigit():
            continue
        try:
            uid = process.stat().st_uid
            for descriptor in (process / "fd").iterdir():
                try:
                    if "inotify" not in os.readlink(descriptor):
                        continue
                    item = by_uid.setdefault(str(uid), {"instances_upper_bound": 0, "watches_upper_bound": 0})
                    item["instances_upper_bound"] += 1
                    item["watches_upper_bound"] += sum(line.startswith("inotify ") for line in (process / "fdinfo" / descriptor.name).read_text().splitlines())
                except FileNotFoundError:
                    pass  # Process/descriptor closed during the observation.
                except PermissionError:
                    unreadable += 1
        except FileNotFoundError:
            pass
        except PermissionError:
            unreadable += 1
    # Inherited/duplicated descriptors can overcount. Never present these as
    # exact kernel instance counts, nor use them to lower limits.
    return {"by_uid_upper_bounds": by_uid, "unreadable": unreadable}


def limits(proc=PROC):
    return {key: int((proc / "sys/fs/inotify" / key).read_text()) for key in ("max_user_instances", "max_user_watches")}


def planned_limits(policy, containers, facts, current):
    peak_instances = max((row["instances_upper_bound"] for row in facts["by_uid_upper_bounds"].values()), default=0)
    peak_watches = max((row["watches_upper_bound"] for row in facts["by_uid_upper_bounds"].values()), default=0)
    reserve = lambda value: (value * (100 + policy["headroom_percent"]) + 99) // 100
    required = {
        "max_user_instances": max(policy["min_instances"], reserve(policy["base_instances"] + containers * policy["instances_per_container"]), reserve(peak_instances)),
        "max_user_watches": max(policy["min_watches"], reserve(containers * policy["watches_per_container"]), reserve(peak_watches)),
    }
    caps = {"max_user_instances": policy["max_instances"], "max_user_watches": policy["max_watches"]}
    desired = {key: max(current[key], min(value, caps[key])) for key, value in required.items()}
    return desired, {key: value for key, value in required.items() if value > desired[key]}


def atomic_write(path, content):
    if path.exists() and path.read_text() == content:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, staged = tempfile.mkstemp(prefix="." + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, "w") as output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(staged, 0o644)
        os.replace(staged, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(staged):
            os.unlink(staged)


def container_density(proc=PROC):
    try:
        cp = subprocess.run(["crictl", "ps", "--quiet"], text=True, capture_output=True, timeout=8, check=True)
        return len(cp.stdout.split())
    except FileNotFoundError:
        # Standalone edge hosts may have containerd without a CRI CLI. The
        # runtime shim count is a conservative density signal on these hosts.
        count = 0
        for process in proc.iterdir():
            if process.name.isdigit():
                try:
                    count += process.joinpath("comm").read_text().strip().startswith("containerd-shim")
                except FileNotFoundError:
                    pass
        return count


def reconcile(policy, dry_run=False, persist=False):
    policy = validate(policy)
    if not policy["enabled"]:
        if persist and not dry_run:
            atomic_write(POLICY_PATH, json.dumps(policy, sort_keys=True) + "\n")
        return {"state": "disabled", "limits_preserved": True}
    containers = container_density()
    current = limits()
    desired, capped = planned_limits(policy, containers, inventory(), current)
    result = {"state": "dry_run" if dry_run else "converged", "containers": containers, "before": current, "desired": desired, "capacity_unmet": capped}
    if dry_run:
        return result
    # Commit validated intent first. If a later step fails it remains available
    # for the next reconcile even when the control plane is disconnected.
    if persist:
        atomic_write(POLICY_PATH, json.dumps(policy, sort_keys=True) + "\n")
    atomic_write(SYSCTL_PATH, "# Managed by Fugue node resource policy; limits are never automatically lowered.\n" + "".join("fs.inotify.%s = %d\n" % item for item in sorted(desired.items())))
    for key, value in desired.items():
        if value > current[key]:
            (PROC / "sys/fs/inotify" / key).write_text(str(value) + "\n")
    result["effective"] = limits()
    result["probe"] = allocation_probe()
    if result["effective"] != desired or result["probe"]["status"] != "pass":
        raise RuntimeError("inotify policy verification failed: " + json.dumps(result))
    return result


def observation():
    result = {"probe": allocation_probe()}
    try:
        result.update(inventory())
        result["limits"] = limits()
        policy = validate(json.loads(POLICY_PATH.read_text()))
        result["warning_percent"] = policy["warning_percent"]
        result["policy_enabled"] = policy["enabled"]
        result["desired"], result["capacity_unmet"] = planned_limits(policy, container_density(), result, result["limits"])
        result["status"] = result["probe"]["status"]
        if (result["capacity_unmet"] or result["desired"] != result["limits"]) and result["status"] == "pass":
            result["status"] = "warning"
        for row in result["by_uid_upper_bounds"].values():
            for key, limit in (("instances_upper_bound", "max_user_instances"), ("watches_upper_bound", "max_user_watches")):
                if row[key] * 100 >= result["limits"][limit] * policy["warning_percent"] and result["status"] == "pass":
                    result["status"] = "warning"
        if result["unreadable"] and result["status"] == "pass":
            result["status"] = "warning"
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        result["status"] = "fail" if result["probe"]["status"] == "fail" else "warning"
        result["observation_state"] = "partial"
        result["error"] = str(exc)
    return result


def main(mode):
    if mode == "observe":
        return observation()
    if mode == "task":
        policy = json.loads(os.environ.get("FUGUE_NODE_UPDATE_TASK_POLICY_JSON", ""))
        return reconcile(policy, dry_run=os.environ.get("FUGUE_NODE_UPDATE_TASK_DRY_RUN", "true") != "false", persist=True)
    if mode == "reconcile":
        if not POLICY_PATH.exists():
            return {"state": "not_configured"}
        return reconcile(json.loads(POLICY_PATH.read_text()))
    raise ValueError("unknown resource policy operation")


if __name__ == "__main__":
    import sys
    try:
        print(json.dumps(main(sys.argv[1]), separators=(",", ":")))
    except Exception as exc:
        print(json.dumps({"state": "failed", "error": str(exc)}, separators=(",", ":")))
        sys.exit(1)
