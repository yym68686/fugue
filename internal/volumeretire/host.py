"""Identity-bound local Retain volume retirement. Never accepts a shell command."""

import contextlib
import fcntl
import hashlib
import json
import os
import re
import shutil
import signal
import socket
import stat
import sys
import tempfile
import time


def beneath(path, root):
    return path == root or path.startswith(root.rstrip("/") + "/")


def read(path):
    with open(path, encoding="utf-8") as stream:
        return stream.read().strip()


def identity():
    return {"hostname": socket.gethostname(), "machine_id": read("/etc/machine-id"),
            "boot_id": read("/proc/sys/kernel/random/boot_id")}


def validate_path(path, root):
    # One direct child of the explicitly selected storage root, never a broad
    # directory. Reject symlink parents as well as a linked target.
    if (not path.startswith("/") or not root.startswith("/") or
            os.path.normpath(path) != path or os.path.normpath(root) != root or
            len(root.split("/")) < 4 or os.path.dirname(path) != root or
            os.path.realpath(root) != root or os.path.realpath(path) != path):
        raise ValueError("unsafe storage root, target path or symlink")
    current = "/"
    for component in root.strip("/").split("/"):
        current = os.path.join(current, component)
        info = os.lstat(current)
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise ValueError("storage parent is not a real directory")
    if os.path.lexists(path):
        info = os.lstat(path)
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise ValueError("volume target is not a real directory")


def scan_tree(path):
    before = os.lstat(path)
    if not stat.S_ISDIR(before.st_mode):
        raise ValueError("target is not a directory")
    digest = hashlib.sha256()
    allocated = files = 0
    pending = [(path, "")]
    seen = set()
    while pending:
        current, relative = pending.pop()
        info = os.lstat(current)
        mode = info.st_mode
        if (info.st_dev != before.st_dev or stat.S_ISLNK(mode) or
                not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)) or
                stat.S_ISREG(mode) and info.st_nlink != 1):
            raise ValueError("linked, shared, special or cross-device entry in volume")
        key = (info.st_dev, info.st_ino)
        if key in seen:
            raise ValueError("duplicate inode in volume")
        seen.add(key)
        files += 1
        if files > 500000:
            raise ValueError("volume metadata scan exceeds safety bound")
        allocated += info.st_blocks * 512
        record = [relative, info.st_dev, info.st_ino, mode, info.st_size,
                  info.st_mtime_ns, info.st_ctime_ns, info.st_blocks]
        digest.update(json.dumps(record, separators=(",", ":")).encode())
        if stat.S_ISDIR(mode):
            with os.scandir(current) as entries:
                names = sorted(entry.name for entry in entries)
            pending.extend((os.path.join(current, name), relative + "/" + name)
                           for name in reversed(names))
        if metadata_version(os.lstat(current)) != metadata_version(info):
            raise ValueError("volume changed during metadata scan")
    if metadata_version(os.lstat(path)) != metadata_version(before):
        raise ValueError("volume root changed during metadata scan")
    return {"device": before.st_dev, "inode": before.st_ino,
            "allocated_bytes": allocated, "entry_count": files,
            "tree_sha256": digest.hexdigest()}


def metadata_version(info):
    # readdir may update atime during a strictly read-only preview.
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns, info.st_blocks)


def unescape_mount(value):
    return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), value)


def active_users(paths, proc="/proc"):
    """Inspect all visible mount namespaces, open files and cwd/root handles.

    Unknown permission errors fail closed. Only processes/descriptors that
    vanished during the read are ignored. A bind mount's filesystem root is
    checked as well as its mountpoint (container paths need not name the host).
    """
    blockers = set()
    namespaces = set()
    # A container bind mount exposes the filesystem-relative root, which need
    # not equal the host pathname when storage is on a separate filesystem.
    with open(proc + "/self/mountinfo", encoding="utf-8") as stream:
        host_mounts = [line.split() for line in stream]
    aliases = []
    for path in paths:
        candidates = [parts for parts in host_mounts if len(parts) >= 7 and
                      beneath(path, unescape_mount(parts[4]).rstrip("/") or "/")]
        if not candidates:
            raise ValueError("cannot resolve volume filesystem mount")
        mount = max(candidates, key=lambda parts: len(parts[4]))
        mountpoint = unescape_mount(mount[4])
        relative = os.path.relpath(path, mountpoint)
        aliases.append((mount[2], os.path.normpath(os.path.join(unescape_mount(mount[3]), relative))))
    for pid in os.listdir(proc):
        if not pid.isdigit():
            continue
        base = os.path.join(proc, pid)
        try:
            namespace = os.stat(base + "/ns/mnt").st_ino
            if namespace not in namespaces:
                namespaces.add(namespace)
                with open(base + "/mountinfo", encoding="utf-8") as stream:
                    for line in stream:
                        parts = line.split()
                        if len(parts) < 7 or "-" not in parts:
                            raise ValueError("invalid process mount table")
                        mount_root, mount_point = map(unescape_mount, parts[3:5])
                        if (any(beneath(mount_root, p) or beneath(mount_point, p) for p in paths) or
                                any(parts[2] == device and beneath(mount_root, relative)
                                    for device, relative in aliases)):
                            blockers.add("mounted_volume")
            for name in ["cwd", "root", "exe"]:
                try:
                    target = os.readlink(base + "/" + name).removesuffix(" (deleted)")
                    if any(beneath(target, p) for p in paths):
                        blockers.add("process_reference")
                except FileNotFoundError:
                    pass
            for name in os.listdir(base + "/fd"):
                try:
                    target = os.readlink(base + "/fd/" + name).removesuffix(" (deleted)")
                    if any(beneath(target, p) for p in paths):
                        blockers.add("open_file")
                except FileNotFoundError:
                    pass
        except (FileNotFoundError, ProcessLookupError):
            pass
    return sorted(blockers)


def filesystem(path):
    info = os.statvfs(path)
    return {"available_bytes": info.f_bavail * info.f_frsize,
            "free_bytes": info.f_bfree * info.f_frsize,
            "total_bytes": info.f_blocks * info.f_frsize}


def inspect(request):
    path, root = request["path"], request["storage_root"]
    validate_path(path, root)
    host = identity()
    if host["hostname"] != request["hostname"] or host["machine_id"] != request["machine_id"]:
        raise ValueError("SSH host does not match Kubernetes node identity")
    tree = scan_tree(path)
    blockers = active_users([path])
    return {"host": host, "tree": tree, "filesystem": filesystem(root),
            "blockers": blockers}


def atomic_json(path, value):
    fd, temporary = tempfile.mkstemp(prefix=os.path.basename(path) + ".tmp-",
                                     dir=os.path.dirname(path))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(os.path.dirname(path), os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)


def retire(request):
    plan = request["plan"]
    path, root = plan["path"], plan["storage_root"]
    validate_path(path, root)
    if not re.fullmatch(r"[a-f0-9]{64}", request["plan_sha256"]):
        raise ValueError("invalid plan digest")
    pv_uid = plan["pv_uid"]
    if not re.fullmatch(r"[a-zA-Z0-9-]{1,64}", pv_uid):
        raise ValueError("invalid PV identity")
    host = identity()
    if host != plan["host"]:
        raise ValueError("node identity or boot changed; preview again")
    if not shutil.rmtree.avoids_symlink_attacks:
        raise ValueError("host lacks fd-safe directory removal")
    receipt_path = os.path.join(root, ".fugue-volume-retire-" + pv_uid + ".json")
    quarantine = os.path.join(root, ".fugue-volume-retired-" + pv_uid)
    lock_path = receipt_path + ".lock"
    lock = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if os.path.lexists(receipt_path):
            fd = os.open(receipt_path, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, encoding="utf-8") as stream:
                receipt = json.load(stream)
            if (receipt.get("plan_sha256") != request["plan_sha256"] or
                    receipt.get("path") != path or receipt.get("pv_uid") != pv_uid):
                raise ValueError("node retirement receipt belongs to another plan")
        else:
            if os.path.lexists(quarantine):
                raise ValueError("quarantine already exists without receipt")
            current = inspect({"path": path, "storage_root": root,
                               "hostname": host["hostname"], "machine_id": host["machine_id"]})
            if current["blockers"] or current["tree"] != plan["tree"]:
                raise ValueError("volume changed or became active since preview")
            receipt = {"plan_sha256": request["plan_sha256"], "pv_uid": pv_uid,
                       "path": path, "quarantine": quarantine, "state": "prepared",
                       "tree": plan["tree"], "filesystem_before": filesystem(root)}
            atomic_json(receipt_path, receipt)
        state = receipt["state"]
        if state == "prepared":
            if os.path.lexists(path) and os.path.lexists(quarantine):
                raise ValueError("source and quarantine both exist")
            if os.path.lexists(path):
                current = inspect({"path": path, "storage_root": root,
                                   "hostname": host["hostname"], "machine_id": host["machine_id"]})
                if current["blockers"] or current["tree"] != plan["tree"]:
                    raise ValueError("volume changed before quarantine")
                os.rename(path, quarantine)
            info = os.lstat(quarantine)
            if (info.st_dev, info.st_ino) != (plan["tree"]["device"], plan["tree"]["inode"]):
                raise ValueError("quarantine identity changed")
            receipt["state"] = "quarantined"
            atomic_json(receipt_path, receipt)
        if receipt["state"] in ("quarantined", "deleting"):
            if os.path.lexists(path):
                raise ValueError("original path reappeared; refusing deletion")
            if os.path.lexists(quarantine):
                info = os.lstat(quarantine)
                if (info.st_dev, info.st_ino) != (plan["tree"]["device"], plan["tree"]["inode"]):
                    raise ValueError("quarantine identity changed")
                # Re-scan after quarantine too, and on every resumed deletion.
                # Renaming changes directory ctime, so compare root identity
                # and entry validity rather than the original tree hash here.
                remaining = scan_tree(quarantine)
                if active_users([path, quarantine]):
                    raise ValueError("volume still has mounts or open files")
                receipt["state"] = "deleting"
                receipt["remaining_bytes_before_attempt"] = remaining["allocated_bytes"]
                atomic_json(receipt_path, receipt)
                shutil.rmtree(quarantine)
            receipt["state"] = "data_deleted"
            receipt["deleted_allocated_bytes"] = plan["tree"]["allocated_bytes"]
            receipt["completed_at_unix"] = int(time.time())
            atomic_json(receipt_path, receipt)
        if receipt["state"] != "data_deleted" or os.path.lexists(path) or os.path.lexists(quarantine):
            raise ValueError("retirement has not verified both data paths absent")
        receipt["filesystem_after"] = filesystem(root)
        receipt["available_bytes_delta"] = (receipt["filesystem_after"]["available_bytes"] -
                                             receipt["filesystem_before"]["available_bytes"])
        receipt["verified_absent"] = True
        atomic_json(receipt_path, receipt)
        return receipt
    finally:
        os.close(lock)


def main():
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError("node operation deadline")))
    signal.alarm(600)
    request = json.load(sys.stdin)
    try:
        if os.geteuid() != 0:
            raise ValueError("complete /proc inspection requires node root access")
        if request["mode"] == "inspect":
            result = inspect(request)
        elif request["mode"] == "retire":
            result = retire(request)
        else:
            raise ValueError("unsupported node operation")
        print(json.dumps(result, sort_keys=True))
    except Exception as error:
        print(json.dumps({"error": str(error)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
