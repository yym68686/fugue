"""Bounded, restartable growth of one existing file-backed LocalPV pool.

Called only by the authenticated node updater; never creates/removes PVs or LVs.
"""
import fcntl
import json
import os
import re
import stat
import subprocess


def run(*args):
    return subprocess.check_output(args, text=True).strip()


def expand(env=os.environ):
    image = env["FUGUE_NODE_UPDATE_TASK_IMAGE_PATH"]
    vg = env["FUGUE_NODE_UPDATE_TASK_VG_NAME"]
    expected = int(env["FUGUE_NODE_UPDATE_TASK_EXPECTED_IMAGE_SIZE_BYTES"])
    target = int(env["FUGUE_NODE_UPDATE_TASK_TARGET_IMAGE_SIZE_BYTES"])
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.+-]{0,126}", vg):
        raise ValueError("invalid volume group")
    if not os.path.isabs(image) or os.path.realpath(image) != image:
        raise ValueError("image must be an absolute canonical path without symlinks")
    if expected <= 0 or target < expected or target > expected + (64 << 30):
        raise ValueError("pool growth must be between zero and 64 GiB")
    with open("/run/fugue-localpv-expand.lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        with open(image, "rb") as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_size not in (expected, target):
                raise ValueError("backing image identity/size changed")
            loops = json.loads(run("losetup", "--json", "--list", "--output", "NAME,BACK-FILE"))["loopdevices"]
            matches = [x["name"] for x in loops if x.get("back-file") == image]
            if len(matches) != 1:
                raise ValueError("backing image must have exactly one loop device")
            loop = matches[0]
            pvs = json.loads(run("pvs", "--reportformat", "json", "--units", "b", "--nosuffix", "-o", "pv_name,vg_name,pv_size"))["report"][0]["pv"]
            members = [p for p in pvs if p["vg_name"].strip() == vg]
            if len(members) != 1 or members[0]["pv_name"].strip() != loop:
                raise ValueError("volume group must contain only the observed loop PV")
            fs = os.fstatvfs(source.fileno())
            # LocalPV discard can legitimately punch holes in the existing
            # image. Preserve those historical extents; reserve only the new
            # interval. Re-filling the entire old sparse file is an unrelated,
            # potentially enormous allocation and can prevent small rescues.
            allocation = target - expected
            reserve = max(5 << 30, (fs.f_blocks * fs.f_frsize + 9) // 10)
            if fs.f_bavail * fs.f_frsize < allocation + reserve:
                raise ValueError(f"insufficient host filesystem headroom: available_bytes={fs.f_bavail * fs.f_frsize} growth_bytes={allocation} reserve_bytes={reserve}")
            plan = dict(image=image, volume_group=vg, loop_device=loop,
                        previous_bytes=info.st_size, target_bytes=target,
                        allocation_bytes=allocation, host_reserve_bytes=reserve)
            print(json.dumps(plan), flush=True)
            if env.get("FUGUE_NODE_UPDATE_TASK_DRY_RUN", "true").lower() != "false":
                return
            # fallocate preserves every existing byte and never shrinks here.
            # Every subsequent step can be repeated after interrupted delivery.
            if os.stat(image).st_ino != info.st_ino:
                raise ValueError("backing image replaced during preflight")
            if allocation:
                run("fallocate", "--offset", str(expected), "--length", str(allocation), image)
            os.fsync(source.fileno())
            run("losetup", "--set-capacity", loop)
            if int(run("blockdev", "--getsize64", loop)) != target:
                raise ValueError("loop capacity did not converge")
            run("pvresize", loop)
            actual = json.loads(run("pvs", "--reportformat", "json", "--units", "b", "--nosuffix", "-o", "pv_size", loop))["report"][0]["pv"][0]
            if int(float(actual["pv_size"])) < target - (8 << 20):
                raise ValueError("physical volume capacity did not converge")


if __name__ == "__main__":
    expand()
