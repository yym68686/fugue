"""Verify exact production mail standby without applying an ownership change."""

import argparse
import json
from pathlib import Path
import subprocess
import time

from scripts.apply_app_mail_reconcile_handoff import (
    API_PATHS, CONTROLLER_PATHS, check_leader, parse_release, read_progress, verify_source,
)
from scripts.provision_observation_worker_database import Provision, ProvisionError
from scripts.verify_observation_rollback_floor import checked_rollback
from scripts.verify_observation_writer_handoff import verify_workload


def verify_standby(provision, release):
    targets = (("api", API_PATHS), ("controller", CONTROLLER_PATHS))
    live = {}
    rollback = {}
    for component, paths in targets:
        workload = release[component]
        verify_source(release[component + "ForwardFloor"], workload["revision"], paths)
        live[component] = verify_workload(provision, workload)
        revision, image = checked_rollback(provision, component, workload)
        try:
            verify_source(release[component + "RollbackFloor"], revision, paths)
        except ProvisionError:
            compatible = False
        else:
            compatible = True
        rollback[component] = {"revision": revision, "image": image, "compatible": compatible}

    primary = provision.find_primary()
    progress = read_progress(provision, primary[0])
    if progress["owner"] != "api" or progress["activated"] is not None or progress["confirmed"] != 0:
        raise ProvisionError("API is not the sole designated standby writer")
    if progress["missing"] != 0 or not progress["seeded"] or progress["active"] < 1:
        raise ProvisionError("mail queue coverage or its active provider witness is missing")
    controller = release["controller"]
    leader = check_leader(provision, controller, live["controller"][2], "api")
    for component, _ in targets:
        if (verify_workload(provision, release[component]) != live[component]
                or checked_rollback(provision, component, release[component]) != (
                    rollback[component]["revision"], rollback[component]["image"],
                )):
            raise ProvisionError("workload or rollback changed during mail standby verification")
    if (provision.find_primary() != primary or read_progress(provision, primary[0]) != progress
            or check_leader(provision, controller, live["controller"][2], "api") != leader):
        raise ProvisionError("database or leader changed during mail standby verification")
    return {
        "database": progress,
        "rollback": rollback,
        "revisions": {name: release[name]["revision"] for name, _ in targets},
        "providerExactGenerationRead": True,
        "writerTransferred": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("observation", type=Path)
    args = parser.parse_args()
    try:
        provision = Provision(json.loads(args.database.read_text()))
        release = parse_release(json.loads(args.observation.read_text()))
        deadline = time.monotonic() + 90
        while True:
            try:
                outcome = verify_standby(provision, release)
                break
            except ProvisionError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(5)
        print(json.dumps(outcome, sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"application mail standby verification stopped: {err}\n")
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError):
        parser.exit(1, "application mail standby verification received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
