#!/usr/bin/env python3
"""Verify the shutdown protocol against the deployed CNPG build in disposable kind."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import time

from scripts import baked_postgres_contract as contract
from scripts.smoke_cnpg_baked_manager import MANIFEST_SHA256, Rehearsal, manifest_objects

MARKER = "fugue.image_migration_drain_nonce"
NS = "manager-check"
NAME = "shutdown-canary"
POD_PATCH = "cnpg.io/podPatch"
POD_PATCH_PATH = "/metadata/annotations/cnpg.io~1podPatch"
BOOTSTRAP_CPU_PATCH = json.dumps([
    {"op": "test", "path": "/spec/initContainers/0/name", "value": "bootstrap-controller"},
    {"op": "replace", "path": "/spec/initContainers/0/resources/requests/cpu", "value": "5m"},
], sort_keys=True, separators=(",", ":"))


def exercise(r, release):
    args = r.args
    if hashlib.sha256(args.operator_manifest.read_bytes()).hexdigest() != MANIFEST_SHA256:
        raise ValueError("operator installation manifest is not the pinned upstream version")
    receipt = release["candidateReceipt"]
    operator = release["candidate"]["image"]
    operand = receipt["baked_postgres_verification"]["image"]
    source = receipt["baked_postgres_verification"]["postgres_base"]
    for image in (operator, operand, source):
        if not re.fullmatch(r"ghcr[.]io/[a-z0-9/._:-]+@sha256:[0-9a-f]{64}", image):
            raise ValueError("rehearsal needs approved digest-pinned images")
    objects = manifest_objects(r.kube("create", "--dry-run=client", "--validate=false", "-f", str(args.operator_manifest), "-o", "json", raw=True))
    deployments = [x for x in objects["items"] if x["kind"] == "Deployment"]
    if len(deployments) != 1:
        raise ValueError("operator Deployment is ambiguous")
    deployment = deployments[0]
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    container["image"], container["command"] = operator, ["/fugue-cnpg-controller"]
    for var in container.get("env", []):
        if var["name"] == "OPERATOR_IMAGE_NAME":
            var["value"] = contract.MANAGER_IMAGE
    r.apply(objects)
    r.kube("-n", deployment["metadata"]["namespace"], "rollout", "status",
           "deployment/" + deployment["metadata"]["name"], "--timeout=120s", raw=True)
    r.apply({"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": NS}})
    r.apply({"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster",
             "metadata": {"name": NAME, "namespace": NS,
                          "annotations": {POD_PATCH: BOOTSTRAP_CPU_PATCH, "example.org/retained": "true"}},
             "spec": {"instances": 1, "imageName": source, "storage": {"size": "1Gi"}, "primaryUpdateMethod": "restart",
                      "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}},
                      "stopDelay": 1800, "smartShutdownTimeout": 180, "postgresql": {"parameters": {"log_min_duration_statement": "-1"}}}})
    old = r.wait(lambda: r.pod(NAME), r.ready, "source primary never became Ready", timeout=180)
    init = old["spec"].get("initContainers", [])
    if len(init) != 1 or init[0]["name"] != "bootstrap-controller" or init[0]["resources"]["requests"]["cpu"] != "5m":
        raise ValueError("legacy bootstrap-only CPU patch was not applied to the source Pod")
    pod = old["metadata"]["name"]
    old_uid = old["metadata"]["uid"]

    def cluster():
        return r.kube("-n", NS, "get", "cluster", NAME, "-o", "json")

    def patch(changes):
        current = cluster()
        expected = [{"op": "test", "path": "/metadata/uid", "value": current["metadata"]["uid"]},
                    {"op": "test", "path": "/spec", "value": current["spec"]}]
        r.kube("-n", NS, "patch", "cluster", NAME, "--type=json", "-p", json.dumps(expected + changes), raw=True)

    def claim():
        return r.kube("-n", NS, "get", "pvc", pod, "-o", "json")["metadata"]["uid"]

    baseline = cluster()["spec"]
    claim_uid = claim()
    physical_id = r.sql(old, "SELECT system_identifier FROM pg_control_system()")
    r.sql(old, "CREATE TABLE drain_continuity(value integer); INSERT INTO drain_continuity VALUES (7);")
    count = "SELECT count(*) FROM pg_stat_activity WHERE application_name='fugue-drain-smoke'"
    if r.sql(old, count) != "0":
        raise ValueError("unexpected rehearsal sessions")

    # A controlled idle connection must stay open until the fast shutdown.
    idle = subprocess.Popen([*r.command, "-n", NS, "exec", "-i", pod, "-c", "postgres", "--",
                             "env", "PGAPPNAME=fugue-drain-smoke", "psql", "-XqAtw", "-U", "postgres", "-d", "postgres"],
                            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True)
    try:
        idle.stdin.write("BEGIN; SELECT 1;\n")
        idle.stdin.flush()
        r.wait(lambda: r.sql(old, count + " AND state='idle in transaction'"), lambda n: n == "1", "did not observe idle transaction", timeout=20)
        idle.stdin.write("ROLLBACK;\n")
        idle.stdin.flush()
        r.wait(lambda: r.sql(old, count + " AND state='idle'"), lambda n: n == "1", "did not observe idle connection", timeout=20)
        patch([{"op": "replace", "path": "/spec/smartShutdownTimeout", "value": 10},
               {"op": "add", "path": "/spec/postgresql/parameters/" + MARKER, "value": "fugue-drain-rehearsal"}])
        r.wait(lambda: r.sql(old, f"SELECT EXISTS(SELECT 1 FROM pg_file_settings WHERE name='{MARKER}' AND applied AND setting=current_setting('{MARKER}', true))"),
               lambda loaded: loaded == "t", "old primary never acknowledged the temporary policy", timeout=30)
        staged = r.pod(NAME)
        if not r.ready(staged) or staged["metadata"]["uid"] != old_uid or staged["status"]["containerStatuses"][0]["restartCount"] != 0:
            raise ValueError("staging the budget restarted the old primary")

        with tempfile.TemporaryFile(mode="w+b") as logs:
            tail = subprocess.Popen([*r.command, "-n", NS, "logs", pod, "-c", "postgres", "-f", "--tail=15"], stdout=logs, stderr=logs)
            try:
                start = time.monotonic()
                patch([{"op": "replace", "path": "/spec/imageName", "value": operand},
                       {"op": "test", "path": POD_PATCH_PATH, "value": BOOTSTRAP_CPU_PATCH},
                       {"op": "remove", "path": POD_PATCH_PATH}])
                target = r.wait(lambda: r.pod(NAME), lambda p: p and p["metadata"]["uid"] != old_uid, "CNPG did not recreate the old primary within the shortened window", timeout=65)
                before_restore = time.monotonic() - start
            finally:
                if tail.poll() is None:
                    tail.terminate()
                tail.wait(timeout=10)
                logs.seek(0)
                messages = []
                for line in logs:
                    try:
                        messages.append(json.loads(line))
                    except (ValueError, UnicodeError):
                        continue
            if not any(x.get("msg") == "Received termination signal" and x.get("smartShutdownTimeout") == 10 for x in messages):
                raise ValueError("old manager did not report using the 10-second budget")
            if not any(x.get("msg") == "Requesting fast shutdown of the PostgreSQL instance" for x in messages):
                raise ValueError("idle connection did not exercise the PostgreSQL fast shutdown path")

        patch([{"op": "replace", "path": "/spec/smartShutdownTimeout", "value": 180},
               {"op": "remove", "path": "/spec/postgresql/parameters/" + MARKER}])
        ready = r.wait(lambda: r.pod(NAME), lambda p: r.ready(p) and p["metadata"]["uid"] == target["metadata"]["uid"],
                       "replacement primary never became Ready", timeout=90)
        final_spec = dict(baseline, imageName=operand)
        final_cluster = cluster()
        annotations = final_cluster["metadata"]["annotations"]
        if POD_PATCH in annotations or annotations.get("example.org/retained") != "true":
            raise ValueError("cutover did not remove only the obsolete bootstrap CPU patch")
        if final_cluster["spec"] != final_spec or claim() != claim_uid or ready["metadata"]["uid"] != target["metadata"]["uid"]:
            raise ValueError("original configuration or storage identity did not survive restoration")
        if ready["spec"].get("initContainers") or r.sql(ready, "SELECT value FROM drain_continuity") != "7" or r.sql(ready, "SELECT system_identifier FROM pg_control_system()") != physical_id:
            raise ValueError("baked instance lost the original PostgreSQL identity or data")
        r.wait(lambda: r.sql(ready, f"SELECT coalesce(current_setting('{MARKER}', true), '')"), lambda value: value == "",
               "temporary marker did not leave the running database", timeout=30)
        time.sleep(10)
        stable = r.pod(NAME)
        if not r.ready(stable) or stable["metadata"]["uid"] != ready["metadata"]["uid"] or stable["status"]["containerStatuses"][0]["restartCount"] != 0:
            raise ValueError("restoration caused an additional rollout")
        print(json.dumps({"old_manager_budget_seconds": 10, "replacement_observed_seconds": round(before_restore, 2),
                          "settings_restored": True, "single_replacement": True, "data_and_storage_verified": True,
                          "obsolete_bootstrap_patch_removed_atomically": True}, sort_keys=True))
    finally:
        if idle.poll() is None:
            idle.terminate()
        idle.wait(timeout=10)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--cluster-name", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--operator-manifest", type=Path, required=True)
    parser.add_argument("--release", type=Path, required=True)
    args = parser.parse_args()
    harness = Rehearsal(args)
    exercise(harness, json.loads(args.release.read_text()))
