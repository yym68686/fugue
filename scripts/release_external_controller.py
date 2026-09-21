#!/usr/bin/env python3
"""Release one pinned controller executable, retaining its database operands."""

import argparse
import copy
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def run(*args, body=None):
    result = subprocess.run(args, input=None if body is None else canonical(body),
                            text=True, capture_output=True, timeout=90)
    if result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr[:1500]}")
    return json.loads(result.stdout) if result.stdout.strip() else None


def kube(*args, body=None):
    return run("kubectl", "--request-timeout=30s", *args, body=body)


def load_config(path):
    config = json.loads(Path(path).read_text())
    required = {"apiVersion", "kind", "namespace", "deployment", "container", "uid",
                "preservedSpecSHA256", "predecessor", "candidate", "operandGuard", "configurationGuards", "candidateReceipt", "apply"}
    if set(config) != required or config["apiVersion"] != "release.fugue.dev/v1" or config["kind"] != "ExternalControllerRelease":
        raise ValueError("invalid external controller release contract")
    for field in ("namespace", "deployment", "container"):
        if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", config[field]):
            raise ValueError("invalid workload identity")
    if not re.fullmatch(r"[0-9a-f-]{36}", config["uid"]) or not re.fullmatch(r"[0-9a-f]{64}", config["preservedSpecSHA256"]):
        raise ValueError("expected workload identity/digest missing")
    for key in ("predecessor", "candidate"):
        artifact = config[key]
        expected = {"image", "revision", "command", "specImage"} if key == "predecessor" else {"image", "revision", "command"}
        if set(artifact) != expected or not re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", artifact["image"]):
            raise ValueError("artifact must have an exact immutable reference")
        if not re.fullmatch(r"[0-9a-f]{7,40}", artifact["revision"]) or len(artifact["command"]) != 1 or not re.fullmatch(r"/[a-zA-Z0-9/_-]+", artifact["command"][0]):
            raise ValueError("invalid executable provenance/command")
    if config["candidate"]["image"] == config["predecessor"]["image"]:
        raise ValueError("candidate must differ from predecessor")
    if not isinstance(config["configurationGuards"], list) or len(config["configurationGuards"]) > 16:
        raise ValueError("configuration guard count exceeds bound")
    if type(config["apply"]) is not bool:
        raise ValueError("apply must be an explicit boolean")
    receipt = config["candidateReceipt"]
    if receipt.get("candidate_image") != config["candidate"]["image"] or receipt.get("source_revision") != config["candidate"]["revision"] or receipt.get("base_image") != config["predecessor"]["image"] or receipt.get("controller_command_verified") is not True or receipt.get("production_deployed") is not False:
        raise ValueError("candidate receipt does not bind both artifacts")
    hashes = receipt.get("instance_manager_sha256", {})
    if set(hashes) != {"manager_amd64", "manager_arm64"} or any(not re.fullmatch(r"[0-9a-f]{64}", v) for v in hashes.values()):
        raise ValueError("candidate instance-manager verification missing")
    for guard in config["configurationGuards"]:
        if set(guard) != {"name", "uid", "dataSHA256"} or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{0,252}", guard["name"]) or not re.fullmatch(r"[0-9a-f]{64}", guard["dataSHA256"]):
            raise ValueError("invalid ConfigMap guard")
    if config["operandGuard"] != "cnpg-instance-identity-v1":
        raise ValueError("unsupported operand guard")
    return config


def container_index(config, deployment):
    containers = deployment["spec"]["template"]["spec"]["containers"]
    matches = [i for i, c in enumerate(containers) if c["name"] == config["container"]]
    if len(matches) != 1:
        raise ValueError("controller container ambiguous or absent")
    return matches[0]


def preserved_spec(config, deployment):
    spec = copy.deepcopy(deployment["spec"])
    container = spec["template"]["spec"]["containers"][container_index(config, deployment)]
    del container["image"]
    del container["command"]
    return digest(spec)


def validate_workload(config, deployment, artifact):
    metadata, spec = deployment["metadata"], deployment["spec"]
    if metadata["uid"] != config["uid"] or metadata.get("deletionTimestamp"):
        raise ValueError("controller UID/lifecycle changed")
    if preserved_spec(config, deployment) != config["preservedSpecSHA256"]:
        raise ValueError("controller configuration drifted outside image/command")
    container = spec["template"]["spec"]["containers"][container_index(config, deployment)]
    allowed = {artifact["image"], artifact.get("specImage", artifact["image"])}
    if container["image"] not in allowed or container["command"] != artifact["command"]:
        raise ValueError("controller is not the declared artifact")
    replicas = spec.get("replicas", 0)
    rolling = spec.get("strategy", {}).get("rollingUpdate", {})
    unavailable = rolling.get("maxUnavailable", 1)
    if isinstance(unavailable, str) and unavailable.endswith("%"):
        unavailable = replicas * int(unavailable[:-1]) // 100
    if replicas < 2 or spec.get("strategy", {}).get("type") != "RollingUpdate" or unavailable != 0:
        raise ValueError("controller rollout requires at least two replicas and zero unavailable")


def operand_snapshot(clusters, pods):
    result = {"clusters": {}, "pods": {}}
    for item in clusters["items"]:
        m, status = item["metadata"], item.get("status", {})
        if m.get("deletionTimestamp") or status.get("currentPrimary") != status.get("targetPrimary"):
            raise ValueError("database deletion or primary transition is active")
        result["clusters"][m["uid"]] = {
            "namespace": m["namespace"], "name": m["name"], "generation": m["generation"],
            "spec": digest(item["spec"]), "primary": status.get("currentPrimary"),
            "ready": status.get("readyInstances", 0), "manager": status.get("cloudNativePGOperatorHash"),
            "architectures": status.get("availableArchitectures", []),
        }
    if not result["clusters"]:
        raise ValueError("database inventory is empty")
    for pod in pods["items"]:
        m, status = pod["metadata"], pod.get("status", {})
        if m.get("deletionTimestamp"):
            raise ValueError("database Pod termination is active")
        result["pods"][m["uid"]] = {
            "namespace": m["namespace"], "name": m["name"], "spec": digest(pod["spec"]),
            "ready": any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions", [])),
            "containers": [{k: c.get(k) for k in ("name", "containerID", "imageID", "restartCount")} for c in status.get("containerStatuses", [])],
        }
    return result


def assert_operands_preserved(before, after):
    for kind in ("clusters", "pods"):
        if before[kind].keys() != after[kind].keys():
            raise ValueError(f"operand {kind} membership changed")
        for uid, original in before[kind].items():
            current = after[kind][uid]
            if any(current.get(k) != v for k, v in original.items() if k != "ready") or current["ready"] < original["ready"]:
                raise ValueError(f"operand {kind} identity, configuration or health changed")


def observe_operands():
    return operand_snapshot(kube("get", "clusters.postgresql.cnpg.io", "-A", "-o", "json"),
                            kube("get", "pods", "-A", "-l", "cnpg.io/cluster", "-o", "json"))


def observe_deployment(config):
    return kube("-n", config["namespace"], "get", "deployment", config["deployment"], "-o", "json")


def verify_registry(artifact):
    value = run("python3", "scripts/verify_registry_image.py", "--image", artifact["image"])
    if value["oci_revision"] != artifact["revision"] or value["verification"] != "registry_manifest_config_and_layer_get":
        raise ValueError("registry artifact provenance differs")
    return value


def check_controller_ready(config, deployment, artifact, verification):
    validate_workload(config, deployment, artifact)
    status = deployment["status"]
    desired = deployment["spec"]["replicas"]
    if status.get("observedGeneration", 0) != deployment["metadata"]["generation"] or any(status.get(k, 0) != desired for k in ("readyReplicas", "updatedReplicas", "availableReplicas", "replicas")) or status.get("unavailableReplicas", 0):
        return False
    selector = ",".join(f"{k}={v}" for k, v in sorted(deployment["spec"]["selector"]["matchLabels"].items()))
    pods = kube("-n", config["namespace"], "get", "pods", "-l", selector, "-o", "json")["items"]
    pods = [p for p in pods if not p["metadata"].get("deletionTimestamp")]
    allowed = {verification["manifest_digest"], verification["index_digest"], verification["config_digest"]}
    if len(pods) != desired:
        return False
    for pod in pods:
        if not any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod.get("status", {}).get("conditions", [])):
            return False
        containers = [c for c in pod.get("status", {}).get("containerStatuses", []) if c["name"] == config["container"]]
        if len(containers) != 1 or not containers[0].get("ready") or containers[0].get("imageID", "").split("@")[-1] not in allowed:
            return False
    return True


def code_patch(config, live, target):
    index = container_index(config, live)
    path = f"/spec/template/spec/containers/{index}"
    container = live["spec"]["template"]["spec"]["containers"][index]
    return [{"op": "test", "path": "/metadata/uid", "value": live["metadata"]["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": live["metadata"]["resourceVersion"]},
            {"op": "test", "path": path + "/name", "value": config["container"]},
            {"op": "test", "path": path + "/image", "value": container["image"]},
            {"op": "test", "path": path + "/command", "value": container["command"]},
            {"op": "replace", "path": path + "/image", "value": target["image"]},
            {"op": "replace", "path": path + "/command", "value": target["command"]}]


def patch(config, live, target, dry_run=False):
    args = ["-n", config["namespace"], "patch", "deployment", config["deployment"], "--type=json", "--patch-file=/dev/stdin", "-o", "json"]
    if dry_run:
        args.append("--dry-run=server")
    return kube(*args, body=code_patch(config, live, target))


def prepare(config):
    live = observe_deployment(config)
    current = config["candidate"] if live["spec"]["template"]["spec"]["containers"][container_index(config, live)]["image"] == config["candidate"]["image"] else config["predecessor"]
    predecessor = verify_registry(config["predecessor"])
    candidate = verify_registry(config["candidate"])
    if not check_controller_ready(config, live, current, candidate if current == config["candidate"] else predecessor):
        raise ValueError("predecessor controller is not fully healthy")
    for guard in config["configurationGuards"]:
        cm = kube("-n", config["namespace"], "get", "configmap", guard["name"], "-o", "json")
        if cm["metadata"]["uid"] != guard["uid"] or digest({"data": cm.get("data", {}), "binaryData": cm.get("binaryData", {})}) != guard["dataSHA256"]:
            raise ValueError("operator ConfigMap changed")
    before = observe_operands()
    expected = config["candidateReceipt"]["instance_manager_sha256"]
    for cluster in before["clusters"].values():
        hashes = {"manager_" + a["goArch"]: a["hash"] for a in cluster["architectures"]}
        if hashes != expected or cluster["manager"] != expected["manager_amd64"]:
            raise ValueError("database instance-manager identity differs from verified candidate")
    preview = patch(config, live, config["candidate"], True)
    validate_workload(config, preview, config["candidate"])
    patch(config, live, config["predecessor"], True)
    return {"schema": "fugue.external-controller.prepared.v1", "config": config, "configDigest": digest(config),
            "predecessorVerification": predecessor, "candidateVerification": candidate, "operands": before,
            "observedUID": live["metadata"]["uid"], "observedResourceVersion": live["metadata"]["resourceVersion"],
            "currentArtifact": current, "preparedAt": time.time()}


def wait_healthy(config, artifact, verification, operands, check_operands=True):
    deadline = time.monotonic() + 240
    healthy_since = None
    while time.monotonic() < deadline:
        if check_operands:
            assert_operands_preserved(operands, observe_operands())
        ready = check_controller_ready(config, observe_deployment(config), artifact, verification)
        healthy_since = (healthy_since or time.monotonic()) if ready else None
        if healthy_since is not None and time.monotonic() - healthy_since >= 30:
            return
        time.sleep(5)
    raise RuntimeError("controller rollout did not converge within 240 seconds")


def execute(config, prepared):
    if not config["apply"]:
        raise ValueError("this release intent authorizes preflight only")
    if prepared.get("schema") != "fugue.external-controller.prepared.v1" or prepared["configDigest"] != digest(config) or prepared["config"] != config or not 0 <= time.time() - prepared["preparedAt"] < 600:
        raise ValueError("prepared release is stale or does not match configuration")
    fresh = prepare(config)
    assert_operands_preserved(prepared["operands"], fresh["operands"])
    live = observe_deployment(config)
    validate_workload(config, live, fresh["currentArtifact"])
    if live["metadata"]["resourceVersion"] != fresh["observedResourceVersion"]:
        raise ValueError("controller changed after final preflight")
    # The two registry/operand preflights can be slow; take one final operand
    # snapshot immediately before the UID/resourceVersion-fenced write.
    assert_operands_preserved(prepared["operands"], observe_operands())
    try:
        patch(config, live, config["candidate"])
        wait_healthy(config, config["candidate"], fresh["candidateVerification"], prepared["operands"])
    except Exception as error:
        # Read back after an uncertain write; never overwrite unrelated drift.
        live = observe_deployment(config)
        index = container_index(config, live)
        if live["spec"]["template"]["spec"]["containers"][index]["image"] in {config["predecessor"]["image"], config["predecessor"]["specImage"]}:
            validate_workload(config, live, config["predecessor"])
            raise RuntimeError(f"candidate failed before activation; predecessor unchanged: {error}") from error
        validate_workload(config, live, config["candidate"])
        patch(config, live, config["predecessor"])
        wait_healthy(config, config["predecessor"], fresh["predecessorVerification"], prepared["operands"], False)
        raise RuntimeError(f"candidate failed; predecessor restored: {error}") from error
    return {"status": "verified", "candidate": config["candidate"], "configDigest": digest(config),
            "operandIdentitiesPreserved": True, "finishedAt": time.time()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--prepared")
    args = parser.parse_args()
    config = load_config(args.config)
    try:
        result = execute(config, json.loads(Path(args.prepared).read_text())) if args.prepared else prepare(config)
    except Exception as error:
        print(canonical({"status": "failed", "configDigest": digest(config), "reason": str(error)[:2000]}))
        raise
    else:
        print(canonical(result))


if __name__ == "__main__":
    main()
