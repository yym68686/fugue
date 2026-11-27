"""Read the exact production rollback that an observation handoff would inherit."""

import hashlib
import json

from scripts.provision_observation_worker_database import ProvisionError
from scripts.verify_observation_writer_handoff import COMMIT, IMAGE, declares_artifact, matches


DIGEST = r"sha256:[a-f0-9]{64}"


def digest(raw):
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_target(raw, workload, expected):
    if not isinstance(raw, dict):
        raise ProvisionError("release monitor target is not an object")
    objects = raw.get("items", [raw])
    deployments = [obj for obj in objects if obj.get("kind") == "Deployment"
                   and obj.get("metadata", {}).get("name") == workload["deployment"]]
    if len(deployments) != 1 or not declares_artifact(deployments[0]["spec"]["template"], expected):
        raise ProvisionError("release monitor manifest does not identify its target")


def checked_rollback(provision, component, workload):
    if component not in {"api", "observation-worker"}:
        raise ProvisionError("unsupported observation rollback component")
    name = f"fugue-release-monitor-{component}"
    try:
        pointer = provision.get("configmap", name)
        metadata, pointer_data = pointer["metadata"], pointer["data"]
        state = json.loads(pointer_data["state.json"])
        record_digest = state["recordDigest"]
        config = state["configSha"]
        if (state["component"] != component or state["consecutiveFailures"] != 0
                or state.get("rollbackStatus") not in (None, "")
                or not matches(record_digest, DIGEST) or not matches(config, COMMIT)):
            raise ProvisionError("release monitor is not healthy and settled")
        record_name = f"fugue-release-record-{component}-{record_digest[7:23]}"
        if pointer_data["recordName"] != record_name:
            raise ProvisionError("release monitor pointer does not match its digest")
        immutable = provision.get("configmap", record_name)
        data = immutable["data"]
        record = json.loads(data["record.json"])
        execution = json.loads(data["execution-plan.json"])
        terminal = json.loads(data["terminal-result.json"])
        if (immutable["immutable"] is not True
                or immutable["metadata"]["labels"]["fugue.pro/component"] != component
                or record["recordDigest"] != record_digest
                or record["component"] != component or record["configSha"] != config
                or execution["component"] != component or execution["configSha"] != config
                or execution["planDigest"] != record["executionPlanDigest"]
                or terminal["component"] != component
                or terminal["status"] != "verified"
                or terminal["executionPlanDigest"] != record["executionPlanDigest"]
                or terminal["receiptDigest"] != record["terminalReceiptDigest"]):
            raise ProvisionError("release monitor record is not bound to the verified execution")
        for field, manifest, record_key in (
            ("forward", "forward.json", "forwardManifestDigest"),
            ("lkg", "lkg.json", "lkgManifestDigest"),
        ):
            target = execution[field]
            manifest_digest = digest(data[manifest])
            if (target["present"] is not True
                    or manifest_digest != record[record_key]
                    or manifest_digest != target["manifestDigest"]
                    or not matches(target["imageRef"], IMAGE)
                    or not matches(target["ociRevision"], COMMIT)):
                raise ProvisionError("release monitor target lacks a pinned manifest and image")
            expected = {**workload, "revision": target["ociRevision"], "image": target["imageRef"]}
            parse_target(json.loads(data[manifest]), workload, expected)
        forward = execution["forward"]
        if forward["ociRevision"] != workload["revision"] or forward["imageRef"] != workload["image"]:
            raise ProvisionError("release monitor is tracking a different live revision")
        if provision.get("configmap", name)["metadata"]["resourceVersion"] != metadata["resourceVersion"]:
            raise ProvisionError("release monitor pointer changed during admission")
        return execution["lkg"]["ociRevision"], execution["lkg"]["imageRef"]
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ProvisionError("production rollback evidence is missing or malformed") from None
