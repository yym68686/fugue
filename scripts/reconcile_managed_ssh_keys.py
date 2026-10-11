import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re

from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json


MANAGER = "fugue-managed-ssh-keys"
GENERATION = "ssh.fugue.dev/keyring-generation"
CONTENT_DIGEST = "ssh.fugue.dev/keyring-digest"
SCHEMAS = {"identity": "fugue.ssh-identity-keyring/v1", "signing": "fugue.managed-ssh-signing/v1"}


def content_digest(value):
    return "sha256:" + hashlib.sha256(canonical(value).encode()).hexdigest()


def validate(config):
    if set(config) != {"apiVersion", "kind", "namespace", "generation", "expectedPreviousGeneration", "restoreMissing", "identity", "signing"} or config["apiVersion"] != "identity.fugue.dev/v1" or config["kind"] != "ManagedSSHKeyrings":
        raise ValueError("invalid managed SSH keyring declaration")
    previous, generation = config["expectedPreviousGeneration"], config["generation"]
    if type(config["restoreMissing"]) is not bool:
        raise ValueError("explicit missing-keyring recovery intent required")
    if type(previous) is not int or type(generation) is not int or not 0 <= previous < generation <= 2**53-1:
        raise ValueError("explicit increasing keyring generation required")
    names = [config["namespace"]]
    for purpose in SCHEMAS:
        selected = config[purpose]
        if set(selected) != {"secretName", "contentDigest", "expectedPreviousDigest"} or not re.fullmatch(r"sha256:[a-f0-9]{64}", selected["contentDigest"]):
            raise ValueError("keyring digest required")
        if (previous == 0 and selected["expectedPreviousDigest"] != "") or (previous > 0 and not re.fullmatch(r"sha256:[a-f0-9]{64}", selected["expectedPreviousDigest"])):
            raise ValueError("keyring predecessor digest required")
        if config["restoreMissing"] and (previous == 0 or selected["contentDigest"] != selected["expectedPreviousDigest"]):
            raise ValueError("recovery must restore the exact previously reviewed keyring, not rotate it")
        names.append(selected["secretName"])
    if names[1] == names[2] or any(not isinstance(name, str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", name) for name in names):
        raise ValueError("separate canonical Kubernetes resources required")
    return config


def validate_material(material, config):
    if not isinstance(material, dict) or set(material) != set(SCHEMAS):
        raise ValueError("both independent keyrings are required")
    seen = set()
    for purpose, schema in SCHEMAS.items():
        keyring = material[purpose]
        if not isinstance(keyring, dict) or set(keyring) != {"schema", "active_key_id", "keys"} or keyring["schema"] != schema or not isinstance(keyring["keys"], dict) or not 1 <= len(keyring["keys"]) <= 16 or keyring["active_key_id"] not in keyring["keys"]:
            raise ValueError("invalid managed SSH keyring")
        for identifier, value in keyring["keys"].items():
            if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", identifier) or not isinstance(value, str):
                raise ValueError("invalid key identity")
            decoded = base64.b64decode(value, validate=True)
            if len(decoded) != 32 or decoded in seen:
                raise ValueError("independent 32-byte key material required")
            seen.add(decoded)
        if content_digest(keyring) != config[purpose]["contentDigest"]:
            raise ValueError("keyring does not match reviewed digest")
    return material


def reconcile(config, material, kube=kubectl):
    validate(config)
    validate_material(material, config)
    planned = []
    for purpose in SCHEMAS:
        selected = config[purpose]
        name, namespace = selected["secretName"], config["namespace"]
        raw = kube(["get", "secret", name, "-n", namespace, "--ignore-not-found", "-o", "json"])
        desired = {"apiVersion": "v1", "kind": "Secret", "metadata": {"name": name, "namespace": namespace, "labels": {"app.kubernetes.io/managed-by": MANAGER, "ssh.fugue.dev/keyring-purpose": purpose}, "annotations": {GENERATION: str(config["generation"]), CONTENT_DIGEST: selected["contentDigest"]}}, "type": "Opaque", "data": {"keyring.json": base64.b64encode(canonical(material[purpose]).encode()).decode()}}
        if raw.strip():
            current = strict_json(raw)
            meta = current.get("metadata", {})
            if meta.get("name") != name or meta.get("namespace") != namespace or meta.get("deletionTimestamp") or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or meta.get("labels", {}).get("ssh.fugue.dev/keyring-purpose") != purpose or current.get("immutable"):
                raise ValueError("foreign or immutable keyring resource")
            current_ring = strict_json(base64.b64decode(current.get("data", {}).get("keyring.json", ""), validate=True))
            annotations = meta.get("annotations", {})
            if content_digest(current_ring) != annotations.get(CONTENT_DIGEST):
                raise ValueError("stored keyring integrity mismatch; restore reviewed material")
            if annotations.get(GENERATION) == str(config["generation"]):
                if current_ring != material[purpose] or annotations.get(CONTENT_DIGEST) != selected["contentDigest"]:
                    raise ValueError("same-generation keyring substitution")
                continue
            if annotations.get(GENERATION) != str(config["expectedPreviousGeneration"]) or annotations.get(CONTENT_DIGEST) != selected["expectedPreviousDigest"]:
                raise ValueError("keyring predecessor changed")
            for identifier, value in current_ring.get("keys", {}).items():
                if material[purpose]["keys"].get(identifier) != value:
                    raise ValueError("rotation must retain all existing decryption and verification keys")
            if not meta.get("resourceVersion") or not meta.get("uid"):
                raise ValueError("keyring identity preconditions missing")
            desired["metadata"]["resourceVersion"] = meta["resourceVersion"]
            desired["metadata"]["uid"] = meta["uid"]
            operation = "replace"
        else:
            if config["expectedPreviousGeneration"] != 0 and not config["restoreMissing"]:
                raise ValueError("existing keyring missing; recovery requires reviewed backup restoration")
            operation = "create"
        planned.append((operation, desired))
    for operation, desired in planned:
        kube([operation, "--dry-run=server", "-f", "-"], desired)
    for operation, desired in planned:
        kube([operation, "-f", "-"], desired)
        observed = strict_json(kube(["get", "secret", desired["metadata"]["name"], "-n", config["namespace"], "-o", "json"]))
        if observed.get("data") != desired["data"] or observed.get("metadata", {}).get("annotations", {}).get(GENERATION) != str(config["generation"]):
            raise ValueError("keyring persistence did not verify")
    return {"verified": True, "generation": config["generation"], "changed_resources": len(planned)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 65536:
        raise ValueError("keyring declaration exceeds bound")
    config = validate(strict_json(raw))
    if args.validate_only:
        print(canonical({"valid": True, "generation": config["generation"]}))
        return
    encoded = os.environ.pop("FUGUE_MANAGED_SSH_KEYRINGS_JSON", "")
    if not encoded or len(encoded) > 65536:
        raise ValueError("independently retained CI keyring secret required")
    print(canonical(reconcile(config, strict_json(encoded))))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("Managed SSH keyring recovery failed: " + type(error).__name__ + "; private material was not emitted") from None
