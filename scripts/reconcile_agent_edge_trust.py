#!/usr/bin/env python3
"""Reconcile explicit Agent trust without changing serving code or policy.

Private material comes only from the configured encrypted CI secret. It must
match public trust committed in Git. No key is created, derived or logged here.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

MANAGER = "fugue-agent-edge-trust"
GENERATION = "agent-edge.fugue.dev/generation"
TRUST_DIGEST = "agent-edge.fugue.dev/trust-digest"
CONTENT_DIGEST = "agent-edge.fugue.dev/content-digest"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest(value):
    return "sha256:" + hashlib.sha256(canonical(value).encode()).hexdigest()


def strict_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate configuration field")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=unique)


def validate_config(config):
    if set(config) != {"apiVersion", "kind", "namespace", "signingSecret", "trustConfigMap", "expectedPreviousGeneration", "expectedPreviousTrustDigest", "publicTrust"}:
        raise ValueError("unknown or missing Agent trust declaration field")
    if config["apiVersion"] != "trust.fugue.dev/v1" or config["kind"] != "AgentEdgeTrust":
        raise ValueError("invalid Agent trust declaration schema")
    for key in ["namespace", "signingSecret", "trustConfigMap"]:
        if not isinstance(config[key], str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", config[key]):
            raise ValueError("canonical Kubernetes identity required")
    public = config["publicTrust"]
    if not isinstance(public, dict) or set(public) != {"schema", "generation", "keys"} or public["schema"] != "fugue.agent-edge-trust/v1":
        raise ValueError("explicit public trust keyring required")
    previous = config["expectedPreviousGeneration"]
    generation = public["generation"]
    if type(previous) is not int or previous < 0 or type(generation) is not int or not previous < generation <= 2**53 - 1:
        raise ValueError("trust generation must advance its explicit predecessor")
    old_digest = config["expectedPreviousTrustDigest"]
    if (previous == 0 and old_digest != "") or (previous > 0 and (not isinstance(old_digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", old_digest))):
        raise ValueError("predecessor trust digest required")
    if not isinstance(public["keys"], list) or not 1 <= len(public["keys"]) <= 16:
        raise ValueError("bounded public trust keys required")
    for key in public["keys"]:
        if not isinstance(key, dict) or set(key) != {"key_id", "public_key", "not_before", "not_after", "revoked"}:
            raise ValueError("invalid public key fields or private material in declaration")
    return config


def validate_pair(config, private, validator):
    # The Go validator checks Ed25519 derivation, canonical IDs, independent
    # public projection, ordered uniqueness and absolute validity windows.
    with tempfile.TemporaryDirectory(prefix="fugue-agent-trust-") as directory:
        private_path = Path(directory) / "private.json"
        public_path = Path(directory) / "public.json"
        for path, value in [(private_path, private), (public_path, config["publicTrust"])]:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w") as stream:
                stream.write(canonical(value))
        result = subprocess.run([validator, "verify", "-private-file", str(private_path), "-public-file", str(public_path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
        if result.returncode:
            raise ValueError("encrypted signing keyring does not validate against declared public trust")


def resources(config, private):
    public = config["publicTrust"]
    result = []
    for kind, name, data in [
        ("ConfigMap", config["trustConfigMap"], {"trust.json": canonical(public)}),
        ("Secret", config["signingSecret"], {"keyring.json": base64.b64encode(canonical(private).encode()).decode()}),
    ]:
        resource = {"apiVersion": "v1", "kind": kind,
                    "metadata": {"namespace": config["namespace"], "name": name,
                                 "labels": {"app.kubernetes.io/managed-by": MANAGER},
                                 "annotations": {GENERATION: str(public["generation"]), TRUST_DIGEST: digest(public), CONTENT_DIGEST: digest(data)}}, "data": data}
        if kind == "Secret":
            resource["type"] = "Opaque"
        result.append(resource)
    return result


def validate_existing(current, desired, config):
    if current is None:
        if config["expectedPreviousGeneration"] != 0:
            raise ValueError("expected previous trust resource is missing")
        return
    meta = current.get("metadata", {})
    annotations = meta.get("annotations", {})
    if current.get("kind") != desired["kind"] or meta.get("name") != desired["metadata"]["name"] or meta.get("namespace") != config["namespace"] or meta.get("deletionTimestamp") or meta.get("ownerReferences") or current.get("immutable") or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER:
        raise ValueError("refusing foreign, terminating or immutable trust resource")
    if desired["kind"] == "Secret" and current.get("type") != "Opaque":
        raise ValueError("unexpected trust Secret type")
    if not meta.get("uid") or not meta.get("resourceVersion") or digest(current.get("data")) != annotations.get(CONTENT_DIGEST):
        raise ValueError("trust resource changed outside its declared generation")
    try:
        generation = int(annotations.get(GENERATION, ""))
    except ValueError:
        raise ValueError("trust resource generation missing") from None
    target = config["publicTrust"]["generation"]
    if generation == target:
        if annotations.get(TRUST_DIGEST) != digest(config["publicTrust"]) or current.get("data") != desired["data"]:
            raise ValueError("same-generation trust content changed")
    elif generation != config["expectedPreviousGeneration"] or annotations.get(TRUST_DIGEST) != config["expectedPreviousTrustDigest"]:
        raise ValueError("trust declaration predecessor does not match current state")


def update_patch(current, desired, config):
    validate_existing(current, desired, config)
    if current["data"] == desired["data"] and all(current["metadata"]["annotations"].get(key) == value for key, value in desired["metadata"]["annotations"].items()):
        return []
    meta = current["metadata"]
    annotations = dict(meta["annotations"])
    annotations.update(desired["metadata"]["annotations"])
    return [{"op": "test", "path": "/metadata/uid", "value": meta["uid"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": meta["resourceVersion"]},
            {"op": "test", "path": "/metadata/annotations", "value": meta["annotations"]},
            {"op": "test", "path": "/metadata/labels", "value": meta["labels"]},
            {"op": "test", "path": "/data", "value": current["data"]},
            {"op": "replace", "path": "/data", "value": desired["data"]},
            {"op": "replace", "path": "/metadata/annotations", "value": annotations}]


def kubectl(args, value=None):
    result = subprocess.run(["kubectl", *args], input=None if value is None else canonical(value), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if result.returncode:
        # Kubernetes errors can quote request bodies. Never print them when
        # handling a signing Secret, even when validation or CAS fails.
        raise RuntimeError("Kubernetes trust operation failed; no private diagnostics emitted")
    return result.stdout


def read_resource(desired):
    raw = kubectl(["get", desired["kind"], desired["metadata"]["name"], "-n", desired["metadata"]["namespace"], "--ignore-not-found", "-o", "json"])
    return strict_json(raw) if raw.strip() else None


def reconcile(config, private, check=False):
    desired = resources(config, private)
    observed = [read_resource(item) for item in desired]
    for old, item in zip(observed, desired):
        validate_existing(old, item, config)
        if check and (old is None or old["data"] != item["data"]):
            raise ValueError("declared trust has not converged")
    if check:
        return
    # Preflight both resources before either changes; updates use UID and
    # resourceVersion tests. A failed second write is resumable at this exact
    # generation. Public trust is published first; policy activation is separate.
    operations = []
    for old, item in zip(observed, desired):
        if old is None:
            operations.append((["create", "-f", "-"], item))
        else:
            patch = update_patch(old, item, config)
            if patch:
                operations.append((["patch", item["kind"], item["metadata"]["name"], "-n", config["namespace"], "--type=json", "--patch-file=/dev/stdin"], patch))
    for args, body in operations:
        kubectl([*args, "--dry-run=server"], body)
    for args, body in operations:
        kubectl(args, body)
    for item in desired:
        old = read_resource(item)
        validate_existing(old, item, config)
        if old is None or old["data"] != item["data"]:
            raise ValueError("trust verification failed after reconciliation")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--validator", required=True)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    raw = Path(args.config).read_bytes()
    if len(raw) > 65536:
        raise ValueError("trust declaration exceeds bound")
    config = validate_config(strict_json(raw))
    secret = os.environ.pop("FUGUE_AGENT_EDGE_SIGNING_KEYRING", "")
    if not secret or len(secret) > 65536:
        raise ValueError("bounded encrypted signing keyring is required")
    private = strict_json(secret)
    validate_pair(config, private, args.validator)
    reconcile(config, private, args.check)
    print(canonical({"verified": True, "generation": config["publicTrust"]["generation"], "trust_digest": digest(config["publicTrust"])}))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Avoid exception reprs from parsers/subprocesses that might contain
        # private input. Unit tests expose typed errors using synthetic data.
        raise SystemExit("Agent Edge trust reconciliation failed; no private data emitted") from None
