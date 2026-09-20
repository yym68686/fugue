#!/usr/bin/env python3
"""Publish diagnostic configuration independently of Fugue code compilation.

Only this publisher's ConfigMaps, signing Secret and read-only observer RBAC
are changed. Existing serving configuration and workloads are never touched.
"""
import argparse
import base64
import copy
import hashlib
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path

OWNER = "fugue.pro/diagnostic-catalog"
CATALOG = "fugue-diagnostic-catalog"
TRUST = "fugue-diagnostic-trust"
KEY = CATALOG + "-signer"
IMAGE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}$")
NAME = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
PKCS8_PREFIX = bytes.fromhex("302e020100300506032b657004220420")
SPKI_PREFIX = bytes.fromhex("302a300506032b6570032100")


def canonical(value):
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def command(args, data=None):
    result = subprocess.run(args, input=data, capture_output=True, timeout=30)
    if result.returncode:
        # Kubernetes input may contain a private signing key; never echo it.
        raise RuntimeError("command failed: " + " ".join(args[:3]))
    return result.stdout


def get(kind, name, namespace=None):
    args = ["kubectl", "get", kind, name, "--ignore-not-found", "-o", "json"]
    if namespace:
        args += ["-n", namespace]
    data = command(args)
    return json.loads(data) if data.strip() else None


def put(document, current):
    if current:
        if current.get("metadata", {}).get("labels", {}).get(OWNER) != "true":
            raise ValueError("refusing to replace diagnostic resource without publisher ownership")
        if all(current.get(key) == value for key, value in document.items() if key != "metadata"):
            return current
        previous_metadata = copy.deepcopy(current["metadata"])
        previous_metadata.setdefault("labels", {}).update(document["metadata"].get("labels", {}))
        document["metadata"] = previous_metadata
    return json.loads(command(["kubectl", "replace" if current else "create", "-f", "-", "-o", "json"], canonical(document)))


def metadata(name, namespace=None):
    out = {"name": name, "labels": {OWNER: "true"}}
    if namespace:
        out["namespace"] = namespace
    return out


def public_key(seed):
    with tempfile.TemporaryDirectory(prefix="fugue-diag-key-") as temp:
        key = Path(temp) / "key.der"
        key.write_bytes(PKCS8_PREFIX + seed)
        key.chmod(0o600)
        der = command(["openssl", "pkey", "-inform", "DER", "-in", str(key), "-pubout", "-outform", "DER"])
        if len(der) != len(SPKI_PREFIX) + 32 or not der.startswith(SPKI_PREFIX):
            raise ValueError("unexpected Ed25519 public key encoding")
        return der[-32:]


def sign(payload, key):
    if len(key) != 64 or public_key(key[:32]) != key[32:]:
        raise ValueError("invalid Ed25519 private key")
    with tempfile.TemporaryDirectory(prefix="fugue-diag-sign-") as temp:
        private = Path(temp) / "key.der"
        private.write_bytes(PKCS8_PREFIX + key[:32])
        private.chmod(0o600)
        source = Path(temp) / "payload"
        source.write_bytes(payload)
        return command(["openssl", "pkeyutl", "-sign", "-rawin", "-keyform", "DER", "-inkey", str(private), "-in", str(source)])


def verify(envelope, keys):
    public = base64.b64decode(keys[envelope["key_id"]], validate=True)
    payload = base64.b64decode(envelope["payload"], validate=True)
    signature = base64.b64decode(envelope["signature"], validate=True)
    if len(public) != 32 or len(signature) != 64 or len(payload) > 192 * 1024:
        raise ValueError("invalid diagnostic catalog signature envelope")
    with tempfile.TemporaryDirectory(prefix="fugue-diag-verify-") as temp:
        root = Path(temp)
        (root / "key").write_bytes(SPKI_PREFIX + public)
        (root / "payload").write_bytes(payload)
        (root / "signature").write_bytes(signature)
        command(["openssl", "pkeyutl", "-verify", "-pubin", "-keyform", "DER", "-inkey", str(root / "key"), "-rawin", "-in", str(root / "payload"), "-sigfile", str(root / "signature")])
    return json.loads(payload)


def validate(config, runner):
    if not NAME.fullmatch(config["namespace"]) or not config.get("reader_rules"):
        raise ValueError("namespace and reader rules must be explicit")
    if not IMAGE.fullmatch(runner) or not runner.startswith(config["repository"] + "@sha256:"):
        raise ValueError("runner is not a digest in the configured repository")
    catalog = config["catalog"]
    if catalog["runner_image"] == "$runner":
        catalog["runner_image"] = runner
    policy = catalog["policy"]
    if catalog["protocol"] != "fugue.diagnostics/v1" or catalog["generation"] < 1 or not IMAGE.fullmatch(catalog["runner_image"]):
        raise ValueError("invalid catalog protocol or runner")
    if not NAME.fullmatch(policy["service_account"]) or not policy["namespaces"] or not policy["profiles"]:
        raise ValueError("invalid observer policy")
    if any(n != "*" and not NAME.fullmatch(n) for n in policy["namespaces"]):
        raise ValueError("invalid target namespace policy")
    if not set(policy["profiles"]) <= {"host-read", "cluster-read", "process-profile"}:
        raise ValueError("unsupported capability profile")
    seen = set()
    if len(catalog["probes"]) > 64:
        raise ValueError("too many probes")
    for probe in catalog["probes"]:
        if probe["image"] == "$runner":
            probe["image"] = runner
        if not NAME.fullmatch(probe["id"]) or probe["id"] in seen:
            raise ValueError("invalid or repeated probe identity")
        seen.add(probe["id"])
        if not IMAGE.fullmatch(probe["image"]) or probe["profile"] not in policy["profiles"]:
            raise ValueError("probe image or capability profile is not authorized")
        if not probe["target_types"] or not set(probe["target_types"]) <= {"node", "node_process", "platform_component"}:
            raise ValueError("unsupported target class")
        if not 5 <= probe["max_duration_seconds"] <= 360 or len(canonical(probe.get("config", {}))) > 32768 or len(probe["parameters"]) > 32:
            raise ValueError("probe exceeds protocol budgets")
        for name, param in probe["parameters"].items():
            if not NAME.fullmatch(name.replace("_", "-")) or not 0 <= param.get("max_length", 0) <= 4096:
                raise ValueError("invalid parameter contract")
    for rule in config["reader_rules"]:
        if not set(rule["verbs"]) <= {"get", "list", "watch"} or rule.get("nonResourceURLs"):
            raise ValueError("observer policy must remain read-only")
        for resource in rule["resources"]:
            if resource in {"*", "secrets", "serviceaccounts"} or "exec" in resource or "proxy" in resource:
                raise ValueError("observer policy includes credentials or execution")
    if len(canonical(catalog)) > 190 * 1024:
        raise ValueError("catalog is too large")
    return catalog


def recover_catalog(data, keys):
    for name in ["catalog.json", "previous-catalog.json"]:
        if name not in data:
            continue
        try:
            candidate = verify(json.loads(data[name]), keys)
            if candidate.get("protocol") == "fugue.diagnostics/v1" and IMAGE.fullmatch(candidate.get("runner_image", "")):
                return candidate
        except (KeyError, ValueError, RuntimeError, TypeError):
            continue
    raise ValueError("no trusted published catalog is available")


def check_revision(previous, incoming):
    if not re.fullmatch(r"[a-f0-9]{40}", incoming):
        raise ValueError("catalog publication requires its exact Git source revision")
    if not previous or previous == incoming:
        return
    if not re.fullmatch(r"[a-f0-9]{40}", previous):
        raise ValueError("previous catalog has an invalid source revision")
    result = subprocess.run(["git", "merge-base", "--is-ancestor", previous, incoming], capture_output=True, timeout=15)
    if result.returncode:
        raise ValueError("refusing a stale or unrelated catalog publication; use a new main commit for recovery")


def publish(config, runner, apply):
    namespace = config["namespace"]
    current = get("configmap", CATALOG, namespace) if apply else None
    if current and current["metadata"].get("labels", {}).get(OWNER) != "true":
        raise ValueError("diagnostic catalog is not owned by the publisher")
    previous = None
    if current:
        trust = get("configmap", TRUST, namespace)
        previous = recover_catalog(current["data"], json.loads(trust["data"]["keys.json"]))
        if not runner:
            runner = previous["runner_image"]
    if apply:
        revision = os.environ.get("GITHUB_SHA", "")
        check_revision(previous.get("source_revision", "") if previous else "", revision)
        config["catalog"]["source_revision"] = revision
    if not runner and not apply:
        runner = config["repository"] + "@sha256:" + "a" * 64
    catalog = validate(config, runner)
    if not apply:
        print("diagnostic publication configuration is valid")
        return
    secret = get("secret", KEY, namespace)
    if secret:
        if secret["metadata"].get("labels", {}).get(OWNER) != "true":
            raise ValueError("diagnostic signing key is not owned by the publisher")
        key = base64.b64decode(secret["data"]["private_key"], validate=True)
    else:
        seed = os.urandom(32)
        key = seed + public_key(seed)
        secret = put({"apiVersion": "v1", "kind": "Secret", "metadata": metadata(KEY, namespace), "type": "Opaque", "data": {"private_key": base64.b64encode(key).decode()}}, None)
    key_id = hashlib.sha256(key[32:]).hexdigest()[:16]
    payload = canonical(catalog)
    envelope = {"key_id": key_id, "payload": base64.b64encode(payload).decode(), "signature": base64.b64encode(sign(payload, key)).decode()}
    keys = {key_id: base64.b64encode(key[32:]).decode()}
    verify(envelope, keys)
    account = catalog["policy"]["service_account"]
    objects = [
        ("serviceaccount", {"apiVersion": "v1", "kind": "ServiceAccount", "metadata": metadata(account, namespace), "automountServiceAccountToken": False}, namespace),
        ("clusterrole", {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "metadata": metadata(account), "rules": config["reader_rules"]}, None),
        ("clusterrolebinding", {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding", "metadata": metadata(account), "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": account}, "subjects": [{"kind": "ServiceAccount", "name": account, "namespace": namespace}]}, None),
    ]
    for kind, resource, scope in objects:
        put(resource, get(kind, account, scope))
    trusted = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata(TRUST, namespace), "data": {"keys.json": canonical(keys).decode()}}
    put(trusted, get("configmap", TRUST, namespace))
    values = {"catalog.json": canonical(envelope).decode()}
    if current:
        values["previous-catalog.json"] = current["data"]["catalog.json"]
        if current["data"]["catalog.json"] == values["catalog.json"]:
            print("diagnostic catalog already matches intent")
            return
    put({"apiVersion": "v1", "kind": "ConfigMap", "metadata": metadata(CATALOG, namespace), "data": values}, current)
    print("published diagnostic catalog:", len(catalog["probes"]), "probes; runner", catalog["runner_image"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--runner-image", default="")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    content = Path(args.config).read_bytes()
    if len(content) > 256 * 1024:
        raise ValueError("publication exceeds 256 KiB")
    publish(json.loads(content), args.runner_image, args.apply)


if __name__ == "__main__":
    main()
