#!/usr/bin/env python3
"""Stage only offline-leaf trust through the declared production lane.

No worker, Service, NetworkPolicy, Secret or lease is changed here. Each
ephemeral installer mounts only the intended node's exact slot relay directory.
Private material is streamed over authenticated Kubernetes exec, not embedded
in a Pod/Job object, command line, CI receipt or artifact.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time


MANAGER = "fugue-edge-relay-trust"
STATE = "/var/lib/fugue/edge-owned"
VOLUME = "worker-state"
WORKDIR = "origin-relay-v1"
MOUNT = "/relay"
FILES = ("ca.pem", "leaf.pem", "key.pem", "policy-verify-key")
DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
UID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
IMAGE = re.compile(r"[a-z0-9][a-z0-9.:/-]*@sha256:[0-9a-f]{64}")
LIMIT = 48 << 10

IDLE = 'i=0; until [ -f /exit/success ]; do [ "$i" -lt 150 ] || exit 1; i=$((i+1)); sleep 1; done'
INSTALL = r'''
umask 077
chmod 0700 /relay
stage=$(mktemp -d /relay/.incoming.XXXXXXXX)
trap 'rm -f -- "$stage/ca.pem" "$stage/leaf.pem" "$stage/key.pem" "$stage/policy-verify-key"; rmdir -- "$stage" 2>/dev/null || true' EXIT
for f in ca.pem leaf.pem key.pem policy-verify-key; do
  IFS= read -r line
  printf '%s' "$line" | base64 -d >"$stage/$f"
  [ -s "$stage/$f" ]
done
if IFS= read -r extra; then exit 21; fi
[ ! -L /relay/trust ]
if [ -e /relay/trust ]; then
  [ -d /relay/trust ]
  [ "$(stat -c '%a' /relay/trust)" = 700 ]
  for f in ca.pem leaf.pem key.pem policy-verify-key; do
    [ ! -L "/relay/trust/$f" ] && [ -f "/relay/trust/$f" ]
    [ "$(stat -c '%a' "/relay/trust/$f")" = 600 ]
    cmp -s "$stage/$f" "/relay/trust/$f"
  done
else
  # -T never treats an unexpectedly appearing trust directory as a container;
  # rename into a nonempty existing directory fails rather than overwriting it.
  sync "$stage/ca.pem" "$stage/leaf.pem" "$stage/key.pem" "$stage/policy-verify-key" "$stage"
  mv -T -- "$stage" /relay/trust
  sync /relay
fi
printf 'relay-trust-staged\n'
touch /exit/success
'''


def canonical(obj):
    return json.dumps(obj, sort_keys=True, separators=(",", ":")).encode()


def strict_json(raw):
    def unique(items):
        value = {}
        for key, item in items:
            if key in value:
                raise ValueError("duplicate field")
            value[key] = item
        return value
    return json.loads(raw, object_pairs_hook=unique)


def matches(pattern, value):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def loc(target):
    return target["edge"], target["slot"]


def validate_declaration(d):
    keys = {"schema", "namespace", "group", "installer_image", "ca_sha256", "policy_verify_key", "targets"}
    if not isinstance(d, dict) or set(d) != keys or d["schema"] != "fugue.edge.origin-relay-trust-stage/v1":
        raise ValueError("declaration schema invalid")
    if not (matches(DNS_LABEL, d["namespace"]) and matches(DNS_LABEL, d["group"]) and matches(IMAGE, d["installer_image"])
            and matches(DIGEST, d["ca_sha256"]) and matches(re.compile(r"[0-9a-f]{64}"), d["policy_verify_key"])):
        raise ValueError("declaration identities invalid")
    targets = d["targets"]
    if not isinstance(targets, list) or not 2 <= len(targets) <= 4:
        raise ValueError("stage authorizes no more than two nodes and two slots each")
    names, recipients, nodes, workloads = set(), set(), {}, {}
    for target in targets:
        fields = {"edge", "slot", "tls_name", "leaf_sha256", "node_uid", "workload", "workload_uid"}
        if not isinstance(target, dict) or set(target) != fields or not (
            matches(DNS_LABEL, target["edge"]) and target["slot"] in ("a", "b") and matches(DNS_LABEL, target["workload"])
            and matches(UID, target["node_uid"]) and matches(UID, target["workload_uid"]) and matches(DIGEST, target["leaf_sha256"])
            and isinstance(target["tls_name"], str) and 1 <= len(target["tls_name"]) <= 253
        ):
            raise ValueError("invalid exact slot target")
        if loc(target) in recipients or target["tls_name"] in names:
            raise ValueError("ambiguous slot target")
        recipients.add(loc(target))
        names.add(target["tls_name"])
        if nodes.setdefault(target["edge"], target["node_uid"]) != target["node_uid"] or workloads.setdefault(
            target["slot"], (target["workload"], target["workload_uid"])
        ) != (target["workload"], target["workload_uid"]):
            raise ValueError("inconsistent exact slot ownership")
    if len(nodes) != 2 or len(set(nodes.values())) != 2 or len(set(workloads.values())) != len(workloads):
        raise ValueError("exactly two independent nodes required")
    return d


def pair_packages(d, private):
    if not isinstance(private, dict) or set(private) != {"ca_pem", "packages"} or not isinstance(private["ca_pem"], str):
        raise ValueError("expected leaf-only private material")
    packages = private["packages"]
    if not isinstance(packages, list) or len(packages) != len(d["targets"]):
        raise ValueError("package set size does not match declaration")
    by_peer = {}
    for p in packages:
        if not isinstance(p, dict) or set(p) != {"edge", "slot", "cert_pem", "key_pem"} or any(not isinstance(v, str) for v in p.values()) or loc(p) in by_peer:
            raise ValueError("unexpected private package fields or duplicate recipient")
        by_peer[loc(p)] = {"cert_pem": p["cert_pem"], "key_pem": p["key_pem"]}
    if set(by_peer) != {loc(t) for t in d["targets"]}:
        raise ValueError("package recipient set differs from declaration")
    return {"ca_pem": private["ca_pem"], "ca_sha256": d["ca_sha256"], "policy_verify_key": d["policy_verify_key"],
            "packages": [{**{key: t[key] for key in ("edge", "slot", "tls_name", "leaf_sha256")}, **by_peer[loc(t)]} for t in d["targets"]]}


def run(args, data=None, timeout=20):
    result = subprocess.run(args, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode:
        # API errors can contain user-controlled input. Do not print them.
        raise RuntimeError("exact release operation rejected")
    return result.stdout


def kubectl(*args, data=None, timeout=20):
    return run(["kubectl", "--request-timeout=15s", *args], data=data, timeout=timeout)


def ready(obj):
    return any(c.get("type") == "Ready" and c.get("status") == "True" for c in obj.get("status", {}).get("conditions", []))


def slot_home(d, target):
    return f'{STATE}/{d["group"]}/slot-{target["slot"]}'


def trust_home(d, target):
    # Rotation stages a new immutable directory. Previous epochs and their
    # withdrawal fences are not overwritten by a newer certificate package.
    return f'{slot_home(d, target)}/{WORKDIR}/{d["ca_sha256"].removeprefix("sha256:")}'


def attest(d, target, node, daemonset):
    nm, dm = node["metadata"], daemonset["metadata"]
    if (nm.get("name"), nm.get("uid")) != (target["edge"], target["node_uid"]) or nm.get("deletionTimestamp") or not ready(node):
        raise ValueError("declared node is not the exact ready node")
    if (dm.get("namespace"), dm.get("name"), dm.get("uid")) != (d["namespace"], target["workload"], target["workload_uid"]) or dm.get("deletionTimestamp"):
        raise ValueError("declared slot workload changed identity")
    spec = daemonset["spec"]["template"]["spec"]
    vol = [v for v in spec["volumes"] if v.get("name") == VOLUME]
    workers = [c for c in spec["containers"] if c.get("name") == "edge"]
    if len(vol) != 1 or vol[0].get("hostPath") != {"path": slot_home(d, target), "type": "DirectoryOrCreate"} or len(workers) != 1:
        raise ValueError("slot does not own the declared state directory")
    m = [a for a in workers[0]["volumeMounts"] if a.get("name") == VOLUME]
    if len(m) != 1 or m[0].get("mountPath") != "/var/lib/fugue/edge" or m[0].get("subPath") or m[0].get("subPathExpr") or m[0].get("readOnly"):
        raise ValueError("worker does not own the exact writable state mount")
    return workers[0]["image"]


def read_checkpoint(d, target):
    node = strict_json(kubectl("get", "node", target["edge"], "-o", "json"))
    ds = strict_json(kubectl("-n", d["namespace"], "get", "daemonset", target["workload"], "-o", "json"))
    image = attest(d, target, node, ds)
    pods = strict_json(kubectl("-n", d["namespace"], "get", "pods", "--field-selector", f'spec.nodeName={target["edge"]}', "-o", "json"))["items"]
    slots = [p for p in pods if any(ref.get("controller") and ref.get("kind") == "DaemonSet" and ref.get("uid") == target["workload_uid"]
                                    for ref in p["metadata"].get("ownerReferences", []))]
    if len(slots) != 1 or not ready(slots[0]):
        raise ValueError("expected one ready slot Pod owned by the exact workload on the target node")
    pod = slots[0]
    if pod["metadata"].get("deletionTimestamp") or pod["spec"].get("nodeName") != target["edge"]:
        raise ValueError("slot Pod is not retained on the exact node")
    # The live slot may still serve its previous image (OnDelete); only the
    # volume needs to agree. The signed release handles code promotion.
    observed = {"metadata": ds["metadata"], "spec": {"template": {"spec": pod["spec"]}}}
    attest(d, target, node, observed)
    return image


def installer(d, t, run_id):
    identity = hashlib.sha256(canonical([t["node_uid"], t["slot"], t["leaf_sha256"], run_id])).hexdigest()[:24]
    labels = {"app.kubernetes.io/managed-by": MANAGER, "fugue.dev/relay-trust-stage": identity}
    return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": f"relay-trust-{identity}", "namespace": d["namespace"], "labels": labels},
            "spec": {"activeDeadlineSeconds": 180, "backoffLimit": 0, "ttlSecondsAfterFinished": 300,
                     "template": {"metadata": {"labels": labels}, "spec": {
                         "nodeName": t["edge"], "automountServiceAccountToken": False, "enableServiceLinks": False,
                         "restartPolicy": "Never", "terminationGracePeriodSeconds": 1,
                         "securityContext": {"runAsUser": 0, "runAsGroup": 0, "seccompProfile": {"type": "RuntimeDefault"}},
                         "containers": [{"name": "installer", "image": d["installer_image"], "command": ["/bin/sh", "-ec", IDLE],
                                         "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                                         "resources": {"requests": {"cpu": "10m", "memory": "8Mi"}, "limits": {"cpu": "100m", "memory": "32Mi"}},
                                         "volumeMounts": [{"name": "relay", "mountPath": MOUNT}, {"name": "exit", "mountPath": "/exit"}]}],
                         "volumes": [{"name": "relay", "hostPath": {"path": trust_home(d, t), "type": "DirectoryOrCreate"}},
                                     {"name": "exit", "emptyDir": {"medium": "Memory", "sizeLimit": "1Mi"}}]}}}}


def verify_installer_pod(pod, desired, job_uid):
    meta, actual = pod["metadata"], pod["spec"]
    expected = desired["spec"]["template"]["spec"]
    if meta.get("deletionTimestamp") or meta.get("namespace") != desired["metadata"]["namespace"] or not meta.get("uid") or not any(
        ref.get("kind") == "Job" and ref.get("uid") == job_uid and ref.get("controller") for ref in meta.get("ownerReferences", [])
    ):
        raise ValueError("installer Pod is not owned by the exact Job")
    for key in ("nodeName", "automountServiceAccountToken", "enableServiceLinks", "restartPolicy", "securityContext", "volumes"):
        if actual.get(key) != expected[key]:
            raise ValueError("installer Pod mount, node or security contract differs")
    if actual.get("initContainers") or actual.get("ephemeralContainers") or any(actual.get(k) for k in ("hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace")):
        raise ValueError("unexpected installer privileges or additional container")
    c = actual["containers"]
    a = expected["containers"][0]
    if len(c) != 1 or any(c[0].get(k) != a[k] for k in ("name", "image", "command", "securityContext", "volumeMounts")) or any(c[0].get(k) for k in ("env", "envFrom", "args", "ports", "volumeDevices", "lifecycle")):
        raise ValueError("installer execution contract differs")


def stage(d, checked):
    ns = d["namespace"]
    jobs = [(t, p, installer(d, t, secrets.token_hex(12))) for t, p in zip(d["targets"], checked["packages"], strict=True)]
    # Validate every target and server-side Job shape before creating any Job.
    images = {read_checkpoint(d, t) for t, _, _ in jobs}
    if d["installer_image"] not in images:
        raise ValueError("installer image not declared by a witnessed slot workload")
    for _, _, job in jobs:
        kubectl("create", "--dry-run=server", "-f", "-", data=canonical(job))
    receipt = []
    for t, p, job in jobs:
        read_checkpoint(d, t)
        created = strict_json(kubectl("create", "-f", "-", "-o", "json", data=canonical(job)))
        name, uid = created["metadata"]["name"], created["metadata"]["uid"]
        if name != job["metadata"]["name"] or not matches(UID, uid):
            raise ValueError("created Job identity differs from declaration")
        match = f"batch.kubernetes.io/controller-uid={uid}"
        deadline = time.monotonic() + 45
        while True:
            pods = strict_json(kubectl("-n", ns, "get", "pods", "-l", match, "-o", "json"))["items"]
            if len(pods) > 1:
                raise ValueError("installer produced duplicate Pods")
            if pods:
                verify_installer_pod(pods[0], job, uid)
                if ready(pods[0]):
                    break
            if time.monotonic() >= deadline:
                raise ValueError("installer Pod did not become ready")
            time.sleep(1)
        pod = pods[0]["metadata"]
        read_checkpoint(d, t)
        data = b"".join(base64.b64encode(s.encode()) + b"\n" for s in (checked["ca_pem"], p["cert_pem"], p["key_pem"], checked["policy_verify_key"] + "\n"))
        ack = kubectl("-n", ns, "exec", "-i", pod["name"], "-c", "installer", "--", "/bin/sh", "-ec", INSTALL, data=data)
        if ack != b"relay-trust-staged\n":
            raise ValueError("private delivery acknowledgement is missing")
        kubectl("-n", ns, "wait", "--for=condition=complete", f"job/{name}", "--timeout=10s")
        receipt.append({"edge": t["edge"], "slot": t["slot"], "node_uid": t["node_uid"], "leaf_sha256": t["leaf_sha256"], "job_uid": uid, "pod_uid": pod["uid"]})
    return {"staged": receipt, "ca_sha256": d["ca_sha256"], "policy_verify_key": d["policy_verify_key"], "lease_published": False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--validator", required=True)
    options = parser.parse_args()
    raw = Path(options.declaration).read_bytes()
    if len(raw) > LIMIT:
        raise ValueError("declaration too large")
    d = validate_declaration(strict_json(raw))
    private = os.environ.pop("FUGUE_ORIGIN_RELAY_LEAF_PACKAGES", "")
    if not private or len(private.encode()) > LIMIT:
        raise ValueError("bounded leaf-only package is required")
    checked = pair_packages(d, strict_json(private))
    run([options.validator, "check"], canonical(checked))
    print(canonical(stage(d, checked)).decode())


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Deliberately sanitize even parser and subprocess exception details.
        raise SystemExit("Edge origin relay trust staging failed closed; no private or lease data emitted") from None
