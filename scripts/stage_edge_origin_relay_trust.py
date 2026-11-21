#!/usr/bin/env python3
"""Stage only offline-leaf trust through the declared production lane.

No Kubernetes object, Worker process or lease is changed here. A bounded
installer runs in the already authorized Worker cgroup and writes only to its
attested slot mount. Credentials enter via stdin, never the command line.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import re
import subprocess


STATE = "/var/lib/fugue/edge-owned"
VOLUME = "worker-state"
MOUNT = "/var/lib/fugue/edge"
DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
UID = re.compile(r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}")
IMAGE = re.compile(r"[a-z0-9][a-z0-9.:/-]*@sha256:[0-9a-f]{64}")
LIMIT = 48 << 10

# No teardown/restart, additional pod allocation, token, or caller-selected
# filesystem path. In-container identity files are init-created and read-only.
CHECK = r'''
root=/var/lib/fugue/edge
identity=/var/run/fugue/edge-identity
[ "$(cat "$identity/edge_id")" = "$1" ]
[ "$(cat "$identity/slot")" = "$2" ]
[ "$(cat "$identity/edge_group_id")" = "$3" ]
owner=$(id -u)
[ ! -L "$root" ]
[ -d "$root" ]
[ -w "$root" ]
parent="$root/origin-relay-v1"
epoch="$parent/$4"
for dir in "$parent" "$epoch"; do
  [ ! -L "$dir" ]
  if [ -e "$dir" ]; then
    [ -d "$dir" ]
    [ "$(stat -c '%u:%a' "$dir")" = "$owner:700" ]
  fi
done
'''
INSTALL = CHECK + r'''
umask 077
for dir in "$parent" "$epoch"; do
  if [ ! -d "$dir" ]; then mkdir -m 0700 -- "$dir"; fi
  [ ! -L "$dir" ]
  [ "$(stat -c '%u:%a' "$dir")" = "$owner:700" ]
done
stage=$(mktemp -d "$epoch/.incoming.XXXXXXXX")
trap 'rm -f -- "$stage/ca.pem" "$stage/leaf.pem" "$stage/key.pem" "$stage/policy-verify-key"; rmdir -- "$stage" 2>/dev/null || true' EXIT
for f in ca.pem leaf.pem key.pem policy-verify-key; do
  IFS= read -r line
  printf '%s' "$line" | base64 -d >"$stage/$f"
  [ -s "$stage/$f" ]
done
if IFS= read -r extra; then exit 21; fi
trust="$epoch/trust"
[ ! -L "$trust" ]
if [ -e "$trust" ]; then
  [ -d "$trust" ]
  [ "$(stat -c '%u:%a' "$trust")" = "$owner:700" ]
  for f in ca.pem leaf.pem key.pem policy-verify-key; do
    [ ! -L "$trust/$f" ]
    [ -f "$trust/$f" ]
    [ "$(stat -c '%u:%a:%h' "$trust/$f")" = "$owner:600:1" ]
    cmp -s "$stage/$f" "$trust/$f"
  done
else
  # -T never treats an unexpectedly appearing trust directory as a container;
  # rename into a nonempty existing directory fails rather than overwriting it.
  sync "$stage/ca.pem" "$stage/leaf.pem" "$stage/key.pem" "$stage/policy-verify-key" "$stage"
  mv -T -- "$stage" "$trust"
  sync "$epoch" "$parent" "$root"
fi
printf 'relay-trust-staged\n'
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
    keys = {"schema", "namespace", "group", "ca_sha256", "policy_verify_key", "targets"}
    if not isinstance(d, dict) or set(d) != keys or d["schema"] != "fugue.edge.origin-relay-trust-stage/v2":
        raise ValueError("declaration schema invalid")
    if not (matches(DNS_LABEL, d["namespace"]) and matches(DNS_LABEL, d["group"])
            and matches(DIGEST, d["ca_sha256"]) and matches(re.compile(r"[0-9a-f]{64}"), d["policy_verify_key"])):
        raise ValueError("declaration identities invalid")
    targets = d["targets"]
    if not isinstance(targets, list) or not 2 <= len(targets) <= 4:
        raise ValueError("stage authorizes no more than two nodes and two slots each")
    names, recipients, nodes, workloads = set(), set(), {}, {}
    for target in targets:
        fields = {"edge", "slot", "tls_name", "leaf_sha256", "node_uid", "workload", "workload_uid", "retained_image"}
        if not isinstance(target, dict) or set(target) != fields or not (
            matches(DNS_LABEL, target["edge"]) and target["slot"] in ("a", "b") and matches(DNS_LABEL, target["workload"])
            and matches(UID, target["node_uid"]) and matches(UID, target["workload_uid"]) and matches(DIGEST, target["leaf_sha256"])
            and matches(IMAGE, target["retained_image"])
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


def attest_workload(d, target, node, daemonset):
    nm, dm = node["metadata"], daemonset["metadata"]
    if (nm.get("name"), nm.get("uid")) != (target["edge"], target["node_uid"]) or nm.get("deletionTimestamp") or not ready(node):
        raise ValueError("declared node is not the exact ready node")
    health = {c["type"]: c["status"] for c in node["status"]["conditions"]}
    if any(health.get(c) != "False" for c in ("MemoryPressure", "DiskPressure", "PIDPressure")):
        raise ValueError("declared node has resource pressure or unknown resource state")
    if (dm.get("namespace"), dm.get("name"), dm.get("uid")) != (d["namespace"], target["workload"], target["workload_uid"]) or dm.get("deletionTimestamp"):
        raise ValueError("declared slot workload changed identity")
    return attest_mount(d, target, daemonset["spec"]["template"]["spec"])


def attest_mount(d, target, spec):
    vol = [v for v in spec["volumes"] if v.get("name") == VOLUME]
    workers = [c for c in spec["containers"] if c.get("name") == "edge"]
    if len(vol) != 1 or vol[0].get("hostPath") != {"path": slot_home(d, target), "type": "DirectoryOrCreate"} or len(workers) != 1:
        raise ValueError("slot does not own the declared state directory")
    m = [a for a in workers[0]["volumeMounts"] if a.get("name") == VOLUME]
    if len(m) != 1 or m[0].get("mountPath") != MOUNT or any(m[0].get(k) for k in ("subPath", "subPathExpr", "readOnly", "mountPropagation")):
        raise ValueError("worker does not own the exact writable state mount")
    return workers[0]


def owned_slot(d, target, pod):
    meta, spec = pod["metadata"], pod["spec"]
    return meta.get("namespace") == d["namespace"] and spec.get("nodeName") == target["edge"] and any(
        ref.get("controller") is True and ref.get("kind") == "DaemonSet" and ref.get("apiVersion") == "apps/v1"
        and (ref.get("name"), ref.get("uid")) == (target["workload"], target["workload_uid"])
        for ref in meta.get("ownerReferences", [])
    )


def attest_retained(d, target, pod, template_worker):
    meta, spec = pod["metadata"], pod["spec"]
    if (not owned_slot(d, target, pod) or not ready(pod) or meta.get("deletionTimestamp")
            or meta.get("labels", {}).get("fugue.io/edge-slot") != target["slot"]
            or not matches(DNS_LABEL, meta.get("name")) or not matches(UID, meta.get("uid"))):
        raise ValueError("retained Pod identity is not exact and Ready")
    if spec.get("automountServiceAccountToken") is not False or any(spec.get(k) for k in (
            "hostNetwork", "hostPID", "hostIPC", "shareProcessNamespace", "ephemeralContainers")):
        raise ValueError("retained Pod is not confined")
    worker = attest_mount(d, target, spec)
    security = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
    status = [c for c in pod["status"].get("containerStatuses", []) if c.get("name") == "edge"]
    if (worker.get("securityContext") != security or worker.get("resources") != template_worker.get("resources")
            or not worker.get("resources", {}).get("limits", {}).get("memory") or len(status) != 1):
        raise ValueError("retained container is not confined to the declared Worker budget")
    c = status[0]
    if (worker.get("image") != target["retained_image"] or not isinstance(c.get("imageID"), str)
            or c["imageID"].removeprefix("docker-pullable://") != target["retained_image"]
            or c.get("ready") is not True or c.get("started") is not True or not c.get("state", {}).get("running")
            or not isinstance(c.get("containerID"), str) or not c["containerID"]):
        raise ValueError("exact pinned Worker container is not running and ready")
    return meta["name"], meta["uid"], c["containerID"]


def read_checkpoint(d, target):
    node = strict_json(kubectl("get", "node", target["edge"], "-o", "json"))
    ds = strict_json(kubectl("-n", d["namespace"], "get", "daemonset", target["workload"], "-o", "json"))
    template_worker = attest_workload(d, target, node, ds)
    pods = strict_json(kubectl("-n", d["namespace"], "get", "pods", "--field-selector", f'spec.nodeName={target["edge"]}', "-o", "json"))["items"]
    slots = [p for p in pods if owned_slot(d, target, p)]
    if len(slots) != 1:
        raise ValueError("expected exactly one retained slot Pod")
    return attest_retained(d, target, slots[0], template_worker)


def worker_exec(d, target, pod, script, data=None):
    command = ["-n", d["namespace"], "exec"]
    if data is not None:
        command.append("-i")
    command += [pod, "-c", "edge", "--", "/bin/sh", "-ec", script, "edge-relay-trust",
                target["edge"], target["slot"], d["group"], d["ca_sha256"].removeprefix("sha256:")]
    return kubectl(*command, data=data)


def stage(d, checked):
    targets = list(zip(d["targets"], checked["packages"], strict=True))
    witnesses = []
    # Complete read-only checks for all recipients before streaming any leaf.
    for t, _ in targets:
        witness = read_checkpoint(d, t)
        if worker_exec(d, t, witness[0], CHECK + "\nprintf 'relay-trust-checked\\n'\n") != b"relay-trust-checked\n":
            raise ValueError("retained Worker identity and filesystem check rejected")
        witnesses.append(witness)
    receipt = []
    for (t, p), witness in zip(targets, witnesses, strict=True):
        if read_checkpoint(d, t) != witness:
            raise ValueError("retained recipient changed before delivery")
        data = b"".join(base64.b64encode(s.encode()) + b"\n" for s in (checked["ca_pem"], p["cert_pem"], p["key_pem"], checked["policy_verify_key"] + "\n"))
        ack = worker_exec(d, t, witness[0], INSTALL, data=data)
        if ack != b"relay-trust-staged\n" or read_checkpoint(d, t) != witness:
            raise ValueError("private delivery acknowledgment is missing or recipient changed")
        receipt.append({"edge": t["edge"], "slot": t["slot"], "node_uid": t["node_uid"], "leaf_sha256": t["leaf_sha256"],
                        "pod_uid": witness[1], "retained_image": t["retained_image"]})
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
