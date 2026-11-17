"""Stream kubectl JSONPath records, returning only possible target references.

Filtering runs beside kubectl, not after transmitting the entire workload
inventory over SSH. Go performs the authoritative typed checks on the result.
"""
import json
import posixpath
import sys

claim, target, pv_uid = sys.argv[1:]


def overlap(a, b):
    a, b = posixpath.normpath(a), posixpath.normpath(b)
    return a == b or a.startswith(b.rstrip("/") + "/") or b.startswith(a.rstrip("/") + "/")


def reference(value):
    if isinstance(value, dict):
        source = value.get("hostPath", {}).get("path")
        if source and overlap(source, target):
            return True
        return any(reference(child) for child in value.values())
    if isinstance(value, list):
        return any(reference(child) for child in value)
    return isinstance(value, str) and ((bool(claim) and value == claim) or target in value)


items = []
count = 0
while True:
    line = sys.stdin.buffer.readline((8 << 20) + 1)
    if not line:
        break
    count += 1
    if len(line) > 8 << 20 or count > 500000:
        raise ValueError("reference inventory exceeds safety bound")
    item = json.loads(line)
    spec = item.get("spec", {})
    include = reference(spec)
    if item.get("kind") == "PersistentVolume":
        include = item.get("metadata", {}).get("uid") == pv_uid or any(
            overlap(spec[kind]["path"], target)
            for kind in ("local", "hostPath") if spec.get(kind, {}).get("path"))
    if item.get("kind") == "StatefulSet":
        for template in spec.get("volumeClaimTemplates", []):
            prefix = template["metadata"]["name"] + "-" + item["metadata"]["name"] + "-"
            include = include or (claim.startswith(prefix) and claim[len(prefix):].isdigit())
    if include:
        item.get("metadata", {}).pop("managedFields", None)
        items.append(item)
print(json.dumps({"items": items}))
