"""Read an exact rollback and two retained DNS assignments. Never write a listener.

This operation intentionally has no --apply, Kubernetes exec, secret reads,
verification overrides, or positive serving receipts. API host comes from the
existing protected production setting, never from a supplied document.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import os
from pathlib import Path
import re

from scripts import reconcile_dns_transport as transport
from scripts.dns_transport_rollback_authority import validate_reference, validate_rollback_binding
from scripts.recover_dns_transport import read_recovery_api, validate_api_origin


def exact(pattern, value):
    return isinstance(value, str) and re.fullmatch(pattern, value) is not None


def inspect(d, domain):
    if (set(d) != {"schema","namespace","node","node_uid","api_address","backends","rollback"} or
            d["schema"] != "fugue.dns.rollback-ledger-inspection/v1" or
            not all(exact(r"[a-z0-9][a-z0-9.-]{0,252}", d[k]) for k in ("namespace","node")) or
            not exact(r"[a-z0-9][a-z0-9.-]{0,252}", domain)):
        raise ValueError("invalid inspection or protected API host")
    uid = r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}"
    api = {"origin":"https://"+domain, "api_address":d["api_address"]}
    validate_api_origin(api)
    ref = d["rollback"]
    validate_reference(ref)
    pair = d["backends"]
    if not exact(uid,d["node_uid"]) or not isinstance(pair,list) or len(pair)!=2:
        raise ValueError("require one exact node and two retained recipients")
    for b in pair:
        if (not isinstance(b,dict) or set(b)!={"component","pod_uid","service_account","revision","image","target_port"} or
                not exact(uid,b["pod_uid"]) or not exact(r"[a-z0-9][a-z0-9.-]{0,62}",b["component"]) or
                not exact(r"[a-z0-9][a-z0-9.-]{0,252}",b["service_account"]) or not exact(r"[0-9a-f]{40}",b["revision"]) or
                not exact(r"[a-z0-9][a-z0-9./:_-]{1,245}@sha256:[0-9a-f]{64}",b["image"]) or
                type(b["target_port"]) is not int or not 1<=b["target_port"]<=65535):
            raise ValueError("invalid retained recipient")
    if any(len({b[k] for b in pair})!=2 for k in ("component","pod_uid","service_account")):
        raise ValueError("candidate and fallback are not independent recipients")

    def check_node():
        n = transport.read_json("get","node",d["node"],"-o","json")["metadata"]
        if n["uid"]!=d["node_uid"] or n["name"]!=d["node"] or n.get("deletionTimestamp"):
            raise ValueError("inspection node replaced")

    def snapshot(b):
        seen = transport.handoff_backend(d["namespace"],{"app.kubernetes.io/component":b["component"]},d["node"],b["target_port"])
        m, s = seen["pod"]["metadata"], seen["pod"]["spec"]
        if (m["uid"]!=b["pod_uid"] or s["serviceAccountName"]!=b["service_account"] or
                m["annotations"].get("fugue.pro/oci-revision")!=b["revision"] or
                [c["image"] for c in s["containers"] if c["name"]=="dns"] != [b["image"]]):
            raise ValueError("retained DNS process identity changed")
        return seen

    check_node()
    with ThreadPoolExecutor(max_workers=2) as pool:
        seen = list(pool.map(snapshot, pair))
    lkg = read_recovery_api(api, "/v1/admin/artifacts/"+ref["artifact_id"]+"/lkg")["lkg"]
    full = read_recovery_api(api, "/v1/platform-state/artifacts/release_set?scope_key=global&channel=full&include_current_rollback_message=true")
    msg = full.get("current_rollback_message") or {}
    receipt = {"schema":d["schema"],"observed_at":datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "current_release":full.get("release",{}).get("id"),"current_message":msg.get("id"),
               "verification_state":full.get("release",{}).get("verification_state"),
               "authorizes_traffic":False, "recipients":[]}
    for b, observed in zip(pair, seen):
        m = observed["pod"]["metadata"]
        current = transport.read_json("get","pod",m["name"],"-n",d["namespace"],"-o","json")["metadata"]
        if any(current.get(k)!=m.get(k) for k in ("uid","resourceVersion")) or current.get("deletionTimestamp"):
            raise ValueError("retained DNS Pod changed during ledger inspection")
        accepted = True
        try:
            validate_rollback_binding(observed["snapshot"],full,lkg,ref)
        except ValueError:
            accepted = False
        receipt["recipients"].append({"pod_uid":b["pod_uid"],"ready_at_read":observed["snapshot"].get("ready") is True,
                                      "exact_rollback_bound":accepted})
    check_node()
    return receipt


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("declaration")
    args = p.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw)>8192: raise SystemExit("inspection declaration exceeds bound")
    receipt = inspect(json.loads(raw),os.environ.get("FUGUE_LEDGER_API_DOMAIN",""))
    print(json.dumps(receipt,sort_keys=True),flush=True)
    if not all(r["exact_rollback_bound"] for r in receipt["recipients"]):
        raise SystemExit("inspection did not establish both current rollback bindings; no traffic authorized")
