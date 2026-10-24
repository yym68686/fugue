"""Exact DNS lane/LKG witnesses; the API repeats these checks atomically."""
import re

from scripts.bootstrap_cell_producer import authority_identity, digest, selected
from scripts.observe_front_candidate import now, timestamp

ID = re.compile(r"[a-zA-Z0-9_.-]{1,256}")
HASH = re.compile(r"sha256:[a-f0-9]{64}")


def validate_ref(ref):
    if not isinstance(ref, dict) or set(ref) != {"artifact_id", "content_hash", "release_id", "fencing_token"} or any(not ID.fullmatch(ref.get(k, "")) for k in ["artifact_id", "release_id"]) or not HASH.fullmatch(ref.get("content_hash", "")) or type(ref["fencing_token"]) is not int or ref["fencing_token"] < 1:
        raise ValueError("exact DNS publication reference required")


def validate(guard):
    if not isinstance(guard, dict) or set(guard) != {"lanes", "lkg_id", "lkg_publication", "verification_evidence_hash"} or not ID.fullmatch(guard.get("lkg_id", "")) or not HASH.fullmatch(guard.get("verification_evidence_hash", "")):
        raise ValueError("exact DNS lane and positive LKG precondition required")
    lanes = guard["lanes"]
    if not isinstance(lanes, dict) or set(lanes) != {"shadow", "gray", "full"} or lanes["shadow"] is None or lanes["full"] is None:
        raise ValueError("all DNS lanes must be declared")
    for ref in [guard["lkg_publication"], *lanes.values()]:
        if ref is not None:
            validate_ref(ref)
    return guard


def key(artifact, channel, canary, guard):
    return "dns-publication/" + digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "release_channel": channel, "canary_rule_ref": canary, "precondition": guard})


def check(api, scope, guard, successor=None, channel=None, canary=""):
    validate(guard)
    for lane, expected in guard["lanes"].items():
        state = selected(api, scope, lane, "release_set")
        if authority_identity(state) == expected:
            continue
        a, r = state.get("artifact", {}), state.get("release", {})
        fence = 0 if expected is None else expected["fencing_token"]
        if lane == channel and successor is not None and a.get("id") == successor["id"] and a.get("content_hash") == successor["content_hash"] and r.get("status") == "active" and r.get("fencing_token") == fence + 1 and r.get("idempotency_key") == key(successor, channel, canary, guard):
            continue
        raise ValueError("DNS publication lane changed: " + lane)
    ref = guard["lkg_publication"]
    lkg = api("GET", "/v1/admin/artifacts/" + ref["artifact_id"] + "/lkg").get("lkg")
    if not lkg or lkg.get("id") != guard["lkg_id"] or lkg.get("artifact_kind") != "release_set" or lkg.get("scope_key") != scope or lkg.get("artifact_id") != ref["artifact_id"] or lkg.get("content_hash") != ref["content_hash"] or lkg.get("verified_by_release_id") != ref["release_id"] or lkg.get("verification_evidence_hash") != guard["verification_evidence_hash"] or timestamp(lkg["expires_at"]) <= now():
        raise ValueError("DNS positive LKG changed or expired")
    return lkg
