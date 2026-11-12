"""Explicit dependency repair through the producer configuration CI lane.

No latest-policy inference: the reviewed declaration pins predecessor, serving
baseline, and the complete signed successor. The API repeats CAS under locks.
"""
import copy
import re
import time

from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.observe_front_candidate import now, timestamp
from scripts.reconfigure_cell_producer import validate_ref
from scripts.publish_agent_edge_shadow import ArtifactHTTPError

SCHEMA = "fugue.producer-dependency-refresh/v1"


def dns_preview(c, api, artifact):
    # A continuously publishing source may be between full selection and LKG
    # verification, or change its fence during a complete read. Retry ONLY the
    # read-only 503 preview, never artifact creation, release, or a CAS failure.
    # Eight attempts and a two-minute start budget bound the lane; the final
    # already-bounded HTTP read can add at most two minutes.
    deadline = time.monotonic() + 120
    for attempt in range(8):
        baseline(c, api)
        if authority_identity(selected(api, "platform-config-producer", "shadow")) != c["policy"]["dns_source"]["producer_policy"]:
            raise ValueError("approved source producer changed")
        try:
            return api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + artifact["id"] + "&view=configuration")
        except ArtifactHTTPError as error:
            if error.status != 503 or attempt == 7 or time.monotonic() + 10 >= deadline:
                raise
            time.sleep(10)


def validate(c):
    if set(c) != {"schema", "generation", "origin", "authority_cell_id", "precondition", "policy"} or c["schema"] != SCHEMA or type(c["generation"]) is not int or c["generation"] < 1:
        raise ValueError("explicit dependency refresh declaration required")
    if not re.fullmatch(r"https://[a-z0-9][a-z0-9.-]+[a-z0-9]", c["origin"]) or not re.fullmatch(r"cell-[a-z0-9]+(?:-[a-z0-9]+)*", c["authority_cell_id"]):
        raise ValueError("canonical origin and neutral cell required")
    r, p = c["precondition"], c["policy"]
    if set(r) != {"operation", "previous_policy", "serving_full", "verification_evidence_hash"} or r["operation"] not in ["placement_constraints", "dns_source"] or not re.fullmatch(r"sha256:[a-f0-9]{64}", r["verification_evidence_hash"]):
        raise ValueError("exact dependency operation and baseline required")
    for k in ["previous_policy", "serving_full"]:
        validate_ref(r[k])
    role = "cell-routes" if r["operation"] == "placement_constraints" else "cell-dns"
    if p.get("publication_role") != role or p.get("authority_cell_id") != c["authority_cell_id"] or p.get("target_scope") != "authority-cell:" + c["authority_cell_id"] or p.get("mode") != "serving" or not p.get("serving") or p["serving"].get("single_publication", False):
        raise ValueError("continuous serving authority required")
    if r["operation"] == "dns_source":
        validate_ref(p["dns_source"]["producer_policy"])
    return c


def check_delta(c, previous):
    old, new = copy.deepcopy(previous), copy.deepcopy(c["policy"])
    if old.get("mode") != "serving" or not old.get("serving") or old["serving"].get("single_publication", False) or old["generation"] == new["generation"]:
        raise ValueError("continuous predecessor and new generation required")
    old.pop("generation")
    new.pop("generation")
    if c["precondition"]["operation"] == "placement_constraints":
        a, b = old["route_placement_transition"], new["route_placement_transition"]
        if a["previous_topology"] != b["previous_topology"] or a["next_topology"] != b["next_topology"]:
            raise ValueError("dependency repair cannot change topology")
        for p in [old, new]:
            p["route_placement_transition"].pop("constraints")
    else:
        if old["dns_source"]["producer_policy"] == new["dns_source"]["producer_policy"]:
            raise ValueError("DNS source must explicitly advance")
        for p in [old, new]:
            p["dns_source"].pop("producer_policy")
    if old != new:
        raise ValueError("dependency repair changes unrelated producer settings")


def baseline(c, api):
    state = selected(api, c["policy"]["target_scope"], "full", "release_set")
    if authority_identity(state) != c["precondition"]["serving_full"]:
        raise ValueError("serving baseline changed")
    a, r = state["artifact"], state["release"]
    lkg = api("GET", "/v1/admin/artifacts/" + a["id"] + "/lkg").get("lkg") or {}
    if a.get("metadata", {}).get("producer_policy_release_id") != c["precondition"]["previous_policy"]["release_id"] or a.get("content", {}).get("publication_role") != c["policy"]["publication_role"] or r.get("released_by_id") != "platform-config-producer" or r.get("released_by_type") != "bootstrap" or lkg.get("artifact_id") != a["id"] or lkg.get("content_hash") != a["content_hash"] or lkg.get("verification_evidence_hash") != c["precondition"]["verification_evidence_hash"] or timestamp(lkg["expires_at"]) <= now():
        raise ValueError("positive predecessor-owned baseline unavailable")
    # An unverified recovery fence is allowed only with the original exact signed
    # positive LKG. The server authenticates its rollback message and provenance.
    if r.get("verification_state") != "verified" and not (r.get("verification_state") == "serving_unverified" and r.get("pinned_rollback_generation") == a["generation"] and r.get("rollback_target_generation") == a["generation"] and r.get("verified_lkg_generation") == a["generation"]):
        raise ValueError("baseline is neither verified nor an exact LKG recovery")
    return state


def publish(c, api, save):
    validate(c)
    scope = "platform-config-producer:" + c["authority_cell_id"]
    predecessor = api("GET", "/v1/admin/artifacts/" + c["precondition"]["previous_policy"]["artifact_id"])["artifact"]
    if predecessor.get("content_hash") != c["precondition"]["previous_policy"]["content_hash"] or digest(predecessor["content"]) != predecessor["content_hash"] or predecessor.get("scope_key") != scope or predecessor.get("status") != "validated":
        raise ValueError("predecessor signature input differs")
    check_delta(c, predecessor["content"])
    baseline(c, api)
    current = selected(api, scope, "shadow")
    if authority_identity(current) != c["precondition"]["previous_policy"] and not exact(current.get("artifact", {}), "policy_snapshot", scope, c["policy"]):
        raise ValueError("producer changed")
    if c["precondition"]["operation"] == "dns_source" and authority_identity(selected(api, "platform-config-producer", "shadow")) != c["policy"]["dns_source"]["producer_policy"]:
        raise ValueError("approved source producer changed")
    artifact = ensure_input(api, "policy_snapshot", scope, c["policy"])
    if c["precondition"]["operation"] == "dns_source":
        preview = dns_preview(c, api, artifact)
    else:
        preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + artifact["id"] + "&view=runtime")
    if c["precondition"]["operation"] == "placement_constraints":
        from scripts.reconfigure_cell_producer import validate_output
        validate_output(c, preview.get("policy", {}))
        if not preview.get("business_snapshot_revision"):
            raise ValueError("exact business snapshot required")
    else:
        intent = preview.get("intent", {})
        if intent.get("publication_role") != "cell-dns" or intent.get("scope") != c["policy"]["target_scope"] or intent.get("routes") or intent.get("tls") or intent.get("dns_route_sources") != [{"scope_key": "global", "policy_artifact_id": c["policy"]["dns_source"]["producer_policy"]["artifact_id"], "policy_digest": c["policy"]["dns_source"]["producer_policy"]["content_hash"]}]:
            raise ValueError("DNS preview does not retain the explicit source authority")
    baseline(c, api)
    key = "producer-reconfiguration/" + digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "precondition": c["precondition"]})
    evidence = {"schema": "fugue.producer-dependency-refresh-result/v1", "operation": c["precondition"]["operation"], "declaration_digest": digest(c), "artifact_id": artifact["id"], "public_transport_changed": False}
    save(evidence)
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "shadow", "producer_reconfiguration": c["precondition"], "idempotency_key": key, "reason": "explicit signed producer dependency refresh; declaration " + digest(c)})
    after = selected(api, scope, "shadow")
    if not exact(after.get("artifact", {}), "policy_snapshot", scope, c["policy"]) or after.get("release", {}).get("idempotency_key") != key:
        raise ValueError("successor was not selected")
    evidence.update(authority=authority_identity(after), completed_at=now().isoformat())
    save(evidence)
    return evidence
