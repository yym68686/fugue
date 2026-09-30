#!/usr/bin/env python3
"""Add one declared Edge to an established Cell's shadow execution membership.

Configuration inputs are immutable; the established full and positive LKG are
transactionally pinned. This does not enroll processes or select public traffic.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import urllib.parse

from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.publish_agent_edge_shadow import API, canonical
from scripts import reconfigure_cell_producer as reconfigure


def validate(c):
    if set(c) != {"schema", "generation", "origin", "authority_cell_id", "producer_generation", "precondition", "static_intent", "projection_policy"} or c["schema"] != "fugue.cell-membership-expansion/v1" or type(c["generation"]) is not int or c["generation"] < 1 or not reconfigure.IDENTITY.fullmatch(c.get("producer_generation", "")):
        raise ValueError("explicit membership expansion declaration required")
    cell = c["authority_cell_id"]
    if not isinstance(cell, str) or len(cell) > 128 or not re.fullmatch(r"cell-[a-z0-9]+(?:-[a-z0-9]+)*", cell):
        raise ValueError("neutral cell required")
    origin = urllib.parse.urlsplit(c["origin"])
    if origin.scheme != "https" or c["origin"] != "https://" + str(origin.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(origin.hostname)):
        raise ValueError("canonical HTTPS origin required")
    pre = c["precondition"]
    if set(pre) != {"operation", "previous_policy", "serving_full", "verification_evidence_hash"} or pre["operation"] != "expand_membership" or not reconfigure.DIGEST.fullmatch(pre["verification_evidence_hash"]):
        raise ValueError("exact membership expansion preconditions required")
    for k in ["previous_policy", "serving_full"]:
        reconfigure.validate_ref(pre[k])
    for value in [c["static_intent"], c["projection_policy"]]:
        if value.get("schema_version") != "fugue.platform.config/v1" or value.get("publication_role") != "cell-routes" or value.get("authority_cell_id") != cell or value.get("scope") != "authority-cell:" + cell or not reconfigure.IDENTITY.fullmatch(value.get("generation", "")):
            raise ValueError("new inputs must retain exact route-only authority")
    topology_digest(c["static_intent"])
    if c["projection_policy"].get("consumer_topology_digest") != topology_digest(c["static_intent"]):
        raise ValueError("projection must pin new execution membership")
    return c


def topology_digest(intent):
    topology = intent.get("edge_topology", {})
    cell = intent["authority_cell_id"]
    edges = topology.get("edges", [])
    ids = [e.get("id") for e in edges]
    if topology.get("schema_version") != "edge-topology/v1" or topology.get("authority_cells") != [{"id": cell}] or not 1 <= len(edges) <= 10000 or any(not isinstance(x, str) or not re.fullmatch(r"[a-z0-9]+(?:[-.][a-z0-9]+)*", x) for x in ids) or ids != sorted(set(ids)) or any(e.get("authority_cell_id") != cell for e in edges) or intent.get("dns_consumers"):
        raise ValueError("explicit canonical route-only membership required")
    # Match the typed Go membership encoding, not the sorted-key artifact hash.
    value = {"publication_role": "cell-routes", "schema_version": "fugue.traffic-consumer-topology/v1", "authority_cell_id": cell, "edge_node_ids": ids, "dns_node_ids": []}
    return "sha256:" + hashlib.sha256(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def read_input(api, policy, prefix, kind):
    artifact = api("GET", "/v1/admin/artifacts/" + policy[prefix + "_artifact_id"])["artifact"]
    if artifact.get("id") != policy[prefix + "_artifact_id"] or artifact.get("artifact_kind") != kind or artifact.get("scope_key") != policy["target_scope"] or artifact.get("status") != "validated" or artifact.get("content_hash") != policy[prefix + "_digest"] or digest(artifact.get("content")) != artifact.get("content_hash"):
        raise ValueError("pinned membership input changed or unavailable")
    return artifact


def validate_expansion(previous, old_static, new_static, old_projection, new_projection):
    def without(value, *fields):
        return {k: v for k, v in value.items() if k not in fields}
    if not previous.get("route_placement_transition") or old_static.get("generation") == new_static.get("generation") or old_projection.get("generation") == new_projection.get("generation") or without(old_static, "generation", "edge_topology") != without(new_static, "generation", "edge_topology") or without(old_projection, "generation", "consumer_topology_digest") != without(new_projection, "generation", "consumer_topology_digest"):
        raise ValueError("membership expansion changes unrelated input configuration")
    before, after = old_static["edge_topology"], new_static["edge_topology"]
    if without(before, "edges") != without(after, "edges") or len(after["edges"]) != len(before["edges"]) + 1 or old_projection.get("consumer_topology_digest") != topology_digest(old_static) or new_projection.get("consumer_topology_digest") != topology_digest(new_static):
        raise ValueError("only one exact declared member may join")
    old = {e["id"]: e for e in before["edges"]}
    new = {e["id"]: e for e in after["edges"]}
    declared = {e["id"]: e for e in previous["route_placement_transition"]["next_topology"]["edges"]}
    if any(new.get(k) != e for k, e in old.items()) or any(declared.get(k) != e for k, e in new.items()):
        raise ValueError("member properties differ from exact previous or declared topology")


def publish(c, api, save):
    validate(c)
    ref = c["precondition"]["previous_policy"]
    owner = "platform-config-producer:" + c["authority_cell_id"]
    old = api("GET", "/v1/admin/artifacts/" + ref["artifact_id"])["artifact"]
    previous = old.get("content", {})
    if old.get("id") != ref["artifact_id"] or old.get("content_hash") != ref["content_hash"] or digest(previous) != ref["content_hash"] or old.get("scope_key") != owner or old.get("status") != "validated" or previous.get("mode") != "serving" or previous.get("serving", {}).get("single_publication") is not True:
        raise ValueError("completed single serving predecessor required")
    old_static = read_input(api, previous, "static_intent", "platform_intent")
    old_projection = read_input(api, previous, "dns_policy", "policy_snapshot")
    validate_expansion(previous, old_static["content"], c["static_intent"], old_projection["content"], c["projection_policy"])
    # No immutable writes until the exact current full and positive checkpoint
    # pass preflight. The store repeats these checks under both scope locks.
    check = {"policy": previous, "precondition": c["precondition"]}
    full = reconfigure.baseline(check, api)
    metadata, release = full["artifact"].get("metadata", {}), full["release"]
    source_pins = {"producer_policy_release_id": ref["release_id"], "producer_static_intent_id": previous["static_intent_artifact_id"], "producer_static_intent_digest": previous["static_intent_digest"], "producer_dns_policy_id": previous["dns_policy_artifact_id"], "producer_dns_policy_digest": previous["dns_policy_digest"]}
    if any(metadata.get(k) != v for k, v in source_pins.items()) or release.get("released_by_type") != "bootstrap" or release.get("released_by_id") != "platform-config-producer" or release.get("verification_state") != "verified" or release.get("verified_lkg_generation") != full["artifact"]["generation"]:
        raise ValueError("full baseline is not the predecessor's verified pinned publication")
    current = selected(api, owner, "shadow")
    if authority_identity(current) != ref and current.get("artifact", {}).get("generation") != c["producer_generation"]:
        raise ValueError("producer predecessor changed")
    static = ensure_input(api, "platform_intent", previous["target_scope"], c["static_intent"])
    projection = ensure_input(api, "policy_snapshot", previous["target_scope"], c["projection_policy"])
    policy = copy.deepcopy(previous)
    policy.update(generation=c["producer_generation"], mode="shadow", static_intent_artifact_id=static["id"], static_intent_digest=static["content_hash"], dns_policy_artifact_id=projection["id"], dns_policy_digest=projection["content_hash"])
    resolved = {k: c[k] for k in ["generation", "origin", "authority_cell_id", "precondition"]}
    resolved.update(schema="fugue.cell-producer-reconfiguration/v1", policy=policy)
    def retain(result):
        save({**result, "membership_declaration_digest": digest(c), "resolved_declaration": resolved, "static_intent_artifact_id": static["id"], "projection_policy_artifact_id": projection["id"]})
    return reconfigure.publish(resolved, api, retain)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("declaration")
    p.add_argument("--validate-only", action="store_true")
    p.add_argument("--evidence")
    args = p.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 1 << 20:
        raise ValueError("membership declaration exceeds bound")
    c = validate(json.loads(raw))
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(c)}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token or not args.evidence:
        raise ValueError("configuration credential and evidence path required")
    publish(c, API(c["origin"], token), lambda v: Path(args.evidence).write_text(canonical(v) + "\n"))
    print(canonical({"completed": True, "declaration_digest": digest(c), "mode": "shadow", "public_transport_changed": False}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("cell membership expansion stopped: " + str(error)) from None
