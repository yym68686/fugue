#!/usr/bin/env python3
"""Reconfigure an established Cell producer with exact transactional pins.

Placement remains observational. Explicit activate_serving enables one observed
gray/full/LKG cycle; refresh_serving permits another after its verified baseline.
These operations change neither executable nor public transport.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import urllib.parse

from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.observe_front_candidate import now, timestamp
from scripts.publish_agent_edge_shadow import API, canonical

IDENTITY = re.compile(r"[a-zA-Z0-9_.-]{1,256}")
DIGEST = re.compile(r"sha256:[a-f0-9]{64}")


def validate_ref(ref):
    if not isinstance(ref, dict) or set(ref) != {"artifact_id", "content_hash", "release_id", "fencing_token"} or any(not IDENTITY.fullmatch(ref.get(k, "")) for k in ["artifact_id", "release_id"]) or not DIGEST.fullmatch(ref.get("content_hash", "")) or type(ref["fencing_token"]) is not int or ref["fencing_token"] < 1:
        raise ValueError("complete canonical publication precondition required")


def validate(config):
    if set(config) != {"schema", "generation", "origin", "authority_cell_id", "precondition", "policy"} or config["schema"] != "fugue.cell-producer-reconfiguration/v1" or type(config["generation"]) is not int or config["generation"] < 1:
        raise ValueError("explicit producer reconfiguration declaration required")
    cell = config["authority_cell_id"]
    if not isinstance(cell, str) or len(cell) > 128 or not re.fullmatch(r"cell-[a-z0-9]+(?:-[a-z0-9]+)*", cell):
        raise ValueError("canonical neutral cell required")
    origin = urllib.parse.urlsplit(config["origin"])
    if origin.scheme != "https" or config["origin"] != "https://" + str(origin.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(origin.hostname)):
        raise ValueError("canonical HTTPS API origin required")
    precondition, policy = config["precondition"], config["policy"]
    required = {"previous_policy", "serving_full", "verification_evidence_hash"}
    operation = precondition.get("operation", "placement")
    if not required.issubset(precondition) or set(precondition) - required - {"operation"} or operation not in ["placement", "activate_serving", "refresh_serving", "continuous_serving", "expand_membership"] or not DIGEST.fullmatch(precondition.get("verification_evidence_hash", "")):
        raise ValueError("exact producer and verified serving baseline required")
    for field in ["previous_policy", "serving_full"]:
        validate_ref(precondition[field])
    fields = {"schema_version", "generation", "publication_role", "authority_cell_id", "mode", "input_source", "target_scope", "interval_seconds", "refresh_seconds", "require_application_domains", "require_route_defaults", "static_intent_artifact_id", "static_intent_digest", "dns_policy_artifact_id", "dns_policy_digest", "route_placement_transition"}
    allowed = {"require_dns_query_policy", "hosted_zone_templates"}
    if operation != "placement":
        allowed.add("serving")
    if not fields.issubset(policy) or set(policy) - fields - allowed or policy["schema_version"] != "fugue.platform.producer/v1" or policy["publication_role"] != "cell-routes" or policy["mode"] != ("shadow" if operation in ["placement", "expand_membership"] else "serving") or policy["input_source"] != "business-static-intent" or policy["authority_cell_id"] != cell or policy["target_scope"] != "authority-cell:" + cell or policy["require_application_domains"] is not True or policy["require_route_defaults"] is not True or policy.get("require_dns_query_policy", False) is not False or policy.get("hosted_zone_templates", []) != []:
        raise ValueError("only an explicitly authorized pinned route-only successor is allowed")
    for k in ["generation", "static_intent_artifact_id", "dns_policy_artifact_id"]:
        if not IDENTITY.fullmatch(policy[k]):
            raise ValueError("canonical immutable policy identity required")
    if any(not DIGEST.fullmatch(policy[k]) for k in ["static_intent_digest", "dns_policy_digest"]) or type(policy["interval_seconds"]) is not int or type(policy["refresh_seconds"]) is not int or not 30 <= policy["interval_seconds"] <= 900 or not max(120, policy["interval_seconds"]) <= policy["refresh_seconds"] <= 3600:
        raise ValueError("invalid source digest or schedule")
    if operation != "placement":
        serving = policy.get("serving", {})
        limits = ["gray_min_seconds", "full_min_seconds", "rollout_timeout_seconds"]
        if set(serving) != {"single_publication", "canary_rule_ref", *limits} or serving["single_publication"] is not (operation != "continuous_serving") or not re.fullmatch(r"cohort=[a-z0-9][a-z0-9_-]{0,63}", serving["canary_rule_ref"]) or any(type(serving[k]) is not int for k in limits) or not all(1 <= serving[k] <= 1800 for k in limits[:2]) or not max(30, max(serving[k] for k in limits[:2]) + policy["interval_seconds"]) <= serving["rollout_timeout_seconds"] <= 3600:
            raise ValueError("explicit bounded single serving publication required")
    transition = policy["route_placement_transition"]
    if not isinstance(transition, dict) or set(transition) != {"previous_topology", "next_topology", "constraints"}:
        raise ValueError("explicit topology and source constraints required")
    previous, following = transition["previous_topology"], transition["next_topology"]
    if previous.get("schema_version") != "edge-topology/v1" or len(previous.get("authority_cells", [])) != len(following.get("authority_cells", [])):
        raise ValueError("complete topology pair required")
    expected, aliases = copy.deepcopy(previous), {}
    for old, target in zip(expected["authority_cells"], following["authority_cells"]):
        if old == target:
            continue
        if not old.get("legacy_group_id") or target != {"id": old["id"]}:
            raise ValueError("only declared authority aliases may change")
        aliases[old.pop("legacy_group_id")] = old["id"]
    if not aliases or expected != following:
        raise ValueError("membership, pools, capabilities, labels and risks must remain identical")
    pins, seen = transition["constraints"], []
    if not isinstance(pins, list) or not 1 <= len(pins) <= 4096:
        raise ValueError("bounded source constraints required")
    for pin in pins:
        if set(pin) != {"source", "source_digest"} or not DIGEST.fullmatch(pin["source_digest"]) or digest(pin["source"]) != pin["source_digest"]:
            raise ValueError("source constraint digest differs")
        source = pin["source"]
        if source.get("edge_group_id") not in aliases and not any(g in aliases for g in source.get("excluded_edge_group_ids", [])):
            raise ValueError("source constraint is unaffected by declared aliases")
        seen.append(source["hostname"])
    if seen != sorted(set(seen)):
        raise ValueError("unique source constraints must be sorted")
    return config


def baseline(config, api):
    scope = config["policy"]["target_scope"]
    state = selected(api, scope, "full", "release_set")
    if authority_identity(state) != config["precondition"]["serving_full"] or state["artifact"].get("content", {}).get("publication_role") != "cell-routes":
        raise ValueError("full serving publication differs from declaration")
    lkg = api("GET", "/v1/admin/artifacts/" + state["artifact"]["id"] + "/lkg").get("lkg")
    if not lkg or lkg.get("artifact_id") != state["artifact"]["id"] or lkg.get("content_hash") != state["artifact"]["content_hash"] or lkg.get("scope_key") != scope or lkg.get("artifact_kind") != "release_set" or lkg.get("verified_by_release_id") != state["release"]["id"] or lkg.get("verification_evidence_hash") != config["precondition"]["verification_evidence_hash"] or timestamp(lkg["expires_at"]) <= now():
        raise ValueError("positive serving LKG is unavailable or changed")
    return state


def validate_output(config, policy):
    expected = config["policy"]
    if policy.get("scope") != expected["target_scope"] or policy.get("authority_cell_id") != config["authority_cell_id"] or policy.get("publication_role") != "cell-routes":
        raise ValueError("projected policy belongs to another authority")
    transition = expected["route_placement_transition"]
    aliases = {old["legacy_group_id"]: new["id"] for old, new in zip(transition["previous_topology"]["authority_cells"], transition["next_topology"]["authority_cells"]) if old != new}
    rules = policy.get("route_constraints", [])
    if len({r["hostname"] for r in rules}) != len(rules):
        raise ValueError("projected constraint ownership is ambiguous")
    by_host = {r["hostname"]: r for r in rules}
    for pin in transition["constraints"]:
        source = copy.deepcopy(pin["source"])
        if source.get("edge_group_id") in aliases:
            source["edge_group_id"] = aliases[source["edge_group_id"]]
        if source.get("excluded_edge_group_ids"):
            source["excluded_edge_group_ids"] = sorted({aliases.get(g, g) for g in source["excluded_edge_group_ids"]})
        if by_host.get(source["hostname"]) != source:
            raise ValueError("projection did not preserve the exact transformed constraint")
    for rule in rules:
        if rule.get("edge_group_id") in aliases or any(g in aliases for g in rule.get("excluded_edge_group_ids", [])):
            raise ValueError("unconverted affected constraint remains")


def publish(config, api, save):
    validate(config)
    operation = config["precondition"].get("operation", "placement")
    scope = "platform-config-producer:" + config["authority_cell_id"]
    full = baseline(config, api)
    current = selected(api, scope, "shadow")
    old = api("GET", "/v1/admin/artifacts/" + config["precondition"]["previous_policy"]["artifact_id"])["artifact"]
    previous = old.get("content", {})
    modes = ["serving"] if operation in ["refresh_serving", "continuous_serving", "expand_membership"] else ["shadow", "paused"]
    if old.get("content_hash") != config["precondition"]["previous_policy"]["content_hash"] or digest(previous) != old.get("content_hash") or old.get("scope_key") != scope or old.get("status") != "validated" or previous.get("mode") not in modes:
        raise ValueError("previous producer policy is untrusted or has incompatible mode")
    if operation in ["refresh_serving", "continuous_serving", "expand_membership"] and (previous.get("serving", {}).get("single_publication") is not True or full["artifact"].get("metadata", {}).get("producer_policy_release_id") != config["precondition"]["previous_policy"]["release_id"] or full["release"].get("released_by_type") != "bootstrap" or full["release"].get("released_by_id") != "platform-config-producer" or full["release"].get("verification_state") != "verified" or full["release"].get("verified_lkg_generation") != full["artifact"]["generation"]):
        raise ValueError("refresh requires the predecessor's own verified single full publication")
    prior, target = copy.deepcopy(previous), copy.deepcopy(config["policy"])
    for value in [prior, target]:
        fields = ["generation"]
        if operation == "expand_membership":
            fields.extend(["static_intent_artifact_id", "static_intent_digest", "dns_policy_artifact_id", "dns_policy_digest"])
        elif operation == "continuous_serving":
            value["serving"]["single_publication"] = False
        elif operation != "refresh_serving":
            fields.append("serving" if operation == "activate_serving" else "route_placement_transition")
        for field in fields:
            value.pop(field, None)
        value["mode"] = "shadow"
    if prior != target:
        raise ValueError("successor changes unrelated producer configuration")
    if operation == "expand_membership":
        # Recheck typed immutable inputs even for a resolved direct declaration;
        # the API repeats signature/content checks inside the release transaction.
        from scripts.expand_cell_membership import validate_expansion, read_input
        old_static = read_input(api, previous, "static_intent", "platform_intent")
        old_projection = read_input(api, previous, "dns_policy", "policy_snapshot")
        new_static = read_input(api, config["policy"], "static_intent", "platform_intent")
        new_projection = read_input(api, config["policy"], "dns_policy", "policy_snapshot")
        validate_expansion(previous, old_static["content"], new_static["content"], old_projection["content"], new_projection["content"])
    if authority_identity(current) != config["precondition"]["previous_policy"] and not exact(current.get("artifact", {}), "policy_snapshot", scope, config["policy"]):
        raise ValueError("current producer policy changed")
    artifact = ensure_input(api, "policy_snapshot", scope, config["policy"])
    preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + artifact["id"])
    validate_output(config, preview.get("policy", {}))
    if not preview.get("business_snapshot_revision"):
        raise ValueError("projection lacks fixed business snapshot")
    key = "producer-reconfiguration/" + digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "precondition": config["precondition"]})
    evidence = {"schema": "fugue.cell-producer-reconfiguration-result/v1", "declaration_digest": digest(config), "artifact_id": artifact["id"], "business_snapshot_revision": preview["business_snapshot_revision"], "serving_full": config["precondition"]["serving_full"], "public_transport_changed": False}
    save(evidence)
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "shadow", "producer_reconfiguration": config["precondition"], "idempotency_key": key, "reason": "exact source-bound Cell producer " + operation + "; declaration " + digest(config)})
    current = selected(api, scope, "shadow")
    if not exact(current.get("artifact", {}), "policy_snapshot", scope, config["policy"]) or current.get("release", {}).get("idempotency_key") != key or authority_identity(current) is None:
        raise ValueError("successor is not the current transaction-bound policy")
    if operation in ["placement", "expand_membership"]:
        baseline(config, api)
        evidence["serving_publication_changed"] = False
    evidence.update(authority=authority_identity(current), completed_at=now().isoformat(), mode=config["policy"]["mode"], operation=operation, serving_publication_authorized=operation in ["activate_serving", "refresh_serving", "continuous_serving"])
    save(evidence)
    return evidence


def main():
    p = argparse.ArgumentParser()
    p.add_argument("declaration")
    p.add_argument("--validate-only", action="store_true")
    p.add_argument("--evidence")
    args = p.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 1 << 20:
        raise ValueError("declaration exceeds bound")
    config = validate(json.loads(raw))
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(config)}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token or not args.evidence:
        raise ValueError("configuration credential and retained evidence path required")
    def save(value):
        Path(args.evidence).write_text(canonical(value) + "\n")
    print(canonical(publish(config, API(config["origin"], token), save)))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("producer reconfiguration stopped: " + str(error)) from None
