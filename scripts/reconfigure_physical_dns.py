#!/usr/bin/env python3
"""Opt one hostname into physical DNS through a fenced producer transaction."""
import argparse
import copy
import json
import math
import os
from pathlib import Path
import re
import urllib.parse

from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.observe_front_candidate import now, timestamp
from scripts.publish_agent_edge_shadow import API, canonical
from scripts.reconfigure_cell_producer import DIGEST, IDENTITY, validate_ref


SCOPE = "platform-config-producer"
POLICY_FIELDS = {"version", "window_seconds", "bucket_seconds", "required_buckets", "minimum_records", "cooldown_seconds", "evidence_max_age_seconds", "advantage_ms", "advantage_ratio", "unknown_cost_ms", "uncertainty_ms", "failure_cost_ms", "capacity_cost_ms", "throughput_cost_ms", "throughput_target_bps", "probe_interval_seconds", "probe_budget_per_interval", "maximum_node_utilization"}


def bounded_json(value):
    if isinstance(value, dict):
        return all(isinstance(key, str) and bounded_json(item) for key, item in value.items())
    if isinstance(value, list):
        return all(bounded_json(item) for item in value)
    if type(value) is float:
        return math.isfinite(value) and value == round(value, 6) and not value.is_integer()
    return value is None or type(value) in [str, int, bool]


def validate(config):
    fields = {"schema", "generation", "origin", "hostname", "producer_generation", "precondition", "projection_policy"}
    if not isinstance(config, dict) or not fields.issubset(config) or set(config) - fields - {"baseline_mode"} or not bounded_json(config) or config["schema"] != "fugue.physical-dns-reconfiguration/v1" or type(config["generation"]) is not int or config["generation"] < 1 or config.get("baseline_mode", "declared") not in ["declared", "latest_verified_same_policy"]:
        raise ValueError("explicit bounded physical DNS declaration required")
    origin = urllib.parse.urlsplit(config["origin"])
    if origin.scheme != "https" or config["origin"] != "https://" + str(origin.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(origin.hostname)):
        raise ValueError("canonical HTTPS API origin required")
    hostname = config["hostname"]
    if not isinstance(hostname, str) or len(hostname) > 253 or len(hostname.split(".")) < 2 or any(not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label) for label in hostname.split(".")):
        raise ValueError("one canonical hostname required")
    precondition = config["precondition"]
    if set(precondition) != {"operation", "previous_policy", "serving_full", "verification_evidence_hash"} or precondition["operation"] != "physical_dns" or not DIGEST.fullmatch(precondition["verification_evidence_hash"]):
        raise ValueError("exact physical DNS transaction precondition required")
    for field in ["previous_policy", "serving_full"]:
        validate_ref(precondition[field])
    policy = config["projection_policy"]
    if policy.get("schema_version") != "fugue.platform.config/v1" or policy.get("scope") != "global" or policy.get("publication_role") or not IDENTITY.fullmatch(policy.get("generation", "")) or not IDENTITY.fullmatch(config["producer_generation"]):
        raise ValueError("canonical global input and producer generations required")
    query = policy.get("dns_query_policy", {})
    routes = query.get("physical_routes", [])
    if query.get("ranking_mode") != "active" or not 1 <= len(routes) <= 64 or len({route.get("hostname") for route in routes}) != len(routes) or sum(route.get("hostname") == hostname for route in routes) != 1:
        raise ValueError("explicit active physical route policy required")
    for route in routes:
        if set(route) != {"hostname", "traffic_class", "policy"} or set(route["policy"]) != POLICY_FIELDS or route["policy"].get("version") not in ["physical-network-cohort-v2", "physical-network-bounded-v3", "physical-network-delivery-v4", "physical-network-failure-aware-v5", "physical-network-comparable-delivery-v6"] or any(route["policy"][field] is None for field in POLICY_FIELDS):
            raise ValueError("physical route must declare complete versioned network policy")
    return config


def check_delta(previous, successor, hostname):
    old, new = copy.deepcopy(previous), copy.deepcopy(successor)
    if old.get("generation") == new.get("generation"):
        raise ValueError("DNS source generation must advance")
    old.pop("generation", None)
    new.pop("generation", None)
    before = old.get("dns_query_policy", {}).pop("physical_routes", [])
    after = new.get("dns_query_policy", {}).pop("physical_routes", [])
    if any(route["hostname"] == hostname for route in before) or len(after) != len(before) + 1 or [route for route in after if route["hostname"] != hostname] != before or old != new:
        raise ValueError("transition may only add the declared hostname without changing existing policy")


def baseline(config, api):
    precondition = config["precondition"]
    full = selected(api, "global", "full", "release_set")
    if authority_identity(full) != precondition["serving_full"] or full["artifact"].get("content", {}).get("publication_role"):
        raise ValueError("serving full publication changed")
    release = full["release"]
    if release.get("verification_state") != "verified" or release.get("verified_lkg_generation") != full["artifact"]["generation"] or release.get("released_by_type") != "bootstrap" or release.get("released_by_id") != "platform-config-producer" or full["artifact"].get("metadata", {}).get("producer_policy_release_id") != precondition["previous_policy"]["release_id"]:
        raise ValueError("baseline is not the predecessor's verified producer publication")
    lkg = api("GET", "/v1/admin/artifacts/" + full["artifact"]["id"] + "/lkg").get("lkg", {})
    if lkg.get("artifact_id") != full["artifact"]["id"] or lkg.get("content_hash") != full["artifact"]["content_hash"] or lkg.get("scope_key") != "global" or lkg.get("artifact_kind") != "release_set" or lkg.get("verified_by_release_id") != release["id"] or lkg.get("verification_evidence_hash") != precondition["verification_evidence_hash"] or timestamp(lkg["expires_at"]) <= now():
        raise ValueError("exact unexpired positive LKG required")
    return full


def resolve_baseline(config, api):
    if config.get("baseline_mode", "declared") == "declared":
        return config
    full = selected(api, "global", "full", "release_set")
    reference = authority_identity(full)
    declared = config["precondition"]["serving_full"]
    if reference == declared:
        baseline(config, api)
        return config
    if reference["fencing_token"] <= declared["fencing_token"] or full["artifact"].get("metadata", {}).get("producer_policy_release_id") != config["precondition"]["previous_policy"]["release_id"]:
        raise ValueError("renewed baseline changed the declared producer or regressed its fence")
    lkg = api("GET", "/v1/admin/artifacts/" + full["artifact"]["id"] + "/lkg").get("lkg", {})
    evidence_hash = lkg.get("verification_evidence_hash", "")
    if not DIGEST.fullmatch(evidence_hash):
        raise ValueError("renewed baseline lacks verified LKG evidence")
    resolved = copy.deepcopy(config)
    resolved["precondition"].update(serving_full=reference, verification_evidence_hash=evidence_hash)
    baseline(resolved, api)
    return resolved


def validate_preview(config, preview, previous=None):
    if not preview.get("business_snapshot_revision") or preview.get("policy", {}).get("scope") != "global":
        raise ValueError("physical projection lacks a ready fixed business snapshot")
    for issue in preview.get("issues", []):
        code, hostname = issue.get("code"), issue.get("hostname", "")
        advisory = code in ["dns_output_equivalence_not_verified", "release_target_equivalence_not_verified"] and not hostname
        unchanged_origin = code == "origin_observation_not_fresh" and hostname and hostname != config["hostname"]
        if not (advisory or unchanged_origin) or previous is None or not previous.get("business_snapshot_revision") or not previous.get("intent") or previous["intent"] != preview.get("intent") or issue not in previous.get("issues", []):
            raise ValueError("physical projection has new, target-specific or unverified issues")
    facts = [fact for fact in preview.get("runtime_snapshot", {}).get("dns_selections", []) if fact.get("hostname") == config["hostname"]]
    if not facts or len({(fact.get("node_id"), fact.get("type")) for fact in facts}) != len(facts):
        raise ValueError("unique actual per-consumer DNS evidence required")
    expected = next(route for route in config["projection_policy"]["dns_query_policy"]["physical_routes"] if route["hostname"] == config["hostname"])
    summaries = []
    for fact in facts:
        selection, evidence = fact.get("physical_selection", {}), fact.get("physical_evidence", {})
        snapshot = evidence.get("snapshot", {})
        receipt = snapshot.get("actual_dns_receipt", {})
        result = evidence.get("result", {})
        if fact.get("type") != "A" or fact.get("reason") != "bound_physical_network_evidence" or selection.get("version") != "physical-edge-network-v1" or not DIGEST.fullmatch(evidence.get("digest", "")) or selection.get("evidence_digest") != evidence["digest"] or selection.get("dns_receipt_id") != receipt.get("decision_id") or receipt.get("node_id") != fact.get("node_id") or receipt.get("write_succeeded") is not True or snapshot.get("hostname") != config["hostname"] or snapshot.get("traffic_class") != expected["traffic_class"] or snapshot.get("policy") != expected["policy"] or snapshot.get("blockers") or result.get("hypothesis") not in ["hold", "switch", "failover"] or result.get("proposed_edge_id") != selection.get("primary_edge_id"):
            raise ValueError("projection lacks bound physical selection evidence")
        primary = [candidate for candidate in result.get("candidates", []) if candidate.get("edge_id") == selection.get("primary_edge_id")]
        if len(primary) != 1 or primary[0].get("ready") is not True or primary[0].get("hard_gates"):
            raise ValueError("physical primary lacks complete trusted evidence")
        summaries.append({"node_id": fact["node_id"], "dns_receipt_id": receipt["decision_id"], "evidence_digest": evidence["digest"], "primary_edge_id": selection["primary_edge_id"], "hypothesis": result["hypothesis"]})
    return summaries


def publish(config, api, save):
    validate(config)
    declaration_digest = digest(config)
    declared_precondition = copy.deepcopy(config["precondition"])
    config = resolve_baseline(config, api)
    baseline(config, api)
    precondition = config["precondition"]
    old = api("GET", "/v1/admin/artifacts/" + precondition["previous_policy"]["artifact_id"])["artifact"]
    previous = old["content"]
    if old.get("content_hash") != precondition["previous_policy"]["content_hash"] or not exact(old, "policy_snapshot", SCOPE, previous) or old.get("status") != "validated" or previous.get("target_scope") != "global" or previous.get("publication_role") or previous.get("route_placement_transition") or previous.get("mode") != "serving" or not previous.get("serving") or previous["serving"].get("single_publication", False) or previous.get("require_dns_query_policy") is not True or previous["generation"] == config["producer_generation"]:
        raise ValueError("exact continuous global predecessor required")
    source = api("GET", "/v1/admin/artifacts/" + previous["dns_policy_artifact_id"])["artifact"]
    if source.get("content_hash") != previous["dns_policy_digest"] or source.get("status") != "validated" or not exact(source, "policy_snapshot", "global", source["content"]):
        raise ValueError("pinned DNS source is untrusted")
    check_delta(source["content"], config["projection_policy"], config["hostname"])
    current = selected(api, SCOPE, "shadow")
    if authority_identity(current) != precondition["previous_policy"] and current.get("artifact", {}).get("generation") != config["producer_generation"]:
        raise ValueError("producer changed before configuration preparation")
    source = ensure_input(api, "policy_snapshot", "global", config["projection_policy"])
    successor = copy.deepcopy(previous)
    successor.update(generation=config["producer_generation"], dns_policy_artifact_id=source["id"], dns_policy_digest=source["content_hash"])
    if authority_identity(current) != precondition["previous_policy"] and not exact(current.get("artifact", {}), "policy_snapshot", SCOPE, successor):
        raise ValueError("successor publication differs from declaration")
    artifact = ensure_input(api, "policy_snapshot", SCOPE, successor)
    previous_preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + old["id"])
    preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + artifact["id"])
    summaries = validate_preview(config, preview, previous_preview)
    baseline(config, api)
    evidence = {"schema": "fugue.physical-dns-reconfiguration-result/v1", "declaration_digest": declaration_digest, "declared_precondition": declared_precondition, "resolved_precondition": precondition, "artifact_id": artifact["id"], "hostname": config["hostname"], "business_snapshot_revision": preview["business_snapshot_revision"], "unchanged_projection_issues": preview.get("issues", []), "selections": summaries, "serving_full_precondition": precondition["serving_full"], "routing_acceptance_complete": False, "producer_activated": False}
    save(evidence)
    key = "producer-reconfiguration/" + digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "precondition": precondition})
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "shadow", "producer_reconfiguration": precondition, "idempotency_key": key, "reason": "Declarative single-host physical DNS opt-in with bound runtime evidence"})
    current = selected(api, SCOPE, "shadow")
    if not exact(current.get("artifact", {}), "policy_snapshot", SCOPE, successor):
        raise ValueError("producer activation not confirmed")
    evidence.update(producer_activated=True, authority=authority_identity(current), completed_at=now().isoformat())
    save(evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--evidence")
    args = parser.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 1 << 20:
        raise ValueError("declaration exceeds bound")
    config = validate(json.loads(raw))
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(config)}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token or not args.evidence:
        raise ValueError("configuration credential and evidence path required")
    def save(value):
        Path(args.evidence).write_text(canonical(value) + "\n")
    print(canonical(publish(config, API(config["origin"], token, response_limit=128 << 20, timeout=120), save)))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("physical DNS reconfiguration stopped: " + str(error)) from None
