"""Publish a fenced dynamic-domain quality policy through the configuration lane."""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import urllib.parse

from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.observe_front_candidate import now
from scripts.publish_agent_edge_shadow import API, canonical
from scripts.reconfigure_cell_producer import DIGEST, IDENTITY, validate_ref
from scripts.reconfigure_physical_dns import POLICY_FIELDS, SCOPE, baseline, bounded_json, resolve_baseline


def validate(config):
    fields = {"schema", "generation", "origin", "producer_generation", "precondition", "projection_policy", "baseline_mode"}
    if not isinstance(config, dict) or set(config) != fields or not bounded_json(config) or config["schema"] != "fugue.dynamic-quality-reconfiguration/v1" or type(config["generation"]) is not int or config["generation"] < 1 or config["baseline_mode"] not in ["declared", "latest_verified_same_policy"]:
        raise ValueError("complete explicit dynamic quality declaration required")
    origin = urllib.parse.urlsplit(config["origin"])
    if origin.scheme != "https" or config["origin"] != "https://" + str(origin.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(origin.hostname)):
        raise ValueError("canonical HTTPS API origin required")
    precondition = config["precondition"]
    if set(precondition) != {"operation", "previous_policy", "serving_full", "verification_evidence_hash"} or precondition["operation"] != "dynamic_quality" or not DIGEST.fullmatch(precondition["verification_evidence_hash"]):
        raise ValueError("fenced dynamic quality precondition required")
    for field in ["previous_policy", "serving_full"]:
        validate_ref(precondition[field])
    policy = config["projection_policy"]
    if policy.get("schema_version") != "fugue.platform.config/v1" or policy.get("scope") != "global" or policy.get("publication_role") or not IDENTITY.fullmatch(policy.get("generation", "")) or not IDENTITY.fullmatch(config["producer_generation"]):
        raise ValueError("explicit global producer and policy identity required")
    query = policy.get("dns_query_policy", {})
    dynamic = query.get("dynamic_quality", {})
    if query.get("ranking_mode") != "active" or not query.get("ordered_projection") or set(dynamic) != {"mode", "policy", "refresh_queries_per_cycle", "refresh_concurrency"} or dynamic["mode"] != "all_dynamic":
        raise ValueError("generic dynamic coverage and safe baseline required")
    if type(dynamic["refresh_queries_per_cycle"]) is not int or not 1 <= dynamic["refresh_queries_per_cycle"] <= 128 or type(dynamic["refresh_concurrency"]) is not int or not 1 <= dynamic["refresh_concurrency"] <= min(8, dynamic["refresh_queries_per_cycle"]):
        raise ValueError("bounded capture concurrency required")
    network = dynamic["policy"]
    if set(network) != POLICY_FIELDS or network.get("version") != "physical-network-delivery-v4" or any(network[key] is None for key in POLICY_FIELDS):
        raise ValueError("complete explicit delivery policy required")
    return config


def check_delta(previous, successor):
    old, new = copy.deepcopy(previous), copy.deepcopy(successor)
    if old.get("generation") == new.get("generation"):
        raise ValueError("policy generation must advance")
    old.pop("generation", None)
    new.pop("generation", None)
    before = old.get("dns_query_policy", {}).pop("dynamic_quality", None)
    after = new.get("dns_query_policy", {}).pop("dynamic_quality", None)
    if after is not None and not new.get("dns_query_policy", {}).get("physical_routes"):
        routes = old.get("dns_query_policy", {}).get("physical_routes", [])
        for route in routes:
            prior = copy.deepcopy(route["policy"])
            prior["version"] = after["policy"]["version"]
            if prior != after["policy"]:
                raise ValueError("universal adoption cannot discard a distinct per-service quality constraint")
        old.get("dns_query_policy", {}).pop("physical_routes", None)
        new.get("dns_query_policy", {}).pop("physical_routes", None)
    if after is None or before == after or old != new:
        raise ValueError("only the generic quality policy may change")


def validate_preview(previous, current):
    if not previous.get("business_snapshot_revision") or not current.get("business_snapshot_revision") or not previous.get("intent") or previous["intent"] != current.get("intent"):
        raise ValueError("quality preview changed frozen business or static intent")
    if any(issue not in previous.get("issues", []) for issue in current.get("issues", [])):
        raise ValueError("quality preview introduced a configuration issue")
    coverage = current.get("runtime_snapshot", {}).get("facts", {}).get("dynamic_quality_coverage", {})
    if coverage.get("mode") != "all_dynamic" or not coverage.get("hostnames") or type(coverage.get("query_count")) is not int or coverage["query_count"] < 1:
        raise ValueError("quality preview lacks complete coverage accounting")
    return coverage


def publish(config, api, save):
    validate(config)
    declaration = digest(config)
    config = resolve_baseline(config, api)
    baseline(config, api)
    precondition = config["precondition"]
    old = api("GET", "/v1/admin/artifacts/" + precondition["previous_policy"]["artifact_id"])["artifact"]
    previous = old["content"]
    if old.get("content_hash") != precondition["previous_policy"]["content_hash"] or not exact(old, "policy_snapshot", SCOPE, previous) or previous.get("target_scope") != "global" or previous.get("publication_role") or previous.get("mode") != "serving" or not previous.get("serving") or previous["serving"].get("single_publication", False):
        raise ValueError("exact continuous global predecessor required")
    source = api("GET", "/v1/admin/artifacts/" + previous["dns_policy_artifact_id"])["artifact"]
    if source.get("content_hash") != previous["dns_policy_digest"] or not exact(source, "policy_snapshot", "global", source["content"]):
        raise ValueError("pinned source integrity mismatch")
    check_delta(source["content"], config["projection_policy"])
    current = selected(api, SCOPE, "shadow")
    if authority_identity(current) != precondition["previous_policy"] and current.get("artifact", {}).get("generation") != config["producer_generation"]:
        raise ValueError("producer changed")
    source = ensure_input(api, "policy_snapshot", "global", config["projection_policy"])
    successor = copy.deepcopy(previous)
    successor.update(generation=config["producer_generation"], dns_policy_artifact_id=source["id"], dns_policy_digest=source["content_hash"])
    if authority_identity(current) != precondition["previous_policy"] and not exact(current.get("artifact", {}), "policy_snapshot", SCOPE, successor):
        raise ValueError("successor differs")
    artifact = ensure_input(api, "policy_snapshot", SCOPE, successor)
    before = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + old["id"])
    preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + artifact["id"])
    coverage = validate_preview(before, preview)
    baseline(config, api)
    evidence = {"schema": "fugue.dynamic-quality-reconfiguration-result/v1", "declaration_digest": declaration, "resolved_precondition": precondition, "artifact_id": artifact["id"], "coverage": coverage, "producer_activated": False, "routing_acceptance_complete": False}
    save(evidence)
    key = "producer-reconfiguration/" + digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "precondition": precondition})
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "shadow", "producer_reconfiguration": precondition, "idempotency_key": key, "reason": "Declarative universal dynamic quality policy with preserved explicit constraints"})
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
        raise SystemExit("dynamic quality reconfiguration stopped: " + str(error)) from None
