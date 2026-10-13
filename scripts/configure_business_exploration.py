"""Pure validation for the existing independent quality-configuration CI lane."""
import copy
from datetime import datetime, timedelta
import math
import re
import json
import urllib.parse

from scripts.bootstrap_cell_producer import digest


class BusinessBaselineChanged(ValueError):
    pass


def resolve_business_anchors(config, api, full, hostname):
    if config.get("business_anchor_mode", "declared") == "declared":
        return config
    resolved = copy.deepcopy(config)
    routes = [route for route in resolved["projection_policy"]["dns_query_policy"].get("physical_routes", []) if route["hostname"] == hostname and "business_exploration" in route]
    if len(routes) != 1:
        raise ValueError("anchor resolution requires one explicit business intent")
    route = routes[0]
    policy = route["business_exploration"]
    for node_id, anchor in policy["initial_dns_publications"].items():
        query = urllib.parse.urlencode({"scope": "global", "traffic_class": route["traffic_class"], "path_prefix": route["path_prefix"], "dns_node_id": node_id})
        result = api("GET", "/v1/edge/quality-shadow/" + urllib.parse.quote(hostname, safe="") + "?" + query)
        snapshot = result.get("snapshot", {})
        if snapshot.get("hostname") != hostname or snapshot.get("traffic_class") != route["traffic_class"] or snapshot.get("path_prefix") != route["path_prefix"] or snapshot.get("policy") != route["policy"] or snapshot.get("blockers") or snapshot.get("service_receipts"):
            raise ValueError("business anchor observation differs from explicit service context")
        receipt = snapshot.get("actual_dns_receipt", {})
        if isinstance(receipt, str):
            receipt = json.loads(receipt)
        if receipt.get("node_id") != node_id or receipt.get("write_succeeded") is not True or receipt.get("rcode") != 0 or receipt.get("qtype") != 1 or not receipt.get("decision_id"):
            raise ValueError("business anchor requires successful actual DNS receipt")
        publication = receipt.get("answer_publication", {})
        state = receipt.get("publication", {})
        if state.get("serving_lkg") or state.get("rejected") or publication.get("release_channel") != "full" or publication.get("release_set_id") != full["artifact"]["id"] or publication.get("parent_digest") != full["artifact"]["content_hash"] or publication.get("fencing_token") != full["release"]["fencing_token"]:
            raise BusinessBaselineChanged("business DNS receipt has not settled on verified full baseline")
        expected_baseline = anchor.get("baseline_edge_id", policy["baseline_edge_id"])
        if snapshot.get("current_edge_id") != expected_baseline:
            raise ValueError("business DNS baseline edge changed; fresh intent required")
        candidates = snapshot.get("candidates", [])
        for edge_id in policy["candidate_edge_ids"]:
            matches = [candidate for candidate in candidates if candidate.get("edge_id") == edge_id]
            if len(matches) != 1 or matches[0].get("hard_gates") or matches[0].get("route_proof_verified") is not True:
                raise ValueError("business candidate lacks current positive route evidence")
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", publication.get("digest", "")) or publication["fencing_token"] < anchor["fencing_token"]:
            raise ValueError("business publication anchor regressed or is malformed")
        if publication["fencing_token"] == anchor["fencing_token"] and publication["digest"] != anchor["digest"]:
            raise ValueError("business publication digest changed without a new fence")
        anchor.update(digest=publication["digest"], fencing_token=publication["fencing_token"])
    binding = digest({"policy": resolved["projection_policy"], "full": config["precondition"]["serving_full"]})[7:23]
    resolved["projection_policy"]["generation"] += "-bound-" + binding
    resolved["producer_generation"] += "-bound-" + binding
    return resolved


FIELDS = set("version intent_id mode baseline_edge_id candidate_edge_ids initial_dns_publications starts_at expires_at exploration_basis_points maximum_candidate_basis_points maximum_step_basis_points bucket_seconds required_buckets minimum_connections_per_bucket cooldown_seconds evidence_max_age_seconds minimum_cohort_coverage_basis_points rtt_advantage_ratio delivery_advantage_ratio maximum_rtt_regression_ratio maximum_delivery_regression_ratio maximum_retransmission_increase stop_rtt_ms stop_retransmission_rate".split())


def validate_business_routes(routes):
    if not isinstance(routes, list) or len(routes) > 256:
        raise ValueError("bounded explicit route list required")
    hosts = set()
    for route in routes:
        host = route.get("hostname", "")
        if host in hosts or not host:
            raise ValueError("duplicate or missing route hostname")
        hosts.add(host)
        if "business_exploration" not in route:
            continue
        policy = route["business_exploration"]
        if not isinstance(policy, dict) or set(policy) != FIELDS or any(value is None for value in policy.values()):
            raise ValueError("complete explicit business exploration policy required")
        if policy["version"] != "business-edge-exploration-v1" or policy["mode"] not in ["observe", "adaptive", "paused"] or not route.get("path_prefix", "").startswith("/") or route.get("traffic_class") not in ["streaming", "dynamic_api"]:
            raise ValueError("unsupported business exploration context")
        candidates = policy["candidate_edge_ids"]
        if not isinstance(candidates, list) or not 2 <= len(candidates) <= 32 or len(set(candidates)) != len(candidates) or policy["baseline_edge_id"] not in candidates:
            raise ValueError("explicit unique candidate set and baseline required")
        for identity in [policy["intent_id"], policy["baseline_edge_id"], *candidates]:
            if not isinstance(identity, str) or not re.fullmatch(r"[^\s\x00]{1,128}", identity):
                raise ValueError("invalid business exploration identity")
        anchors = policy["initial_dns_publications"]
        if not isinstance(anchors, dict) or not 1 <= len(anchors) <= 32:
            raise ValueError("bounded initial DNS publications required")
        for node_id, anchor in anchors.items():
            if not isinstance(node_id, str) or not re.fullmatch(r"[^\s\x00]{1,128}", node_id) or not isinstance(anchor, dict) or set(anchor) not in ({"digest", "fencing_token"}, {"digest", "fencing_token", "baseline_edge_id"}) or not isinstance(anchor["digest"], str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", anchor["digest"]) or type(anchor["fencing_token"]) is not int or anchor["fencing_token"] < 1:
                raise ValueError("exact initial DNS publication required for each DNS node")
            if "baseline_edge_id" in anchor and anchor["baseline_edge_id"] not in candidates:
                raise ValueError("DNS baseline must be a declared candidate")
        bounds = {"exploration_basis_points": (len(candidates)-1, 1000), "maximum_candidate_basis_points": (policy["exploration_basis_points"], 9000),
                  "maximum_step_basis_points": (1, 1000), "bucket_seconds": (60, 3600), "required_buckets": (2, 12),
                  "minimum_connections_per_bucket": (3, 10000), "cooldown_seconds": (60, 86400),
                  "evidence_max_age_seconds": (policy["bucket_seconds"], 86400), "minimum_cohort_coverage_basis_points": (1, 10000)}
        for field, (minimum, maximum) in bounds.items():
            if type(policy[field]) is not int or not minimum <= policy[field] <= maximum:
                raise ValueError("business exploration bound invalid: " + field)
        for field in ["rtt_advantage_ratio", "delivery_advantage_ratio", "maximum_rtt_regression_ratio", "maximum_delivery_regression_ratio", "maximum_retransmission_increase", "stop_retransmission_rate", "stop_rtt_ms"]:
            value = policy[field]
            if type(value) not in [int, float] or not math.isfinite(value) or not 0 <= value <= (60000 if field == "stop_rtt_ms" else 1):
                raise ValueError("business exploration metric threshold invalid")
        if any(policy[field] <= 0 for field in ["rtt_advantage_ratio", "delivery_advantage_ratio", "stop_retransmission_rate", "stop_rtt_ms"]):
            raise ValueError("positive advantage and stop thresholds required")
        start, end = (datetime.fromisoformat(policy[key].replace("Z", "+00:00")) for key in ["starts_at", "expires_at"])
        if start.utcoffset() is None or end.utcoffset() is None or not timedelta(0) < end-start <= timedelta(days=7):
            raise ValueError("bounded absolute exploration lease required")


def check_business_delta(previous, successor):
    old, new = copy.deepcopy(previous), copy.deepcopy(successor)
    if old.pop("generation", None) == new.pop("generation", None):
        raise ValueError("business exploration generation must advance")
    old_routes = old.get("dns_query_policy", {}).pop("physical_routes", [])
    new_routes = new.get("dns_query_policy", {}).pop("physical_routes", [])
    validate_business_routes(old_routes)
    validate_business_routes(new_routes)
    if old != new:
        raise ValueError("business exploration changed unrelated constraints")
    before, after = ({route["hostname"]: route for route in routes} for routes in [old_routes, new_routes])
    changed = [host for host in before.keys() | after.keys() if before.get(host) != after.get(host)]
    if len(changed) != 1:
        raise ValueError("exactly one business exploration hostname must change")
    host = changed[0]
    prior, current = before.get(host), after.get(host)
    if not (prior or {}).get("business_exploration") and not (current or {}).get("business_exploration"):
        raise ValueError("transition is not a business exploration change")
    if prior and current:
        old_policy, new_policy = prior.pop("business_exploration", None), current.pop("business_exploration", None)
        if old_policy and new_policy and old_policy["intent_id"] == new_policy["intent_id"]:
            raise ValueError("changed exploration requires a new intent identity")
        prior.pop("path_prefix", None)
        current.pop("path_prefix", None)
        if prior != current:
            raise ValueError("business exploration changed route ownership or legacy quality settings")
    return host


def validate_business_preview(preview, policy, hostname):
    routes = [route for route in policy["dns_query_policy"].get("physical_routes", []) if route["hostname"] == hostname and route.get("business_exploration")]
    facts = preview.get("runtime_snapshot", {}).get("facts", {}).get("business_exploration_coverage", [])
    coverage = [entry for entry in facts if entry.get("hostname") == hostname]
    if not routes:
        if coverage:
            raise ValueError("removed exploration still appears in preview")
        return {"hostname": hostname, "exploration_removed": True}
    intent = routes[0]["business_exploration"]
    expected = {entry["node_id"] for entry in preview.get("runtime_snapshot", {}).get("dns_selections", []) if entry.get("hostname") == hostname and entry.get("type") == "A"}
    if not expected or len(coverage) != len(expected) or {entry.get("dns_node_id") for entry in coverage} != expected:
        raise ValueError("business preview omits a public DNS assignment")
    for entry in coverage:
        if entry.get("intent_id") != intent["intent_id"] or entry.get("action") == "withhold" or not re.fullmatch(r"sha256:[0-9a-f]{64}", entry.get("allocation_receipt_digest", "")):
            raise ValueError("business preview lacks exact replayable allocation")
        if intent["mode"] == "paused" and entry.get("action") != "baseline_only":
            raise ValueError("paused business intent did not withdraw exploration")
    return {"hostname": hostname, "intent_id": intent["intent_id"], "assignments": coverage}
