import argparse
import copy
import json
import os
from pathlib import Path

from scripts import reconfigure_physical_dns as physical
from scripts.audit_dns_selector_retirement import audit_artifact
from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.publish_agent_edge_shadow import API, canonical


def legacy_order(record):
    policy = record.get("answer_policy", {})
    mode = policy.get("policy_kind")
    candidates = record.get("candidates", [])
    if mode not in ["geo", "latency_aware", "weighted", "global", "pinned"] or not candidates or record.get("scoped_candidates"):
        raise ValueError("unscoped legacy baseline required")
    has_score = any(candidate.get("score", 0) > 0 for candidate in candidates)
    def score(candidate):
        priority, weight, quality = candidate.get("priority", 0), candidate.get("weight", 0), candidate.get("score", 0)
        if mode == "latency_aware":
            return (int(quality) if quality > 0 else 500000) + priority * 10 - weight if has_score else priority * 10 - weight * 20
        return priority * 100 - (weight * 20 if mode == "weighted" else 0) - (250 if candidate.get("reason", "").lower() == "same_region" else 0) + (int(quality) if quality > 0 else 0)
    ordered = sorted(candidates, key=lambda candidate: (score(candidate), candidate.get("priority", 0), -candidate.get("weight", 0), candidate.get("edge_group_id", ""), candidate["ip"]))
    selected_group = policy.get("selected_edge_group_id", "").strip().lower()
    for index, candidate in enumerate(ordered):
        if selected_group and candidate.get("edge_group_id", "").strip().lower() == selected_group:
            ordered.insert(0, ordered.pop(index))
            break
    identities = list(dict.fromkeys(candidate.get("edge_id") for candidate in ordered))
    if not all(identities) or len(identities) > 256:
        raise ValueError("physical candidate ownership required")
    return {"version": "physical-order-v1", "ordered_edge_ids": identities}


def baseline_orders(artifact):
    audit_artifact(artifact)
    overrides, allowed = [], set()
    for view in artifact["content"]["query_views"]:
        for record in view["records"]:
            if not record.get("candidates"):
                continue
            allowed.update(candidate["edge_id"] for candidate in record["candidates"])
            if record["answer_policy"]["policy_kind"] == "physical_quality":
                continue
            overrides.append({"node_id": view["node_id"], "hostname": record["name"], "type": record["type"], "order": legacy_order(record)})
    return sorted(overrides, key=lambda entry: (entry["node_id"], entry["hostname"], entry["type"])), allowed


def validate(config):
    fields = {"schema", "generation", "origin", "producer_generation", "precondition", "projection_policy", "baseline_mode"}
    if not isinstance(config, dict) or set(config) not in [fields, fields | {"order_baseline_mode"}] or config["schema"] != "fugue.dns-selector-retirement/v1" or not physical.bounded_json(config):
        raise ValueError("explicit retirement declaration required")
    if config.get("order_baseline_mode", "declared") not in ["declared", "latest_verified_same_policy"] or config.get("order_baseline_mode") == "latest_verified_same_policy" and config["baseline_mode"] != "latest_verified_same_policy":
        raise ValueError("order renewal requires the same verified producer baseline")
    probe = copy.deepcopy(config)
    probe.pop("order_baseline_mode", None)
    probe["schema"] = "fugue.physical-dns-reconfiguration/v1"
    routes = config["projection_policy"].get("dns_query_policy", {}).get("physical_routes", [])
    if not routes:
        raise ValueError("existing measured physical policy required")
    probe["hostname"] = routes[0]["hostname"]
    probe["precondition"]["operation"] = "physical_dns"
    physical.validate(probe)
    query = config["projection_policy"]["dns_query_policy"]
    projection = query.get("ordered_projection", {})
    if config["precondition"].get("operation") != "retire_dns_selector" or query.get("ecs_enabled") is not False or query.get("exploration_percent") != 0 or set(projection) != {"default_order", "overrides"}:
        raise ValueError("retirement requires explicit non-geographic non-exploratory order")
    entries = projection["overrides"]
    if not isinstance(entries, list) or not entries or len(entries) > 20000:
        raise ValueError("complete bounded normal-order overrides required")
    seen = set()
    for entry in entries:
        if set(entry) != {"node_id", "hostname", "type", "order"} or entry["type"] not in ["A", "AAAA"]:
            raise ValueError("explicit per-consumer family override required")
        identity = (entry["node_id"], entry["hostname"], entry["type"])
        if not all(isinstance(item, str) and item for item in identity) or identity in seen:
            raise ValueError("duplicate or invalid override")
        seen.add(identity)
    for order in [projection["default_order"]] + [entry["order"] for entry in entries]:
        if set(order) != {"version", "ordered_edge_ids"} or order["version"] != "physical-order-v1":
            raise ValueError("explicit order version required")
        identities = order["ordered_edge_ids"]
        if not isinstance(identities, list) or not 1 <= len(identities) <= 256 or any(not isinstance(identity, str) or not identity or len(identity) > 128 or identity.strip() != identity for identity in identities) or len(set(identities)) != len(identities):
            raise ValueError("invalid physical order")
    return config


def check_delta(before, after):
    old, new = copy.deepcopy(before), copy.deepcopy(after)
    if old.pop("generation", None) == new.pop("generation", None):
        raise ValueError("source generation must advance")
    old_query, new_query = old["dns_query_policy"], new["dns_query_policy"]
    if old_query.get("ordered_projection") or not new_query.pop("ordered_projection", None) or any(client.get("rules") for client in old.get("dns_client_policies", [])):
        raise ValueError("only an unmapped legacy predecessor can retire")
    for key in ["ecs_enabled", "exploration_percent"]:
        new_query[key] = old_query[key]
    if old != new:
        raise ValueError("retirement changed constraints, static ownership or measured policy")


def dns_baseline(api, full):
    content = full["artifact"]["content"]
    members = list(zip(content["artifact_ids"], content["artifact_kinds"], strict=True))
    identities = [identity for identity, kind in members if kind == "dns_answer_bundle"]
    if len(identities) != 1:
        raise ValueError("one signed baseline DNS member required")
    artifact = api("GET", "/v1/admin/artifacts/" + identities[0])["artifact"]
    if artifact.get("scope_key") != "global" or artifact.get("metadata", {}).get("release_set_generation") != full["artifact"]["generation"]:
        raise ValueError("DNS member does not belong to exact full baseline")
    return artifact


def check_orders(config, artifact):
    expected, allowed = baseline_orders(artifact)
    projection = config["projection_policy"]["dns_query_policy"]["ordered_projection"]
    actual = sorted(projection["overrides"], key=lambda entry: (entry["node_id"], entry["hostname"], entry["type"]))
    if actual != expected or set(projection["default_order"]["ordered_edge_ids"]) != allowed:
        raise ValueError("declared orders or membership differ from the signed baseline")
    return expected


def resolve_orders(config, artifact):
    if config.get("order_baseline_mode", "declared") == "declared":
        return config
    expected, allowed = baseline_orders(artifact)
    projection = config["projection_policy"]["dns_query_policy"]["ordered_projection"]
    def membership(entries):
        return {(entry["node_id"], entry["hostname"], entry["type"]): set(entry["order"]["ordered_edge_ids"]) for entry in entries}
    if membership(expected) != membership(projection["overrides"]) or set(projection["default_order"]["ordered_edge_ids"]) != allowed:
        raise ValueError("renewed order changed declared records or physical membership")
    resolved = copy.deepcopy(config)
    resolved["projection_policy"]["dns_query_policy"]["ordered_projection"]["overrides"] = expected
    generation = config["producer_generation"] + "-" + digest(expected).split(":")[1][:12]
    resolved["producer_generation"] = generation
    resolved["projection_policy"]["generation"] = generation
    validate(resolved)
    return resolved


def validate_preview(config, preview, previous):
    if not preview.get("business_snapshot_revision") or not previous.get("business_snapshot_revision") or not previous.get("intent") or previous["intent"] != preview.get("intent"):
        raise ValueError("retirement requires unchanged business and static intent content from identified snapshots")
    for issue in preview.get("issues", []):
        if issue.get("code") not in ["dns_output_equivalence_not_verified", "release_target_equivalence_not_verified", "origin_observation_not_fresh"] or issue not in previous.get("issues", []):
            raise ValueError("new or unrecognized projection issue")
    expected = {(entry["node_id"], entry["hostname"], entry["type"]): entry["order"] for entry in config["projection_policy"]["dns_query_policy"]["ordered_projection"]["overrides"]}
    rules = preview.get("policy", {}).get("dns_answer_rules", [])
    for rule in rules:
        if rule["selection_mode"] == "physical_quality":
            continue
        key = (rule["node_id"], rule["hostname"], rule["type"])
        if rule["selection_mode"] != "physical_order" or rule.get("physical_order") != expected.pop(key, None):
            raise ValueError("preview changed a declared order")
    if expected:
        raise ValueError("preview omitted declared dynamic records")
    summaries = []
    for route in config["projection_policy"]["dns_query_policy"]["physical_routes"]:
        physical_config = dict(config, hostname=route["hostname"])
        summaries.extend(physical.validate_preview(physical_config, preview, previous))
    return summaries


def publish(config, api, save):
    validate(config)
    declaration = digest(config)
    config = physical.resolve_baseline(config, api)
    full = physical.baseline(config, api)
    baseline = dns_baseline(api, full)
    config = resolve_orders(config, baseline)
    orders = check_orders(config, baseline)
    precondition = config["precondition"]
    current = selected(api, physical.SCOPE, "shadow")
    if authority_identity(current) != precondition["previous_policy"] and current.get("artifact", {}).get("generation") != config["producer_generation"]:
        raise ValueError("producer changed before retirement")
    old = api("GET", "/v1/admin/artifacts/" + precondition["previous_policy"]["artifact_id"])["artifact"]
    previous = old["content"]
    if old.get("content_hash") != precondition["previous_policy"]["content_hash"] or old.get("status") != "validated" or not exact(old, "policy_snapshot", physical.SCOPE, previous) or previous.get("mode") != "serving" or previous.get("target_scope") != "global" or previous.get("publication_role") or previous.get("route_placement_transition") or not previous.get("serving") or previous["serving"].get("single_publication", False):
        raise ValueError("continuous global predecessor required")
    source = api("GET", "/v1/admin/artifacts/" + previous["dns_policy_artifact_id"])["artifact"]
    if source.get("content_hash") != previous["dns_policy_digest"] or not exact(source, "policy_snapshot", "global", source["content"]):
        raise ValueError("pinned source differs")
    check_delta(source["content"], config["projection_policy"])
    source = ensure_input(api, "policy_snapshot", "global", config["projection_policy"])
    successor = dict(previous, generation=config["producer_generation"], dns_policy_artifact_id=source["id"], dns_policy_digest=source["content_hash"])
    if authority_identity(current) != precondition["previous_policy"] and not exact(current.get("artifact", {}), "policy_snapshot", physical.SCOPE, successor):
        raise ValueError("same-generation successor differs")
    artifact = ensure_input(api, "policy_snapshot", physical.SCOPE, successor)
    previous_preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + old["id"])
    preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + artifact["id"])
    summaries = validate_preview(config, preview, previous_preview)
    physical.baseline(config, api)
    evidence = {"schema": "fugue.dns-selector-retirement-result/v1", "declaration_digest": declaration, "resolved_precondition": precondition, "baseline_dns_artifact_id": baseline["id"], "baseline_dns_digest": baseline["content_hash"], "preserved_order_count": len(orders), "resolved_orders_digest": digest(orders), "resolved_source_artifact_id": source["id"], "resolved_source_digest": source["content_hash"], "selections": summaries, "artifact_id": artifact["id"], "producer_activated": False, "routing_acceptance_complete": False}
    evidence["business_snapshots"] = {"previous_revision": previous_preview["business_snapshot_revision"], "successor_revision": preview["business_snapshot_revision"], "unchanged_intent_digest": digest(preview["intent"])}
    save(evidence)
    key = "producer-reconfiguration/" + digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "precondition": precondition})
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "shadow", "producer_reconfiguration": precondition, "idempotency_key": key, "reason": "Retire legacy selector using preserved signed physical orders and unchanged measured policies"})
    current = selected(api, physical.SCOPE, "shadow")
    if not exact(current.get("artifact", {}), "policy_snapshot", physical.SCOPE, successor):
        raise ValueError("retirement producer activation not confirmed")
    evidence.update(producer_activated=True, authority=authority_identity(current))
    save(evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--evidence")
    args = parser.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 4 << 20:
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
    print(canonical(publish(config, API(config["origin"], token, response_limit=16 << 20), save)))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("DNS selector retirement stopped: " + str(error)) from None
