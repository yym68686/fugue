import argparse
import hashlib
import json
from pathlib import Path


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def audit_artifact(document):
    artifact = document.get("artifact", document)
    if artifact.get("artifact_kind") != "dns_answer_bundle" or artifact.get("status") != "validated":
        raise ValueError("validated DNS artifact export required")
    content = artifact.get("content")
    if not isinstance(content, dict):
        raise ValueError("DNS artifact content required")
    digest = "sha256:" + hashlib.sha256(canonical(content).encode()).hexdigest()
    if artifact.get("content_hash") != digest:
        raise ValueError("DNS artifact content digest differs")
    views = content.get("query_views")
    rules = content.get("policy", {}).get("dns_answer_rules")
    if not isinstance(views, list) or not views or not isinstance(rules, list):
        raise ValueError("explicit query views and answer rules required")
    declared = {}
    for rule in rules:
        key = (rule.get("node_id"), rule.get("hostname"), rule.get("type"))
        if not all(key) or key in declared or not rule.get("selection_mode"):
            raise ValueError("duplicate or incomplete DNS rule")
        declared[key] = rule
    dependencies, physical, ordered, static, seen = [], [], [], [], set()
    for view in views:
        if not view.get("node_id") or not view.get("zone") or not isinstance(view.get("records"), list):
            raise ValueError("incomplete DNS query view")
        for record in view["records"]:
            key = (view["node_id"], record.get("name"), record.get("type"))
            if not all(key) or key in seen:
                raise ValueError("duplicate or incomplete query record")
            seen.add(key)
            policy = record.get("answer_policy", {})
            kind = policy.get("policy_kind", "")
            rule = declared.pop(key, None)
            identity = {"node_id": key[0], "hostname": key[1], "type": key[2]}
            if rule is None:
                if kind or record.get("candidates") or record.get("scoped_candidates"):
                    raise ValueError("dynamic record lacks an explicit rule")
                static.append(identity)
                continue
            if rule["selection_mode"] != kind:
                raise ValueError("query record and rule disagree")
            candidates = record.get("candidates")
            if not isinstance(candidates, list) or not candidates:
                raise ValueError("dynamic query lacks candidates")
            if kind not in ["physical_quality", "physical_order"]:
                dependencies.append(dict(identity, policy_kind=kind,
                    scoped_profiles=len(record.get("scoped_candidates", [])),
                    exploration_percent=rule.get("exploration_percent")))
                continue
            if kind == "physical_order":
                declared_order = policy.get("physical_order", {})
                order = declared_order.get("ordered_edge_ids", [])
                authorized = {candidate.get("edge_id") for candidate in candidates}
                if (declared_order.get("version") != "physical-order-v1" or
                        declared_order != rule.get("physical_order") or not order or
                        len(set(order)) != len(order) or not set(order).issubset(authorized) or
                        record.get("scoped_candidates") or policy.get("physical_selection") or
                        rule.get("exploration_percent", 0) or rule.get("scoped_selection_mode") or
                        policy.get("ecs_enabled") or rule.get("ecs_enabled") or
                        policy.get("exploration_percent", 0) or policy.get("selected_edge_group_id") or
                        policy.get("preferred_edge_groups") or policy.get("fallback_edge_groups")):
                    raise ValueError("ordered query contains legacy or ambiguous selection authority")
                ordered.append(identity)
                continue
            selection = policy.get("physical_selection", {})
            order = selection.get("ordered_edge_ids", [])
            authorized = {candidate.get("edge_id") for candidate in candidates}
            if (selection.get("version") != "physical-edge-network-v1" or
                    not order or order[0] != selection.get("primary_edge_id") or
                    len(set(order)) != len(order) or not set(order).issubset(authorized) or
                    selection.get("scope") != "global" or record.get("scoped_candidates") or
                    rule.get("exploration_percent", 0) or rule.get("scoped_selection_mode") or
                    policy.get("exploration_percent", 0) or policy.get("selected_edge_group_id") or
                    policy.get("preferred_edge_groups") or policy.get("fallback_edge_groups")):
                raise ValueError("physical query contains legacy or ambiguous selection authority")
            physical.append(identity)
    if declared:
        raise ValueError("answer rule has no query record")
    return {
        "artifact_id": artifact.get("id"), "content_hash": digest,
        "export_digest_verified": True, "signature_verified": False,
        "serving_state_verified": False, "positive_lkg_verified": False,
        "legacy_queries": dependencies,
        "legacy_hostname_count": len({entry["hostname"] for entry in dependencies}),
        "physical_query_count": len(physical), "ordered_query_count": len(ordered), "static_query_count": len(static),
        "query_policy_compatible_with_legacy_removal": not dependencies,
    }


def main():
    parser = argparse.ArgumentParser(description="Read-only audit of exported DNS artifact selector dependencies.")
    parser.add_argument("artifacts", nargs="+")
    args = parser.parse_args()
    results = []
    for filename in args.artifacts:
        raw = Path(filename).read_bytes()
        if len(raw) > 32 << 20:
            raise ValueError("DNS artifact export exceeds bound")
        results.append(audit_artifact(json.loads(raw)))
    compatible = all(result["query_policy_compatible_with_legacy_removal"] for result in results)
    print(canonical({"schema": "fugue.dns-selector-retirement-audit/v1", "artifacts": results,
        "all_exported_query_policies_compatible": compatible,
        "production_retirement_authorized": False,
        "limitations": ["exports_do_not_prove_live_assignments_or_positive_lkg",
                        "digest_is_not_signature_verification",
                        "all_serving_and_recovery_artifacts_require_separate_verification"]}))
    return 0 if compatible else 2


if __name__ == "__main__":
    raise SystemExit(main())
