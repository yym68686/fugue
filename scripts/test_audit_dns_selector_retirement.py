import copy
import hashlib
import unittest

from scripts.audit_dns_selector_retirement import audit_artifact, canonical


def fixture(kind="geo"):
    record = {"name": "app.example.test", "type": "A", "values": ["8.8.8.8"],
        "answer_policy": {"policy_kind": kind},
        "candidates": [{"edge_id": "edge-a", "ip": "8.8.8.8"}]}
    if kind == "physical_quality":
        record["answer_policy"]["physical_selection"] = {
            "version": "physical-edge-network-v1", "scope": "global",
            "primary_edge_id": "edge-a", "ordered_edge_ids": ["edge-a"]}
    content = {"query_views": [{"node_id": "dns-a", "zone": "example.test", "records": [record]}],
        "policy": {"dns_answer_rules": [{"node_id": "dns-a", "hostname": "app.example.test",
            "type": "A", "selection_mode": kind, "exploration_percent": 0}]}}
    artifact = {"id": "artifact-example", "artifact_kind": "dns_answer_bundle",
        "status": "validated", "content": content}
    return seal(artifact)


def seal(artifact):
    artifact["content_hash"] = "sha256:" + hashlib.sha256(canonical(artifact["content"]).encode()).hexdigest()
    return artifact


class RetirementAuditTests(unittest.TestCase):
    def test_explicit_order_is_separate_from_measured_quality(self):
        artifact = fixture("physical_order")
        rule = artifact["content"]["policy"]["dns_answer_rules"][0]
        policy = artifact["content"]["query_views"][0]["records"][0]["answer_policy"]
        rule["physical_order"] = {"version": "physical-order-v1", "ordered_edge_ids": ["edge-a"]}
        policy["physical_order"] = copy.deepcopy(rule["physical_order"])
        seal(artifact)
        result = audit_artifact(artifact)
        self.assertTrue(result["query_policy_compatible_with_legacy_removal"])
        self.assertEqual(result["ordered_query_count"], 1)
        self.assertEqual(result["physical_query_count"], 0)
        for scenario in ["unknown", "different_rule", "ecs", "exploration", "measurement"]:
            changed = copy.deepcopy(artifact)
            policy = changed["content"]["query_views"][0]["records"][0]["answer_policy"]
            if scenario == "unknown": policy["physical_order"]["ordered_edge_ids"] = ["foreign"]
            elif scenario == "different_rule": changed["content"]["policy"]["dns_answer_rules"][0].pop("physical_order")
            elif scenario == "ecs": policy["ecs_enabled"] = True
            elif scenario == "exploration": policy["exploration_percent"] = 5
            elif scenario == "measurement": policy["physical_selection"] = {"primary_edge_id": "edge-a"}
            with self.assertRaises(ValueError): audit_artifact(seal(changed))

    def test_legacy_artifact_blocks_removal_without_inferring_live_state(self):
        for kind in ["geo", "latency_aware", "weighted", "unrecognized"]:
            result = audit_artifact({"artifact": fixture(kind)})
            self.assertEqual(result["legacy_hostname_count"], 1)
            self.assertFalse(result["query_policy_compatible_with_legacy_removal"])
            self.assertFalse(result["signature_verified"])
            self.assertFalse(result["serving_state_verified"])
            self.assertFalse(result["positive_lkg_verified"])

    def test_physical_and_static_queries_are_separate(self):
        artifact = fixture("physical_quality")
        artifact["content"]["query_views"][0]["records"].append({
            "name": "static.example.test", "type": "A", "values": ["9.9.9.9"]})
        before = copy.deepcopy(seal(artifact))
        result = audit_artifact(artifact)
        self.assertTrue(result["query_policy_compatible_with_legacy_removal"])
        self.assertEqual(result["physical_query_count"], 1)
        self.assertEqual(result["static_query_count"], 1)
        self.assertEqual(artifact, before)

    def test_incomplete_or_conflicting_exports_fail_closed(self):
        for name in ["digest", "missing_views", "missing_rule", "orphan_rule", "duplicate",
                     "mode", "unbound_edge", "exploration", "group_override"]:
            with self.subTest(name=name):
                artifact = fixture("physical_quality")
                content = artifact["content"]
                record = content["query_views"][0]["records"][0]
                if name == "digest":
                    artifact["content_hash"] = "sha256:" + "a" * 64
                elif name == "missing_views":
                    content.pop("query_views")
                elif name == "missing_rule":
                    content["policy"]["dns_answer_rules"] = []
                elif name == "orphan_rule":
                    content["query_views"][0]["records"] = []
                elif name == "duplicate":
                    content["query_views"][0]["records"].append(copy.deepcopy(record))
                elif name == "mode":
                    record["answer_policy"]["policy_kind"] = "geo"
                elif name == "unbound_edge":
                    record["answer_policy"]["physical_selection"]["ordered_edge_ids"].append("edge-other")
                elif name == "exploration":
                    content["policy"]["dns_answer_rules"][0]["exploration_percent"] = 5
                elif name == "group_override":
                    record["answer_policy"]["selected_edge_group_id"] = "group-a"
                if name != "digest":
                    seal(artifact)
                with self.assertRaises(ValueError):
                    audit_artifact(artifact)


if __name__ == "__main__":
    unittest.main()
