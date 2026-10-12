import copy
import unittest

from scripts import configure_business_exploration as business
from scripts import configure_dynamic_quality as quality
from scripts.test_configure_dynamic_quality import fixture as quality_fixture


def fixture():
    config, _ = quality_fixture()
    previous = copy.deepcopy(config["projection_policy"])
    previous["generation"] = "previous"
    config["precondition"]["operation"] = "business_exploration"
    policy = {"version": "business-edge-exploration-v1", "intent_id": "intent-a", "mode": "observe", "baseline_edge_id": "edge-a", "candidate_edge_ids": ["edge-a", "edge-b"],
              "initial_dns_publications": {"dns-a": {"digest": "sha256:" + "a"*64, "fencing_token": 1}},
              "starts_at": "2026-01-01T00:00:00Z", "expires_at": "2026-01-02T00:00:00Z", "exploration_basis_points": 100, "maximum_candidate_basis_points": 1000, "maximum_step_basis_points": 50,
              "bucket_seconds": 300, "required_buckets": 3, "minimum_connections_per_bucket": 3, "cooldown_seconds": 600, "evidence_max_age_seconds": 600,
              "minimum_cohort_coverage_basis_points": 9000, "rtt_advantage_ratio": .15, "delivery_advantage_ratio": .2, "maximum_rtt_regression_ratio": .1,
              "maximum_delivery_regression_ratio": .1, "maximum_retransmission_increase": .01, "stop_rtt_ms": 3000, "stop_retransmission_rate": .2}
    config["projection_policy"]["dns_query_policy"]["physical_routes"] = [{"hostname": "app.example.test", "path_prefix": "/", "traffic_class": "dynamic_api", "policy": copy.deepcopy(config["projection_policy"]["dns_query_policy"]["dynamic_quality"]["policy"]), "business_exploration": policy}]
    return config, previous


class BusinessExplorationConfigurationTests(unittest.TestCase):
    def test_explicit_enrollment_and_independent_phase_change(self):
        config, previous = fixture()
        quality.validate(config)
        self.assertEqual(business.check_business_delta(previous, config["projection_policy"]), "app.example.test")
        for mode in ["adaptive", "paused", "remove"]:
            successor = copy.deepcopy(config["projection_policy"])
            successor["generation"] = "next-"+mode
            if mode == "remove":
                successor["dns_query_policy"].pop("physical_routes")
            else:
                policy = successor["dns_query_policy"]["physical_routes"][0]["business_exploration"]
                policy["mode"] = mode
                with self.assertRaises(ValueError):
                    business.check_business_delta(config["projection_policy"], successor)
                policy["intent_id"] = "intent-"+mode
            business.check_business_delta(config["projection_policy"], successor)
        self.assertEqual(config["projection_policy"]["dns_query_policy"]["physical_routes"][0]["business_exploration"]["mode"], "observe")

    def test_complete_policy_and_no_unrelated_changes(self):
        config, previous = fixture()
        policy = config["projection_policy"]["dns_query_policy"]["physical_routes"][0]["business_exploration"]
        for field in business.FIELDS:
            value = policy.pop(field)
            with self.assertRaises(ValueError):
                quality.validate(config)
            policy[field] = value
        for field in ["ordered_projection", "minimum_ttl_seconds"]:
            candidate = copy.deepcopy(config["projection_policy"])
            candidate["dns_query_policy"].pop(field)
            with self.assertRaises(ValueError):
                business.check_business_delta(previous, candidate)
        candidate = copy.deepcopy(config["projection_policy"])
        other = copy.deepcopy(candidate["dns_query_policy"]["physical_routes"][0])
        other["hostname"] = "other.example.test"
        candidate["dns_query_policy"]["physical_routes"].append(other)
        with self.assertRaises(ValueError):
            business.check_business_delta(previous, candidate)

    def test_preview_requires_every_assignment_and_paused_withdrawal(self):
        config, _ = fixture()
        policy = config["projection_policy"]
        entries = [{"hostname": "app.example.test", "dns_node_id": node, "intent_id": "intent-a", "action": "initialize", "allocation_receipt_digest": "sha256:"+"a"*64} for node in ["dns-a", "dns-b"]]
        preview = {"runtime_snapshot": {"dns_selections": [{"hostname": "app.example.test", "node_id": node, "type": "A"} for node in ["dns-a", "dns-b"]], "facts": {"business_exploration_coverage": entries}}}
        business.validate_business_preview(preview, policy, "app.example.test")
        entries.pop()
        with self.assertRaises(ValueError):
            business.validate_business_preview(preview, policy, "app.example.test")
        entries.append(dict(entries[0], dns_node_id="dns-b"))
        policy["dns_query_policy"]["physical_routes"][0]["business_exploration"]["mode"] = "paused"
        with self.assertRaises(ValueError):
            business.validate_business_preview(preview, policy, "app.example.test")
        for entry in entries:
            entry["action"] = "baseline_only"
        business.validate_business_preview(preview, policy, "app.example.test")


if __name__ == "__main__":
    unittest.main()
