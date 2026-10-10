import copy
import unittest

from scripts import configure_dynamic_quality as quality
from scripts.test_reconfigure_physical_dns import fixture as physical_fixture


def fixture():
    config, _, source = physical_fixture()
    config.pop("hostname")
    config["schema"] = "fugue.dynamic-quality-reconfiguration/v1"
    config["baseline_mode"] = "latest_verified_same_policy"
    config["precondition"]["operation"] = "dynamic_quality"
    network = config["projection_policy"]["dns_query_policy"].pop("physical_routes")[0]["policy"]
    network["version"] = "physical-network-delivery-v4"
    ordered = {"default_order": {"version": "physical-order-v1", "ordered_edge_ids": ["edge-a", "edge-b"]}, "overrides": []}
    source["dns_query_policy"]["ordered_projection"] = copy.deepcopy(ordered)
    query = config["projection_policy"]["dns_query_policy"]
    query["ordered_projection"] = ordered
    query["dynamic_quality"] = {"mode": "all_dynamic", "policy": network, "refresh_queries_per_cycle": 16, "refresh_concurrency": 4}
    return config, source


class DynamicQualityConfigurationTests(unittest.TestCase):
    def test_generic_delta_preserves_constraints_and_orders(self):
        config, source = fixture()
        quality.validate(config)
        quality.check_delta(source, config["projection_policy"])
        for key in ["minimum_ttl_seconds", "ordered_projection", "physical_routes"]:
            changed = copy.deepcopy(config["projection_policy"])
            changed["dns_query_policy"][key] = None
            with self.assertRaises(ValueError):
                quality.check_delta(source, changed)

    def test_incomplete_or_unbounded_policies_reject(self):
        for field in ["mode", "policy", "refresh_queries_per_cycle", "refresh_concurrency"]:
            config, _ = fixture()
            del config["projection_policy"]["dns_query_policy"]["dynamic_quality"][field]
            with self.assertRaises(ValueError):
                quality.validate(config)
        for concurrency in [0, 9, True]:
            config, _ = fixture()
            config["projection_policy"]["dns_query_policy"]["dynamic_quality"]["refresh_concurrency"] = concurrency
            with self.assertRaises(ValueError):
                quality.validate(config)

    def test_preview_requires_unchanged_intent_and_explicit_learning(self):
        before = {"business_snapshot_revision": "before", "intent": {"dns": [{"hostname": "static.example.test", "type": "A", "values": ["8.8.8.8"]}]}, "issues": []}
        after = copy.deepcopy(before)
        after["business_snapshot_revision"] = "after"
        after["runtime_snapshot"] = {"facts": {"dynamic_quality_coverage": {"mode": "all_dynamic", "query_count": 2, "hostnames": {"app.example.test": "learning_current_ready_primary", "static.example.test": "static_constraint"}}}}
        quality.validate_preview(before, after)
        after["intent"]["dns"][0]["values"] = ["8.8.4.4"]
        with self.assertRaises(ValueError):
            quality.validate_preview(before, after)


if __name__ == "__main__":
    unittest.main()
