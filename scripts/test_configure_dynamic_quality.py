import copy
import unittest
from unittest.mock import patch

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
    def test_configuration_waits_only_for_verified_same_producer_baseline(self):
        config, _ = fixture()
        resolved = copy.deepcopy(config)
        resolved["precondition"]["serving_full"]["fencing_token"] += 2
        with patch.object(quality, "resolve_baseline", side_effect=[ValueError("baseline is not the predecessor's verified producer publication"), resolved]), patch.object(quality, "baseline"), patch.object(quality.time, "sleep") as wait:
            self.assertEqual(quality.wait_verified_baseline(config, None), resolved)
            wait.assert_called_once_with(10)
        with patch.object(quality, "resolve_baseline", side_effect=ValueError("renewed baseline changed the declared producer or regressed its fence")), patch.object(quality.time, "sleep") as wait:
            with self.assertRaises(ValueError):
                quality.wait_verified_baseline(config, None)
            wait.assert_not_called()

    def test_entrypoint_uses_supported_bounded_transport(self):
        config, _ = fixture()
        import json
        with patch.object(quality.Path, "read_bytes", return_value=json.dumps(config).encode()), patch.object(quality, "publish", return_value={"accepted": True}) as publish, patch.object(quality.os.environ, "pop", return_value="synthetic-key"), patch("sys.argv", ["configure", "declaration.json", "--evidence", "evidence.json"]):
            quality.main()
        self.assertEqual(publish.call_args.args[1].response_limit, 128 << 20)
        self.assertEqual(publish.call_args.args[1].timeout, 120)

    def test_large_preview_bounds_require_explicit_configuration(self):
        default = quality.API("https://api.example.test", "synthetic-key")
        self.assertEqual((default.response_limit, default.timeout), (1 << 20, 30))
        for options in [{"response_limit": 129 << 20}, {"response_limit": True}, {"timeout": 121}, {"timeout": True}, {"timeout": 0}]:
            with self.assertRaises(ValueError):
                quality.API("https://api.example.test", "synthetic-key", **options)

    def test_generic_delta_preserves_constraints_and_orders(self):
        config, source = fixture()
        quality.validate(config)
        quality.check_delta(source, config["projection_policy"])
        for key in ["minimum_ttl_seconds", "ordered_projection"]:
            changed = copy.deepcopy(config["projection_policy"])
            changed["dns_query_policy"][key] = None
            with self.assertRaises(ValueError):
                quality.check_delta(source, changed)

    def test_universal_adoption_removes_only_equivalent_quality_overrides(self):
        config, source = fixture()
        policy = copy.deepcopy(config["projection_policy"]["dns_query_policy"]["dynamic_quality"]["policy"])
        policy["version"] = "physical-network-bounded-v3"
        source["dns_query_policy"]["physical_routes"] = [{"hostname": "app.example.test", "traffic_class": "streaming", "policy": policy}]
        quality.check_delta(source, config["projection_policy"])
        source["dns_query_policy"]["physical_routes"][0]["policy"]["advantage_ms"] += 10
        with self.assertRaises(ValueError):
            quality.check_delta(source, config["projection_policy"])

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
