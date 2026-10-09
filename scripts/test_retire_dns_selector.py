import copy
import unittest

from scripts import retire_dns_selector as retirement
from scripts.test_reconfigure_physical_dns import fixture as physical_fixture
from scripts.test_reconfigure_physical_dns import Ledger as PhysicalLedger
from scripts.test_audit_dns_selector_retirement import fixture as dns_fixture, seal


def fixture():
    config, old, source = physical_fixture()
    source["dns_query_policy"].update(ecs_enabled=True, exploration_percent=5)
    source["dns_query_policy"]["physical_routes"] = copy.deepcopy(config["projection_policy"]["dns_query_policy"]["physical_routes"])
    config["schema"] = "fugue.dns-selector-retirement/v1"
    config.pop("hostname")
    config["baseline_mode"] = "declared"
    config["precondition"]["operation"] = "retire_dns_selector"
    config["projection_policy"] = copy.deepcopy(source)
    config["projection_policy"]["generation"] = "retired"
    query = config["projection_policy"]["dns_query_policy"]
    query.update(ecs_enabled=False, exploration_percent=0)
    baseline = dns_fixture()
    baseline["content"]["query_views"][0]["records"][0]["name"] = "other.example.test"
    baseline["content"]["policy"]["dns_answer_rules"][0]["hostname"] = "other.example.test"
    seal(baseline)
    overrides, allowed = retirement.baseline_orders(baseline)
    query["ordered_projection"] = {"default_order": {"version": "physical-order-v1", "ordered_edge_ids": sorted(allowed)}, "overrides": overrides}
    return config, source, baseline


class Ledger(PhysicalLedger):
    def __init__(self, config, source, baseline):
        _, old, _ = physical_fixture()
        old["dns_policy_digest"] = retirement.digest(source)
        config["precondition"]["previous_policy"]["content_hash"] = retirement.digest(old)
        super().__init__(dict(config, hostname="app.example.test"), old, source)
        baseline.update(scope_key="global", generation="baseline", metadata={"release_set_generation": "full-current"})
        self.artifacts[baseline["id"]] = copy.deepcopy(baseline)
        full = self.authorities[("release_set", "global", "full")]
        full["artifact"]["content"].update(artifact_ids=[baseline["id"]], artifact_kinds=["dns_answer_bundle"])
        self.order = config["projection_policy"]["dns_query_policy"]["ordered_projection"]["overrides"][0]
        self.preview["intent"] = {"generation": "unchanged", "routes": [{"hostname": "app.example.test"}]}

    def __call__(self, method, path, body=None):
        result = super().__call__(method, path, body)
        if method == "GET" and path.startswith("/v1/admin/platform-config/routes/project?"):
            rule = {key: self.order[key] for key in ["node_id", "hostname", "type"]}
            rule.update(selection_mode="physical_order", physical_order=self.order["order"])
            result["policy"]["dns_answer_rules"] = [rule]
        return result


class RetirementTests(unittest.TestCase):
    def test_explicit_order_renewal_preserves_exact_current_full_membership(self):
        config, _, baseline = fixture()
        record = baseline["content"]["query_views"][0]["records"][0]
        record["candidates"].append({"edge_id": "edge-b", "edge_group_id": "group-b", "ip": "9.9.9.9", "priority": 1})
        seal(baseline)
        original, allowed = retirement.baseline_orders(baseline)
        projection = config["projection_policy"]["dns_query_policy"]["ordered_projection"]
        projection["overrides"] = original
        projection["default_order"]["ordered_edge_ids"] = sorted(allowed)
        record["candidates"][1]["priority"] = -1
        seal(baseline)
        with self.assertRaises(ValueError): retirement.check_orders(config, baseline)
        config.update(baseline_mode="latest_verified_same_policy", order_baseline_mode="latest_verified_same_policy")
        before = copy.deepcopy(config)
        resolved = retirement.resolve_orders(config, baseline)
        expected = retirement.check_orders(resolved, baseline)
        self.assertEqual(expected[0]["order"]["ordered_edge_ids"], ["edge-b", "edge-a"])
        self.assertEqual(config, before)
        self.assertNotEqual(config["producer_generation"], resolved["producer_generation"])
        self.assertEqual(resolved, retirement.resolve_orders(config, baseline))
        before_query = copy.deepcopy(config["projection_policy"]["dns_query_policy"])
        after_query = copy.deepcopy(resolved["projection_policy"]["dns_query_policy"])
        before_query.pop("ordered_projection")
        after_query.pop("ordered_projection")
        self.assertEqual(before_query, after_query)
        record["candidates"][1]["edge_id"] = "foreign"
        seal(baseline)
        with self.assertRaises(ValueError): retirement.resolve_orders(config, baseline)

    def test_fenced_publish_and_retry_preserve_full_lkg(self):
        config, source, baseline = fixture()
        ledger = Ledger(config, source, baseline)
        full = copy.deepcopy(ledger.authorities[("release_set", "global", "full")])
        lkg = copy.deepcopy(ledger.lkg)
        first = retirement.publish(config, ledger, lambda _: None)
        second = retirement.publish(config, ledger, lambda _: None)
        self.assertTrue(first["producer_activated"])
        self.assertFalse(first["routing_acceptance_complete"])
        self.assertEqual(first["authority"], second["authority"])
        self.assertEqual(full, ledger.authorities[("release_set", "global", "full")])
        self.assertEqual(lkg, ledger.lkg)

    def test_baseline_or_capture_race_never_activates(self):
        for failure in ["changed_order", "foreign_producer", "unverified", "capture_race", "missing_quality", "changed_intent"]:
            config, source, baseline = fixture()
            ledger = Ledger(config, source, baseline)
            if failure == "changed_order":
                record = ledger.artifacts[baseline["id"]]["content"]["query_views"][0]["records"][0]
                record["candidates"][0]["edge_id"] = "foreign"
                seal(ledger.artifacts[baseline["id"]])
            elif failure == "foreign_producer": ledger.authorities[("policy_snapshot", retirement.physical.SCOPE, "shadow")]["release"]["fencing_token"] += 1
            elif failure == "unverified": ledger.authorities[("release_set", "global", "full")]["release"]["verification_state"] = "unverified"
            elif failure == "capture_race": ledger.on_capture = lambda api: api.authorities[("release_set", "global", "full")]["release"].update(fencing_token=99)
            elif failure == "missing_quality": ledger.preview["runtime_snapshot"]["dns_selections"] = []
            elif failure == "changed_intent": ledger.on_capture = lambda api: api.preview["intent"].update(generation=api.preview["intent"]["generation"] + "-next")
            with self.assertRaises(ValueError): retirement.publish(config, ledger, lambda _: None)
            self.assertEqual([], ledger.release_bodies)

    def test_preserves_baseline_without_manufacturing_measurements(self):
        config, source, baseline = fixture()
        before = copy.deepcopy(baseline)
        retirement.validate(config)
        retirement.check_delta(source, config["projection_policy"])
        orders = retirement.check_orders(config, baseline)
        self.assertEqual(orders[0]["order"]["ordered_edge_ids"], ["edge-a"])
        self.assertEqual(before, baseline)

    def test_changed_membership_or_normal_order_rejected(self):
        for scenario in ["drop", "foreign", "static", "default", "duplicate", "version", "ecs", "exploration", "extra"]:
            with self.subTest(scenario=scenario):
                config, _, baseline = fixture()
                query = config["projection_policy"]["dns_query_policy"]
                projection = query["ordered_projection"]
                if scenario == "drop": projection["overrides"] = []
                elif scenario == "foreign": projection["overrides"][0]["order"]["ordered_edge_ids"] = ["foreign"]
                elif scenario == "static": projection["overrides"][0]["hostname"] = "static.example.test"
                elif scenario == "default": projection["default_order"]["ordered_edge_ids"] = ["foreign"]
                elif scenario == "duplicate": projection["overrides"].append(copy.deepcopy(projection["overrides"][0]))
                elif scenario == "version": projection["default_order"]["version"] = "next"
                elif scenario == "ecs": query["ecs_enabled"] = True
                elif scenario == "exploration": query["exploration_percent"] = 1
                elif scenario == "extra": config["force"] = True
                with self.assertRaises(ValueError):
                    retirement.validate(config)
                    retirement.check_orders(config, baseline)

    def test_policy_change_boundary(self):
        for scenario in ["ttl", "network", "scope", "clients", "generation"]:
            config, source, _ = fixture()
            changed = config["projection_policy"]
            if scenario == "ttl": changed["dns_query_policy"]["minimum_ttl_seconds"] += 1
            elif scenario == "network": changed["dns_query_policy"]["physical_routes"][0]["policy"]["advantage_ms"] += 1
            elif scenario == "scope": changed["scope"] = "foreign"
            elif scenario == "clients": source["dns_client_policies"] = [{"node_id": "dns-a", "rules": [{"cidr": "10.0.0.0/8"}]}]
            elif scenario == "generation": changed["generation"] = source["generation"]
            with self.assertRaises(ValueError): retirement.check_delta(source, changed)

    def test_global_normal_order_selected_group_before_score(self):
        baseline = dns_fixture("latency_aware")
        record = baseline["content"]["query_views"][0]["records"][0]
        record["candidates"] = [{"edge_id": "edge-a", "edge_group_id": "group-a", "ip": "8.8.8.8", "score": 300, "weight": 100}, {"edge_id": "edge-b", "edge_group_id": "group-b", "ip": "9.9.9.9", "score": 20, "weight": 100}, {"edge_id": "edge-c", "edge_group_id": "group-b", "ip": "1.1.1.1", "score": 0, "weight": 100}]
        self.assertEqual(retirement.legacy_order(record)["ordered_edge_ids"], ["edge-b", "edge-a", "edge-c"])
        record["answer_policy"]["selected_edge_group_id"] = "group-a"
        self.assertEqual(retirement.legacy_order(record)["ordered_edge_ids"], ["edge-a", "edge-b", "edge-c"])
        record["scoped_candidates"] = [{"scope_key": "country:aa"}]
        with self.assertRaises(ValueError): retirement.legacy_order(record)

    def test_static_records_are_not_migrated_and_digest_tamper_rejected(self):
        config, _, baseline = fixture()
        baseline["content"]["query_views"][0]["records"].append({"name": "static.example.test", "type": "A", "values": ["1.1.1.1"]})
        seal(baseline)
        self.assertEqual(len(retirement.check_orders(config, baseline)), 1)
        baseline["content"]["query_views"][0]["records"][0]["candidates"][0]["edge_id"] = "foreign"
        with self.assertRaises(ValueError): retirement.check_orders(config, baseline)


if __name__ == "__main__":
    unittest.main()
