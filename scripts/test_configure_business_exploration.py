import copy
import unittest
from unittest.mock import patch

from scripts import configure_business_exploration as business
from scripts import configure_dynamic_quality as quality
from scripts.test_configure_dynamic_quality import fixture as quality_fixture
from scripts.test_reconfigure_physical_dns import Ledger, fixture as physical_fixture


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
    def test_unified_policy_relaxes_comparison_without_legacy_ceiling(self):
        config, _ = fixture()
        routes = config["projection_policy"]["dns_query_policy"]["physical_routes"]
        policy = routes[0]["business_exploration"]
        policy.update(version="business-edge-unified-v2", maximum_candidate_basis_points=10000, minimum_connections_per_bucket=1,
                      unified={"minimum_connections_per_cohort": 5, "rtt_advantage_ms": 20, "promotion_minimum_connections": 8, "promotion_minimum_coverage_basis_points": 9000})
        business.validate_business_routes(routes)
        for field, value in [("maximum_candidate_basis_points", 1000), ("unified", {}), ("exploration_basis_points", 1)]:
            invalid = copy.deepcopy(routes)
            invalid[0]["business_exploration"][field] = value
            with self.assertRaises(ValueError):
                business.validate_business_routes(invalid)

    def anchor_fixture(self):
        config, _ = fixture()
        config["business_anchor_mode"] = "latest_verified_same_baseline"
        route = config["projection_policy"]["dns_query_policy"]["physical_routes"][0]
        route["business_exploration"]["initial_dns_publications"]["dns-b"] = {"digest": "sha256:" + "a" * 64, "fencing_token": 1, "baseline_edge_id": "edge-b"}
        full = {"artifact": {"id": config["precondition"]["serving_full"]["artifact_id"], "content_hash": config["precondition"]["serving_full"]["content_hash"]}, "release": {"fencing_token": config["precondition"]["serving_full"]["fencing_token"]}}
        snapshots = {}
        for node_id, baseline in [("dns-a", "edge-a"), ("dns-b", "edge-b")]:
            snapshots[node_id] = {"snapshot": {"hostname": route["hostname"], "traffic_class": route["traffic_class"], "path_prefix": route["path_prefix"], "policy": route["policy"], "blockers": [], "current_edge_id": baseline,
                "candidates": [{"edge_id": edge, "route_proof_verified": True, "hard_gates": []} for edge in ["edge-a", "edge-b"]],
                "actual_dns_receipt": {"node_id": node_id, "decision_id": "decision-" + node_id, "rcode": 0, "qtype": 1, "write_succeeded": True, "publication": {"serving_lkg": False, "rejected": False},
                    "answer_publication": {"release_set_id": full["artifact"]["id"], "parent_digest": full["artifact"]["content_hash"], "digest": "sha256:" + "b" * 64, "fencing_token": full["release"]["fencing_token"], "release_channel": "full"}}}}
        def api(method, path):
            self.assertEqual(method, "GET")
            node_id = business.urllib.parse.parse_qs(business.urllib.parse.urlsplit(path).query)["dns_node_id"][0]
            return copy.deepcopy(snapshots[node_id])
        return config, full, snapshots, api

    def test_binding_refresh_is_explicit_and_preserves_each_baseline_and_budget(self):
        config, full, _, api = self.anchor_fixture()
        original = copy.deepcopy(config)
        quality.validate(config)
        resolved = business.resolve_business_anchors(config, api, full, "app.example.test")
        self.assertEqual(config, original)
        declared_policy = original["projection_policy"]["dns_query_policy"]["physical_routes"][0]["business_exploration"]
        actual_policy = resolved["projection_policy"]["dns_query_policy"]["physical_routes"][0]["business_exploration"]
        self.assertEqual({key: value for key, value in declared_policy.items() if key != "initial_dns_publications"}, {key: value for key, value in actual_policy.items() if key != "initial_dns_publications"})
        self.assertEqual(actual_policy["initial_dns_publications"]["dns-b"]["baseline_edge_id"], "edge-b")
        self.assertEqual(actual_policy["initial_dns_publications"]["dns-a"]["fencing_token"], full["release"]["fencing_token"])
        self.assertEqual(resolved, business.resolve_business_anchors(config, api, full, "app.example.test"))
        self.assertNotEqual(config["producer_generation"], resolved["producer_generation"])
        config.pop("business_anchor_mode")
        self.assertIs(business.resolve_business_anchors(config, None, None, "app.example.test"), config)

    def test_binding_rejects_changed_context_baseline_or_negative_evidence(self):
        for scenario in ["node", "context", "edge", "lkg", "rejected", "parent", "fence", "write", "rcode", "candidate", "digest"]:
            with self.subTest(scenario=scenario):
                config, full, snapshots, api = self.anchor_fixture()
                snapshot = snapshots["dns-b"]["snapshot"]
                receipt = snapshot["actual_dns_receipt"]
                if scenario == "node": receipt["node_id"] = "dns-other"
                elif scenario == "context": snapshot["path_prefix"] = "/other"
                elif scenario == "edge": snapshot["current_edge_id"] = "edge-a"
                elif scenario == "lkg": receipt["publication"]["serving_lkg"] = True
                elif scenario == "rejected": receipt["publication"]["rejected"] = True
                elif scenario == "parent": receipt["answer_publication"]["parent_digest"] = "sha256:" + "f" * 64
                elif scenario == "fence": receipt["answer_publication"]["fencing_token"] -= 1
                elif scenario == "write": receipt["write_succeeded"] = False
                elif scenario == "rcode": receipt["rcode"] = 2
                elif scenario == "candidate": snapshot["candidates"][1]["hard_gates"] = ["route_unverified"]
                elif scenario == "digest": receipt["answer_publication"]["digest"] = "invalid"
                with self.assertRaises(ValueError):
                    business.resolve_business_anchors(config, api, full, "app.example.test")

    def test_binding_context_failure_names_only_safe_fields_and_blockers(self):
        config, full, snapshots, api = self.anchor_fixture()
        snapshot = snapshots["dns-b"]["snapshot"]
        snapshot["blockers"] = ["actual_dns_receipt_not_retained", "Bearer secret-key", "private_lowercase_value", {"error": "sensitive"}]
        snapshot["path_prefix"] = "/different"
        with self.assertRaisesRegex(ValueError, "path_prefix,blockers; anchor_index=1; blockers=actual_dns_receipt_not_retained,unclassified,unclassified,unclassified$") as failure:
            business.resolve_business_anchors(config, api, full, "app.example.test")
        self.assertNotIn("secret-key", str(failure.exception))
        self.assertNotIn("private_lowercase_value", str(failure.exception))
        self.assertNotIn("sensitive", str(failure.exception))
        self.assertNotIn("/different", str(failure.exception))

    def test_binding_mode_cannot_implicitly_enable_or_cross_operation(self):
        for field, value in [("business_anchor_mode", "automatic"), ("baseline_mode", "declared")]:
            config, _, _, _ = self.anchor_fixture()
            config[field] = value
            with self.assertRaises(ValueError): quality.validate(config)
        config, _, _, _ = self.anchor_fixture()
        config["precondition"]["operation"] = "dynamic_quality"
        with self.assertRaises(ValueError): quality.validate(config)

    def test_binding_retry_is_bounded_and_does_not_retry_unrelated_failure(self):
        config, _, _, _ = self.anchor_fixture()
        with patch.object(quality, "publish_once", side_effect=[business.BusinessBaselineChanged("advanced"), {"producer_activated": True}]) as publish, patch.object(quality.time, "sleep"):
            self.assertTrue(quality.publish(config, None, None)["producer_activated"])
            self.assertEqual(publish.call_count, 2)
        with patch.object(quality, "publish_once", side_effect=business.BusinessBaselineChanged("advanced")) as publish, patch.object(quality.time, "sleep"):
            with self.assertRaises(ValueError): quality.publish(config, None, None)
            self.assertEqual(publish.call_count, 3)
        with patch.object(quality, "publish_once", side_effect=ValueError("baseline changed")) as publish, patch.object(quality.time, "sleep"):
            with self.assertRaises(ValueError): quality.publish(config, None, None)
            publish.assert_called_once()

    def test_bound_publication_keeps_cas_and_never_releases_after_preview_drift(self):
        for drift in [False, True]:
            with self.subTest(drift=drift):
                config, _, _, quality_api = self.anchor_fixture()
                _, source = fixture()
                _, old, _ = physical_fixture()
                old["dns_policy_digest"] = quality.digest(source)
                config["precondition"]["previous_policy"]["content_hash"] = quality.digest(old)
                ledger = Ledger(dict(config, hostname="app.example.test"), old, source)
                ledger.preview.update(intent={"generation": "unchanged"}, business_snapshot_revision="fixed")
                coverage = [{"hostname": "app.example.test", "dns_node_id": node, "intent_id": "intent-a", "action": "initialize", "allocation_receipt_digest": "sha256:" + "a" * 64} for node in ["dns-a", "dns-b"]]
                ledger.preview["runtime_snapshot"] = {"dns_selections": [{"hostname": "app.example.test", "node_id": node, "type": "A"} for node in ["dns-a", "dns-b"]], "facts": {"business_exploration_coverage": coverage, "dynamic_quality_coverage": {"mode": "all_dynamic", "query_count": 2, "hostnames": {"app.example.test": "business_exploration"}}}}
                def api(method, path, body=None):
                    if path.startswith("/v1/edge/quality-shadow/"):
                        return quality_api(method, path)
                    if drift and path.startswith("/v1/admin/platform-config/routes/project?"):
                        ledger.authorities[("release_set", "global", "full")]["release"]["fencing_token"] += 1
                    return ledger(method, path, body)
                if drift:
                    with self.assertRaises(business.BusinessBaselineChanged):
                        quality.publish_once(config, api, lambda value: None)
                    self.assertEqual(ledger.release_bodies, [])
                else:
                    evidence = quality.publish_once(config, api, lambda value: None)
                    self.assertTrue(evidence["producer_activated"])
                    self.assertEqual(evidence["declaration_digest"], quality.digest(config))
                    self.assertEqual(evidence["resolved_precondition"], config["precondition"])
                    self.assertEqual(evidence["resolved_dns_anchors"]["dns-b"]["baseline_edge_id"], "edge-b")
                    self.assertEqual(len(ledger.release_bodies), 1)

    def test_explicit_per_dns_baseline_must_be_a_candidate(self):
        config, _ = fixture()
        policy = config["projection_policy"]["dns_query_policy"]["physical_routes"][0]["business_exploration"]
        anchor = copy.deepcopy(policy["initial_dns_publications"]["dns-a"])
        anchor["baseline_edge_id"] = "edge-b"
        policy["initial_dns_publications"]["dns-b"] = anchor
        quality.validate(config)
        for invalid in [None, "", "undeclared"]:
            anchor["baseline_edge_id"] = invalid
            with self.assertRaises(ValueError):
                quality.validate(config)

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
