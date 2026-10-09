import copy
import datetime
from pathlib import Path
import unittest

from scripts import reconfigure_physical_dns as routing
from scripts.publish_agent_edge_shadow import API
from scripts.test_bootstrap_cell_producer import ArtifactAPI


def fixture():
    policy = {"version": "physical-network-cohort-v2", "window_seconds": 1800, "bucket_seconds": 300, "required_buckets": 3, "minimum_records": 3, "cooldown_seconds": 900, "evidence_max_age_seconds": 600, "advantage_ms": 20, "advantage_ratio": 0.15, "unknown_cost_ms": 30, "uncertainty_ms": 5, "failure_cost_ms": 1000, "capacity_cost_ms": 200, "throughput_cost_ms": 100, "throughput_target_bps": 1048576, "probe_interval_seconds": 300, "probe_budget_per_interval": 1, "maximum_node_utilization": 0.85}
    source = {"schema_version": "fugue.platform.config/v1", "scope": "global", "generation": "dns-before", "dns_query_policy": {"ranking_mode": "active", "preference_mode": "runtime_locality", "minimum_ttl_seconds": 60, "maximum_ttl_seconds": 120}}
    old = {"schema_version": "fugue.platform.producer/v1", "generation": "producer-before", "target_scope": "global", "mode": "serving", "input_source": "business-static-intent", "interval_seconds": 60, "refresh_seconds": 600, "require_application_domains": True, "require_route_defaults": True, "require_dns_query_policy": True, "static_intent_artifact_id": "static-input", "static_intent_digest": "sha256:" + "a" * 64, "dns_policy_artifact_id": "dns-source", "dns_policy_digest": routing.digest(source), "serving": {"canary_rule_ref": "cohort=complete", "gray_min_seconds": 120, "full_min_seconds": 120, "rollout_timeout_seconds": 600}}
    projection = copy.deepcopy(source)
    projection["generation"] = "dns-after"
    projection["dns_query_policy"]["physical_routes"] = [{"hostname": "app.example.test", "traffic_class": "streaming", "policy": policy}]
    precondition = {"operation": "physical_dns", "previous_policy": {"artifact_id": "old-policy", "content_hash": routing.digest(old), "release_id": "old-release", "fencing_token": 7}, "serving_full": {"artifact_id": "serving-artifact", "content_hash": "sha256:" + "c" * 64, "release_id": "serving-release", "fencing_token": 9}, "verification_evidence_hash": "sha256:" + "d" * 64}
    config = {"schema": "fugue.physical-dns-reconfiguration/v1", "generation": 1, "origin": "https://api.example.test", "hostname": "app.example.test", "producer_generation": "producer-after", "precondition": precondition, "projection_policy": projection}
    return config, old, source


class Ledger(ArtifactAPI):
    def __init__(self, config, old, source):
        super().__init__(config)
        self.fail_release = False
        self.release_bodies = []
        self.expected_precondition = config["precondition"]
        self.on_capture = None
        def state(reference, kind, scope, channel, content):
            artifact = {"id": reference["artifact_id"], "content_hash": reference["content_hash"], "content": copy.deepcopy(content), "artifact_kind": kind, "scope_key": scope, "status": "validated", "generation": content["generation"]}
            self.artifacts[artifact["id"]] = artifact
            return {"artifact": artifact, "release": {"id": reference["release_id"], "artifact_id": artifact["id"], "artifact_kind": kind, "scope_key": scope, "release_channel": channel, "status": "active", "fencing_token": reference["fencing_token"]}}
        precondition = config["precondition"]
        self.authorities[("policy_snapshot", routing.SCOPE, "shadow")] = state(precondition["previous_policy"], "policy_snapshot", routing.SCOPE, "shadow", old)
        self.artifacts["dns-source"] = {"id": "dns-source", "artifact_kind": "policy_snapshot", "scope_key": "global", "content_hash": routing.digest(source), "content": copy.deepcopy(source), "status": "validated", "generation": source["generation"]}
        full = state(precondition["serving_full"], "release_set", "global", "full", {"generation": "full-current"})
        full["artifact"]["metadata"] = {"producer_policy_release_id": "old-release"}
        full["release"].update(verification_state="verified", verified_lkg_generation="full-current", released_by_type="bootstrap", released_by_id="platform-config-producer")
        self.authorities[("release_set", "global", "full")] = full
        self.lkg = {**precondition["serving_full"], "artifact_kind": "release_set", "scope_key": "global", "verified_by_release_id": "serving-release", "verification_evidence_hash": precondition["verification_evidence_hash"], "expires_at": (routing.now() + datetime.timedelta(hours=1)).isoformat()}
        digest = "sha256:" + "e" * 64
        route = config["projection_policy"]["dns_query_policy"]["physical_routes"][0]
        fact = {"node_id": "dns-a", "hostname": config["hostname"], "type": "A", "reason": "bound_physical_network_evidence", "physical_selection": {"version": "physical-edge-network-v1", "evidence_digest": digest, "dns_receipt_id": "dns-receipt", "primary_edge_id": "edge-a"}, "physical_evidence": {"digest": digest, "snapshot": {"hostname": config["hostname"], "traffic_class": route["traffic_class"], "policy": route["policy"], "blockers": [], "actual_dns_receipt": {"node_id": "dns-a", "decision_id": "dns-receipt", "write_succeeded": True}}, "result": {"hypothesis": "hold", "proposed_edge_id": "edge-a", "candidates": [{"edge_id": "edge-a", "ready": True, "hard_gates": []}]}}}
        self.preview = {"business_snapshot_revision": "business-fixed", "migration_ready": False, "issues": [], "policy": {"scope": "global"}, "runtime_snapshot": {"dns_selections": [fact]}}

    def __call__(self, method, path, body=None):
        if method == "GET" and path.endswith("/lkg"):
            return {"lkg": copy.deepcopy(self.lkg)}
        if method == "GET" and path.startswith("/v1/admin/platform-config/routes/project?"):
            if self.on_capture:
                self.on_capture(self)
            return copy.deepcopy(self.preview)
        if method == "POST" and path.endswith("/release"):
            self.release_bodies.append(copy.deepcopy(body))
            if self.fail_release:
                raise RuntimeError("atomic precondition conflict")
            artifact = self.artifacts[path.split("/")[-2]]
            expected = "producer-reconfiguration/" + routing.digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "precondition": self.expected_precondition})
            assert body["idempotency_key"] == expected
            assert set(body) == {"release_channel", "producer_reconfiguration", "idempotency_key", "reason"}
            assert body["release_channel"] == "shadow"
            assert body["producer_reconfiguration"] == self.expected_precondition
        return super().__call__(method, path, body)


class PhysicalDNSConfigurationTests(unittest.TestCase):
    def test_existing_projection_advisories_do_not_block_unrelated_route_opt_in(self):
        config, old, source = fixture()
        ledger = Ledger(config, old, source)
        ledger.preview["intent"] = {"generation": "unchanged-intent", "routes": [{"hostname": config["hostname"]}]}
        ledger.preview["issues"] = [{"code": "dns_output_equivalence_not_verified"}, {"code": "release_target_equivalence_not_verified"}, {"code": "origin_observation_not_fresh", "hostname": "idle.example.test", "path_prefix": "/"}]
        result = routing.publish(config, ledger, lambda _: None)
        self.assertTrue(result["producer_activated"])
        self.assertEqual(ledger.preview["issues"], result["unchanged_projection_issues"])
        previous = copy.deepcopy(ledger.preview)
        for scenario in ["new issue", "target origin", "hard issue", "changed intent"]:
            with self.subTest(scenario=scenario):
                changed = copy.deepcopy(previous)
                if scenario == "new issue": changed["issues"][2]["hostname"] = "new.example.test"
                elif scenario == "target origin": changed["issues"][2]["hostname"] = config["hostname"]
                elif scenario == "hard issue": changed["issues"].append({"code": "intent_requires_validation_repair"})
                elif scenario == "changed intent": changed["intent"]["generation"] = "changed"
                with self.assertRaises(ValueError):
                    routing.validate_preview(config, changed, previous)

    def test_queue_delay_resolves_only_verified_renewal_under_same_policy(self):
        config, old, source = fixture()
        config["baseline_mode"] = "latest_verified_same_policy"
        ledger = Ledger(config, old, source)
        full = ledger.authorities[("release_set", "global", "full")]
        full["release"]["fencing_token"] += 1
        full["release"]["id"] = "renewed-release"
        ledger.lkg["verified_by_release_id"] = "renewed-release"
        before = copy.deepcopy(config)
        resolved = routing.resolve_baseline(config, ledger)
        ledger.expected_precondition = resolved["precondition"]
        result = routing.publish(config, ledger, lambda _: None)
        self.assertEqual(before, config)
        self.assertEqual(routing.digest(config), result["declaration_digest"])
        self.assertEqual(before["precondition"], result["declared_precondition"])
        self.assertEqual(resolved["precondition"], result["resolved_precondition"])
        self.assertTrue(result["producer_activated"])

    def test_baseline_refresh_never_weakens_lkg_or_transaction_guards(self):
        for failure in ["regressed", "foreign producer", "unverified", "expired", "foreign lkg", "after capture"]:
            with self.subTest(failure=failure):
                config, old, source = fixture()
                config["baseline_mode"] = "latest_verified_same_policy"
                ledger = Ledger(config, old, source)
                full = ledger.authorities[("release_set", "global", "full")]
                full["release"]["fencing_token"] += 1
                if failure == "regressed": full["release"]["fencing_token"] -= 2
                elif failure == "foreign producer": full["artifact"]["metadata"]["producer_policy_release_id"] = "foreign"
                elif failure == "unverified": full["release"]["verification_state"] = "serving_unverified"
                elif failure == "expired": ledger.lkg["expires_at"] = (routing.now() - datetime.timedelta(seconds=1)).isoformat()
                elif failure == "foreign lkg": ledger.lkg["artifact_id"] = "foreign"
                elif failure == "after capture": ledger.on_capture = lambda api: api.authorities[("release_set", "global", "full")]["release"].update(fencing_token=99)
                with self.assertRaises(ValueError):
                    routing.publish(config, ledger, lambda _: None)
                self.assertEqual([], ledger.release_bodies)

    def test_bounded_network_policy_requires_exact_shape(self):
        config, _, _ = fixture()
        policy = config["projection_policy"]["dns_query_policy"]["physical_routes"][0]["policy"]
        policy["version"] = "physical-network-bounded-v3"
        self.assertEqual(config, routing.validate(config))
        policy["unknown_cost_ms"] = None
        with self.assertRaises(ValueError):
            routing.validate(config)

    def test_success_and_idempotent_retry_never_claim_routing_acceptance(self):
        config, old, source = fixture()
        ledger = Ledger(config, old, source)
        full, lkg = copy.deepcopy(ledger.authorities[("release_set", "global", "full")]), copy.deepcopy(ledger.lkg)
        receipts = []
        first = routing.publish(config, ledger, lambda value: receipts.append(copy.deepcopy(value)))
        second = routing.publish(config, ledger, lambda _: None)
        self.assertEqual(first["authority"], second["authority"])
        self.assertFalse(first["routing_acceptance_complete"])
        self.assertTrue(first["producer_activated"])
        self.assertFalse(receipts[0]["producer_activated"])
        successor = ledger.authorities[("policy_snapshot", routing.SCOPE, "shadow")]["artifact"]["content"]
        for key in old:
            if key not in ["generation", "dns_policy_artifact_id", "dns_policy_digest"]:
                self.assertEqual(old[key], successor[key])
        self.assertEqual(full, ledger.authorities[("release_set", "global", "full")])
        self.assertEqual(lkg, ledger.lkg)

    def test_preconditions_and_incomplete_evidence_never_release(self):
        for failure in ["foreign current", "changed full", "expired lkg", "bad evidence", "unverified", "forged source", "unrelated strategy", "two hosts", "same generation", "unready", "no receipt", "foreign receipt", "unready primary", "bad digest", "snapshot blockers", "different policy", "after capture", "same generation conflict"]:
            with self.subTest(failure=failure):
                config, old, source = fixture()
                ledger = Ledger(config, old, source)
                full = ledger.authorities[("release_set", "global", "full")]
                fact = ledger.preview["runtime_snapshot"]["dns_selections"][0]
                evidence = fact["physical_evidence"]
                if failure == "foreign current": ledger.authorities[("policy_snapshot", routing.SCOPE, "shadow")]["release"]["fencing_token"] += 1
                elif failure == "changed full": full["release"]["fencing_token"] += 1
                elif failure == "expired lkg": ledger.lkg["expires_at"] = (routing.now() - datetime.timedelta(seconds=1)).isoformat()
                elif failure == "bad evidence": ledger.lkg["verification_evidence_hash"] = "sha256:" + "f" * 64
                elif failure == "unverified": full["release"]["verification_state"] = "serving_unverified"
                elif failure == "forged source": ledger.artifacts["dns-source"]["content"]["generation"] = "forged"
                elif failure == "unrelated strategy": config["projection_policy"]["dns_query_policy"]["minimum_ttl_seconds"] = 30
                elif failure == "two hosts": config["projection_policy"]["dns_query_policy"]["physical_routes"].append({**copy.deepcopy(config["projection_policy"]["dns_query_policy"]["physical_routes"][0]), "hostname": "other.example.test"})
                elif failure == "same generation": config["producer_generation"] = old["generation"]
                elif failure == "unready": ledger.preview["issues"] = [{"code": "dns_tls_probe_failed"}]
                elif failure == "no receipt": evidence["snapshot"]["actual_dns_receipt"] = {}
                elif failure == "foreign receipt": evidence["snapshot"]["actual_dns_receipt"]["node_id"] = "dns-other"
                elif failure == "unready primary": evidence["result"]["candidates"][0]["ready"] = False
                elif failure == "bad digest": fact["physical_selection"]["evidence_digest"] = "sha256:" + "f" * 64
                elif failure == "snapshot blockers": evidence["snapshot"]["blockers"] = ["actual_dns_receipt_not_bound"]
                elif failure == "different policy": evidence["snapshot"]["policy"] = {}
                elif failure == "after capture": ledger.on_capture = lambda api: api.lkg.update(verification_evidence_hash="sha256:" + "f" * 64)
                elif failure == "same generation conflict":
                    ledger.authorities[("policy_snapshot", routing.SCOPE, "shadow")]["artifact"]["generation"] = config["producer_generation"]
                with self.assertRaises(ValueError):
                    routing.publish(config, ledger, lambda _: None)
                self.assertEqual([], ledger.release_bodies)

    def test_server_rejects_race_without_lkg_mutation(self):
        config, old, source = fixture()
        ledger = Ledger(config, old, source)
        ledger.fail_release = True
        before = copy.deepcopy(ledger.lkg)
        with self.assertRaises(RuntimeError):
            routing.publish(config, ledger, lambda _: None)
        self.assertEqual(before, ledger.lkg)
        self.assertEqual("old-policy", ledger.authorities[("policy_snapshot", routing.SCOPE, "shadow")]["artifact"]["id"])

    def test_lost_response_reuses_exact_idempotency_key(self):
        config, old, source = fixture()
        ledger = Ledger(config, old, source)
        ledger.lose_release_response = True
        with self.assertRaises(RuntimeError):
            routing.publish(config, ledger, lambda _: None)
        result = routing.publish(config, ledger, lambda _: None)
        self.assertTrue(result["producer_activated"])
        self.assertEqual(ledger.release_bodies[0], ledger.release_bodies[1])
        ledger.lkg["verification_evidence_hash"] = "sha256:" + "f" * 64
        with self.assertRaises(ValueError):
            routing.publish(config, ledger, lambda _: None)
        self.assertEqual(2, len(ledger.release_bodies))

    def test_unknown_and_ambiguous_values_fail_closed(self):
        for key, value in [("unknown", True), ("origin", "https://user:secret@api.example.test"), ("hostname", "*.example.test"), ("generation", True)]:
            config, _, _ = fixture()
            config[key] = value
            with self.assertRaises(ValueError): routing.validate(config)
        for value in [float("nan"), float("inf"), 20.0]:
            config, _, _ = fixture()
            config["projection_policy"]["dns_query_policy"]["physical_routes"][0]["policy"]["advantage_ms"] = value
            with self.assertRaises(ValueError): routing.validate(config)
        config, _, _ = fixture()
        del config["projection_policy"]["dns_query_policy"]["physical_routes"][0]["policy"]["probe_budget_per_interval"]
        with self.assertRaises(ValueError): routing.validate(config)

    def test_existing_opt_ins_and_static_constraints_are_preserved(self):
        config, _, source = fixture()
        route = {**copy.deepcopy(config["projection_policy"]["dns_query_policy"]["physical_routes"][0]), "hostname": "existing.example.test"}
        source["dns_query_policy"]["physical_routes"] = [copy.deepcopy(route)]
        config["projection_policy"]["dns_query_policy"]["physical_routes"].append(copy.deepcopy(route))
        routing.check_delta(source, config["projection_policy"], config["hostname"])
        config["projection_policy"]["dns_query_policy"]["physical_routes"][1]["policy"]["cooldown_seconds"] = 0
        with self.assertRaises(ValueError): routing.check_delta(source, config["projection_policy"], config["hostname"])

    def test_api_size_bounds_are_explicit_and_default_unchanged(self):
        self.assertEqual(1 << 20, API("https://api.example.test", "test").response_limit)
        self.assertEqual(16 << 20, API("https://api.example.test", "test", 16 << 20).response_limit)
        for size in [0, True, 16 << 21]:
            with self.assertRaises(ValueError): API("https://api.example.test", "test", size)

    def test_workflow_is_a_configuration_lane_not_a_component_deploy(self):
        workflow = Path(".github/workflows/ci.yml").read_text()
        lane = workflow.split("\n  physical_dns_reconfiguration:", 1)[1].split("\n  dns_probe_egress_plan:", 1)[0]
        for value in ["scripts.test_reconfigure_physical_dns", "scripts.reconfigure_physical_dns", "git fetch origin main", "cancel-in-progress: false", "routing-physical-dns"]:
            self.assertIn(value, lane)
        for forbidden in ["kubectl", "docker", "needs: prepush", "needs: deploy_api", "rollout restart", "helm"]:
            self.assertNotIn(forbidden, lane)


if __name__ == "__main__":
    unittest.main()
