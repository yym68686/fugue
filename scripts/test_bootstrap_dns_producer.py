import copy
import datetime
import unittest

from scripts import bootstrap_cell_producer as bootstrap
from scripts import bootstrap_dns_producer as dns
from scripts.test_bootstrap_cell_producer import ArtifactAPI


def fixture():
    def ref(name, fence=1):
        return {"artifact_id": name, "content_hash": "sha256:" + "a" * 64, "release_id": name + "-release", "fencing_token": fence}
    full = ref("dns-full")
    producer = {"schema_version": "fugue.platform.producer/v1", "generation": "dns-first", "mode": "serving", "input_source": "verified-global-dns", "publication_role": "cell-dns", "authority_cell_id": "cell-dns", "target_scope": "authority-cell:cell-dns", "interval_seconds": 60, "refresh_seconds": 600, "serving": {"canary_rule_ref": "cohort=complete", "gray_min_seconds": 120, "full_min_seconds": 120, "rollout_timeout_seconds": 600}, "dns_source": {"producer_policy": ref("source-policy"), "dns_node_ids": ["dns-a"], "previous_topology": {"schema_version": "edge-topology/v1", "authority_cells": [{"id": "cell-a", "legacy_group_id": "group-a"}], "serving_pools": [{"id": "pool"}], "edges": [{"id": "edge-a", "authority_cell_id": "cell-a", "serving_pool_ids": ["pool"], "capabilities": ["http", "tls"], "failure_domains": {"host": "edge-a"}}]}}}
    return {"schema": dns.SCHEMA, "generation": 1, "origin": "https://api.example.test", "authority_cell_id": "cell-dns", "expected_previous_policy": None, "producer": producer, "baseline": {"lanes": {"shadow": full, "gray": full, "full": full}, "lkg_id": "positive", "lkg_publication": full, "verification_evidence_hash": "sha256:" + "b" * 64}}


class Ledger(ArtifactAPI):
    def __init__(self, c):
        super().__init__(c)
        self.preview_changes = None
        self.fail_release = False
        self.lose_response = False
        self.guard = copy.deepcopy(c["baseline"])
        scope = c["producer"]["target_scope"]
        for channel, pin in self.guard["lanes"].items():
            self.authorities[("release_set", scope, channel)] = self.state(pin, "release_set", scope, channel)
        pin = c["producer"]["dns_source"]["producer_policy"]
        self.authorities[("policy_snapshot", "platform-config-producer", "shadow")] = self.state(pin, "policy_snapshot", "platform-config-producer", "shadow")

    def state(self, pin, kind, scope, channel):
        content = {"publication_role": "cell-dns", "artifact_kinds": ["dns_answer_bundle"], "consumer_topology": {"schema_version": "fugue.traffic-consumer-topology/v1", "publication_role": "cell-dns", "authority_cell_id": "cell-dns", "edge_node_ids": [], "dns_node_ids": ["dns-a"]}}
        return {"artifact": {"id": pin["artifact_id"], "content_hash": pin["content_hash"], "status": "validated", "artifact_kind": kind, "scope_key": scope, "generation": "baseline", "content": content}, "release": {"id": pin["release_id"], "artifact_id": pin["artifact_id"], "status": "active", "artifact_kind": kind, "scope_key": scope, "release_channel": channel, "fencing_token": pin["fencing_token"], "verification_state": "verified", "verified_lkg_generation": "baseline"}}

    def __call__(self, method, path, body=None):
        if method == "GET" and path.endswith("/lkg"):
            p = self.guard["lkg_publication"]
            return {"lkg": {"id": self.guard["lkg_id"], "artifact_kind": "release_set", "scope_key": "authority-cell:cell-dns", "artifact_id": p["artifact_id"], "content_hash": p["content_hash"], "verified_by_release_id": p["release_id"], "verification_evidence_hash": self.guard["verification_evidence_hash"], "expires_at": (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)).isoformat()}}
        if method == "GET" and path.startswith("/v1/admin/platform-config/routes/project?"):
            assert path.endswith("&view=configuration")
            preview = {"intent": {"publication_role": "cell-dns", "scope": "authority-cell:cell-dns", "authority_cell_id": "cell-dns", "dns_consumers": [{"node_id": "dns-a"}]}, "policy": {"publication_role": "cell-dns", "scope": "authority-cell:cell-dns", "authority_cell_id": "cell-dns"}}
            if self.preview_changes:
                self.preview_changes(self, preview)
            return preview
        if method == "POST" and path.endswith("/release"):
            if self.fail_release:
                raise RuntimeError("publication rejected")
            result = super().__call__(method, path, body)
            state = self.authorities[("policy_snapshot", "platform-config-producer:cell-dns", "shadow")]
            state["release"]["idempotency_key"] = body["idempotency_key"]
            if self.lose_response:
                self.lose_response = False
                raise RuntimeError("lost activation response")
            return state
        return super().__call__(method, path, body)


class DNSBootstrapTests(unittest.TestCase):
    def test_only_exact_initial_producer_is_activated_and_retry_is_read_only(self):
        c = fixture()
        api = Ledger(c)
        prior = copy.deepcopy(api.authorities)
        result = bootstrap.publish(c, api)
        self.assertEqual(c["producer"], result["artifact"]["content"])
        self.assertEqual(prior, {k: api.authorities[k] for k in prior})
        self.assertEqual(1, sum(path.endswith("/release") for path, _ in api.writes))
        writes = copy.deepcopy(api.writes)
        self.assertEqual(result, bootstrap.publish(c, api))
        self.assertEqual(writes, api.writes)
        for path, body in api.writes:
            self.assertNotIn("prepare-consumers", path)
            self.assertNotIn("verify-lkg", path)
            if path.endswith("/release"):
                self.assertEqual("shadow", body["release_channel"])

    def test_changed_baseline_source_or_preview_prevents_activation(self):
        for scenario in ["full", "source", "LKG", "foreign preview", "routes", "membership", "race", "failed release"]:
            with self.subTest(scenario=scenario):
                c = fixture()
                api = Ledger(c)
                def change(ledger, preview):
                    if scenario in ["full", "source"]:
                        key = ("release_set", "authority-cell:cell-dns", "full") if scenario == "full" else ("policy_snapshot", "platform-config-producer", "shadow")
                        ledger.authorities[key]["release"]["fencing_token"] += 1
                    elif scenario == "LKG": ledger.guard["lkg_id"] = "replacement"
                    elif scenario == "foreign preview": preview["policy"]["scope"] = "global"
                    elif scenario == "routes": preview["intent"]["routes"] = [{"hostname": "foreign.example.test"}]
                    elif scenario == "membership": preview["intent"]["dns_consumers"] = [{"node_id": "wrong"}]
                    elif scenario == "race":
                        ledger.authorities[("policy_snapshot", "platform-config-producer:cell-dns", "shadow")] = ledger.state({"artifact_id": "foreign", "content_hash": "sha256:" + "c" * 64, "release_id": "foreign-release", "fencing_token": 1}, "policy_snapshot", "platform-config-producer:cell-dns", "shadow")
                api.preview_changes = change
                api.fail_release = scenario == "failed release"
                with self.assertRaises((ValueError, RuntimeError)):
                    bootstrap.publish(c, api)
                self.assertFalse(any(path.endswith("/release") for path, _ in api.writes))

    def test_lost_response_retry_cannot_replace_a_later_policy(self):
        c = fixture()
        api = Ledger(c)
        api.lose_response = True
        with self.assertRaisesRegex(RuntimeError, "lost"):
            bootstrap.publish(c, api)
        writes = copy.deepcopy(api.writes)
        bootstrap.publish(c, api)
        self.assertEqual(writes, api.writes)
        api.authorities[("policy_snapshot", "platform-config-producer:cell-dns", "shadow")]["release"]["idempotency_key"] = "different"
        with self.assertRaisesRegex(ValueError, "another declaration"):
            bootstrap.publish(c, api)

    def test_declaration_rejects_unbounded_or_mixed_authority(self):
        for change in [lambda c: c.update(extra=True), lambda c: c.update(expected_previous_policy={}), lambda c: c["producer"].update(mode="shadow"), lambda c: c["producer"].update(target_scope="global"), lambda c: c["producer"].update(static_intent_artifact_id="unrelated"), lambda c: c["producer"]["dns_source"]["dns_node_ids"].append("dns-a"), lambda c: c["producer"]["serving"].update(gray_min_seconds=0), lambda c: c.update(api_connect_address="127.0.0.1")]:
            c = fixture()
            change(c)
            with self.assertRaises(ValueError):
                bootstrap.validate(c)


if __name__ == "__main__":
    unittest.main()
