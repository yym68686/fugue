import copy
import datetime
import unittest
from scripts import reconfigure_cell_producer as config
from scripts.test_bootstrap_cell_producer import ArtifactAPI


def fixture():
    topology = {"schema_version": "edge-topology/v1", "authority_cells": [{"id": "cell-a", "legacy_group_id": "edge-group-original"}], "serving_pools": [{"id": "pool-public"}], "edges": [{"id": "edge-a", "authority_cell_id": "cell-a", "serving_pool_ids": ["pool-public"], "capabilities": ["http", "tls"], "failure_domains": {"host": "edge-a"}}]}
    following = copy.deepcopy(topology)
    following["authority_cells"][0].pop("legacy_group_id")
    source = {"id": "constraint", "hostname": "app.example.test", "app_id": "app-a", "tenant_id": "tenant-a", "edge_group_id": "edge-group-original", "excluded_edge_ids": ["edge-denied"], "route_policy": "edge_enabled", "enabled": True, "min_healthy_edge_nodes": 2}
    old = {"schema_version": "fugue.platform.producer/v1", "generation": "old", "authority_cell_id": "cell-a", "publication_role": "cell-routes", "mode": "shadow", "input_source": "business-static-intent", "target_scope": "authority-cell:cell-a", "interval_seconds": 60, "refresh_seconds": 300, "require_application_domains": True, "require_route_defaults": True, "static_intent_artifact_id": "static", "static_intent_digest": "sha256:" + "a" * 64, "dns_policy_artifact_id": "projection", "dns_policy_digest": "sha256:" + "b" * 64}
    target = copy.deepcopy(old)
    target.update(generation="next", route_placement_transition={"previous_topology": topology, "next_topology": following, "constraints": [{"source": source, "source_digest": config.digest(source)}]})
    predecessor = {"artifact_id": "old-policy", "content_hash": config.digest(old), "release_id": "old-release", "fencing_token": 1}
    full = {"artifact_id": "full-parent", "content_hash": "sha256:" + "c" * 64, "release_id": "full-release", "fencing_token": 2}
    declaration = {"schema": "fugue.cell-producer-reconfiguration/v1", "generation": 1, "origin": "https://api.example.test", "authority_cell_id": "cell-a", "precondition": {"previous_policy": predecessor, "serving_full": full, "verification_evidence_hash": "sha256:" + "d" * 64}, "policy": target}
    return declaration, old


class Ledger(ArtifactAPI):
    def __init__(self, declaration, old):
        super().__init__(declaration)
        self.fail_release = False
        self.lost_response = False
        self.bad_preview = False
        self.release_bodies = []
        def state(ref, kind, scope, channel, content):
            a = {"id": ref["artifact_id"], "content_hash": ref["content_hash"], "content": copy.deepcopy(content), "artifact_kind": kind, "scope_key": scope, "status": "validated", "generation": content.get("generation", "baseline")}
            self.artifacts[a["id"]] = a
            return {"artifact": a, "release": {"id": ref["release_id"], "artifact_id": a["id"], "artifact_kind": kind, "scope_key": scope, "release_channel": channel, "fencing_token": ref["fencing_token"], "status": "active"}}
        pre = declaration["precondition"]
        self.authorities[("policy_snapshot", "platform-config-producer:cell-a", "shadow")] = state(pre["previous_policy"], "policy_snapshot", "platform-config-producer:cell-a", "shadow", old)
        self.authorities[("release_set", "authority-cell:cell-a", "full")] = state(pre["serving_full"], "release_set", "authority-cell:cell-a", "full", {"publication_role": "cell-routes"})
        self.lkg = {**pre["serving_full"], "artifact_kind": "release_set", "scope_key": "authority-cell:cell-a", "verified_by_release_id": "full-release", "verification_evidence_hash": pre["verification_evidence_hash"], "expires_at": (config.now() + datetime.timedelta(hours=1)).isoformat()}

    def __call__(self, method, path, body=None):
        if method == "GET" and path.endswith("/lkg"):
            return {"lkg": copy.deepcopy(self.lkg)}
        if method == "GET" and path.startswith("/v1/admin/platform-config/routes/project?"):
            source = copy.deepcopy(self.config["policy"]["route_placement_transition"]["constraints"][0]["source"])
            source["edge_group_id"] = "cell-a"
            if self.bad_preview:
                source["excluded_edge_ids"] = []
            return {"policy": {"scope": "authority-cell:cell-a", "authority_cell_id": "cell-a", "publication_role": "cell-routes", "route_constraints": [source]}, "business_snapshot_revision": "current"}
        if method == "POST" and path.endswith("/release"):
            self.release_bodies.append(copy.deepcopy(body))
            assert body["producer_reconfiguration"] == self.config["precondition"]
            assert body["release_channel"] == "shadow"
            if self.fail_release:
                raise RuntimeError("transactional predecessor conflict")
            current_key = ("policy_snapshot", "platform-config-producer:cell-a", "shadow")
            result = super().__call__(method, path, body)
            self.authorities[current_key]["release"]["idempotency_key"] = body["idempotency_key"]
            if self.lost_response:
                self.lost_response = False
                raise RuntimeError("response lost after commit")
            return result
        return super().__call__(method, path, body)


class ReconfigurationTests(unittest.TestCase):
    def test_publish_changes_only_shadow_policy_and_preserves_baseline(self):
        declaration, old = fixture()
        api = Ledger(declaration, old)
        before = copy.deepcopy(api.authorities[("release_set", "authority-cell:cell-a", "full")])
        evidence = []
        result = config.publish(declaration, api, evidence.append)
        self.assertFalse(result["serving_publication_changed"])
        self.assertEqual(before, api.authorities[("release_set", "authority-cell:cell-a", "full")])
        body = api.release_bodies[-1]
        artifact = api.artifacts[result["artifact_id"]]
        self.assertEqual(body["idempotency_key"], "producer-reconfiguration/" + config.digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "precondition": declaration["precondition"]}))
        self.assertEqual(config.publish(declaration, api, evidence.append)["artifact_id"], result["artifact_id"])

    def test_lost_response_retry_still_uses_transactional_guard(self):
        declaration, old = fixture()
        api = Ledger(declaration, old)
        api.lost_response = True
        with self.assertRaisesRegex(RuntimeError, "lost"):
            config.publish(declaration, api, lambda _: None)
        result = config.publish(declaration, api, lambda _: None)
        self.assertEqual(len(api.release_bodies), 2)
        self.assertEqual(api.release_bodies[0], api.release_bodies[1])
        self.assertEqual(result["mode"], "shadow")

    def test_invalid_or_changed_configuration_never_publishes(self):
        for failure in ["source_digest", "country", "missing_baseline", "expired_lkg", "wrong_previous", "schedule", "bad_preview", "transaction_conflict"]:
            with self.subTest(failure=failure):
                declaration, old = fixture()
                api = Ledger(declaration, old)
                if failure == "source_digest":
                    declaration["policy"]["route_placement_transition"]["constraints"][0]["source"]["min_healthy_edge_nodes"] = 1
                elif failure == "country":
                    declaration["policy"]["route_placement_transition"]["next_topology"]["edges"][0]["labels"] = {"country": "us"}
                elif failure == "missing_baseline":
                    del api.authorities[("release_set", "authority-cell:cell-a", "full")]
                elif failure == "expired_lkg":
                    api.lkg["expires_at"] = "2020-01-01T00:00:00Z"
                elif failure == "wrong_previous":
                    declaration["precondition"]["previous_policy"]["fencing_token"] += 1
                elif failure == "schedule":
                    declaration["policy"]["interval_seconds"] = 90
                elif failure == "bad_preview":
                    api.bad_preview = True
                elif failure == "transaction_conflict":
                    api.fail_release = True
                with self.assertRaises((ValueError, RuntimeError)):
                    config.publish(declaration, api, lambda _: None)
                self.assertEqual(api.authorities[("policy_snapshot", "platform-config-producer:cell-a", "shadow")]["artifact"]["id"], "old-policy")


if __name__ == "__main__":
    unittest.main()
