import copy
import hashlib
import json
import unittest
import urllib.parse

from scripts import bootstrap_cell_producer as bootstrap


def fixture():
    cell = "cell-a"
    scope = "authority-cell:" + cell
    topology = {
        "schema_version": "edge-topology/v1",
        "authority_cells": [{"id": cell}],
        "serving_pools": [{"id": "pool-public"}],
        "edges": [{"id": "edge-a", "authority_cell_id": cell, "serving_pool_ids": ["pool-public"], "capabilities": ["http", "tls"], "failure_domains": {"host": "edge-a"}}],
    }
    intent = {"schema_version": "fugue.platform.config/v1", "generation": "intent", "scope": scope, "authority_cell_id": cell, "edge_topology": topology, "dns_consumers": [{"node_id": "dns-a", "edge_group_id": cell, "zones": ["example.test"], "probe_label": "probe", "probe_ttl": 60}]}
    membership = {"schema_version": "fugue.traffic-consumer-topology/v1", "authority_cell_id": cell, "edge_node_ids": ["edge-a"], "dns_node_ids": ["dns-a"]}
    topology_digest = "sha256:" + hashlib.sha256(json.dumps(membership, separators=(",", ":")).encode()).hexdigest()
    projection = {"schema_version": "fugue.platform.config/v1", "generation": "policy", "scope": scope, "authority_cell_id": cell, "consumer_topology_digest": topology_digest, "dns_placement_mode": "consumer_readiness", "dns_query_policy": {"ranking_mode": "disabled", "preference_mode": "runtime_locality", "minimum_ttl_seconds": 60, "maximum_ttl_seconds": 120}}
    producer = {"schema_version": "fugue.platform.producer/v1", "generation": "producer", "authority_cell_id": cell, "mode": "shadow", "input_source": "business-static-intent", "target_scope": scope, "interval_seconds": 60, "refresh_seconds": 600, "require_application_domains": True, "require_route_defaults": True, "require_dns_query_policy": True, "hosted_zone_templates": []}
    return {"schema": "fugue.cell-producer-bootstrap/v1", "generation": 1, "origin": "https://api.example.test", "authority_cell_id": cell, "expected_previous_policy": None, "static_intent": intent, "projection_policy": projection, "producer": producer}


class ArtifactAPI:
    """Small immutable artifact ledger with independently mutable authorities."""
    def __init__(self, config):
        self.config = config
        self.artifacts, self.authorities, self.writes = {}, {}, []
        self.on_preview = None
        self.foreign_preview = self.invalid_input = self.lose_release_response = False

    def __call__(self, method, raw_path, body=None):
        path, _, query = raw_path.partition("?")
        query = urllib.parse.parse_qs(query)
        if method == "GET" and path.startswith("/v1/platform-state/artifacts/"):
            key = (path.rsplit("/", 1)[1], query["scope_key"][0], query["channel"][0])
            return copy.deepcopy(self.authorities.get(key, {}))
        if method == "GET" and path == "/v1/admin/artifacts":
            return {"artifacts": [{k: a[k] for k in ["id", "generation"]} for a in self.artifacts.values() if a["artifact_kind"] == query["kind"][0] and a["scope_key"] == query["scope"][0]]}
        if method == "GET" and path.startswith("/v1/admin/artifacts/"):
            return {"artifact": copy.deepcopy(self.artifacts[path.rsplit("/", 1)[1]])}
        if method == "GET" and path == "/v1/admin/platform-config/routes/project":
            producer = self.artifacts[query["producer_policy_artifact_id"][0]]
            assert producer["status"] == "validated"
            content = producer["content"]
            intent = self.artifacts[content["static_intent_artifact_id"]]
            policy = self.artifacts[content["dns_policy_artifact_id"]]
            assert intent["content_hash"] == content["static_intent_digest"]
            assert policy["content_hash"] == content["dns_policy_digest"]
            if self.on_preview:
                self.on_preview(self)
            projected = copy.deepcopy(intent["content"])
            if self.foreign_preview:
                projected["authority_cell_id"] = "cell-other"
            return {"intent": projected, "policy": copy.deepcopy(policy["content"]), "business_snapshot_revision": "business-revision-current"}
        if method != "POST":
            raise AssertionError((method, raw_path))
        self.writes.append((path, copy.deepcopy(body)))
        if path == "/v1/admin/artifacts":
            ident = "artifact_" + str(len(self.artifacts) + 1)
            for a in self.artifacts.values():
                assert (a["artifact_kind"], a["scope_key"], a["generation"]) != (body["artifact_kind"], body["scope"]["key"], body["generation"])
            artifact = {"id": ident, "artifact_kind": body["artifact_kind"], "scope_key": body["scope"]["key"], "generation": body["generation"], "content": copy.deepcopy(body["content"]), "content_hash": bootstrap.digest(body["content"]), "status": "draft"}
            self.artifacts[ident] = artifact
            return {"artifact": copy.deepcopy(artifact)}
        ident, action = path.split("/")[-2:]
        artifact = self.artifacts[ident]
        if action == "validate":
            artifact["status"] = "invalid" if self.invalid_input else "validated"
            return {"pass": not self.invalid_input, "artifact": copy.deepcopy(artifact)}
        if action == "release":
            assert artifact["status"] == "validated"
            key = (artifact["artifact_kind"], artifact["scope_key"], body["release_channel"])
            state = {"artifact": copy.deepcopy(artifact), "release": {"id": "release_1", "artifact_id": ident, "artifact_kind": artifact["artifact_kind"], "scope_key": artifact["scope_key"], "status": "active", "release_channel": body["release_channel"], "fencing_token": 1}}
            self.authorities[key] = state
            if self.lose_release_response:
                self.lose_release_response = False
                raise RuntimeError("lost release response")
            return copy.deepcopy(state)
        raise AssertionError((method, path, body))


class BootstrapTests(unittest.TestCase):
    def test_explicit_route_only_inputs_have_no_dns_authority(self):
        config = fixture()
        for key in ["static_intent", "projection_policy", "producer"]:
            config[key]["publication_role"] = "cell-routes"
        config["static_intent"].pop("dns_consumers")
        policy = config["projection_policy"]
        policy.pop("dns_query_policy")
        policy.pop("dns_placement_mode")
        policy["tls_readiness"] = {"probe_interval_seconds": 30, "probe_timeout_seconds": 5, "fact_freshness_seconds": 120, "max_probes": 4096, "max_concurrency": 8}
        membership = {"publication_role": "cell-routes", "schema_version": "fugue.traffic-consumer-topology/v1", "authority_cell_id": "cell-a", "edge_node_ids": ["edge-a"], "dns_node_ids": []}
        policy["consumer_topology_digest"] = "sha256:" + hashlib.sha256(json.dumps(membership, separators=(",", ":")).encode()).hexdigest()
        config["producer"]["require_dns_query_policy"] = False
        api = ArtifactAPI(config)
        result = bootstrap.publish(config, api)
        self.assertEqual(result["artifact"]["content"]["publication_role"], "cell-routes")
        self.assertEqual(set(api.authorities), {("policy_snapshot", "platform-config-producer:cell-a", "shadow")})
        for key in ["static_intent", "projection_policy"]:
            changed = copy.deepcopy(config)
            changed[key].pop("publication_role")
            with self.assertRaises(ValueError):
                bootstrap.validate(changed)

    def test_validate_requires_exact_cell_scope_and_shadow_only(self):
        config = fixture()
        self.assertIs(bootstrap.validate(config), config)
        for mutate in [
            lambda c: c["producer"].update(mode="serving"),
            lambda c: c["producer"].update(target_scope="global"),
            lambda c: c["projection_policy"].update(consumer_topology_digest="sha256:" + "0" * 64),
            lambda c: c["static_intent"].update(authority_cell_id="cell-b"),
        ]:
            changed = copy.deepcopy(config)
            mutate(changed)
            with self.assertRaises(ValueError):
                bootstrap.validate(changed)

    def test_enrollment_pins_inputs_and_only_releases_shadow_policy(self):
        config = fixture()
        api = ArtifactAPI(config)
        result = bootstrap.publish(config, api)
        self.assertEqual(len(api.artifacts), 3)
        self.assertEqual(set(api.authorities), {("policy_snapshot", "platform-config-producer:cell-a", "shadow")})
        releases = [(p, b) for p, b in api.writes if p.endswith("/release")]
        self.assertEqual(len(releases), 1)
        self.assertEqual(releases[0][1]["release_channel"], "shadow")
        self.assertEqual(result["artifact"]["content"]["target_scope"], "authority-cell:cell-a")
        count = len(api.writes)
        self.assertEqual(bootstrap.publish(config, api), result)
        self.assertEqual(len(api.writes), count)

    def test_lost_release_response_retry_reuses_exact_immutable_inputs(self):
        config = fixture()
        api = ArtifactAPI(config)
        api.lose_release_response = True
        with self.assertRaisesRegex(RuntimeError, "lost release"):
            bootstrap.publish(config, api)
        count = len(api.writes)
        bootstrap.publish(config, api)
        self.assertEqual(len(api.writes), count)
        self.assertEqual(len(api.artifacts), 3)

    def test_refuses_established_or_concurrently_created_serving_authority(self):
        config = fixture()
        for key in [("release_set", "authority-cell:cell-a", "gray"), ("release_set", "authority-cell:cell-a", "full"), ("policy_snapshot", "platform-config-producer:cell-a", "full")]:
            for concurrent in [False, True]:
                with self.subTest(key=key, concurrent=concurrent):
                    api = ArtifactAPI(config)
                    def install(fake):
                        fake.authorities[key] = {"artifact": {"id": "serving", "artifact_kind": key[0], "scope_key": key[1]}, "release": {"artifact_id": "serving", "artifact_kind": key[0], "scope_key": key[1], "release_channel": key[2]}}
                    if concurrent:
                        api.on_preview = install
                    else:
                        install(api)
                    with self.assertRaisesRegex(ValueError, "established serving"):
                        bootstrap.publish(config, api)
                    self.assertFalse(any(p.endswith("/release") for p, _ in api.writes))

    def test_preparation_failure_cannot_publish(self):
        config = fixture()
        for failure in ["foreign_preview", "invalid_input", "authority_drift"]:
            with self.subTest(failure=failure):
                api = ArtifactAPI(config)
                if failure == "authority_drift":
                    def drift(fake):
                        fake.authorities[("policy_snapshot", "platform-config-producer:cell-a", "shadow")] = {"artifact": {"id": "other", "status": "validated", "content_hash": "sha256:" + "0" * 64}, "release": {"id": "other_release", "status": "active", "fencing_token": 4}}
                    api.on_preview = drift
                else:
                    setattr(api, failure, True)
                with self.assertRaises(ValueError):
                    bootstrap.publish(config, api)
                self.assertFalse(any(p.endswith("/release") for p, _ in api.writes))

    def test_same_generation_input_drift_is_rejected_without_writes(self):
        config = fixture()
        api = ArtifactAPI(config)
        bootstrap.publish(config, api)
        changed = copy.deepcopy(config)
        changed["static_intent"]["dns_consumers"][0]["probe_label"] = "different-probe"
        count = len(api.writes)
        with self.assertRaisesRegex(ValueError, "immutable input differs"):
            bootstrap.publish(changed, api)
        self.assertEqual(len(api.writes), count)

    def test_foreign_selected_lane_is_not_adopted_as_predecessor(self):
        config = fixture()
        api = ArtifactAPI(config)
        bootstrap.publish(config, api)
        state = api.authorities[("policy_snapshot", "platform-config-producer:cell-a", "shadow")]
        state["release"]["scope_key"] = "global"
        count = len(api.writes)
        with self.assertRaisesRegex(ValueError, "requested lane"):
            bootstrap.publish(config, api)
        self.assertEqual(len(api.writes), count)


if __name__ == "__main__":
    unittest.main()
