import copy
import datetime
import unittest
from unittest.mock import patch

from scripts import stage_dns_authority_transition as p


def fixture():
    h = "sha256:" + "a" * 64
    topology = {"schema_version": "edge-topology/v1", "authority_cells": [{"id": "cell-a", "legacy_group_id": "edge-group-a"}], "serving_pools": [{"id": "pool-public"}], "edges": [{"id": "edge-a", "authority_cell_id": "cell-a", "serving_pool_ids": ["pool-public"], "capabilities": ["http", "tls"], "failure_domains": {"host": "host-a"}}]}
    def art(ident, kind):
        return {"id": ident, "artifact_kind": kind, "content_hash": h, "content": {}}
    def pub(prefix):
        return {"parent": {**art(prefix + "-parent", "release_set"), "content": {"consumer_topology": {"authority_cell_id": "cell-a"}}}, "release": {"id": prefix + "-full", "fencing_token": 1}, "children": {kind: art(prefix + "-" + short, kind) for short, kind in [("route", "edge_route_bundle"), ("tls", "caddy_route_config"), ("dns", "dns_answer_bundle")]}}
    old, cell = pub("previous"), pub("neutral")
    ref = p.reference(cell, "cell-a")
    cfg = {"schema": "fugue.dns-authority-transition-stage/v1", "generation": 1, "origin": "https://api.example.test", "authority_cell_id": "cell-dns", "previous_topology": topology, "route_publications": [ref], "dns_node_ids": ["dns-a"], "expected_previous_shadow": None, "capture": {"timeout_seconds": 60, "poll_seconds": 5, "max_source_age_seconds": 60}}
    intent = {"scope": "global", "generation": "old-intent", "routes": [{"hostname": "app.example.test"}], "tls": [{}], "cache_policies": [{}], "application_domains": {}, "dns_consumers": [{"node_id": "dns-a", "edge_group_id": "edge-group-a"}], "dns": [{"hostname": "app.example.test", "edge_group_id": "edge-group-a", "fallback_edge_group_id": "edge-group-a", "type": "A", "ttl": 60}], "acme_challenges": [{"name": "_acme.example.test", "value": "opaque-value"}]}
    policy = {"scope": "global", "generation": "old-policy", "route_constraints": [{}], "traffic_constraints": [{}], "tls_readiness": {}, "minimum_healthy_edges": 2, "dns_readiness": {"fact_freshness_seconds": 30}, "dns_route_state_constraints": [{"hostname": "app.example.test", "mode": "omit"}], "dns_answer_rules": [{"hostname": "app.example.test", "preferred_edge_groups": ["edge-group-a"], "fallback_edge_groups": ["edge-group-a"]}]}
    snapshot = {"captured_at": "2026-08-01T12:00:00Z", "origins": [{}], "releases": [{}], "tls_domains": [{}], "dns_placements": [{}], "facts": {}, "dns_edge_endpoints": [{"edge_id": "edge-a", "edge_group_id": "edge-group-a", "a": ["93.184.216.34"]}], "dns_consumers": [{"node_id": "dns-a", "edge_group_id": "edge-group-a"}], "dns_selections": [{"selected_edge_group_id": "edge-group-a", "candidates": [{"edge_id": "edge-a", "edge_group_id": "edge-group-a", "weight": 100}], "scoped_candidates": [{"scope_key": "scope-a", "selected_edge_group_id": "edge-group-a", "candidates": [{"edge_id": "edge-a", "edge_group_id": "edge-group-a"}]}]}]}
    return cfg, old, cell, intent, policy, snapshot


class DNSAuthorityStageTest(unittest.TestCase):
    def continuous_fixture(self):
        cfg, old, cell, intent, policy, facts = fixture()
        cfg["schema"] = "fugue.dns-authority-transition-stage/v2"
        cfg.pop("route_publications")
        pin = {"artifact_id": "producer", "content_hash": "sha256:" + "d" * 64, "release_id": "producer-release", "fencing_token": 4}
        cfg["route_sources"] = [{"authority_cell_id": "cell-a", "producer_policy": pin}]
        cell["parent"].update(scope_key="authority-cell:cell-a", metadata={"producer_policy_release_id": pin["release_id"]})
        cell["parent"]["content"]["publication_role"] = "cell-routes"
        producer = {"content_hash": pin["content_hash"], "content": {"authority_cell_id": "cell-a", "target_scope": "authority-cell:cell-a", "publication_role": "cell-routes", "mode": "serving", "serving": {"single_publication": False}}}
        return cfg, old, cell, intent, policy, facts, producer

    def test_runtime_sources_are_explicit_signed_approvals_not_inferred(self):
        cfg, old, cell, intent, policy, facts, _ = self.continuous_fixture()
        cfg["schema"] = "fugue.dns-authority-transition-stage/v3"
        cfg["global_producer_policy"] = {"artifact_id": "global-producer", "content_hash": "sha256:" + "e" * 64, "release_id": "global-producer-release", "fencing_token": 3}
        self.assertEqual(cfg, p.validate(cfg))
        result = p.compose(cfg, old, [cell], intent, policy, facts)
        self.assertEqual(result["intent"]["dns_route_sources"], p.runtime_source_approvals(cfg))
        self.assertEqual(result["cell_route_publications"][0]["producer_policy"], cfg["route_sources"][0]["producer_policy"])
        self.assertEqual(result["previous_traffic_publication"]["producer_policy"], cfg["global_producer_policy"])
        self.assertEqual(result["intent"]["dns"][0]["ttl"], intent["dns"][0]["ttl"])
        del cfg["global_producer_policy"]
        with self.assertRaises(ValueError): p.validate(cfg)

    def test_continuous_sources_require_exact_current_policy_and_owned_full(self):
        for change in ["valid", "replaced-policy", "foreign-owner", "wrong-cell", "single-publication", "shadow-policy"]:
            with self.subTest(change=change):
                cfg, _, cell, _, _, _, producer = self.continuous_fixture()
                self.assertEqual(cfg, p.validate(cfg))
                pin = copy.deepcopy(cfg["route_sources"][0]["producer_policy"])
                if change == "replaced-policy": pin["fencing_token"] += 1
                if change == "foreign-owner": cell["parent"]["metadata"]["producer_policy_release_id"] = "other"
                if change == "wrong-cell": cell["parent"]["scope_key"] = "authority-cell:cell-other"
                if change == "single-publication": producer["content"]["serving"]["single_publication"] = True
                if change == "shadow-policy": producer["content"]["mode"] = "shadow"
                with patch.object(p, "selected"), patch.object(p, "authority_identity", return_value=pin), patch.object(p, "artifact", return_value=producer):
                    if change == "valid":
                        p.authorize_route_source(None, cfg["route_sources"][0], cell["parent"])
                    else:
                        with self.assertRaises(ValueError):
                            p.authorize_route_source(None, cfg["route_sources"][0], cell["parent"])

    def test_dynamic_declaration_never_accepts_unbound_or_multiple_source_modes(self):
        for change in [lambda c: c.update(route_publications=[]), lambda c: c["route_sources"][0]["producer_policy"].pop("release_id"), lambda c: c["route_sources"][0]["producer_policy"].update(fencing_token=True), lambda c: c["route_sources"].append(c["route_sources"][0]), lambda c: c["route_sources"][0].update(authority_cell_id="cell-foreign")]:
            cfg, *_ = self.continuous_fixture()
            change(cfg)
            with self.assertRaises(ValueError): p.validate(cfg)

    def test_continuous_capture_re_resolves_fences_after_conflict(self):
        cfg, old, cell, intent, policy, facts, _ = self.continuous_fixture()
        old["release"]["released_at"] = p.now().isoformat()
        successor = copy.deepcopy(cell)
        successor["parent"]["id"] = "successor-parent"
        successor["release"].update(id="successor-full", fencing_token=2)
        for error in ["HTTP 409 source changed", "HTTP 400: authority transition changes compiled route behavior or hard constraints"]:
            with self.subTest(error=error):
                calls, saved, requests = [], [], []
                parent = {"id": "dns-parent", "content_hash": "sha256:" + "b" * 64, "content": {"publication_role": "cell-dns"}}
                dns = {"id": "dns-child", "content_hash": "sha256:" + "c" * 64, "content": {}}
                current = {"artifact": parent, "release": {"id": "dns-shadow", "fencing_token": 1}}
                def api(method, path, body=None):
                    if path.endswith("/compiler-input"): return {"runtime_snapshot": facts}
                    if method == "POST": calls.append((path, body))
                    if path.endswith("/compile"):
                        self.assertEqual(saved[-1]["route_publications"], body["intent"]["cell_route_publications"])
                        requests.append(copy.deepcopy(body))
                        if len(requests) == 1: raise RuntimeError(error)
                        dns["content"]["previous_traffic_publication"] = body["previous_traffic_publication"]
                        return {"release_artifact": parent, "dns_artifact": dns}
                    if path.endswith("/release"): self.assertEqual("shadow", body["release_channel"])
                    return {}
                with patch.object(p, "no_serving_authority"), patch.object(p, "selected", side_effect=[{}, {}, current]), patch.object(p, "resolve_route_publications", side_effect=[[cell], [successor]]), patch.object(p, "full_source", return_value=old), patch.object(p, "publication", return_value=old), patch.object(p, "source_input", side_effect=[intent, policy, intent, policy]), patch.object(p, "authorize_route_source") as authorize, patch.object(p.time, "sleep"):
                    result = p.stage(cfg, api, saved.append)
                self.assertEqual([p.reference(successor, "cell-a")], result["route_publications"])
                self.assertNotEqual(requests[0]["intent"]["generation"], requests[1]["intent"]["generation"])
                authorize.assert_called_once_with(api, cfg["route_sources"][0], successor["parent"])
                self.assertEqual(1, sum(path.endswith("/release") for path, _ in calls))
                self.assertFalse(result["serving_published"])

    def test_continuous_capture_does_not_retry_other_compiler_rejections(self):
        cfg, old, cell, intent, policy, facts, _ = self.continuous_fixture()
        old["release"]["released_at"] = p.now().isoformat()
        def api(method, path, body=None):
            if path.endswith("/compiler-input"): return {"runtime_snapshot": facts}
            if path.endswith("/compile"): raise RuntimeError("HTTP 400: invalid signature")
            self.fail("unexpected mutation")
        with patch.object(p, "no_serving_authority"), patch.object(p, "selected", return_value={}), patch.object(p, "resolve_route_publications", return_value=[cell]), patch.object(p, "full_source", return_value=old), patch.object(p, "publication", return_value=old), patch.object(p, "source_input", side_effect=[intent, policy]), patch.object(p.time, "sleep") as wait, self.assertRaisesRegex(RuntimeError, "invalid signature"):
            p.stage(cfg, api, lambda _: None)
        wait.assert_not_called()

    def test_projection_preserves_inputs_constraints_and_all_physical_members(self):
        cfg, old, cell, intent, policy, facts = fixture()
        p.validate(cfg)
        before = copy.deepcopy((cfg, old, cell, intent, policy, facts))
        result = p.compose(cfg, old, [cell], intent, policy, facts)
        self.assertEqual(before, (cfg, old, cell, intent, policy, facts))
        self.assertEqual("cell-dns", result["intent"]["authority_cell_id"])
        self.assertEqual([{"id": "cell-a"}], result["intent"]["edge_topology"]["authority_cells"])
        self.assertEqual(cfg["previous_topology"]["edges"], result["intent"]["edge_topology"]["edges"])
        self.assertEqual("cell-a", result["runtime_snapshot"]["dns_edge_endpoints"][0]["edge_group_id"])
        self.assertEqual("cell-dns", result["runtime_snapshot"]["dns_consumers"][0]["edge_group_id"])
        self.assertEqual(2, result["policy"]["minimum_healthy_edges"])
        self.assertEqual(policy["dns_route_state_constraints"], result["policy"]["dns_route_state_constraints"])
        self.assertEqual(intent["acme_challenges"], result["intent"]["acme_challenges"])
        for field in ["routes", "tls", "cache_policies", "application_domains"]:
            self.assertNotIn(field, result["intent"])
        for field in ["origins", "releases", "tls_domains", "dns_placements", "facts"]:
            self.assertNotIn(field, result["runtime_snapshot"])
        self.assertEqual(p.reference(old), result["previous_traffic_publication"]["reference"])

    def test_invalid_declarations_and_undeclared_physical_inputs_fail(self):
        for change in [lambda c: c.update(generation=True), lambda c: c.update(origin="http://api.example.test"), lambda c: c.update(dns_node_ids=["dns-a", "dns-a"]), lambda c: c.update(authority_cell_id="cell-a"), lambda c: c["route_publications"][0].update(release_channel="gray"), lambda c: c["capture"].update(timeout_seconds=10000), lambda c: c.update(unreviewed=True)]:
            cfg, *_ = fixture()
            change(cfg)
            with self.assertRaises(ValueError):
                p.validate(cfg)
        for field in ["dns_edge_endpoints", "dns_consumers"]:
            cfg, old, cell, intent, policy, facts = fixture()
            facts[field][0]["edge_id" if field == "dns_edge_endpoints" else "node_id"] = "foreign"
            with self.assertRaises(ValueError):
                p.compose(cfg, old, [cell], intent, policy, facts)

    def test_source_input_uses_signed_typed_lineage_metadata_not_content_map_hash(self):
        cfg, old, *_ = fixture()
        old["parent"].update(scope_key="global", content={"lineage": {"intent_generation": "intent-one", "intent_digest": "typed-hash"}})
        value = {"content_hash": "different-map-hash", "metadata": {"intent_digest": "typed-hash"}, "content": {"generation": "intent-one"}}
        api = lambda *args: {"artifacts": [{"id": "input-one", "generation": "intent-one"}]}
        with patch.object(p, "artifact", return_value=value):
            self.assertEqual(value["content"], p.source_input(api, old["parent"], "platform_intent", "intent_generation"))
        value["metadata"]["intent_digest"] = "forged"
        with patch.object(p, "artifact", return_value=value), self.assertRaises(ValueError):
            p.source_input(api, old["parent"], "platform_intent", "intent_generation")

    def test_existing_serving_authority_prevents_any_write(self):
        cfg, *_ = fixture()
        writes = []
        def api(*args):
            if args[0] != "GET": writes.append(args)
            return {"artifact": {"id": "already-serving"}}
        with patch.object(p, "selected", return_value={"artifact": {"id": "already-serving"}}), self.assertRaises(ValueError):
            p.stage(cfg, api, lambda _: None)
        self.assertEqual([], writes)

    def test_new_stage_writes_only_compile_shadow_and_expectations(self):
        cfg, old, cell, intent, policy, facts = fixture()
        old["release"]["released_at"] = p.now().isoformat()
        req = p.compose(cfg, old, [cell], intent, policy, facts)
        parent = {"id": "dns-parent", "content_hash": "sha256:" + "b" * 64, "content": {"publication_role": "cell-dns"}}
        dns = {"id": "dns-child", "content_hash": "sha256:" + "c" * 64, "content": {"previous_traffic_publication": req["previous_traffic_publication"]}}
        current = {"artifact": parent, "release": {"id": "dns-shadow", "fencing_token": 1}}
        writes, saved = [], []
        def api(method, path, body=None):
            if path.endswith("/compiler-input"):
                return {"runtime_snapshot": facts}
            if method == "POST":
                writes.append((path, body))
                if path.endswith("/compile"):
                    return {"release_artifact": parent, "dns_artifact": dns}
                if path.endswith("/release"):
                    self.assertEqual("shadow", body["release_channel"])
                    self.assertTrue(saved)
            return {}
        with patch.object(p, "no_serving_authority"), patch.object(p, "selected", side_effect=[{}, {}, current]), patch.object(p, "full_source", side_effect=[{}, old]), patch.object(p, "publication", side_effect=[cell, old]), patch.object(p, "source_input", side_effect=[intent, policy]):
            result = p.stage(cfg, api, saved.append)
        self.assertFalse(result["public_transport_changed"])
        self.assertFalse(result["serving_published"])
        self.assertEqual(["/v1/admin/platform-config/compile", "/v1/admin/artifacts/dns-parent/release", "/v1/admin/platform-config/release-set/prepare-consumers"], [v[0] for v in writes])


    def test_completed_staging_only_reprepares_same_shadow(self):
        cfg, old, cell, intent, policy, facts = fixture()
        req = p.compose(cfg, old, [cell], intent, policy, facts)
        parent = {"id": "dns-parent", "content_hash": "sha256:" + "b" * 64, "content": {"publication_role": "cell-dns", "artifact_kinds": ["dns_answer_bundle"], "artifact_ids": ["dns-child"]}}
        child = {"id": "dns-child", "content_hash": "sha256:" + "c" * 64, "content": {"cell_dns_source": {"intent": req["intent"]}}}
        current = {"artifact": parent, "release": {"id": "dns-shadow", "fencing_token": 1, "idempotency_key": "dns-transition-shadow/" + p.digest(cfg) + "/" + parent["content_hash"]}}
        writes = []
        with patch.object(p, "no_serving_authority"), patch.object(p, "selected", return_value=current), patch.object(p, "artifact", side_effect=[parent, child]):
            result = p.stage(cfg, lambda *a: writes.append(a), lambda _: None)
        self.assertTrue(result["resumed"])
        self.assertFalse(result["serving_published"])
        self.assertEqual(["/v1/admin/platform-config/release-set/prepare-consumers"], [x[1] for x in writes])


if __name__ == "__main__":
    unittest.main()
