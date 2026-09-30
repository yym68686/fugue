import copy
import unittest

from scripts import expand_cell_membership as expansion
from scripts.test_reconfigure_cell_producer import fixture as configuration_fixture, Ledger


def fixture():
    configuration, _ = configuration_fixture()
    previous = copy.deepcopy(configuration["policy"])
    previous.update(mode="serving", serving={"single_publication": True, "canary_rule_ref": "cohort=complete", "gray_min_seconds": 30, "full_min_seconds": 60, "rollout_timeout_seconds": 300})
    transition = previous["route_placement_transition"]
    added = copy.deepcopy(transition["next_topology"]["edges"][0])
    added.update(id="edge-b", failure_domains={"host": "edge-b"})
    for key in ["previous_topology", "next_topology"]:
        transition[key]["edges"].append(copy.deepcopy(added))
    static = {"schema_version": "fugue.platform.config/v1", "publication_role": "cell-routes", "authority_cell_id": "cell-a", "scope": "authority-cell:cell-a", "generation": "static-old", "edge_topology": copy.deepcopy(transition["next_topology"])}
    static["edge_topology"]["edges"] = static["edge_topology"]["edges"][:1]
    projection = {"schema_version": "fugue.platform.config/v1", "publication_role": "cell-routes", "authority_cell_id": "cell-a", "scope": "authority-cell:cell-a", "generation": "projection-old", "consumer_topology_digest": expansion.topology_digest(static), "minimum_healthy_edges": 1}
    previous.update(static_intent_artifact_id="static-old", static_intent_digest=expansion.digest(static), dns_policy_artifact_id="projection-old", dns_policy_digest=expansion.digest(projection))
    configuration["policy"] = copy.deepcopy(previous)
    configuration["precondition"]["previous_policy"]["content_hash"] = expansion.digest(previous)
    configuration["precondition"]["operation"] = "expand_membership"
    api = Ledger(configuration, previous)
    for ident, kind, content in [("static-old", "platform_intent", static), ("projection-old", "policy_snapshot", projection)]:
        api.artifacts[ident] = {"id": ident, "artifact_kind": kind, "scope_key": content["scope"], "generation": content["generation"], "content": copy.deepcopy(content), "content_hash": expansion.digest(content), "status": "validated"}
    full = api.authorities[("release_set", "authority-cell:cell-a", "full")]
    full["artifact"]["metadata"] = {"producer_policy_release_id": "old-release", "producer_static_intent_id": previous["static_intent_artifact_id"], "producer_static_intent_digest": previous["static_intent_digest"], "producer_dns_policy_id": previous["dns_policy_artifact_id"], "producer_dns_policy_digest": previous["dns_policy_digest"]}
    full["release"].update(released_by_type="bootstrap", released_by_id="platform-config-producer", verification_state="verified", verified_lkg_generation=full["artifact"]["generation"])
    target_static, target_projection = copy.deepcopy(static), copy.deepcopy(projection)
    target_static["generation"] = "static-expanded"
    target_static["edge_topology"]["edges"].append(added)
    target_projection.update(generation="projection-expanded", consumer_topology_digest=expansion.topology_digest(target_static))
    declaration = {"schema": "fugue.cell-membership-expansion/v1", "generation": 1, "origin": configuration["origin"], "authority_cell_id": "cell-a", "producer_generation": "producer-expanded", "precondition": configuration["precondition"], "static_intent": target_static, "projection_policy": target_projection}
    return declaration, api


class MembershipTests(unittest.TestCase):
    def test_single_declared_addition_only_publishes_shadow_and_retry_reuses_inputs(self):
        c, api = fixture()
        before = copy.deepcopy(api.authorities[("release_set", "authority-cell:cell-a", "full")])
        old_lkg = copy.deepcopy(api.lkg)
        evidence = []
        api.lost_response = True
        with self.assertRaisesRegex(RuntimeError, "lost"):
            expansion.publish(c, api, evidence.append)
        count = len(api.artifacts)
        result = expansion.publish(c, api, evidence.append)
        self.assertEqual(len(api.artifacts), count)
        self.assertEqual(before, api.authorities[("release_set", "authority-cell:cell-a", "full")])
        self.assertEqual(old_lkg, api.lkg)
        self.assertEqual(result["mode"], "shadow")
        self.assertFalse(result["serving_publication_authorized"])
        self.assertFalse(result["serving_publication_changed"])
        self.assertEqual(api.release_bodies[0], api.release_bodies[1])
        resolved = evidence[-1]["resolved_declaration"]
        self.assertEqual(resolved["precondition"]["operation"], "expand_membership")
        self.assertEqual(resolved["policy"]["static_intent_digest"], expansion.digest(c["static_intent"]))

    def test_unsafe_expansion_rejected_before_artifact_creation(self):
        for failure in ["removed", "old member", "new member", "extra", "static route", "floor", "digest", "stale predecessor", "foreign baseline", "source revoked"]:
            with self.subTest(failure=failure):
                c, api = fixture()
                edges = c["static_intent"]["edge_topology"]["edges"]
                if failure == "removed":
                    edges.pop(0)
                elif failure == "old member":
                    edges[0]["failure_domains"]["host"] = "different"
                elif failure == "new member":
                    edges[1]["capabilities"] = ["http"]
                elif failure == "extra":
                    edge = copy.deepcopy(edges[1]); edge["id"] = "edge-c"; edges.append(edge)
                elif failure == "static route":
                    c["static_intent"]["routes"] = [{"hostname": "changed.example.test"}]
                elif failure == "floor":
                    c["projection_policy"]["minimum_healthy_edges"] = 2
                elif failure == "digest":
                    c["projection_policy"]["consumer_topology_digest"] = "sha256:" + "f" * 64
                elif failure == "stale predecessor":
                    c["precondition"]["previous_policy"]["fencing_token"] += 1
                elif failure == "foreign baseline":
                    api.authorities[("release_set", "authority-cell:cell-a", "full")]["artifact"]["metadata"]["producer_policy_release_id"] = "another"
                elif failure == "source revoked":
                    api.artifacts["static-old"]["status"] = "invalid"
                if failure in ["removed", "old member", "new member", "extra"]:
                    c["projection_policy"]["consumer_topology_digest"] = expansion.topology_digest(c["static_intent"])
                with self.assertRaises(ValueError):
                    expansion.publish(c, api, lambda _: None)
                self.assertEqual(api.writes, [])


if __name__ == "__main__":
    unittest.main()
