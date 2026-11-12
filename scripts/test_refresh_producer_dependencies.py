import copy
import unittest
from unittest.mock import patch

from scripts import refresh_producer_dependencies as repair
from scripts import reconfigure_cell_producer as runner
from scripts.test_reconfigure_cell_producer import fixture, Ledger


class DependencyRefreshTests(unittest.TestCase):
    def test_dns_preview_retries_only_readonly_unavailability_with_fixed_authority(self):
        source = {"artifact_id": "source", "content_hash": "sha256:" + "a" * 64, "release_id": "source-release", "fencing_token": 2}
        c = {"policy": {"dns_source": {"producer_policy": source}}}
        state = {"artifact": {"id": source["artifact_id"], "content_hash": source["content_hash"], "status": "validated"}, "release": {"id": source["release_id"], "fencing_token": source["fencing_token"], "status": "active"}}
        for scenario in ["recover", "forbidden", "transport", "exhausted", "deadline", "source changed", "baseline changed"]:
            with self.subTest(scenario=scenario):
                calls = []
                def api(method, path, body=None):
                    self.assertEqual("GET", method)
                    self.assertIsNone(body)
                    calls.append(path)
                    if scenario == "transport": raise RuntimeError("transport unavailable")
                    if scenario == "forbidden": raise repair.ArtifactHTTPError(403)
                    if scenario == "recover" and len(calls) == 3: return {"intent": {}}
                    raise repair.ArtifactHTTPError(503)
                with patch.object(repair, "baseline") as baseline, patch.object(repair, "selected", return_value=state) as selected, patch.object(repair.time, "sleep") as sleep, patch.object(repair.time, "monotonic", side_effect=[0, 121] if scenario == "deadline" else None, return_value=0):
                    if scenario == "source changed": selected.return_value = {}
                    if scenario == "baseline changed": baseline.side_effect = ValueError("baseline changed")
                    if scenario == "recover":
                        self.assertEqual({"intent": {}}, repair.dns_preview(c, api, {"id": "candidate"}))
                        self.assertEqual(3, len(calls))
                        self.assertEqual(2, sleep.call_count)
                    else:
                        with self.assertRaises((RuntimeError, ValueError)):
                            repair.dns_preview(c, api, {"id": "candidate"})
                        self.assertEqual(8 if scenario == "exhausted" else 0 if scenario in ["source changed", "baseline changed"] else 1, len(calls))
                        self.assertEqual(7 if scenario == "exhausted" else 0, sleep.call_count)

    def fixture(self):
        c, _ = fixture()
        c["schema"] = repair.SCHEMA
        c["precondition"]["operation"] = "placement_constraints"
        c["policy"].update(mode="serving", serving={"single_publication": False, "canary_rule_ref": "cohort=complete", "gray_min_seconds": 30, "full_min_seconds": 60, "rollout_timeout_seconds": 300})
        old = copy.deepcopy(c["policy"])
        old["generation"] = "predecessor"
        c["precondition"]["previous_policy"]["content_hash"] = repair.digest(old)
        api = Ledger(c, old)
        full = api.authorities[("release_set", "authority-cell:cell-a", "full")]
        full["artifact"]["metadata"] = {"producer_policy_release_id": "old-release"}
        full["release"].update(verification_state="verified", released_by_id="platform-config-producer", released_by_type="bootstrap")
        return c, old, api

    def test_dispatch_publish_retry_and_baseline_preserved(self):
        c, old, api = self.fixture()
        self.assertEqual(c, runner.validate(c))
        baseline = copy.deepcopy(api.lkg)
        result = runner.publish(c, api, lambda _: None)
        self.assertEqual("placement_constraints", result["operation"])
        self.assertEqual(result["authority"], runner.publish(c, api, lambda _: None)["authority"])
        self.assertEqual(baseline, api.lkg)

    def test_changed_input_topology_or_baseline_prevents_publication(self):
        for field in ["source", "topology", "schedule", "stale", "preview", "expired"]:
            with self.subTest(field=field):
                c, old, api = self.fixture()
                if field == "source": c["policy"]["static_intent_artifact_id"] = "foreign"
                if field == "topology": c["policy"]["route_placement_transition"]["next_topology"]["edges"][0]["labels"] = {"region": "changed"}
                if field == "schedule": c["policy"]["refresh_seconds"] += 1
                if field == "stale": c["precondition"]["serving_full"]["fencing_token"] += 1
                if field == "preview": api.bad_preview = True
                if field == "expired": api.lkg["expires_at"] = "2000-01-01T00:00:00Z"
                with self.assertRaises(ValueError): runner.publish(c, api, lambda _: None)
                self.assertEqual([], api.release_bodies)

    def test_dns_handoff_changes_only_explicit_source(self):
        c, _, _ = self.fixture()
        c["precondition"]["operation"] = "dns_source"
        p = c["policy"]
        for key in ["route_placement_transition", "static_intent_artifact_id", "static_intent_digest", "dns_policy_artifact_id", "dns_policy_digest", "require_application_domains", "require_route_defaults"]:
            p.pop(key)
        p.update(publication_role="cell-dns", input_source="verified-global-dns", dns_source={"producer_policy": copy.deepcopy(c["precondition"]["previous_policy"]), "previous_topology": {}, "dns_node_ids": ["dns-a"]})
        old = copy.deepcopy(p)
        old["generation"] = "old-dns"
        p["dns_source"]["producer_policy"]["release_id"] = "approved-successor"
        repair.check_delta(c, old)
        for field in ["member", "topology", "serving"]:
            wrong = copy.deepcopy(c)
            if field == "member": wrong["policy"]["dns_source"]["dns_node_ids"].append("dns-b")
            if field == "topology": wrong["policy"]["dns_source"]["previous_topology"] = {"changed": True}
            if field == "serving": wrong["policy"]["serving"]["full_min_seconds"] += 1
            with self.assertRaises(ValueError): repair.check_delta(wrong, old)


if __name__ == "__main__":
    unittest.main()
