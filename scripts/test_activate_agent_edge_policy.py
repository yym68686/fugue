import copy
import datetime
import json
import unittest
from unittest.mock import patch

from scripts import activate_agent_edge_policy as active


def fixture():
    policy = {"schema_version": "fugue.agent-edge-policy/v1", "scope": active.SCOPE, "generation": "agent-active-one", "mode": "active", "origin": "https://api.example.test", "constraint": {"min_candidates": 1}}
    config = {"apiVersion": "configuration.fugue.dev/v1", "kind": "AgentEdgeActivePolicy", "origin": policy["origin"], "expectedShadowGeneration": "agent-shadow-one", "policy": policy, "consumer": {"namespace": "test-system", "deployment": "test-agent", "container": "agent", "runtimeId": "runtime_test", "sourceSha": "a" * 40, "checkpointPath": "/state/checkpoint.json"}, "observation": {"samples": 5, "intervalSeconds": 10, "minimumGrants": 3, "minimumDistinctCells": 2, "maxHeartbeatAgeSeconds": 45}}
    config["observation"]["maxRecoveredDegradationSeconds"] = 0
    baseline = dict(policy, mode="shadow", generation="agent-shadow-one")
    authority = {"artifact": {"id": "policy-one", "artifact_kind": "policy_snapshot", "scope_key": active.SCOPE, "generation": baseline["generation"], "content": baseline, "status": "validated", "content_hash": active.digest(baseline)}, "release": {"id": "release-one", "status": "active", "release_channel": "shadow", "fencing_token": 1}}
    public = {"generation": 1}
    return config, authority, public


class ActivationTests(unittest.TestCase):
    def test_observation_requires_stable_consumer_and_actual_renewal(self):
        config, authority, public = fixture()
        active.validate(config)
        for bad in [None, "pod", "image", "grant", "heartbeat"]:
            with self.subTest(bad=bad):
                def sample(*_):
                    i = sample.calls
                    sample.calls += 1
                    return {"pod_uid": str(i) if bad == "pod" else "pod-one", "image_id": str(i) if bad == "image" else "image-one", "grant_digest": "same" if bad == "grant" else str(i), "heartbeat": "same" if bad == "heartbeat" else str(i), "log_start_at": "2026-01-01T00:00:00Z"}
                sample.calls = 0
                with patch.object(active, "selected_authority", return_value=(authority, "shadow")), patch.object(active, "sample", side_effect=sample), patch.object(active.time, "sleep"):
                    if bad:
                        with self.assertRaises(ValueError):
                            active.prepare(config, None, public, "validator")
                    else:
                        witness = active.prepare(config, None, public, "validator")
                        active.validate_witness(config, public, witness)

    def test_observation_failure_cannot_become_automatic_verification(self):
        config, authority, public = fixture()
        calls = []
        with patch.object(active, "selected_authority", return_value=(authority, "shadow")), patch.object(active, "sample", side_effect=ValueError("no live permission")), patch.object(active.time, "sleep"):
            with self.assertRaises(ValueError):
                active.prepare(config, lambda *args: calls.append(args), public, "validator")
        self.assertEqual(calls, [])

    def test_watch_retains_baseline_log_interval_across_all_samples(self):
        config, authority, public = fixture()
        calls = []
        def sample(*args):
            calls.append(args)
            i = len(calls)
            return {"pod_uid": "one", "image_id": "one", "grant_digest": str(i), "heartbeat": str(i), "log_start_at": "2026-01-01T00:00:00Z"}
        with patch.object(active, "selected_authority", return_value=(authority, "active")), patch.object(active, "sample", side_effect=sample), patch.object(active.time, "sleep"):
            active.prepare_window(config, None, public, "validator")
        self.assertEqual(len(calls[0]), 6)
        self.assertEqual([x[6] for x in calls[1:]], ["2026-01-01T00:00:00Z"] * (config["observation"]["samples"] - 1))

    def test_degraded_or_acquisition_gap_stops_active_window(self):
        base = {"grant_digest":"grant","primary":"edge-a","standbys":["edge-b"],"degraded":False,"mode":"shadow"}
        good = "agent_edge_selection " + json.dumps(base)
        self.assertEqual(active.measured_choice(good,"grant"),base)
        for error in ["degraded", "acquisition", "old degraded", "no primary", "other error"]:
            value = dict(base)
            if error in ["degraded", "old degraded"]: value["degraded"] = True
            if error == "old degraded": value["grant_digest"] = "previous"
            if error == "no primary": value.pop("primary")
            logs = "agent_edge_selection " + json.dumps(value)
            if error == "acquisition": logs += " error=Agent Edge acquisition returned HTTP 503"
            if error == "other error": logs += " error=invalid signature"
            with self.assertRaises(ValueError): active.measured_choice(logs+"\n"+good,"grant")

    def test_explicit_recovery_bound_requires_measured_recovery_and_live_primary(self):
        clock = active.now()
        def entry(seconds, *, degraded=False, grant="old", error="", primary="edge-a", expired=False):
            at = clock + datetime.timedelta(seconds=seconds)
            value = {"grant_digest": grant, "primary": primary, "standbys": [] if degraded else ["edge-b"], "degraded": degraded, "valid_until": (at - datetime.timedelta(seconds=1) if expired else clock + datetime.timedelta(seconds=30)).isoformat()}
            return at.isoformat() + " agent_edge_selection " + json.dumps(value) + error

        good = entry(-100) + "\n" + entry(-80, degraded=True) + "\n" + entry(-60, grant="current")
        choice = active.measured_choice(good, "current", 30)
        self.assertEqual(choice["recovery"], {"degradations": 1, "max_degraded_seconds": 20, "acquisition_failures": 0})
        acquisition = entry(-100) + "\n" + entry(-80, error=" error=Agent Edge acquisition returned HTTP 503") + "\n" + entry(-60, grant="current")
        self.assertEqual(active.measured_choice(acquisition, "current", 30)["recovery"]["acquisition_failures"], 1)
        for bad in [
            entry(-80, degraded=True) + "\n" + entry(-60, grant="current"),
            entry(-100) + "\n" + entry(-80, degraded=True),
            entry(-100) + "\n" + entry(-80, degraded=True) + "\n" + entry(-20, grant="current"),
            entry(-100) + "\n" + entry(-80, primary="") + "\n" + entry(-60, grant="current"),
            entry(-100) + "\n" + entry(-80, expired=True) + "\n" + entry(-60, grant="current"),
            entry(-100) + "\n" + entry(-80, error=" error=invalid signature") + "\n" + entry(-60, grant="current"),
            entry(-100) + "\n" + entry(-80, error=" error=Agent Edge acquisition returned HTTP 503") + "\n" + entry(-60),
            entry(-100, error=" error=Agent Edge acquisition returned HTTP 503") + "\n" + entry(-70, error=" error=Agent Edge acquisition returned HTTP 503") + "\n" + entry(-50, grant="current"),
            good + "\nheartbeat failed: request failed",
            good + "\npoll failed: no live Agent Edge permission",
            entry(-80) + "\n" + entry(-100, grant="current"),
            entry(10, grant="current"),
        ]:
            with self.subTest(logs=bad), self.assertRaises(ValueError):
                active.measured_choice(bad, "current", 30)

    def test_witness_tampering_expiration_and_wrong_declaration_are_rejected_before_write(self):
        config, authority, public = fixture()
        witness = {"schema": "fugue.agent-edge-activation-evidence/v1", "declaration_digest": active.digest(config), "trust_digest": active.digest(public), "authority": authority, "mode": "shadow", "observations": [{}] * 5, "completed_at": active.now().isoformat()}
        witness["evidence_digest"] = active.digest(witness)
        for bad in ["digest", "expired", "declaration", "trust", "sample count"]:
            changed = copy.deepcopy(witness)
            if bad == "digest":
                changed["mode"] = "active"
            if bad == "expired":
                changed["completed_at"] = (active.now() - datetime.timedelta(minutes=6)).isoformat()
            if bad == "declaration":
                changed["declaration_digest"] = "foreign"
            if bad == "trust":
                changed["trust_digest"] = "foreign"
            if bad == "sample count":
                changed["observations"] = []
            if bad != "digest":
                changed.pop("evidence_digest")
                changed["evidence_digest"] = active.digest(changed)
            calls = []
            with self.assertRaises(ValueError):
                active.activate(config, lambda *args: calls.append(args), public, "validator", changed)
            self.assertEqual(calls, [])

    def test_full_foreign_authority_and_changed_shadow_constraints_are_not_overridden(self):
        config, authority, _ = fixture()
        for changed in ["full", "constraint"]:
            shadow = copy.deepcopy(authority)
            if changed == "constraint":
                shadow["artifact"]["content"]["constraint"]["min_candidates"] = 2
            with patch.object(active, "current", side_effect=lambda api, channel: {"artifact": {"id": "foreign"}} if changed == "full" and channel == "full" else (shadow if channel == "shadow" else {})):
                with self.assertRaises(ValueError):
                    active.selected_authority(None, config)


if __name__ == "__main__":
    unittest.main()
