import copy
import datetime
import unittest
from scripts import renew_producer_publication as renewal


def fixture():
    policy = {"generation": "policy-v1", "mode": "serving", "target_scope": "global", "serving": {"single_publication": False}}
    pin = {"artifact_id": "policy", "content_hash": renewal.digest(policy), "release_id": "policy-release", "fencing_token": 7}
    full = {"artifact_id": "baseline", "content_hash": "sha256:" + "b" * 64, "release_id": "full-release", "fencing_token": 9}
    row = {"scope": "global", "policy": pin, "full": full, "verification_evidence_hash": "sha256:" + "c" * 64, "failed_release_id": "failed-gray"}
    config = {"schema": "fugue.producer-publication-renewal/v1", "generation": 1, "origin": "https://api.example.test", "producers": [row]}
    def state(pin, scope, channel, kind, content):
        return {"artifact": {"id": pin["artifact_id"], "content_hash": pin["content_hash"], "artifact_kind": kind, "scope_key": scope, "generation": content["generation"], "content": content, "status": "validated"}, "release": {"id": pin["release_id"], "artifact_id": pin["artifact_id"], "artifact_kind": kind, "scope_key": scope, "release_channel": channel, "fencing_token": pin["fencing_token"], "status": "active"}}
    current = state(pin, "platform-config-producer", "shadow", "policy_snapshot", policy)
    baseline = state(full, "global", "full", "release_set", {"generation": "baseline-v1"})
    baseline["artifact"]["metadata"] = {"producer_policy_release_id": pin["release_id"]}
    baseline["release"].update(verification_state="verified", verified_lkg_generation="baseline-v1")
    baseline["lkg"] = {**full, "verified_by_release_id": full["release_id"], "verification_evidence_hash": row["verification_evidence_hash"], "expires_at": (renewal.now() + datetime.timedelta(hours=1)).isoformat()}
    gray = state({**full, "release_id": "failed-gray"}, "global", "gray", "release_set", {"generation": "failed"})
    gray["release"].update(verification_state="failed", verification_evidence={"producer_policy_release_id": pin["release_id"], "failed_source_digest": "sha256:" + "d" * 64, "recovered_by_release_id": full["release_id"]})
    return config, current, baseline, gray


class RenewalTests(unittest.TestCase):
    def test_exact_recovery_and_no_blind_mutation_retry(self):
        for scenario in ["success", "changed-policy", "unverified", "expired", "foreign-failure", "changed-full", "lost-response"]:
            with self.subTest(scenario=scenario):
                config, current, baseline, gray = fixture()
                writes = []
                if scenario == "changed-policy": current["release"]["fencing_token"] += 1
                if scenario == "unverified": baseline["release"]["verification_state"] = "serving_unverified"
                if scenario == "expired": baseline["lkg"]["expires_at"] = (renewal.now() - datetime.timedelta(seconds=1)).isoformat()
                if scenario == "foreign-failure": gray["release"]["verification_evidence"]["recovered_by_release_id"] = "other"
                if scenario == "changed-full": baseline["release"]["id"] = "other"
                def api(method, path, body=None):
                    if method == "GET":
                        return copy.deepcopy(current if "policy_snapshot" in path else baseline if "channel=full" in path else gray)
                    self.assertEqual(path, "/v1/admin/artifacts/policy/rollback")
                    self.assertEqual(set(body), {"release_channel", "to_generation", "reason"})
                    self.assertEqual(body["release_channel"], "shadow")
                    self.assertEqual(body["to_generation"], "policy-v1")
                    writes.append(body)
                    current["release"].update(id="renewed-policy", fencing_token=8, reason=body["reason"])
                    if scenario == "lost-response": raise RuntimeError("response lost")
                    return {}
                if scenario in ["success", "lost-response"]:
                    if scenario == "lost-response":
                        with self.assertRaises(RuntimeError): renewal.renew(config, api, lambda _: None)
                    result = renewal.renew(config, api, lambda _: None)
                    self.assertEqual(result["results"][0]["authority"]["fencing_token"], 8)
                    renewal.renew(config, api, lambda _: None)
                    self.assertEqual(len(writes), 1)
                else:
                    with self.assertRaises(ValueError): renewal.renew(config, api, lambda _: None)
                    self.assertEqual(writes, [])

    def test_unknown_configuration_rejected(self):
        config, *_ = fixture()
        config["force"] = True
        with self.assertRaises(ValueError): renewal.validate(config)
