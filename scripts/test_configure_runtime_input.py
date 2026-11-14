import copy
import datetime
import json
import os
import unittest
from unittest.mock import patch

from scripts import configure_runtime_input as runtime
from scripts import reconfigure_cell_producer as runner
from scripts.test_bootstrap_cell_producer import ArtifactAPI


def fixture(cell=False):
    scope = "authority-cell:cell-a" if cell else "global"
    source = {"schema_version": "fugue.platform.config/v1", "scope": scope, "generation": "projection-before", "max_stale_seconds": 600}
    old = {"schema_version": "fugue.platform.producer/v1", "generation": "producer-before", "target_scope": scope, "mode": "serving", "input_source": "business-static-intent", "static_intent_artifact_id": "static", "static_intent_digest": "sha256:" + "a" * 64, "dns_policy_artifact_id": "projection", "dns_policy_digest": runtime.digest(source), "interval_seconds": 60, "refresh_seconds": 600, "serving": {"canary_rule_ref": "cohort=complete", "gray_min_seconds": 30, "full_min_seconds": 60, "rollout_timeout_seconds": 600}}
    if cell:
        source.update(publication_role="cell-routes", authority_cell_id="cell-a")
        old.update(publication_role="cell-routes", authority_cell_id="cell-a", dns_policy_digest=runtime.digest(source))
    c = {"schema": runtime.SCHEMA, "generation": 1, "origin": "https://api.example.test", "target_scope": scope, "producer_generation": "producer-after", "projection_generation": "projection-after", "baseline_mode": "latest_verified_same_policy", "precondition": {"operation": "runtime_input", "previous_policy": {"artifact_id": "old-policy", "content_hash": runtime.digest(old), "release_id": "old-release", "fencing_token": 2}, "serving_full": {"artifact_id": "full", "content_hash": "sha256:" + "b" * 64, "release_id": "full-release", "fencing_token": 5}, "verification_evidence_hash": "sha256:" + "c" * 64}}
    return c, old, source


class Ledger(ArtifactAPI):
    def __init__(self, c, old, source):
        super().__init__(c)
        self.release_bodies = []
        self.on_preview = None
        self.lost_response = False
        self.bad_preview = False
        self.fail_release = False
        def state(ref, kind, scope, channel, content):
            a = {"id": ref["artifact_id"], "content_hash": ref["content_hash"], "generation": content["generation"], "content": copy.deepcopy(content), "scope_key": scope, "artifact_kind": kind, "status": "validated"}
            self.artifacts[a["id"]] = a
            return {"artifact": a, "release": {"id": ref["release_id"], "artifact_id": a["id"], "artifact_kind": kind, "scope_key": scope, "release_channel": channel, "status": "active", "fencing_token": ref["fencing_token"]}}
        pre = c["precondition"]
        self.authorities[("policy_snapshot", runtime.producer_scope(c), "shadow")] = state(pre["previous_policy"], "policy_snapshot", runtime.producer_scope(c), "shadow", old)
        self.artifacts["projection"] = {"id": "projection", "artifact_kind": "policy_snapshot", "scope_key": c["target_scope"], "generation": source["generation"], "content": copy.deepcopy(source), "content_hash": runtime.digest(source), "status": "validated"}
        full = state(pre["serving_full"], "release_set", c["target_scope"], "full", {"generation": "full-generation", "publication_role": old.get("publication_role", "")})
        full["artifact"]["metadata"] = {"producer_policy_release_id": "old-release"}
        full["release"].update(verification_state="verified", verified_lkg_generation="full-generation", released_by_type="bootstrap", released_by_id="platform-config-producer")
        self.authorities[("release_set", c["target_scope"], "full")] = full
        self.lkg = {**pre["serving_full"], "artifact_kind": "release_set", "scope_key": c["target_scope"], "verified_by_release_id": "full-release", "verification_evidence_hash": pre["verification_evidence_hash"], "expires_at": (runtime.now() + datetime.timedelta(days=1)).isoformat()}

    def __call__(self, method, path, body=None):
        if method == "GET" and path.endswith("/lkg"):
            return {"lkg": copy.deepcopy(self.lkg)}
        if method == "GET" and path.startswith("/v1/admin/platform-config/routes/project?"):
            assert "&view=configuration" in path
            if self.on_preview:
                self.on_preview(self)
            return {"business_snapshot_revision": "fixed", "policy": {"scope": self.config["target_scope"], "runtime_input_mode": "invalid" if self.bad_preview else "configuration-v1"}}
        if method == "POST" and path.endswith("/release"):
            self.release_bodies.append(copy.deepcopy(body))
            assert body["release_channel"] == "shadow"
            assert body["producer_reconfiguration"]["operation"] == "runtime_input"
            if self.fail_release:
                raise RuntimeError("transactional conflict")
            result = super().__call__(method, path, body)
            self.authorities[("policy_snapshot", runtime.producer_scope(self.config), "shadow")]["release"]["idempotency_key"] = body["idempotency_key"]
            if self.lost_response:
                self.lost_response = False
                raise RuntimeError("response lost")
            return result
        return super().__call__(method, path, body)


class RuntimeInputConfigurationTests(unittest.TestCase):
    def test_global_and_cell_preserve_controls_full_lkg_and_retry(self):
        for cell in (False, True):
            c, old, source = fixture(cell)
            api = Ledger(c, old, source)
            before, lkg = copy.deepcopy(api.authorities[("release_set", c["target_scope"], "full")]), copy.deepcopy(api.lkg)
            declaration = copy.deepcopy(c)
            self.assertEqual(c, runner.validate(c))
            result = runner.publish(c, api, lambda _: None)
            self.assertTrue(result["producer_activated"])
            self.assertEqual(0, result["audit_inputs_deleted"])
            projection = next(a for a in api.artifacts.values() if a["generation"] == c["projection_generation"])
            self.assertEqual({**source, "generation": c["projection_generation"], "runtime_input_mode": "configuration-v1"}, projection["content"])
            target = api.authorities[("policy_snapshot", runtime.producer_scope(c), "shadow")]["artifact"]["content"]
            for k in old:
                if k not in ("generation", "dns_policy_artifact_id", "dns_policy_digest"):
                    self.assertEqual(old[k], target[k])
            self.assertEqual(result["authority"], runner.publish(c, api, lambda _: None)["authority"])
            self.assertEqual(declaration, c)
            self.assertEqual(before, api.authorities[("release_set", c["target_scope"], "full")])
            self.assertEqual(lkg, api.lkg)

    def test_queue_resolves_only_new_verified_same_predecessor(self):
        c, old, source = fixture()
        api = Ledger(c, old, source)
        full = api.authorities[("release_set", "global", "full")]
        full["release"].update(id="renewed", fencing_token=6)
        api.lkg["verified_by_release_id"] = "renewed"
        result = runner.publish(c, api, lambda _: None)
        self.assertEqual(5, result["declared_precondition"]["serving_full"]["fencing_token"])
        self.assertEqual(6, result["resolved_precondition"]["serving_full"]["fencing_token"])

    @patch.object(runtime.time, "sleep")
    def test_safety_failures_never_publish(self, sleep):
        for failure in ("operation", "origin", "mode", "scope", "source", "projection", "same-generation", "full", "unverified", "wrong-lkg", "expired", "foreign-producer", "preview", "after-preview", "renewed-foreign", "regressed"):
            with self.subTest(failure=failure):
                c, old, source = fixture()
                api = Ledger(c, old, source)
                full = api.authorities[("release_set", "global", "full")]
                if failure == "operation": c["precondition"]["operation"] = "dns_availability_recovery"
                elif failure == "origin": c["origin"] = "https://api.example.test/redirect"
                elif failure == "mode": c["runtime_input_mode"] = "future"
                elif failure == "scope": c["target_scope"] = "foreign"
                elif failure == "source": api.artifacts["projection"]["content"]["generation"] = "forged"
                elif failure == "projection": c["projection_policy"] = {"max_stale_seconds": 900}
                elif failure == "same-generation": c["projection_generation"] = source["generation"]
                elif failure == "full": c["baseline_mode"] = "declared"; c["precondition"]["serving_full"]["fencing_token"] += 1
                elif failure == "unverified": full["release"]["verification_state"] = "serving_unverified"
                elif failure == "wrong-lkg": api.lkg["verified_by_release_id"] = "foreign"
                elif failure == "expired": api.lkg["expires_at"] = "2000-01-01T00:00:00Z"
                elif failure == "foreign-producer": full["artifact"]["metadata"]["producer_policy_release_id"] = "foreign"
                elif failure == "preview": api.bad_preview = True
                elif failure == "after-preview": api.on_preview = lambda a: a.lkg.update(verification_evidence_hash="sha256:" + "d" * 64)
                elif failure == "renewed-foreign": full["release"]["fencing_token"] += 1; full["artifact"]["metadata"]["producer_policy_release_id"] = "foreign"
                elif failure == "regressed": full["release"]["fencing_token"] -= 1
                with self.assertRaises(ValueError): runner.publish(c, api, lambda _: None)
                self.assertEqual([], api.release_bodies)

    def test_server_race_and_lost_response(self):
        for lost in (False, True):
            c, old, source = fixture()
            api = Ledger(c, old, source)
            api.lost_response, api.fail_release = lost, not lost
            with self.assertRaises(RuntimeError): runner.publish(c, api, lambda _: None)
            if lost:
                runner.publish(c, api, lambda _: None)
                self.assertEqual(api.release_bodies[0]["idempotency_key"], api.release_bodies[1]["idempotency_key"])

    @patch.object(runtime.time, "sleep")
    def test_waits_only_for_readonly_verification_then_rechecks_exact_cas(self, sleep):
        c, old, source = fixture()
        api = Ledger(c, old, source)
        full = api.authorities[("release_set", "global", "full")]
        full["release"]["verification_state"] = "serving_unverified"
        sleep.side_effect = lambda _: full["release"].update(verification_state="verified")
        runner.publish(c, api, lambda _: None)
        sleep.assert_called_once_with(10)

    @patch.object(runtime.time, "sleep")
    def test_newer_gray_is_waited_out_without_bypassing_server_guard(self, sleep):
        c, old, source = fixture()
        api = Ledger(c, old, source)
        full = api.authorities[("release_set", "global", "full")]
        full["release"]["released_at"] = "2026-01-01T00:00:00Z"
        gray = copy.deepcopy(full)
        gray["release"].update(id="gray", release_channel="gray", released_at="2026-01-01T00:00:01Z", verification_state="serving_unverified")
        api.authorities[("release_set", "global", "gray")] = gray
        sleep.side_effect = lambda _: gray["release"].update(verification_state="failed")
        runner.publish(c, api, lambda _: None)
        sleep.assert_called_once_with(10)

    @patch.object(runtime, "command", return_value="legacy")
    def test_old_or_invalid_revision_refused(self, command):
        for revision in ("main", "", "a" * 40):
            with self.assertRaises(ValueError): runtime.compatible_revision(revision)

    @patch.object(runtime, "compatible_revision")
    @patch.object(runtime, "readers", return_value=[{"revision": "a" * 40, "rollback_revision": "b" * 40}])
    def test_current_and_rollback_both_required(self, readers, compatible):
        runtime.check_readers()
        self.assertEqual(["a" * 40, "b" * 40], [c.args[0] for c in compatible.call_args_list])

    def test_entrypoint_checks_readers_before_any_api_write(self):
        c, _, _ = fixture()
        with patch("sys.argv", ["reconfigure", "declaration.json", "--evidence", "evidence.json"]), patch.dict(os.environ, {"FUGUE_API_KEY": "synthetic-key"}), patch("pathlib.Path.read_bytes", return_value=json.dumps(c).encode()), patch.object(runtime, "check_readers", side_effect=ValueError("incompatible rollback")), patch.object(runner, "API") as api:
            with self.assertRaises(ValueError): runner.main()
            api.assert_not_called()


if __name__ == "__main__":
    unittest.main()
