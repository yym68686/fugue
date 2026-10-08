import copy
import unittest
from pathlib import Path

from scripts import publish_agent_edge_shadow as policy


def fixture():
    return {"apiVersion": "configuration.fugue.dev/v1", "kind": "AgentEdgeShadowPolicy", "origin": "https://api.example.test", "expectedPreviousGeneration": "", "policy": {"schema_version": "fugue.agent-edge-policy/v1", "scope": policy.SCOPE, "mode": "shadow", "origin": "https://api.example.test", "generation": "agent-shadow-one"}}


class FakeAPI:
    def __init__(self, config):
        self.config, self.artifacts = config, []
        self.shadow, self.full = {}, {}
        self.calls = []
        self.valid = True
        self.fail_release = False
        self.change_authority = False

    def __call__(self, method, path, body=None):
        self.calls.append((method, path, copy.deepcopy(body)))
        if path.endswith("channel=shadow"):
            return copy.deepcopy(self.shadow)
        if path.endswith("channel=full"):
            return copy.deepcopy(self.full)
        if method == "GET" and path.startswith("/v1/admin/artifacts?"):
            return {"artifacts": copy.deepcopy(self.artifacts)}
        if path == "/v1/admin/artifacts":
            artifact = dict(body, id="artifact-one", scope_key=policy.SCOPE, status="draft", content_hash=policy.digest(body["content"]))
            self.artifacts.append(artifact)
            return {"artifact": copy.deepcopy(artifact)}
        if path.endswith("/validate"):
            self.artifacts[0]["status"] = "validated" if self.valid else "invalid"
            if self.change_authority:
                self.shadow = {"artifact": {"generation": "concurrent-change"}, "release": {"id": "another"}}
            return {"pass": self.valid, "artifact": copy.deepcopy(self.artifacts[0])}
        if path.endswith("/release"):
            if self.fail_release:
                raise RuntimeError("simulated unavailable publisher")
            assert body["release_channel"] == "shadow"
            self.shadow = {"artifact": copy.deepcopy(self.artifacts[0]), "release": {"id": "release-one", "status": "active", "release_channel": "shadow", "fencing_token": 1}}
            return copy.deepcopy(self.shadow)
        raise AssertionError("unexpected API operation")


class ShadowPolicyTests(unittest.TestCase):
    def test_initial_shadow_lane_requires_an_explicit_configuration_change(self):
        workflow = Path(".github/workflows/ci.yml").read_text()
        lane = workflow.split("\n  agent_edge_shadow_policy:", 1)[1].split("\n  cell_trust_plan:", 1)[0]
        self.assertIn('if ! git diff --quiet "$base" HEAD -- "$config"; then', lane)
        self.assertNotIn('HEAD -- "$config" scripts/publish_agent_edge_shadow.py', lane)

    def test_publish_only_shadow_without_verification_or_full_mutations(self):
        config = policy.validate(fixture())
        api = FakeAPI(config)
        result = policy.publish(config, api)
        self.assertEqual(result["release"]["release_channel"], "shadow")
        self.assertFalse(any("verify-lkg" in path for _, path, _ in api.calls))
        before = len([c for c in api.calls if c[0] == "POST"])
        policy.publish(config, api)
        self.assertEqual(before, len([c for c in api.calls if c[0] == "POST"]))

    def test_failed_release_reuses_exact_validated_immutable_draft(self):
        config = fixture()
        api = FakeAPI(config)
        api.fail_release = True
        with self.assertRaises(RuntimeError):
            policy.publish(config, api)
        api.fail_release = False
        policy.publish(config, api)
        self.assertEqual(len(api.artifacts), 1)

    def test_rejects_full_authority_validation_failure_drift_and_concurrent_changes(self):
        for scenario in ["full exists", "invalid", "concurrent", "foreign predecessor", "same generation drift"]:
            with self.subTest(scenario=scenario):
                config, api = fixture(), FakeAPI(fixture())
                if scenario == "full exists":
                    api.full = {"artifact": {"id": "active-authority"}}
                if scenario == "invalid":
                    api.valid = False
                if scenario == "concurrent":
                    api.change_authority = True
                if scenario == "foreign predecessor":
                    api.shadow = {"artifact": {"generation": "foreign"}}
                if scenario == "same generation drift":
                    api.shadow = {"artifact": {"generation": config["policy"]["generation"], "content": {"mode": "active"}}}
                with self.assertRaises(ValueError):
                    policy.publish(config, api)
                self.assertFalse(any(path.endswith("/release") for _, path, _ in api.calls))

    def test_declaration_cannot_enable_active_or_foreign_origin(self):
        for scenario in ["active", "plaintext", "port", "foreign origin", "unknown field"]:
            config = fixture()
            if scenario == "active":
                config["policy"]["mode"] = "active"
            if scenario == "plaintext":
                config["origin"] = "http://api.example.test"
            if scenario == "port":
                config["origin"] += ":443"
            if scenario == "foreign origin":
                config["policy"]["origin"] = "https://foreign.example.test"
            if scenario == "unknown field":
                config["force"] = True
            with self.assertRaises(ValueError):
                policy.validate(config)


if __name__ == "__main__":
    unittest.main()
