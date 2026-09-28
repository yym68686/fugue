import copy
import json
import unittest

from scripts import provision_runtime_agent_identity as identity


class IdentityTests(unittest.TestCase):
    def fixture(self):
        config = {"apiVersion": "identity.fugue.dev/v1", "kind": "RuntimeAgentIdentity", "origin": "https://api.example.test", "namespace": "test-system", "secretName": "test-agent", "runtime": {"name": "test-agent", "type": "external-owned", "endpoint": "", "labels": {"purpose": "control-observation"}}}
        runtime = dict(config["runtime"], id="runtime_test_1", access_mode="private", pool_mode="dedicated")
        return config, runtime

    def test_first_provision_is_private_and_replay_preserves_credentials(self):
        config, runtime = self.fixture()
        state, writes = {}, []
        creates = []

        def api(method, path, body=None):
            if method == "GET" and path == "/v1/runtimes":
                return {"runtimes": []}
            if method == "GET":
                return {"runtime": runtime}
            creates.append(path)
            return {"runtime": runtime, "runtime_key": "fugue_rt_synthetic"}

        def kube(args, body=None):
            if args[0] == "get":
                return json.dumps(state["secret"]) if "secret" in state else ""
            if "--dry-run=server" not in args:
                writes.append(args)
                state["secret"] = copy.deepcopy(body)
            return ""

        identity.validate(config)
        self.assertEqual(identity.provision(config, api, kube), "runtime_test_1")
        self.assertTrue(state["secret"]["immutable"])
        self.assertEqual(identity.provision(config, api, kube), "runtime_test_1")
        self.assertEqual(len(writes), 1)
        self.assertEqual(creates, ["/v1/runtimes"])

    def test_existing_unowned_runtime_is_never_rotated(self):
        config, runtime = self.fixture()

        def api(method, path, body=None):
            self.assertEqual(method, "GET")
            return {"runtimes": [runtime]}

        with self.assertRaises(ValueError):
            identity.provision(config, api, lambda args, body=None: "")

    def test_kubernetes_preflight_failure_prevents_runtime_creation(self):
        config, _ = self.fixture()

        def api(method, path, body=None):
            self.assertEqual(method, "GET")
            return {"runtimes": []}

        def kube(args, body=None):
            if args[0] == "get":
                return ""
            raise RuntimeError("synthetic admission denial")

        with self.assertRaises(RuntimeError):
            identity.provision(config, api, kube)

    def test_foreign_or_shared_runtime_is_not_installed(self):
        config, runtime = self.fixture()
        for key, value in [("type", "managed-shared"), ("access_mode", "shared"), ("pool_mode", "shared"), ("name", "foreign")]:
            bad = dict(runtime, **{key: value})
            with self.assertRaises(ValueError):
                identity.validate_runtime(bad, config)


if __name__ == "__main__":
    unittest.main()
