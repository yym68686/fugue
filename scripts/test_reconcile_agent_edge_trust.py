import copy
import json
import unittest
from unittest.mock import patch

from scripts import reconcile_agent_edge_trust as trust


def fixture():
    public = {"schema": "fugue.agent-edge-trust/v1", "generation": 1, "keys": [{"key_id": "key-a", "public_key": "synthetic-public", "not_before": "2026-01-01T00:00:00Z", "not_after": "2027-01-01T00:00:00Z", "revoked": False}]}
    config = {"apiVersion": "trust.fugue.dev/v1", "kind": "AgentEdgeTrust", "namespace": "test-system", "signingSecret": "test-signing", "trustConfigMap": "test-trust", "expectedPreviousGeneration": 0, "expectedPreviousTrustDigest": "", "publicTrust": public}
    private = {"schema": "fugue.agent-edge-signing/v1", "generation": 1, "keys": [dict(public["keys"][0], private_key="synthetic-private")]}
    return config, private


def persisted(resource):
    result = copy.deepcopy(resource)
    result["metadata"].update(uid="test-uid-" + resource["kind"], resourceVersion="1")
    return result


class AgentTrustTests(unittest.TestCase):
    def test_public_declaration_never_contains_private_material(self):
        config, private = fixture()
        self.assertEqual(trust.validate_config(config), config)
        public, secret = trust.resources(config, private)
        self.assertNotIn("synthetic-private", json.dumps(public))
        self.assertEqual(set(public["data"]), {"trust.json"})
        self.assertEqual(set(secret["data"]), {"keyring.json"})
        config["publicTrust"]["keys"][0]["private_key"] = "unsafe"
        with self.assertRaises(ValueError):
            trust.validate_config(config)
        with self.assertRaises(ValueError):
            trust.strict_json('{"generation":1,"generation":2}')

    def test_generation_and_current_content_are_both_fenced(self):
        config, private = fixture()
        desired = trust.resources(config, private)[0]
        for scenario in ["foreign", "drift", "same generation changed", "newer generation", "wrong predecessor", "terminating", "identity missing", "immutable"]:
            with self.subTest(scenario=scenario):
                current = persisted(desired)
                if scenario == "foreign":
                    current["metadata"]["labels"]["app.kubernetes.io/managed-by"] = "other"
                if scenario in ["drift", "same generation changed"]:
                    current["data"]["trust.json"] = "changed"
                    if scenario == "same generation changed":
                        current["metadata"]["annotations"][trust.CONTENT_DIGEST] = trust.digest(current["data"])
                if scenario == "newer generation":
                    current["metadata"]["annotations"][trust.GENERATION] = "2"
                if scenario == "wrong predecessor":
                    current["metadata"]["annotations"][trust.GENERATION] = "0"
                if scenario == "terminating":
                    current["metadata"]["deletionTimestamp"] = "2026-02-01T00:00:00Z"
                if scenario == "identity missing":
                    del current["metadata"]["uid"]
                if scenario == "immutable":
                    current["immutable"] = True
                with self.assertRaises(ValueError):
                    trust.validate_existing(current, desired, config)

    def test_rotation_preserves_foreign_metadata_and_uses_cas(self):
        config, private = fixture()
        old = persisted(trust.resources(config, private)[1])
        old["metadata"]["annotations"]["unrelated"] = "preserve"
        config["expectedPreviousGeneration"] = 1
        config["expectedPreviousTrustDigest"] = trust.digest(config["publicTrust"])
        config["publicTrust"]["generation"] = 2
        config["publicTrust"]["keys"][0]["revoked"] = True
        private["generation"] = 2
        private["keys"][0]["revoked"] = True
        private["keys"][0]["private_key"] = ""
        desired = trust.resources(config, private)[1]
        operations = trust.update_patch(old, desired, config)
        self.assertEqual([item["path"] for item in operations[:5]], ["/metadata/uid", "/metadata/resourceVersion", "/metadata/annotations", "/metadata/labels", "/data"])
        self.assertTrue(all(item["op"] == "test" for item in operations[:5]))
        self.assertEqual(operations[-1]["value"]["unrelated"], "preserve")
        with self.assertRaises(ValueError):
            trust.validate_existing(None, desired, config)

    def test_partial_initial_write_resumes_without_replacing_new_trust(self):
        config, private = fixture()
        state = {}
        written = []
        fail_secret = [True]

        def read(item):
            return copy.deepcopy(state.get(item["kind"]))

        def write(args, body=None):
            if "--dry-run=server" in args:
                return ""
            self.assertEqual(args[0], "create")
            if body["kind"] == "Secret" and fail_secret[0]:
                raise RuntimeError("synthetic write failure")
            written.append(body["kind"])
            state[body["kind"]] = persisted(body)
            return ""

        with patch.object(trust, "read_resource", side_effect=read), patch.object(trust, "kubectl", side_effect=write):
            with self.assertRaises(RuntimeError):
                trust.reconcile(config, private)
            self.assertEqual(written, ["ConfigMap"])
            fail_secret[0] = False
            trust.reconcile(config, private)
            self.assertEqual(written, ["ConfigMap", "Secret"])
            state["ConfigMap"]["metadata"]["annotations"]["unrelated"] = "preserved"
            trust.reconcile(config, private)
            trust.reconcile(config, private, check=True)
            self.assertEqual(written, ["ConfigMap", "Secret"])

    def test_preflight_rejection_does_not_partially_publish(self):
        config, private = fixture()
        writes = []

        def reject(args, body=None):
            if "--dry-run=server" not in args:
                writes.append(body)
            if body["kind"] == "Secret":
                raise RuntimeError("synthetic admission failure")
            return ""

        with patch.object(trust, "read_resource", return_value=None), patch.object(trust, "kubectl", side_effect=reject):
            with self.assertRaises(RuntimeError):
                trust.reconcile(config, private)
        self.assertEqual(writes, [])


if __name__ == "__main__":
    unittest.main()
