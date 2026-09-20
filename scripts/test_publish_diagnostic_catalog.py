import base64
import copy
import hashlib
import json
import os
import unittest
from unittest import mock

from scripts import publish_diagnostic_catalog as publisher


def config():
    return {
        "namespace": "system-test", "repository": "registry.example/diagnostics",
        "catalog": {"protocol": "fugue.diagnostics/v1", "generation": 1, "runner_image": "$runner",
                    "policy": {"namespaces": ["system-test"], "profiles": ["cluster-read"], "service_account": "observer-test"},
                    "probes": [{"id": "sample-test", "image": "$runner", "profile": "cluster-read", "target_types": ["node"],
                                "max_duration_seconds": 30, "parameters": {}, "config": {"collectors": []}}]},
        "reader_rules": [{"apiGroups": [""], "resources": ["pods"], "verbs": ["get", "list"]}],
    }


class DiagnosticPublicationTest(unittest.TestCase):
    def test_signature_rejects_changed_payload_and_key(self):
        seed = os.urandom(32)
        public = publisher.public_key(seed)
        payload = publisher.canonical({"observation": "original"})
        key_id = hashlib.sha256(public).hexdigest()[:16]
        envelope = {"key_id": key_id, "payload": base64.b64encode(payload).decode(),
                    "signature": base64.b64encode(publisher.sign(payload, seed + public)).decode()}
        keys = {key_id: base64.b64encode(public).decode()}
        self.assertEqual(publisher.verify(envelope, keys), {"observation": "original"})
        changed = dict(envelope, payload=base64.b64encode(b'{"observation":"changed"}').decode())
        with self.assertRaises(RuntimeError):
            publisher.verify(changed, keys)
        with self.assertRaises(KeyError):
            publisher.verify(envelope, {})

    def test_policy_cannot_add_mutations_or_credentials(self):
        image = "registry.example/diagnostics@sha256:" + "a" * 64
        for rule in [{"apiGroups": [""], "resources": ["pods"], "verbs": ["delete"]},
                     {"apiGroups": [""], "resources": ["secrets"], "verbs": ["get"]},
                     {"apiGroups": [""], "resources": ["pods/exec"], "verbs": ["get"]}]:
            value = config()
            value["reader_rules"] = [rule]
            with self.assertRaises(ValueError):
                publisher.validate(value, image)
        with self.assertRaises(ValueError):
            publisher.validate(config(), "registry.example/diagnostics:latest")

    def test_probe_updates_need_no_workload_mutation(self):
        value = config()
        image = "registry.example/diagnostics@sha256:" + "b" * 64
        seen = []
        def put(document, old):
            seen.append(copy.deepcopy(document))
            return document
        with mock.patch.dict(os.environ, {"GITHUB_SHA": "a" * 40}), mock.patch.object(publisher, "get", return_value=None), mock.patch.object(publisher, "put", side_effect=put):
            publisher.publish(value, image, True)
        self.assertEqual({x["kind"] for x in seen}, {"Secret", "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "ConfigMap"})
        signed = next(x for x in seen if x["metadata"]["name"] == publisher.CATALOG)
        trust = next(x for x in seen if x["metadata"]["name"] == publisher.TRUST)
        recovered = publisher.verify(json.loads(signed["data"]["catalog.json"]), json.loads(trust["data"]["keys.json"]))
        self.assertEqual(recovered["probes"][0]["image"], image)
        self.assertNotIn("Deployment", {x["kind"] for x in seen})

    def test_old_workflow_cannot_replace_newer_configuration(self):
        with mock.patch.object(publisher.subprocess, "run", return_value=mock.Mock(returncode=1)):
            with self.assertRaisesRegex(ValueError, "stale"):
                publisher.check_revision("b" * 40, "a" * 40)
        with self.assertRaises(ValueError):
            publisher.check_revision("", "not-a-git-revision")

    def test_publication_preserves_resource_version_precondition(self):
        document = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": publisher.metadata("catalog-test")}
        old = {"metadata": {"labels": {publisher.OWNER: "true"}, "resourceVersion": "42"}}
        with mock.patch.object(publisher, "command", return_value=b"{}") as command:
            publisher.put(document, old)
            sent = json.loads(command.call_args.args[1])
            self.assertEqual(sent["metadata"]["resourceVersion"], "42")
        with self.assertRaises(ValueError):
            publisher.put(document, {"metadata": {"labels": {}, "resourceVersion": "43"}})


if __name__ == "__main__":
    unittest.main()
