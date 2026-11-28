import copy
import json
import unittest
from unittest.mock import Mock

from scripts.provision_observation_worker_database import ProvisionError
from scripts.verify_app_mail_provider_runtime import APP_LABEL, observe_provider_runtime


DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.test/fixture/mail@" + DIGEST
SOURCE = {
    "id": "app_fixture", "tenant": "tenant_fixture", "image": IMAGE,
    "revision": "b" * 40, "repository": "https://github.com/example/fixture",
    "sourceImage": "registry-build.example.test/fixture/mail@" + DIGEST,
}


def owner(kind, uid):
    return {"kind": kind, "uid": uid, "controller": True}


class ProviderRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.provision = Mock()
        self.provision.identity_sql.return_value = "SELECT 1;"
        self.provision.sql.return_value = "1\n" + json.dumps(SOURCE)
        containers = [{"name": "application", "image": IMAGE}]
        self.objects = {
            "deployments": [{
                "metadata": {"uid": "deployment-uid", "generation": 7,
                             "labels": {APP_LABEL: SOURCE["id"]}},
                "spec": {"replicas": 1, "template": {"spec": {"containers": containers}}},
                "status": dict(observedGeneration=7, replicas=1, readyReplicas=1,
                               updatedReplicas=1, availableReplicas=1),
            }],
            "replicasets": [{
                "metadata": {"uid": "replicaset-uid",
                             "ownerReferences": [owner("Deployment", "deployment-uid")]},
            }],
            "pods": [{
                "metadata": {"uid": "pod-uid",
                             "ownerReferences": [owner("ReplicaSet", "replicaset-uid")]},
                "spec": {"containers": containers},
                "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}],
                           "containerStatuses": [{"name": "application", "ready": True,
                                                  "imageID": IMAGE}]},
            }],
        }

        def read(verb, kind, *args):
            self.assertEqual(verb, "get")
            self.assertEqual(args, ("--namespace", "fg-tenant-fixture",
                                    "-l", f"{APP_LABEL}={SOURCE['id']}", "-o", "json"))
            return json.dumps({"items": self.objects[kind]})

        self.provision.kube.side_effect = read

    def test_binds_running_digest_without_returning_workload_details(self):
        evidence, witness = observe_provider_runtime(self.provision, "primary")
        self.assertEqual(evidence, dict(repository=SOURCE["repository"], revision=SOURCE["revision"],
                                       imageDigest=DIGEST, readyPods=1))
        self.assertEqual(witness["deployment-uid"][1], ["pod-uid"])
        for call in self.provision.sql.call_args_list:
            self.assertIn("BEGIN READ ONLY;", call.args[1])
            self.assertNotIn("spec_json,", call.args[1])

    def test_rejects_unbound_old_and_unready_pods(self):
        pristine = copy.deepcopy(self.objects)
        for mutation, expected_message in (
            (lambda pod: pod["metadata"].update(ownerReferences=[]), "unbound provider Pod"),
            (lambda pod: pod["status"]["containerStatuses"][0].update(imageID="sha256:" + "c" * 64),
             "bound image digest"),
            (lambda pod: pod["metadata"].update(deletionTimestamp="now"), "bound image digest"),
        ):
            with self.subTest(reason=expected_message):
                self.objects = copy.deepcopy(pristine)
                mutation(self.objects["pods"][0])
                with self.assertRaisesRegex(ProvisionError, expected_message):
                    observe_provider_runtime(self.provision, "primary")

    def test_rejects_mutable_mismatched_or_changed_source(self):
        for changes, expected_message in (
            ({"image": "registry.example.test/fixture/mail:latest"}, "workload has no immutable image"),
            ({"sourceImage": "registry.example.test/fixture/mail:latest"}, "build has no immutable image"),
            ({"revision": None}, "no full Git source revision"),
            ({"repository": None}, "no canonical GitHub source repository"),
            ({"sourceImage": IMAGE.replace("a" * 64, "c" * 64)}, "digests differ"),
        ):
            with self.subTest(reason=expected_message):
                self.provision.sql.return_value = "1\n" + json.dumps({**SOURCE, **changes})
                with self.assertRaisesRegex(ProvisionError, expected_message):
                    observe_provider_runtime(self.provision, "primary")
        self.provision.sql.side_effect = [
            "1\n" + json.dumps(SOURCE),
            "1\n" + json.dumps({**SOURCE, "revision": "c" * 40}),
        ]
        with self.assertRaisesRegex(ProvisionError, "source changed"):
            observe_provider_runtime(self.provision, "primary")

    def test_reports_all_missing_witnesses_without_raw_values(self):
        self.provision.sql.return_value = "1\n" + json.dumps(
            dict(SOURCE, repository="private-value", revision=None, image=None, sourceImage=None))
        with self.assertRaises(ProvisionError) as result:
            observe_provider_runtime(self.provision, "primary")
        message = str(result.exception)
        for detail in ("source repository", "source revision", "build has no immutable image",
                       "workload has no immutable image"):
            self.assertIn(detail, message)
        self.assertNotIn("private-value", message)
