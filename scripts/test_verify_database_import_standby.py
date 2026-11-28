import copy
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts.provision_observation_worker_database import ProvisionError
from scripts.verify_database_import_standby import (
    COMPONENT, REPOSITORY, STANDBY_METRICS, component_from_receipt, verify_standby, verify_template,
)


SHA = "a" * 40
TOP = "sha256:" + "b" * 64
PLATFORM = "sha256:" + "c" * 64
CONFIG = "sha256:" + "d" * 64
RECEIPT = {
    "apiVersion": "release.fugue.dev/v2", "kind": "ImmutableComponentArtifactReceipt", "component": COMPONENT,
    "sourceSha": SHA, "configSha": SHA, "ociRevision": SHA, "topDigest": TOP,
    "platformManifestDigest": PLATFORM, "configDigest": CONFIG, "immutableRef": f"{REPOSITORY}@{TOP}",
}


class Fixture:
    config = {"namespace": "fixture"}
    metrics = STANDBY_METRICS
    owner = "api"
    fence = False

    def find_primary(self):
        return "postgres-1", "primary-uid"

    def kube(self, *args):
        return "ok" if args[-1].endswith("/readyz") else "\n".join(self.metrics)

    def identity_sql(self):
        return "SELECT 1;"

    def sql(self, _primary, body):
        if "BEGIN READ ONLY" not in body:
            raise AssertionError("standby verifier attempted a write")
        return "1\n" + json.dumps({"owner": self.owner, "activeFence": self.fence})


class VerifyImportStandbyTest(unittest.TestCase):
    def test_binds_receipt_config_and_runtime_image(self):
        expected = component_from_receipt(RECEIPT, SHA)
        for field, value in (("ociRevision", "e" * 40), ("platformManifestDigest", "latest"),
                             ("immutableRef", f"{REPOSITORY}:latest")):
            with self.subTest(field=field), self.assertRaises(ProvisionError):
                component_from_receipt({**RECEIPT, field: value}, SHA)
        resources = Path(__file__).resolve().parents[1] / "deploy/releases/database-import-worker/resources.json"
        template = json.loads(resources.read_text())["items"][0]["spec"]["template"]
        verify_template(template, expected)
        pod = copy.deepcopy(template)
        pod.update(kind="Pod", status={"containerStatuses": [{"name": COMPONENT, "imageID": f"{REPOSITORY}@{PLATFORM}"}]})
        verify_template(pod, expected)
        pod["status"]["containerStatuses"][0]["imageID"] = "sha256:" + "0" * 64
        with self.assertRaises(ProvisionError):
            verify_template(pod, expected)
        template["spec"]["containers"][0]["env"].append({"name": "UNRELATED", "value": "fixture"})
        with self.assertRaises(ProvisionError):
            verify_template(template, expected)

    @patch("scripts.verify_database_import_standby.verify_workload", return_value=("deployment-uid", 1, [("worker-pod", "pod-uid")]))
    def test_requires_matching_standby_in_pod_and_database(self, _verify):
        fixture = Fixture()
        expected = component_from_receipt(RECEIPT, SHA)
        self.assertFalse(verify_standby(fixture, expected)["writerTransferred"])
        for field, value in (("owner", "worker"), ("fence", True), ("metrics", {"fugue_database_import_worker_owns_execution 1"})):
            with self.subTest(field=field), patch.object(fixture, field, value), self.assertRaises(ProvisionError):
                verify_standby(fixture, expected)


if __name__ == "__main__":
    unittest.main()
