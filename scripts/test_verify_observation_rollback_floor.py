import copy
import json
import unittest
from unittest.mock import Mock

from scripts.provision_observation_worker_database import ProvisionError
from scripts.verify_observation_rollback_floor import checked_rollback, digest


class ObservationRollbackFloorTest(unittest.TestCase):
    def fixture(self):
        component = "api"
        workload = {
            "deployment": "example-api", "container": "api",
            "revision": "b" * 40, "image": "registry.example/platform/api@sha256:" + "b" * 64,
        }
        state_name = "fugue-release-monitor-api"
        record_name = "fugue-release-record-api-" + "c" * 16
        rollback_revision = "a" * 40
        rollback_image = "registry.example/platform/api@sha256:" + "a" * 64
        plan = {"component": component, "configSha": workload["revision"], "planDigest": "sha256:" + "e" * 64}
        terminal = {"component": component, "status": "verified", "executionPlanDigest": plan["planDigest"],
                    "receiptDigest": "sha256:" + "f" * 64}
        record = {"recordDigest": "sha256:" + "c" * 64, "component": component, "configSha": workload["revision"],
                  "executionPlanDigest": plan["planDigest"], "terminalReceiptDigest": terminal["receiptDigest"]}
        data = {"terminal-result.json": json.dumps(terminal)}
        for field, revision, image in (
            ("forward", workload["revision"], workload["image"]),
            ("lkg", rollback_revision, rollback_image),
        ):
            payload = {"kind": "Deployment", "metadata": {"name": workload["deployment"]}, "spec": {
                "template": {"metadata": {"annotations": {"fugue.pro/oci-revision": revision}},
                             "spec": {"containers": [{"name": "api", "image": image}]}},
            }}
            manifest = json.dumps({"items": [payload]}, separators=(",", ":"))
            key = "forwardManifestDigest" if field == "forward" else "lkgManifestDigest"
            record[key] = digest(manifest)
            plan[field] = {"present": True, "imageRef": image, "ociRevision": revision,
                           "manifestDigest": record[key]}
            data[field + ".json"] = manifest
        data["record.json"] = json.dumps(record)
        data["execution-plan.json"] = json.dumps(plan)
        pointer = {"metadata": {"resourceVersion": "17"}, "data": {"recordName": record_name, "state.json": json.dumps({
            "component": component, "configSha": workload["revision"],
            "consecutiveFailures": 0, "recordDigest": record["recordDigest"],
        })}}
        immutable = {"immutable": True, "metadata": {"labels": {"fugue.pro/component": component}}, "data": data}
        objects = {state_name: pointer, record_name: immutable}
        provision = Mock()
        provision.get.side_effect = lambda kind, name: objects[name]
        return provision, component, workload, objects, (rollback_revision, rollback_image)

    def test_returns_exact_lkg_for_the_current_workload(self):
        provision, component, workload, _, expected = self.fixture()
        self.assertEqual(checked_rollback(provision, component, workload), expected)

    def test_refuses_different_live_source_and_mutable_or_tampered_record(self):
        provision, component, workload, objects, _ = self.fixture()
        with self.assertRaises(ProvisionError):
            checked_rollback(provision, component, {**workload, "revision": "d" * 40})
        record = next(v for v in objects.values() if "immutable" in v)
        record["immutable"] = False
        with self.assertRaises(ProvisionError):
            checked_rollback(provision, component, workload)
        record["immutable"] = True
        record["data"]["lkg.json"] += " "
        with self.assertRaises(ProvisionError):
            checked_rollback(provision, component, workload)

    def test_refuses_pointer_that_moved_during_admission(self):
        provision, component, workload, objects, _ = self.fixture()
        pointer, record = list(objects.values())
        changed = copy.deepcopy(pointer)
        changed["metadata"]["resourceVersion"] = "18"
        provision.get.side_effect = [pointer, record, changed]
        with self.assertRaises(ProvisionError):
            checked_rollback(provision, component, workload)


if __name__ == "__main__":
    unittest.main()
