import copy
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import release_external_controller as release


def fixture():
    old = {"image": "registry.example/operator@sha256:" + "a" * 64, "specImage": "registry.example/operator:1", "revision": "a" * 9, "command": ["/manager"]}
    new = {"image": "registry.example/controller@sha256:" + "b" * 64, "revision": "b" * 40, "command": ["/controller"]}
    config = {"apiVersion": "release.fugue.dev/v1", "kind": "ExternalControllerRelease", "namespace": "system-test", "deployment": "operator", "container": "manager",
              "uid": "11111111-1111-1111-1111-111111111111", "predecessor": old, "candidate": new, "operandGuard": "cnpg-instance-identity-v1", "configurationGuards": [], "apply": False,
              "candidateReceipt": {"base_image": old["image"], "candidate_image": new["image"], "source_revision": new["revision"], "controller_command_verified": True, "production_deployed": False,
                                   "instance_manager_sha256": {"manager_amd64": "c" * 64, "manager_arm64": "d" * 64}}}
    deployment = {"metadata": {"uid": config["uid"], "resourceVersion": "42", "generation": 1}, "spec": {"replicas": 2, "strategy": {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": "25%"}},
                  "selector": {"matchLabels": {"component": "operator"}}, "template": {"spec": {"containers": [{"name": "manager", "image": old["specImage"], "command": old["command"], "env": [{"name": "OPERATOR_IMAGE_NAME", "value": old["specImage"]}]}]}}},
                  "status": {"observedGeneration": 1, "readyReplicas": 2, "availableReplicas": 2, "updatedReplicas": 2, "replicas": 2}}
    config["preservedSpecSHA256"] = release.preserved_spec(config, deployment)
    return config, deployment


class ExternalControllerReleaseTests(unittest.TestCase):
    def test_contract_binds_candidate_receipt_and_explicit_preflight(self):
        config, _ = fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "intent.json"
            path.write_text(json.dumps(config))
            self.assertEqual(release.load_config(path), config)
            for key, value in [("candidate_image", config["predecessor"]["image"]), ("controller_command_verified", False), ("instance_manager_sha256", {})]:
                altered = copy.deepcopy(config)
                altered["candidateReceipt"][key] = value
                path.write_text(json.dumps(altered))
                with self.subTest(key=key), self.assertRaises(ValueError):
                    release.load_config(path)
        with self.assertRaisesRegex(ValueError, "preflight only"):
            release.execute(config, {})

    def test_preserved_spec_and_patch_allow_only_two_code_fields(self):
        config, deployment = fixture()
        original = copy.deepcopy(deployment)
        release.validate_workload(config, deployment, config["predecessor"])
        operations = release.code_patch(config, deployment, config["candidate"])
        self.assertEqual([p["path"] for p in operations if p["op"] == "replace"], ["/spec/template/spec/containers/0/image", "/spec/template/spec/containers/0/command"])
        self.assertEqual(operations[:2], [{"op": "test", "path": "/metadata/uid", "value": config["uid"]}, {"op": "test", "path": "/metadata/resourceVersion", "value": "42"}])
        self.assertEqual(deployment, original)
        for field, value in [("env", []), ("args", ["other"]), ("resources", {"limits": {"cpu": "9"}})]:
            altered = copy.deepcopy(deployment)
            altered["spec"]["template"]["spec"]["containers"][0][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "configuration drifted"):
                    release.validate_workload(config, altered, config["predecessor"])

    def test_subsequent_release_requires_verified_predecessor_and_same_operands(self):
        config, _ = fixture()
        previous = copy.deepcopy(config["candidateReceipt"])
        config["predecessor"] = dict(config["candidate"], specImage=config["candidate"]["image"])
        config["candidate"].update(image="registry.example/controller@sha256:" + "e" * 64, revision="e" * 40)
        config["candidateReceipt"].update(candidate_image=config["candidate"]["image"], source_revision=config["candidate"]["revision"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "intent.json"
            path.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "predecessor receipt"):
                release.load_config(path)
            config["predecessorReceipt"] = previous
            path.write_text(json.dumps(config))
            self.assertEqual(release.load_config(path), config)
            for key, value in [("candidate_image", config["candidate"]["image"]), ("source_revision", "f" * 40), ("base_image", config["candidate"]["image"]), ("instance_manager_sha256", {"manager_amd64": "f" * 64, "manager_arm64": "d" * 64}), ("controller_command_verified", False)]:
                altered = copy.deepcopy(config)
                altered["predecessorReceipt"][key] = value
                path.write_text(json.dumps(altered))
                with self.subTest(key=key), self.assertRaisesRegex(ValueError, "predecessor receipt"):
                    release.load_config(path)

    def test_database_guard_detects_replacement_primary_and_health(self):
        clusters = {"items": [{"metadata": {"name": "db", "namespace": "sample", "uid": "cluster-id", "generation": 2}, "spec": {"instances": 1}, "status": {"currentPrimary": "db-1", "targetPrimary": "db-1", "readyInstances": 1}}]}
        pods = {"items": [{"metadata": {"name": "db-1", "namespace": "sample", "uid": "pod-id"}, "spec": {"nodeName": "node-a"}, "status": {"conditions": [{"type": "Ready", "status": "True"}], "containerStatuses": [{"name": "db", "containerID": "original", "imageID": "pinned", "restartCount": 0}]}}]}
        before = release.operand_snapshot(clusters, pods)
        release.assert_operands_preserved(before, release.operand_snapshot(clusters, pods))
        for kind, field, value in [("clusters", "primary", "db-2"), ("clusters", "ready", 0), ("clusters", "spec", "changed"), ("pods", "ready", False), ("pods", "containers", [])]:
            after = copy.deepcopy(before)
            next(iter(after[kind].values()))[field] = value
            with self.subTest(kind=kind, field=field), self.assertRaises(ValueError):
                release.assert_operands_preserved(before, after)
        after = copy.deepcopy(before)
        after["pods"] = {"replacement": next(iter(after["pods"].values()))}
        with self.assertRaises(ValueError):
            release.assert_operands_preserved(before, after)

    def test_failed_candidate_rolls_back_only_its_exact_code(self):
        config, deployment = fixture()
        config["apply"] = True
        after = copy.deepcopy(deployment)
        after["metadata"]["resourceVersion"] = "43"
        after["spec"]["template"]["spec"]["containers"][0].update(image=config["candidate"]["image"], command=config["candidate"]["command"])
        prepared = {"schema": "fugue.external-controller.prepared.v1", "configDigest": release.digest(config), "config": config, "preparedAt": time.time(), "operands": {"clusters": {}, "pods": {}},
                    "observedResourceVersion": "42", "currentArtifact": config["predecessor"], "candidateVerification": {}, "predecessorVerification": {}}
        with patch.object(release, "prepare", return_value=prepared), patch.object(release, "observe_operands", return_value=prepared["operands"]), patch.object(release, "observe_deployment", side_effect=[deployment, after]), patch.object(release, "patch") as write, patch.object(release, "wait_healthy", side_effect=[RuntimeError("failed health"), None]):
            with self.assertRaisesRegex(RuntimeError, "predecessor restored"):
                release.execute(config, prepared)
            self.assertEqual([call.args[2] for call in write.call_args_list], [config["candidate"], config["predecessor"]])
        after["spec"]["replicas"] = 3
        with patch.object(release, "prepare", return_value=prepared), patch.object(release, "observe_operands", return_value=prepared["operands"]), patch.object(release, "observe_deployment", side_effect=[deployment, after]), patch.object(release, "patch") as write, patch.object(release, "wait_healthy", side_effect=RuntimeError("failed health")):
            with self.assertRaisesRegex(ValueError, "configuration drifted"):
                release.execute(config, prepared)
            self.assertEqual(write.call_count, 1)


if __name__ == "__main__":
    unittest.main()
