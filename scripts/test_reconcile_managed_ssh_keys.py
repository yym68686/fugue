import base64
import copy
import json
import unittest

from scripts import reconcile_managed_ssh_keys as keys


class ManagedSSHKeysTest(unittest.TestCase):
    def fixture(self):
        material = {purpose: {"schema": schema, "active_key_id": purpose + "-v1", "keys": {purpose + "-v1": base64.b64encode(bytes([index+1])*32).decode()}} for index, (purpose, schema) in enumerate(keys.SCHEMAS.items())}
        config = {"apiVersion": "identity.fugue.dev/v1", "kind": "ManagedSSHKeyrings", "namespace": "test-system", "generation": 1, "expectedPreviousGeneration": 0, "restoreMissing": False}
        for purpose in keys.SCHEMAS:
            config[purpose] = {"secretName": "ssh-" + purpose, "contentDigest": keys.content_digest(material[purpose]), "expectedPreviousDigest": ""}
        state, writes = {}, []

        def kube(args, value=None):
            if args[0] == "get":
                return json.dumps(state[args[2]]) if args[2] in state else ""
            if "--dry-run=server" not in args:
                desired = copy.deepcopy(value)
                desired["metadata"].update(uid="uid-"+desired["metadata"]["name"], resourceVersion=str(len(writes)+1))
                state[desired["metadata"]["name"]] = desired
                writes.append(args[0])
            return ""
        return material, config, state, writes, kube

    def test_replay_preserves_independent_material(self):
        material, config, state, writes, kube = self.fixture()
        self.assertEqual(keys.reconcile(config, material, kube)["changed_resources"], 2)
        self.assertEqual(keys.reconcile(config, material, kube)["changed_resources"], 0)
        self.assertEqual(writes, ["create", "create"])

    def test_rotation_preserves_old_keys_and_recovers_partial_update(self):
        material, config, state, writes, kube = self.fixture()
        keys.reconcile(config, material, kube)
        updated = copy.deepcopy(config)
        updated.update(generation=2, expectedPreviousGeneration=1)
        for index, purpose in enumerate(keys.SCHEMAS):
            material[purpose]["keys"][purpose+"-v2"] = base64.b64encode(bytes([index+4])*32).decode()
            material[purpose]["active_key_id"] = purpose+"-v2"
            updated[purpose].update(contentDigest=keys.content_digest(material[purpose]), expectedPreviousDigest=config[purpose]["contentDigest"])
        attempted = []
        def interrupted(args, value=None):
            if args[0] == "replace" and "--dry-run=server" not in args:
                attempted.append(1)
                if len(attempted) == 2:
                    raise RuntimeError("synthetic interrupted publication")
            return kube(args, value)
        with self.assertRaises(RuntimeError):
            keys.reconcile(updated, material, interrupted)
        self.assertEqual(keys.reconcile(updated, material, kube)["changed_resources"], 1)
        self.assertEqual(keys.reconcile(updated, material, kube)["changed_resources"], 0)

    def test_missing_or_corrupt_prior_keys_are_not_recreated(self):
        material, config, state, writes, kube = self.fixture()
        keys.reconcile(config, material, kube)
        config.update(generation=2, expectedPreviousGeneration=1)
        for purpose in keys.SCHEMAS:
            config[purpose]["expectedPreviousDigest"] = config[purpose]["contentDigest"]
        del state["ssh-identity"]
        with self.assertRaises(ValueError):
            keys.reconcile(config, material, kube)
        self.assertEqual(len(writes), 2)

    def test_reviewed_recovery_restores_exact_backup_without_new_material(self):
        material, config, state, writes, kube = self.fixture()
        keys.reconcile(config, material, kube)
        previous_data = copy.deepcopy(state["ssh-identity"]["data"])
        config.update(generation=2, expectedPreviousGeneration=1, restoreMissing=True)
        for purpose in keys.SCHEMAS:
            config[purpose]["expectedPreviousDigest"] = config[purpose]["contentDigest"]
        del state["ssh-identity"]
        keys.reconcile(config, material, kube)
        self.assertEqual(state["ssh-identity"]["data"], previous_data)
        config["identity"]["contentDigest"] = "sha256:"+"a"*64
        with self.assertRaises(ValueError):
            keys.validate(config)

    def test_foreign_resource_and_key_reuse_fail_before_writes(self):
        material, config, state, writes, kube = self.fixture()
        material["signing"]["keys"]["signing-v1"] = material["identity"]["keys"]["identity-v1"]
        config["signing"]["contentDigest"] = keys.content_digest(material["signing"])
        with self.assertRaises(ValueError):
            keys.reconcile(config, material, kube)
        self.assertEqual(writes, [])


if __name__ == "__main__":
    unittest.main()
