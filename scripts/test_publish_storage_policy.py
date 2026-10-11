import copy
import unittest
from scripts.publish_storage_policy import manifest


class StoragePolicyTest(unittest.TestCase):
    def test_bounds_and_config_only_resource(self):
        base = {"namespace": "control", "name": "storage-policy", "policy": {"version": 1, "compiler_input_chunks": False, "checkpoint_pages": False, "database_max_open": 0, "database_max_idle": 16, "database_idle_seconds": 300}}
        self.assertEqual(manifest(base)["kind"], "ConfigMap")
        for key, value in (("database_max_open", -1), ("database_max_open", 257), ("database_max_open", 2), ("compiler_input_chunks", "true"), ("version", 2)):
            changed = copy.deepcopy(base)
            changed["policy"][key] = value
            with self.assertRaises(ValueError):
                manifest(changed)
