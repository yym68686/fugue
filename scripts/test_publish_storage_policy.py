import copy
import unittest
from scripts.publish_storage_policy import manifest


class StoragePolicyTest(unittest.TestCase):
    def test_retention_cannot_select_business_tables_or_remove_minimum_history(self):
        base = {"namespace": "control", "name": "storage-policy", "policy": {"version": 1, "compiler_input_chunks": False, "checkpoint_pages": False, "database_max_open": 0, "database_max_idle": 16, "database_idle_seconds": 300}}
        original = manifest(base)["data"]["policy.json"]
        base["observationRetention"] = {"version": 1, "enabled": True, "interval_seconds": 30, "batch_rows": 500,
            "heartbeat_days": 7, "heartbeat_rows_per_chain": 10000, "probe_days": 7, "probe_rows_per_table": 1000000}
        self.assertEqual(manifest(base)["data"]["policy.json"], original)
        self.assertIn("observation-retention.json", manifest(base)["data"])
        for key, value in (("tables", ["fugue_apps"]), ("probe_days", 1), ("heartbeat_days", 0), ("batch_rows", 1001), ("enabled", "true")):
            changed = copy.deepcopy(base)
            changed["observationRetention"][key] = value
            with self.assertRaises(ValueError):
                manifest(changed)

    def test_bounds_and_config_only_resource(self):
        base = {"namespace": "control", "name": "storage-policy", "policy": {"version": 1, "compiler_input_chunks": False, "checkpoint_pages": False, "database_max_open": 0, "database_max_idle": 16, "database_idle_seconds": 300}}
        self.assertEqual(manifest(base)["kind"], "ConfigMap")
        for key, value in (("database_max_open", -1), ("database_max_open", 257), ("database_max_open", 2), ("compiler_input_chunks", "true"), ("version", 2)):
            changed = copy.deepcopy(base)
            changed["policy"][key] = value
            with self.assertRaises(ValueError):
                manifest(changed)
