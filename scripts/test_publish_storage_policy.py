import copy
import json
import unittest
from scripts.publish_storage_policy import manifest


class StoragePolicyTest(unittest.TestCase):
    def test_block_profile_is_bounded_and_does_not_change_read_format(self):
        base = {"namespace": "control", "name": "storage-policy", "policy": {"version": 1, "compiler_input_chunks": True, "checkpoint_pages": True, "database_max_open": 24, "database_max_idle": 8, "database_idle_seconds": 60}}
        base['compilerInputBlocks'] = {'version': 1, 'minimum_bytes': 256, 'boundary_bits': 4}
        self.assertEqual(json.loads(manifest(base)['data']['compiler-input-blocks.json']), base['compilerInputBlocks'])
        for field, value in [('minimum_bytes', 0), ('minimum_bytes', 65536), ('boundary_bits', 5), ('boundary_bits', True), ('rewrite_history', True)]:
            changed = copy.deepcopy(base)
            changed['compilerInputBlocks'][field] = value
            with self.assertRaises(ValueError):
                manifest(changed)

    def test_artifact_reference_writer_uses_separate_explicit_activation(self):
        base = {"namespace": "control", "name": "storage-policy", "policy": {"version": 1, "compiler_input_chunks": True, "checkpoint_pages": True, "database_max_open": 24, "database_max_idle": 8, "database_idle_seconds": 60}}
        original = manifest(base)['data']['policy.json']
        for enabled in (False, True):
            base['artifactContentReferences'] = {'version': 1, 'enabled': enabled}
            data = manifest(base)['data']
            self.assertEqual(data['policy.json'], original)
            self.assertEqual(json.loads(data['artifact-content-references.json'])['enabled'], enabled)
        base['artifactContentReferences']['delete_history'] = True
        with self.assertRaises(ValueError):
            manifest(base)

    def test_separated_writer_is_independent(self):
        base = {"namespace": "control", "name": "storage-policy", "policy": {"version": 1, "compiler_input_chunks": True, "checkpoint_pages": True, "database_max_open": 24, "database_max_idle": 8, "database_idle_seconds": 60}}
        before = manifest(base)["data"]["policy.json"]
        base["compilerInputSeparated"] = {"version": 1, "enabled": True}
        got = manifest(base)["data"]
        self.assertEqual(before, got["policy.json"])
        self.assertTrue(json.loads(got["compiler-input-separated.json"])["enabled"])
        base["compilerInputSeparated"]["delete_history"] = True
        with self.assertRaises(ValueError):
            manifest(base)
    def test_shared_writer_has_independent_fail_closed_declaration(self):
        base = {"namespace": "control", "name": "storage-policy", "policy": {"version": 1, "compiler_input_chunks": True, "checkpoint_pages": True, "database_max_open": 24, "database_max_idle": 8, "database_idle_seconds": 60}}
        before = manifest(base)["data"]["policy.json"]
        self.assertNotIn("compiler-input-shared.json", manifest(base)["data"])
        for enabled in (False, True):
            base["compilerInputShared"] = {"version": 1, "enabled": enabled}
            got = manifest(base)["data"]
            self.assertEqual(before, got["policy.json"])
            self.assertEqual(enabled, json.loads(got["compiler-input-shared.json"])["enabled"])
        for value in ({"version": 2, "enabled": True}, {"version": 1, "enabled": "true"}, {"version": 1, "enabled": True, "rewrite_history": True}):
            base["compilerInputShared"] = value
            with self.assertRaises(ValueError):
                manifest(base)

    def test_codec_uses_independent_file_for_reader_only_rollback(self):
        base = {"namespace": "control", "name": "storage-policy", "policy": {"version": 1, "compiler_input_chunks": True, "checkpoint_pages": True, "database_max_open": 24, "database_max_idle": 8, "database_idle_seconds": 60}}
        before = manifest(base)["data"]["policy.json"]
        for enabled in (True, False):
            base["compilerInputCodec"] = {"version": 1, "enabled": enabled}
            got = manifest(base)["data"]
            self.assertEqual(got["policy.json"], before)
            self.assertEqual(json.loads(got["compiler-input-codec.json"])["enabled"], enabled)
        for codec in ({"version": 2, "enabled": True}, {"version": True, "enabled": True}, {"version": 1, "enabled": "true"}, {"version": 1, "enabled": True, "tables": ["all"]}):
            base["compilerInputCodec"] = codec
            with self.assertRaises(ValueError):
                manifest(base)

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
