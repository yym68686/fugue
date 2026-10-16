import datetime
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts.publish_node_resource_policy import API, TASK, publish


class PublishNodePolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = json.loads(Path("deploy/environments/production/node-resource-policy/policy.json").read_text())
        self.now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    def test_only_explicit_fresh_capable_targets_receive_policy(self):
        writes = []
        def api(method, path, body=None):
            if path == "/v1/node-updaters":
                return {"node_updaters": [{"id": "up-" + name, "cluster_node_name": name, "status": "active", "last_seen_at": self.now, "capabilities": [TASK]} for name in ("selected", "unselected")]}
            if method == "POST":
                writes.append(body)
                return {"task": {"id": "task-1"}}
            return {"tasks": [{"id": "task-1", "status": "completed"}]}
        result = publish(api, ["selected"], self.policy, apply=True)
        self.assertEqual(list(result), ["selected"])
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]["node_updater_id"], "up-selected")
        self.assertEqual(writes[0]["payload"]["dry_run"], "false")

    def test_missing_capability_or_stale_node_is_not_success(self):
        for stamp, capabilities in ((self.now, []), ("2000-01-01T00:00:00Z", [TASK])):
            def api(method, path, body=None):
                self.assertEqual(method, "GET")
                return {"node_updaters": [{"id": "up", "cluster_node_name": "selected", "status": "active", "last_seen_at": stamp, "capabilities": capabilities}]}
            with self.assertRaisesRegex(RuntimeError, "unverified"):
                publish(api, ["selected"], self.policy)

    def test_https_origin_required(self):
        for origin in ("http://example.test", "https://user:password@example.test", "https://example.test?key=secret"):
            with self.assertRaises(ValueError):
                API(origin, "fixture-token")

    def upgrade_api(self, current="v46", served="v47", capabilities=None):
        writes, polls = [], []
        def api(method, path, body=None):
            if path == "/v1/node-updaters":
                polls.append(True)
                version = served if len(polls) > 1 else current
                return {"node_updaters": [{"id": "up", "cluster_node_name": "selected", "status": "active", "last_seen_at": self.now, "updater_version": version, "capabilities": [TASK] if capabilities is None or len(polls) > 1 else capabilities}]}
            if path == "/install/node-updater.sh":
                return 'FUGUE_NODE_UPDATER_SCRIPT_VERSION="%s"\n%s' % (served, TASK)
            if method == "POST":
                writes.append(body)
                return {"task": {"id": "task-1"}}
            return {"tasks": [{"id": "task-1", "status": "completed"}]}
        return api, writes

    @patch("scripts.publish_node_resource_policy.time.sleep")
    def test_capable_old_updater_requires_fresh_version_observation(self, sleep):
        api, writes = self.upgrade_api()
        result = publish(api, ["selected"], self.policy, apply=True, wait_seconds=30, upgrade_updaters=True, minimum_updater_version="v47")
        self.assertEqual(list(result), ["selected"])
        self.assertEqual([w["type"] for w in writes].count("upgrade-node-updater"), 1)
        self.assertEqual([w["type"] for w in writes].count(TASK), 1)
        self.assertEqual(writes[1]["payload"]["target_version"], "v47")
        sleep.assert_called_once()

    def test_unavailable_new_code_does_not_block_configuration_write(self):
        api, writes = self.upgrade_api(served="v46")
        with self.assertRaisesRegex(RuntimeError, "unverified"):
            publish(api, ["selected"], self.policy, apply=True, upgrade_updaters=True, minimum_updater_version="v47")
        self.assertEqual([w["type"] for w in writes], [TASK])
        self.assertEqual(writes[0]["payload"]["dry_run"], "false")

    def test_policy_recovery_can_use_existing_executor(self):
        api, writes = self.upgrade_api()
        publish(api, ["selected"], self.policy, apply=True, minimum_updater_version="v47")
        self.assertEqual([w["type"] for w in writes], [TASK])

    def test_script_download_failure_preserves_policy_publication(self):
        base_api, writes = self.upgrade_api()
        def api(method, path, body=None):
            if path == "/install/node-updater.sh":
                raise RuntimeError("script unavailable")
            return base_api(method, path, body)
        with self.assertRaisesRegex(RuntimeError, "script unavailable"):
            publish(api, ["selected"], self.policy, apply=True, upgrade_updaters=True, minimum_updater_version="v47")
        self.assertEqual([w["type"] for w in writes], [TASK])

    def test_newer_version_is_not_downgraded(self):
        api, writes = self.upgrade_api(current="v48")
        publish(api, ["selected"], self.policy, upgrade_updaters=True, minimum_updater_version="v47")
        self.assertEqual([w["type"] for w in writes], [TASK])

    @patch("scripts.publish_node_resource_policy.time.sleep")
    def test_missing_capability_upgrades_before_policy(self, sleep):
        api, writes = self.upgrade_api(capabilities=[])
        publish(api, ["selected"], self.policy, wait_seconds=30, upgrade_updaters=True, minimum_updater_version="v47")
        self.assertEqual([w["type"] for w in writes], ["upgrade-node-updater", TASK])

    def test_invalid_minimum_rejected_before_any_api_call(self):
        def api(*args):
            self.fail("invalid intent must not access API")
        for version in ("v0", "v047", "latest", 47, "v47\n"):
            with self.assertRaisesRegex(ValueError, "minimum_updater_version"):
                publish(api, ["selected"], self.policy, minimum_updater_version=version)
