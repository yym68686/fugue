import datetime
import json
from pathlib import Path
import unittest

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
