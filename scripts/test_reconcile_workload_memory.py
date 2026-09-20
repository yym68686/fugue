import json
import tempfile
import unittest
from pathlib import Path
from scripts.reconcile_workload_memory import limit_range, load_policy, resize_patch


class WorkloadMemoryPolicyTests(unittest.TestCase):
    def test_memory_default_does_not_impose_a_limit(self):
        policy = limit_range("fg-example", "64Mi")
        self.assertEqual(policy["spec"]["limits"], [{"type": "Container", "defaultRequest": {"memory": "64Mi"}}])

    def test_resize_is_fenced_and_preserves_explicit_resources(self):
        pod = {"metadata": {"resourceVersion": "42"}, "status": {"phase": "Running"}, "spec": {"containers": [
            {"name": "unset"}, {"name": "explicit", "resources": {"requests": {"memory": "16Mi"}}},
            {"name": "bounded", "resources": {"limits": {"memory": "32Mi"}, "requests": {"cpu": "10m"}}}]}}
        patch = resize_patch(pod, "64Mi")
        self.assertEqual(patch["metadata"], {"resourceVersion": "42"})
        self.assertEqual(patch["spec"]["containers"], [
            {"name": "unset", "resources": {"requests": {"memory": "64Mi"}}},
            {"name": "bounded", "resources": {"requests": {"memory": "32Mi"}}}])
        self.assertNotIn("resources", pod["spec"]["containers"][0])
        pod["status"]["phase"] = "Succeeded"
        self.assertIsNone(resize_patch(pod, "64Mi"))

    def test_policy_rejects_unbounded_namespace_selection(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)/"policy.json"
            base = {"apiVersion": "constraints.fugue.dev/v1", "kind": "WorkloadMemoryPolicy", "namespacePrefixes": ["fg-"], "defaultMemoryRequest": "64Mi", "resizeExisting": True, "controlPlaneNamespace": "system", "controllerServiceAccount": "controller"}
            for change in [{"namespacePrefixes": [""]}, {"defaultMemoryRequest": "0Mi"}, {"resizeExisting": "true"}]:
                p.write_text(json.dumps(base | change))
                with self.assertRaises(ValueError): load_policy(p)


if __name__ == "__main__":
    unittest.main()
