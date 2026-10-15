import copy
import unittest
from scripts.install_local_node_resource_policy import HOST_INSTALL, verified_executor


class HostPolicyExecutorTests(unittest.TestCase):
    def test_exact_ready_privileged_executor_required(self):
        config = {"local_node": "node-fixture", "local_executor": {"container": "executor", "daemonset": "host-executor", "image": "registry.example/executor@sha256:" + "a" * 64}}
        pod = {"metadata": {"name": "executor-fixture", "ownerReferences": [{"kind": "DaemonSet", "name": "host-executor", "controller": True}]},
               "spec": {"nodeName": "node-fixture", "hostPID": True, "volumes": [{"name": "root", "hostPath": {"path": "/"}}],
                        "containers": [{"name": "executor", "securityContext": {"privileged": True, "runAsUser": 0}, "volumeMounts": [{"name": "root", "mountPath": "/host"}]}]},
               "status": {"containerStatuses": [{"name": "executor", "ready": True, "imageID": config["local_executor"]["image"]}]}}
        self.assertEqual(verified_executor(config, {"items": [pod]}), "executor-fixture")
        for field in ("node", "image", "readonly", "privilege", "ready", "owner"):
            changed = copy.deepcopy(pod)
            if field == "node": changed["spec"]["nodeName"] = "different"
            if field == "image": changed["status"]["containerStatuses"][0]["imageID"] = "different"
            if field == "readonly": changed["spec"]["containers"][0]["volumeMounts"][0]["readOnly"] = True
            if field == "privilege": changed["spec"]["containers"][0]["securityContext"]["privileged"] = False
            if field == "ready": changed["status"]["containerStatuses"][0]["ready"] = False
            if field == "owner": changed["metadata"]["ownerReferences"] = []
            with self.assertRaises(ValueError, msg=field):
                verified_executor(config, {"items": [changed]})
        compile(HOST_INSTALL, "host-installer", "exec")
