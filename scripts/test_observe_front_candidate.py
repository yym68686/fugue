import copy
import json
import unittest
from unittest.mock import patch

from scripts import observe_front_candidate as front


def fixture():
    return {"schema": "fugue.front-observation/v1", "namespace": "test-system", "node": "node-a", "group": "cell-a", "legacySelector": {"slot": "old"}, "candidateSelector": {"slot": "new"}, "candidateSource": "a" * 40, "activationPath": "/state/activation.json", "probes": [{"host": "api.example.test", "path": "/"}, {"host": "web.example.test", "path": "/"}], "samples": 3, "intervalSeconds": 10}


class FrontObservationTests(unittest.TestCase):
    def test_declaration_is_explicit_bounded_and_cannot_name_same_front(self):
        config = fixture()
        front.validate(config)
        for key, value in [("samples", 0), ("samples", True), ("intervalSeconds", 600), ("candidateSource", "main"), ("candidateSelector", config["legacySelector"]), ("activationPath", "../state"), ("probes", [{"host": "api.example.test", "path": "/"}]), ("legacySelector", {"slot": "a,b"})]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                front.validate(dict(config, **{key: value}))

    def test_equivalence_does_not_grant_authority_and_rejects_changed_state(self):
        config = fixture()
        for bad in [None, "same pod", "publication", "activation", "wrong edge", "wrong group", "recreated"]:
            with self.subTest(bad=bad):
                calls = []
                def pod(config, selector, candidate):
                    count = len(calls)
                    calls.append(candidate)
                    uid = "new" if candidate and bad != "same pod" else "old"
                    if bad == "recreated" and count > 1 and candidate:
                        uid = "replacement"
                    return {"metadata": {"uid": uid}, "spec": {}, "status": {"podIP": "10.0.0.2" if candidate else "10.0.0.1", "containerStatuses": [{"imageID": "registry.example/front@sha256:" + "b" * 64}]}}
                def state(config, observed):
                    return {"generation": 1 if bad != "activation" or observed["metadata"]["uid"] == "old" else 2}
                def proof(address, *_):
                    return {"edge": "other" if bad == "wrong edge" else "node-a", "group": "other" if bad == "wrong group" else "cell-a", "version": address if bad == "publication" else "same"}
                with patch.object(front, "pod", side_effect=pod), patch.object(front, "state", side_effect=state), patch.object(front, "proof", side_effect=proof), patch.object(front.time, "sleep"):
                    if bad:
                        with self.assertRaises(ValueError):
                            front.observe(config)
                    else:
                        result = front.observe(config)
                        self.assertFalse(result["authorizes_traffic"])
                        self.assertEqual(len(result["observations"]), 3)

    def test_candidate_pod_cannot_claim_health_with_public_ports_or_writable_state(self):
        config = fixture()
        container = {"name": "edge-front", "env": [{"name": "FUGUE_EDGE_FRONT_EDGE_GROUP_ID", "value": "cell-a"}, {"name": "FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE", "value": "/state/activation.json"}, {"name": "FUGUE_EDGE_FRONT_REQUIRE_ACTIVATION_STATE", "value": "true"}], "ports": [{"containerPort": 443}], "volumeMounts": [{"readOnly": True}]}
        base = {"metadata": {"uid": "new", "namespace": "test-system", "annotations": {"fugue.pro/source-commit": "a" * 40}}, "spec": {"nodeName": "node-a", "automountServiceAccountToken": False, "containers": [container]}, "status": {"phase": "Running", "podIP": "10.0.0.2", "conditions": [{"type": "Ready", "status": "True"}], "containerStatuses": [{"name": "edge-front", "ready": True, "restartCount": 0, "imageID": "registry.example/front@sha256:" + "b" * 64}]}}
        for bad in [None, "host port", "host network", "writable", "restarted", "source", "api token"]:
            changed = copy.deepcopy(base)
            if bad == "host port": changed["spec"]["containers"][0]["ports"][0]["hostPort"] = 443
            if bad == "host network": changed["spec"]["hostNetwork"] = True
            if bad == "writable": changed["spec"]["containers"][0]["volumeMounts"][0]["readOnly"] = False
            if bad == "restarted": changed["status"]["containerStatuses"][0]["restartCount"] = 1
            if bad == "source": changed["metadata"]["annotations"]["fugue.pro/source-commit"] = "c" * 40
            if bad == "api token": changed["spec"]["automountServiceAccountToken"] = True
            with self.subTest(bad=bad), patch.object(front, "read", return_value={"items": [changed]}):
                if bad:
                    with self.assertRaises(ValueError): front.pod(config, config["candidateSelector"], True)
                else:
                    self.assertEqual(front.pod(config, config["candidateSelector"], True), base)


if __name__ == "__main__":
    unittest.main()
