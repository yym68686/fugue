import copy
import unittest
from unittest.mock import patch

from scripts import stage_front_serving_transport as stage


def config():
    return {"schema": "fugue.front-serving-stage/v1", "generation": 1, "namespace": "test-system", "services": [{"name": "front-a", "observation": "deploy/environments/production/front-observation/test.json", "probeListener": "front-probe-a"}]}


def profile():
    return {"node": "node-a", "namespace": "test-system", "candidateSelector": {"slot": "candidate"}}


def service():
    value = config()
    target = stage.desired(value, value["services"][0], profile())
    target["metadata"].update(uid="service-one", resourceVersion="7")
    return target


class InternalFrontStageTests(unittest.TestCase):
    def test_stage_has_no_public_network_address_or_nodeport(self):
        value = config()
        stage.validate(value)
        target = stage.desired(value, value["services"][0], profile())
        self.assertEqual(target["spec"]["type"], "ClusterIP")
        self.assertEqual(target["spec"]["externalIPs"], [])
        self.assertNotIn("externalTrafficPolicy", target["spec"])
        self.assertEqual([p["port"] for p in target["spec"]["ports"]], [80, 443])
        self.assertTrue(all("nodePort" not in p for p in target["spec"]["ports"]))
        for key in ["address", "externalIPs", "port", "phase"]:
            changed = copy.deepcopy(value)
            changed["services"][0][key] = "injected"
            with self.subTest(key=key), self.assertRaises(ValueError): stage.validate(changed)

    def test_public_drifted_or_foreign_service_cannot_be_staged(self):
        target = service()
        stage.validate_existing(target, target)
        for bad in ["public", "nodeport", "phase", "owner", "drift", "foreign", "replay", "uid"]:
            changed = copy.deepcopy(target)
            if bad == "public": changed["spec"]["externalIPs"] = ["8.8.8.8"]
            if bad == "nodeport": changed["spec"]["type"] = "NodePort"
            if bad == "phase": changed["metadata"]["annotations"]["transport.fugue.dev/phase"] = "serving"
            if bad == "owner": changed["metadata"]["labels"]["app.kubernetes.io/managed-by"] = "foreign"
            if bad == "drift": changed["spec"]["selector"] = {"slot": "old"}
            if bad == "foreign": changed["metadata"]["managedFields"] = [{"manager": "other", "fieldsV1": {"f:spec": {"f:selector": {}}}}]
            if bad == "replay": changed["metadata"]["annotations"][stage.GENERATION] = "2"
            if bad == "uid": changed["metadata"].pop("uid")
            with self.subTest(bad=bad), self.assertRaises(ValueError): stage.validate_existing(changed, target)

    def test_endpoint_gate_requires_exact_local_ready_pod_and_service_owner(self):
        value, p, current = config(), profile(), service()
        candidate = {"metadata": {"uid": "pod-one", "name": "candidate"}, "status": {"podIP": "10.0.0.2"}}
        base = {"items": [{"metadata": {"ownerReferences": [{"kind": "Service", "uid": "service-one", "controller": True}]}, "ports": [{"name": "http", "protocol": "TCP", "port": 80}, {"name": "https", "protocol": "TCP", "port": 443}], "endpoints": [{"nodeName": "node-a", "addresses": ["10.0.0.2"], "conditions": {"ready": True}, "targetRef": {"kind": "Pod", "uid": "pod-one", "name": "candidate", "namespace": "test-system"}}]}]}
        for bad in [None, "owner", "pod", "node", "ready", "terminating", "ports", "duplicate", "remote extra", "foreign service", "truncated"]:
            observed = copy.deepcopy(base)
            item = observed["items"][0]
            endpoint = item["endpoints"][0]
            if bad == "owner": item["metadata"]["ownerReferences"][0]["uid"] = "foreign"
            if bad == "pod": endpoint["targetRef"]["uid"] = "recreated"
            if bad == "node": endpoint["nodeName"] = "other"
            if bad == "ready": endpoint["conditions"]["ready"] = False
            if bad == "terminating": endpoint["conditions"]["terminating"] = True
            if bad == "ports": item["ports"][1]["port"] = 8443
            if bad == "duplicate": item["endpoints"].append(copy.deepcopy(endpoint))
            if bad == "remote extra": item["endpoints"].append(dict(copy.deepcopy(endpoint), nodeName="node-b"))
            selected_current = copy.deepcopy(current)
            if bad == "foreign service": selected_current["metadata"]["name"] = "foreign"
            if bad == "truncated": observed["metadata"] = {"continue": "next"}
            with self.subTest(bad=bad), patch.object(stage.probe, "kubectl", return_value=selected_current), patch.object(stage.front, "read", return_value=observed):
                if bad:
                    with self.assertRaises(ValueError): stage.endpoint_witness(value, value["services"][0], p, candidate)
                else:
                    self.assertEqual(stage.endpoint_witness(value, value["services"][0], p, candidate)["pod_uid"], "pod-one")


if __name__ == "__main__":
    unittest.main()
