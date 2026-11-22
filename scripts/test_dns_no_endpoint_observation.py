import copy
import unittest
from uuid import UUID
from scripts.dns_no_endpoint_observation import stable_observation


def fixture():
    def metadata(name, number):
        return {"name": name, "uid": str(UUID(int=number)), "resourceVersion": "70", "namespace": "fixture"}

    n = "node-x"
    node = {"apiVersion": "v1", "kind": "Node", "metadata": metadata(n, 1),
            "status": {"addresses": [{"address": "192.0.2.12"}], "conditions": [{"type": "Ready", "status": "True"}]}}
    d = {"namespace": "fixture", "name": "listener", "address": "192.0.2.12", "port": 53,
         "targetPort": 9053, "selector": {"role": "dns-a"}}
    svc = {"apiVersion": "v1", "kind": "Service", "metadata": metadata("listener", 2),
           "spec": {"type": "ClusterIP", "selector": d["selector"].copy(), "externalIPs": [d["address"]],
                    "externalTrafficPolicy": "Local", "internalTrafficPolicy": "Local",
                    "ports": [{"protocol": p, "port": 53, "targetPort": 9053} for p in ["UDP", "TCP"]]}}
    pod = {"metadata": dict(metadata("pod-x", 3), labels=d["selector"].copy()), "spec": {"nodeName": n},
           "status": {"phase": "Running", "podIPs": [{"ip": "192.0.2.22"}],
                      "conditions": [{"type": "Ready", "status": "False"}]}}
    sl = {"metadata": dict(metadata("slice", 4), labels={"kubernetes.io/service-name": "listener"},
                           ownerReferences=[{"apiVersion": "v1", "kind": "Service", "name": "listener",
                                             "uid": svc["metadata"]["uid"], "controller": True}]), "addressType": "IPv4",
          "ports": [{"protocol": p, "port": 9053} for p in ["UDP", "TCP"]],
          "endpoints": [{"nodeName": n, "addresses": ["192.0.2.22"], "targetRef": {"kind": "Pod", "name": "pod-x",
                         "namespace": "fixture", "uid": pod["metadata"]["uid"]},
                         "conditions": {"ready": False, "serving": False, "terminating": False}}]}
    return [svc, {"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSliceList", "metadata": {}, "items": [sl]},
            {"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": [pod]}, node, d]


class DiagnosticTest(unittest.TestCase):
    def test_stable_explicitly_false_is_only_diagnostic(self):
        a = fixture()
        self.assertIs(stable_observation(a, copy.deepcopy(a))["authorizes_traffic"], False)

    def test_rejects_ambiguity_at_either_observation(self):
        for edit in [
            lambda s, p, o: s["spec"].update(publishNotReadyAddresses=True),
            lambda s, p, o: s["spec"].update(internalTrafficPolicy="Cluster"),
            lambda s, p, o: s["spec"]["ports"][0].update(port=15353),
            lambda s, p, o: p["metadata"].update(labels={}),
            lambda s, p, o: p["metadata"].update(deletionTimestamp="now"),
            lambda s, p, o: p["status"]["conditions"][0].update(status="Unknown"),
            lambda s, p, o: o["metadata"]["ownerReferences"][0].update(uid=str(UUID(int=9))),
            lambda s, p, o: o["endpoints"][0]["targetRef"].update(uid=str(UUID(int=9))),
            lambda s, p, o: o["endpoints"][0].update(nodeName="other"),
            lambda s, p, o: o["endpoints"][0]["conditions"].pop("ready"),
            lambda s, p, o: o["endpoints"][0]["conditions"].update(serving=True),
            lambda s, p, o: o["endpoints"][0]["conditions"].update(terminating=True),
            lambda s, p, o: o["endpoints"].append(copy.deepcopy(o["endpoints"][0])),
            lambda s, p, o: o["ports"][0].update(port=53),
        ]:
            good, bad = fixture(), fixture()
            edit(bad[0], bad[2]["items"][0], bad[1]["items"][0])
            for pair in ((good, bad), (bad, good)):
                with self.subTest(edit=edit, pair=pair[0] is good), self.assertRaises(ValueError):
                    stable_observation(*pair)

    def test_incomplete_inventory_and_interleaved_writes(self):
        for index in (1, 2):
            for field, value in (("continue", "more"), ("remainingItemCount", 1)):
                a, b = fixture(), fixture()
                b[index]["metadata"][field] = value
                with self.assertRaises(ValueError):
                    stable_observation(a, b)
            a, b = fixture(), fixture()
            b[index]["items"].clear()
            with self.assertRaises(ValueError):
                stable_observation(a, b)
        for index in (0, 1, 2, 3):
            a, b = fixture(), fixture()
            ob = b[index]["items"][0] if index in (1, 2) else b[index]
            ob["metadata"]["resourceVersion"] = "71"
            with self.assertRaises(ValueError):
                stable_observation(a, b)
