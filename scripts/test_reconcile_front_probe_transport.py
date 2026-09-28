import copy
import datetime
import unittest
from unittest.mock import patch

from scripts import reconcile_front_probe_transport as transport


def config():
    return {"schema": "fugue.front-probe-transport/v1", "generation": 1, "namespace": "test-system", "listeners": [{"name": "front-probe-a", "address": "8.8.8.8", "port": 15443, "observation": "deploy/environments/production/front-observation/test-a.json"}]}


def profile():
    return {"namespace": "test-system", "node": "node-a", "candidateSelector": {"slot": "candidate"}, "probes": [{"host": "api.example.test", "path": "/"}]}


class FrontProbeTransportTests(unittest.TestCase):
    def test_only_non_serving_ports_and_explicit_profiles_are_allowed(self):
        value = config()
        transport.validate(value)
        for key, bad in [("port", 443), ("port", 80), ("port", 53), ("port", True), ("address", "127.0.0.1"), ("observation", "../../secret.json")]:
            changed = copy.deepcopy(value)
            changed["listeners"][0][key] = bad
            with self.subTest(key=key, bad=bad), self.assertRaises(ValueError):
                transport.validate(changed)
        changed = copy.deepcopy(value)
        changed["listeners"].append(dict(changed["listeners"][0], name="other"))
        with self.assertRaises(ValueError): transport.validate(changed)

    def test_service_drift_replay_foreign_owner_and_cas(self):
        value = config()
        target = transport.desired(value, value["listeners"][0], profile())
        self.assertEqual(target["spec"]["ports"], [{"name": "https-probe", "protocol": "TCP", "port": 15443, "targetPort": 443}])
        self.assertFalse(target["spec"]["publishNotReadyAddresses"])
        live = copy.deepcopy(target)
        live["metadata"].update(uid="uid-one", resourceVersion="7")
        self.assertEqual(transport.mutation(live, target), (None, None))
        next_target = transport.desired(dict(value, generation=2), value["listeners"][0], profile())
        command, body = transport.mutation(live, next_target)
        self.assertIn("--type=json", command)
        self.assertIn('"path":"/metadata/uid"', body)
        self.assertIn('"path":"/metadata/resourceVersion"', body)
        self.assertIn('"path":"/spec"', body)
        for bad in ["unowned", "drift", "foreign writer", "replay", "same generation mutation", "missing uid"]:
            observed, desired = copy.deepcopy(live), copy.deepcopy(target)
            if bad == "unowned": observed["metadata"]["labels"]["app.kubernetes.io/managed-by"] = "other"
            if bad == "drift": observed["spec"]["selector"] = {"slot": "other"}
            if bad == "foreign writer": observed["metadata"]["managedFields"] = [{"manager": "other", "fieldsV1": {"f:spec": {"f:selector": {}}}}]
            if bad == "replay": desired["metadata"]["annotations"][transport.GENERATION] = "0"
            if bad == "same generation mutation": desired["metadata"]["annotations"][transport.DIGEST] = "different"
            if bad == "missing uid": observed["metadata"].pop("uid")
            with self.subTest(bad=bad), self.assertRaises(ValueError): transport.mutation(observed, desired)

    def test_invalid_witness_and_recreated_front_never_write(self):
        value = config()
        observation = profile()
        plan = {"listener": value["listeners"][0], "profile_digest": transport.digest(observation), "desired": transport.desired(value, value["listeners"][0], observation), "current": None, "evidence": {"observations": [{"old_uid": "old", "candidate_uid": "new", "candidate_image": "image", "activation": {"generation": 1}}]}}
        base = {"schema": "fugue.front-probe-transport-evidence/v1", "declaration_digest": transport.digest(value), "prepared_at": transport.front.now().isoformat(), "plans": [plan]}
        for bad in ["digest", "expired", "profile", "recreated", "service changed"]:
            witness = copy.deepcopy(base)
            if bad == "expired": witness["prepared_at"] = (transport.front.now()-datetime.timedelta(hours=1)).isoformat()
            if bad == "profile": witness["plans"][0]["profile_digest"] = "foreign"
            witness["evidence_digest"] = transport.digest(witness)
            if bad == "digest": witness["evidence_digest"] = "wrong"
            fresh = dict(plan["evidence"]["observations"][0])
            if bad == "recreated": fresh["candidate_uid"] = "replacement"
            with self.subTest(bad=bad), patch.object(transport, "profile", return_value=observation), patch.object(transport, "check_listener_collision"), patch.object(transport.front, "observe", return_value={"observations": [fresh]}), patch.object(transport, "current_service", return_value={} if bad == "service changed" else None), patch.object(transport, "kubectl") as mutation, self.assertRaises(ValueError):
                transport.apply(value, witness)
            mutation.assert_not_called()

    def test_probe_listener_conflicts_are_rejected_before_mutation(self):
        value, observation = config(), profile()
        listener = value["listeners"][0]
        observer = {"metadata": {"namespace": "test-system", "name": "observer"}, "spec": {"hostNetwork": True, "containers": [{"name": "observer"}]}, "status": {"phase": "Running"}}
        for bad in [None, "service", "node port", "host port", "socket", "missing observer", "truncated"]:
            services = {"items": [], "metadata": {}}
            pods = {"items": [copy.deepcopy(observer)], "metadata": {}}
            if bad in ["service", "node port"]:
                services["items"] = [{"metadata": {"namespace": "other", "name": "other"}, "spec": {"externalIPs": [listener["address"]], "ports": [{"port" if bad == "service" else "nodePort": listener["port"], "protocol": "TCP"}]}}]
            if bad == "host port": pods["items"][0]["spec"]["containers"][0]["ports"] = [{"hostPort": listener["port"], "protocol": "TCP"}]
            if bad == "missing observer": pods["items"] = []
            if bad == "truncated": services["metadata"]["continue"] = "next"
            from types import SimpleNamespace
            sockets = "0: 00000000:%04X 00000000:0000 0A rest\n" % (listener["port"] if bad == "socket" else 8080)
            with self.subTest(bad=bad), patch.object(transport.front, "read", side_effect=[services, pods]), patch.object(transport.subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=sockets)):
                if bad:
                    with self.assertRaises(ValueError): transport.check_listener_collision(value, listener, observation)
                else:
                    transport.check_listener_collision(value, listener, observation)


if __name__ == "__main__":
    unittest.main()
