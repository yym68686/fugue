import copy
import unittest
from contextlib import ExitStack
from unittest.mock import patch

from scripts import replace_front_executor as replacement


def fixture():
    profile = {"schema": "fugue.front-observation/v1", "namespace": "test-system", "node": "node-a", "group": "group-a", "legacySelector": {"executor": "old"}, "candidateSelector": {"executor": "new"},
               "candidateSource": "a" * 40, "activationPath": "/state/activation.json", "probes": [{"host": "first.example.test", "path": "/"}, {"host": "second.example.test", "path": "/api"}], "samples": 3, "intervalSeconds": 10}
    config = {"schema": "fugue.front-executor-replacement/v1", "service": "public-front", "serviceUID": "service-one", "address": "8.8.8.8", "expectedGeneration": 2, "generation": 3, "rollbackGeneration": 4, "verificationSeconds": 20, "observation": profile}
    previous = {"metadata": {"name": "public-front", "namespace": "test-system", "uid": "service-one", "resourceVersion": "10", "labels": {"app.kubernetes.io/managed-by": replacement.stage.MANAGER}, "annotations": {replacement.stage.GENERATION: "2", "transport.fugue.dev/phase": "serving"}},
                "spec": {"type": "ClusterIP", "clusterIP": "10.1.0.1", "selector": profile["legacySelector"], "externalIPs": ["8.8.8.8"], "externalTrafficPolicy": "Local", "ports": [{"port": 80, "targetPort": 80, "protocol": "TCP"}, {"port": 443, "targetPort": 443, "protocol": "TCP"}]}}
    previous["metadata"]["annotations"][replacement.stage.DIGEST] = replacement.transport.digest(replacement.projection(previous["spec"]))
    evidence = {"schema": "fugue.front-executor-replacement-evidence/v1", "declaration_digest": replacement.transport.digest(config), "at": replacement.front.now().isoformat(), "baseline": previous,
                "old_uid": "old", "candidate_uid": "new", "observations": {"observations": [{"activation": {"generation": 10}}]}}
    evidence["digest"] = replacement.transport.digest(evidence)
    return config, previous, evidence


class ReplacementTests(unittest.TestCase):
    def test_declaration_and_baseline_are_exact(self):
        config, previous, _ = fixture()
        replacement.validate(config)
        replacement.baseline(config, previous)
        for field, value in [("generation", 2), ("rollbackGeneration", 3), ("verificationSeconds", 0), ("address", "127.0.0.1"), ("serviceUID", "")]:
            with self.subTest(field=field), self.assertRaises(ValueError):
                replacement.validate(dict(config, **{field: value}))
        for change in ["uid", "owner", "generation", "digest", "phase", "selector", "address", "ports"]:
            changed = copy.deepcopy(previous)
            if change == "uid": changed["metadata"]["uid"] = "new-service"
            if change == "owner": changed["metadata"]["labels"]["app.kubernetes.io/managed-by"] = "foreign"
            if change == "generation": changed["metadata"]["annotations"][replacement.stage.GENERATION] = "3"
            if change == "digest": changed["metadata"]["annotations"][replacement.stage.DIGEST] = "unknown"
            if change == "phase": changed["metadata"]["annotations"]["transport.fugue.dev/phase"] = "staged"
            if change == "selector": changed["spec"]["selector"] = {"executor": "other"}
            if change == "address": changed["spec"]["externalIPs"] = ["9.9.9.9"]
            if change == "ports": changed["spec"]["ports"][0]["targetPort"] = 8080
            with self.subTest(change=change), self.assertRaises(ValueError):
                replacement.baseline(config, changed)

    def test_cas_cannot_modify_listener_or_cluster_identity(self):
        config, previous, _ = fixture()
        spec, annotations = replacement.selected(config, previous)
        operations = replacement.cas_patch(previous, spec, annotations)
        self.assertEqual([item["path"] for item in operations if item["op"] != "test"], ["/spec/selector", "/metadata/annotations"])
        self.assertEqual(len([item for item in operations if item["op"] == "test"]), 5)
        for key, value in [("clusterIP", "10.1.0.2"), ("externalIPs", []), ("ports", [])]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                replacement.cas_patch(previous, dict(spec, **{key: value}), annotations)

    def test_invalid_evidence_is_not_accepted(self):
        config, _, evidence = fixture()
        replacement.validate_evidence(config, evidence)
        for key, value in [("candidate_uid", "other"), ("declaration_digest", "other"), ("at", "2020-01-01T00:00:00+00:00")]:
            changed = copy.deepcopy(evidence)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                replacement.validate_evidence(config, changed)

    def test_handoff_preserves_connections_and_fences_recovery(self):
        for failure in [None, "held_socket", "candidate_socket", "lost_write_response", "new_writer", "changed_before_write"]:
            with self.subTest(failure=failure), ExitStack() as stack:
                config, previous, evidence = fixture()
                live = [copy.deepcopy(previous)]
                writes, sockets = [], []
                clock = [0.0]
                old = {"metadata": {"uid": "old", "name": "old"}, "status": {"podIP": "10.0.0.1"}}
                candidate = {"metadata": {"uid": "new", "name": "new"}, "status": {"podIP": "10.0.0.2"}}
                proof = {"edge": "node-a", "group": "group-a", "digest": "same"}

                class Socket:
                    def __init__(self, *_):
                        self.index, self.closed = len(sockets), False
                        sockets.append(self)

                    def proof(self, _):
                        if failure == "held_socket" and self.index == 0 and writes:
                            raise OSError("held connection lost")
                        if failure == "new_writer" and writes:
                            live[0]["metadata"]["annotations"][replacement.stage.GENERATION] = "99"
                            raise OSError("verification failed with concurrent authority")
                        return proof

                    def close(self):
                        self.closed = True

                def write(_, observed, spec, annotations, dry_run=False):
                    if observed != live[0]:
                        raise ValueError("stale CAS")
                    live[0] = copy.deepcopy(observed)
                    live[0]["spec"], live[0]["metadata"]["annotations"] = copy.deepcopy(spec), copy.deepcopy(annotations)
                    writes.append(annotations[replacement.stage.GENERATION])
                    if failure == "lost_write_response" and len(writes) == 1:
                        raise OSError("write response lost")
                    return copy.deepcopy(live[0])

                def fact(_, pod, socket):
                    if failure == "candidate_socket" and pod is candidate:
                        raise ValueError("wrong Front received new socket")
                    return {"pod_uid": pod["metadata"]["uid"], "connection_id": str(socket.index)}

                def sleep(duration):
                    clock[0] += duration

                if failure == "changed_before_write":
                    live[0]["metadata"]["resourceVersion"] = "11"
                for target, name, value in [(replacement, "current", lambda _: copy.deepcopy(live[0])), (replacement, "executors", lambda _: (old, candidate)), (replacement, "write", write),
                                            (replacement.front, "state", lambda *_: {"generation": 10}), (replacement.front, "proof", lambda *_: proof), (replacement.front, "pod", lambda *_: old),
                                            (replacement.handoff, "redirect", lambda *_: None), (replacement.stage, "selected_endpoint_witness", lambda *_: {}),
                                            (replacement.connection, "HeldTLS", Socket), (replacement.connection, "fact", fact), (replacement.time, "monotonic", lambda: clock[0]), (replacement.time, "sleep", sleep)]:
                    stack.enter_context(patch.object(target, name, value))
                if failure:
                    with self.assertRaises((ValueError, OSError)):
                        replacement.apply(config, evidence)
                    expected = [] if failure == "changed_before_write" else ["3"] if failure == "new_writer" else ["3", "4"]
                    self.assertEqual(writes, expected)
                    if len(writes) == 2:
                        self.assertEqual(live[0]["spec"], previous["spec"])
                else:
                    result = replacement.apply(config, evidence)
                    self.assertEqual(result["result"], "verified")
                    self.assertEqual(writes, ["3"])
                    self.assertGreaterEqual(len(result["observations"]), 3)
                    self.assertEqual(len({item["old_connection"]["connection_id"] for item in result["observations"]}), 1)
                self.assertTrue(all(socket.closed for socket in sockets))

    def test_recovery_is_idempotent_and_never_overwrites_new_authority(self):
        config, previous, evidence = fixture()
        for state in ["unchanged", "compensated", "different"]:
            value = copy.deepcopy(previous)
            if state == "compensated":
                value["spec"], value["metadata"]["annotations"] = replacement.selected(config, previous, rollback=True)
            if state == "different":
                value["metadata"]["uid"] = "replaced-service"
            with self.subTest(state=state), patch.object(replacement, "current", return_value=value), patch.object(replacement, "write") as write:
                if state == "different":
                    with self.assertRaises(ValueError): replacement.recover(config, evidence)
                else:
                    self.assertIn(replacement.recover(config, evidence)["result"], ["unchanged", "already_compensated"])
                write.assert_not_called()


if __name__ == "__main__":
    unittest.main()
