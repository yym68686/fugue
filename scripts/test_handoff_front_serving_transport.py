import copy
import datetime
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from scripts import handoff_front_serving_transport as handoff


def fixture():
    config = {"schema": "fugue.front-serving-handoff/v1", "namespace": "test-system", "service": "front-a", "address": "8.8.8.8", "observation": "deploy/environments/production/front-observation/test.json", "expectedGeneration": 1, "generation": 2, "rollbackGeneration": 3, "verificationSeconds": 15}
    profile = {"namespace": "test-system", "node": "node-a", "group": "cell-a", "legacySelector": {"slot": "old"}, "candidateSelector": {"slot": "new"}, "probes": [{"host": "api.example.test", "path": "/"}]}
    baseline = {"metadata": {"name": "front-a", "namespace": "test-system", "uid": "service-one", "resourceVersion": "1", "labels": {"app.kubernetes.io/managed-by": handoff.stage.MANAGER}, "annotations": {handoff.stage.GENERATION: "1", handoff.stage.DIGEST: "old", "transport.fugue.dev/phase": "staged"}}, "spec": {"type": "ClusterIP", "clusterIP": "10.1.0.2", "selector": {"slot": "new"}, "ports": [{"name": "http", "port": 80, "targetPort": 80, "protocol": "TCP"}, {"name": "https", "port": 443, "targetPort": 443, "protocol": "TCP"}]}}
    selected = handoff.selected_spec(config, baseline)
    annotations = handoff.target_annotations(config, baseline, selected)
    external = {"id": "external-one"}
    endpoint = {"service_uid": "service-one", "pod_uid": "new", "service_version": "1"}
    evidence = {"schema": "fugue.front-public-handoff-evidence/v1", "declaration_digest": handoff.probe.digest(config), "profile_digest": handoff.probe.digest(profile), "external_evidence_digest": handoff.probe.digest(external), "at": handoff.front.now().isoformat(), "baseline": baseline, "candidate_uid": "new", "old_uid": "old", "endpoint": endpoint, "observed": {"observations": [{"activation": {"generation": 1}}]}, "selected_spec": selected, "selected_annotations": annotations}
    evidence["digest"] = handoff.probe.digest(evidence)
    return config, profile, baseline, evidence, external


class PublicFrontHandoffTests(unittest.TestCase):
    def test_external_evidence_requires_exact_current_declaration_and_elapsed_window(self):
        config, profile, _, _, _ = fixture()
        transport = {"schema": "fugue.front-probe-transport/v1", "generation": 1, "namespace": "test-system", "listeners": [{"name": "probe-one", "address": config["address"], "port": 15443, "observation": config["observation"]}]}
        now = handoff.front.now()
        rows = [{"at": (now-datetime.timedelta(seconds=90-i*15)).isoformat(), "listener": "probe-one", "profile_digest": handoff.probe.digest(profile), "address": config["address"], "port": 15443, "proofs": [{"host": "api.example.test", "path": "/", "proof_digest": "sha256:"+"a"*64}]} for i in range(5)]
        evidence = {"schema": "fugue.front-external-probe-evidence/v1", "authorizes_traffic": False, "declaration_digest": handoff.probe.digest(transport), "observations": rows}
        import json
        for bad in [None, "stale", "duplicates", "duration", "profile", "address", "route", "declaration", "claims authority"]:
            changed = copy.deepcopy(evidence)
            if bad == "stale": changed["observations"][0]["at"] = (now-datetime.timedelta(hours=1)).isoformat()
            if bad == "duplicates": changed["observations"][1]["at"] = changed["observations"][0]["at"]
            if bad == "duration":
                for i, row in enumerate(changed["observations"]): row["at"] = (now-datetime.timedelta(seconds=10-i)).isoformat()
            if bad == "profile": changed["observations"][0]["profile_digest"] = "foreign"
            if bad == "address": changed["observations"][0]["address"] = "9.9.9.9"
            if bad == "route": changed["observations"][0]["proofs"][0]["host"] = "foreign.example.test"
            if bad == "declaration": changed["declaration_digest"] = "foreign"
            if bad == "claims authority": changed["authorizes_traffic"] = True
            with self.subTest(bad=bad), patch.object(handoff.Path, "read_text", return_value=json.dumps(transport)):
                if bad:
                    with self.assertRaises(ValueError): handoff.external_evidence(config, profile, changed)
                else:
                    self.assertEqual(handoff.external_evidence(config, profile, changed), transport["listeners"][0])

    def test_public_address_cannot_capture_another_service(self):
        config, _, _, _, _ = fixture()
        old = {"spec": {"containers": [{"ports": [{"hostPort": 80}, {"hostPort": 443}]}]}}
        for bad in [None, "other externalIP", "loadbalancer", "wrong hostIP", "missing hostport", "truncated"]:
            pod = copy.deepcopy(old)
            services = {"items": []}
            if bad in ["other externalIP", "loadbalancer"]:
                service = {"metadata": {"name": "other", "namespace": "foreign"}, "spec": {"ports": [{"port": 443}]}}
                if bad == "other externalIP": service["spec"]["externalIPs"] = [config["address"]]
                else: service["status"] = {"loadBalancer": {"ingress": [{"ip": config["address"]}]}}
                services["items"] = [service]
            if bad == "wrong hostIP": pod["spec"]["containers"][0]["ports"][0]["hostIP"] = "9.9.9.9"
            if bad == "missing hostport": pod["spec"]["containers"][0]["ports"].pop()
            if bad == "truncated": services["metadata"] = {"continue": "next"}
            with self.subTest(bad=bad), patch.object(handoff.front, "read", return_value=services):
                if bad:
                    with self.assertRaises(ValueError): handoff.require_original_listener_ownership(config, pod)
                else:
                    handoff.require_original_listener_ownership(config, pod)

    def test_generation_bounds_and_cas_do_not_rewrite_selector_or_cluster_identity(self):
        config, _, baseline, evidence, _ = fixture()
        handoff.validate(config)
        for key, value in [("generation", 1), ("rollbackGeneration", 2), ("verificationSeconds", 0), ("address", "127.0.0.1")]:
            with self.subTest(key=key), self.assertRaises(ValueError): handoff.validate(dict(config, **{key: value}))
        patch_ops = handoff.cas_patch(baseline, evidence["selected_spec"], evidence["selected_annotations"])
        self.assertEqual([p["path"] for p in patch_ops if p["op"] == "test"], ["/metadata/uid", "/metadata/resourceVersion", "/spec", "/metadata/annotations", "/metadata/labels"])
        self.assertFalse(any(p["op"] != "test" and any(x in p["path"] for x in ["selector", "clusterIP", "ports"]) for p in patch_ops))
        self.assertIn({"op": "add", "path": "/spec/externalIPs", "value": ["8.8.8.8"]}, patch_ops)

    def test_handoff_preserves_old_socket_and_compensates_only_its_own_applied_generation(self):
        for failure in [None, "old connection", "new connection", "public route", "lost write response", "newer writer", "prewrite identity"]:
            with self.subTest(failure=failure):
                config, profile, baseline, evidence, external = fixture()
                live = [copy.deepcopy(baseline)]
                writes, sockets = [], []
                clock = [0.0]
                old = {"metadata": {"uid": "old", "name": "old"}, "status": {"podIP": "10.0.0.1"}}
                candidate = {"metadata": {"uid": "changed" if failure == "prewrite identity" else "new", "name": "new"}, "status": {"podIP": "10.0.0.2"}}
                proof = {"edge": "node-a", "group": "cell-a", "version": "one"}
                class Socket:
                    def __init__(self, *_):
                        self.requests, self.closed, self.index = 0, False, len(sockets)
                        sockets.append(self)
                    def proof(self, _):
                        self.requests += 1
                        if failure == "old connection" and self.index == 0 and writes:
                            raise OSError("original socket closed")
                        return proof
                    def close(self): self.closed = True
                def write(config, observed, spec, annotations, dry_run=False):
                    writes.append(annotations[handoff.stage.GENERATION])
                    value = copy.deepcopy(observed)
                    value["metadata"]["resourceVersion"] = str(len(writes)+1)
                    value["metadata"]["annotations"] = annotations
                    value["spec"].update(spec)
                    if "externalTrafficPolicy" not in spec: value["spec"].pop("externalTrafficPolicy", None)
                    live[0] = value
                    if failure == "lost write response" and len(writes) == 1:
                        raise OSError("response lost after CAS")
                    return copy.deepcopy(value)
                def fact(profile, pod, held):
                    if writes and pod["metadata"]["uid"] == "new" and failure == "new connection":
                        raise ValueError("new connection remains on original Front")
                    return {"pod_uid": pod["metadata"]["uid"], "connection_id": str(held.index)}
                def route(*args):
                    if writes and failure in ["public route", "newer writer"]:
                        if failure == "newer writer": live[0]["metadata"]["annotations"][handoff.stage.GENERATION] = "4"
                        raise ValueError("route changed")
                    return proof
                with patch.object(handoff, "load_stage", return_value=({}, {}, profile)), patch.object(handoff, "external_evidence"), patch.object(handoff, "require_original_listener_ownership"), patch.object(handoff.front, "pod", side_effect=lambda p, s, c: candidate if c else old), patch.object(handoff.stage, "verify_probe_listener", return_value=candidate), patch.object(handoff.stage, "endpoint_witness", return_value=evidence["endpoint"]), patch.object(handoff, "current", side_effect=lambda _: copy.deepcopy(live[0])), patch.object(handoff.front, "state", return_value={"generation": 1}), patch.object(handoff.connection, "HeldTLS", Socket), patch.object(handoff.connection, "fact", side_effect=fact), patch.object(handoff.front, "proof", side_effect=route), patch.object(handoff, "redirect"), patch.object(handoff, "write", side_effect=write), patch.object(handoff.time, "monotonic", side_effect=lambda: clock[0]), patch.object(handoff.time, "sleep", side_effect=lambda n: clock.__setitem__(0, clock[0]+n)):
                    if failure:
                        with self.assertRaises((ValueError, OSError)):
                            handoff.apply(config, evidence, external)
                    else:
                        result = handoff.apply(config, evidence, external)
                        self.assertEqual(result["result"], "verified")
                        self.assertTrue(result["old_front_retained"])
                        self.assertEqual(len(result["observations"]), 3)
                self.assertTrue(all(s.closed for s in sockets))
                if failure == "prewrite identity":
                    self.assertEqual(writes, [])
                elif failure == "newer writer":
                    self.assertEqual(writes, ["2"])

                    self.assertEqual(live[0]["metadata"]["annotations"][handoff.stage.GENERATION], "4")
                elif failure:
                    self.assertEqual(writes, ["2", "3"])
                    self.assertEqual(live[0]["spec"]["externalIPs"], [])
                    self.assertNotIn("externalTrafficPolicy", live[0]["spec"])
                else:
                    self.assertEqual(writes, ["2"])

    def test_external_failure_compensates_only_exact_handoff_and_accepts_no_write(self):
        for state in ["selected", "unchanged", "already compensated", "new writer", "replaced", "stale", "wrong old pod"]:
            with self.subTest(state=state):
                config, profile, baseline, evidence, _ = fixture()
                live = copy.deepcopy(baseline)
                if state != "unchanged":
                    live["spec"].update(evidence["selected_spec"])
                    live["metadata"]["annotations"] = evidence["selected_annotations"]
                if state == "already compensated":
                    spec = {k: baseline["spec"].get(k) for k in ["type", "selector", "ports"]}
                    spec.update(externalIPs=[], publishNotReadyAddresses=False)
                    live["spec"].update(spec)
                    live["spec"].pop("externalTrafficPolicy")
                    live["metadata"]["annotations"] = handoff.target_annotations(config, baseline, spec, rollback=True)
                if state == "new writer": live["metadata"]["annotations"][handoff.stage.GENERATION] = "4"
                if state == "replaced": live["metadata"]["uid"] = "other"
                if state == "stale":
                    evidence["at"] = (handoff.front.now()-datetime.timedelta(hours=1)).isoformat()
                    evidence.pop("digest")
                    evidence["digest"] = handoff.probe.digest(evidence)
                old = {"metadata": {"uid": "other" if state == "wrong old pod" else "old"}, "status": {"podIP": "10.0.0.1"}}
                socket = SimpleNamespace(proof=lambda _: None, close=lambda: None)
                with patch.object(handoff, "load_stage", return_value=({}, {}, profile)), patch.object(handoff, "current", return_value=live), patch.object(handoff.front, "pod", return_value=old), patch.object(handoff, "require_original_listener_ownership"), patch.object(handoff.front, "proof", return_value={"edge": profile["node"], "group": profile["group"]}), patch.object(handoff, "compensate", return_value=live) as compensate, patch.object(handoff.connection, "HeldTLS", return_value=socket), patch.object(handoff.connection, "fact", return_value={"pod_uid": "old"}):
                    if state in ["new writer", "replaced", "stale", "wrong old pod"]:
                        with self.assertRaises(ValueError): handoff.recover(config, evidence)
                        compensate.assert_not_called()
                    else:
                        result = handoff.recover(config, evidence)
                        self.assertEqual(result["result"], {"selected": "compensated", "unchanged": "unchanged", "already compensated": "already_compensated"}[state])
                        self.assertEqual(compensate.call_count, int(state == "selected"))

    def test_changed_retained_mutation_is_rejected_even_with_recomputed_digest(self):
        config, profile, _, evidence, external = fixture()
        evidence["selected_spec"]["externalIPs"] = ["9.9.9.9"]
        evidence.pop("digest")
        evidence["digest"] = handoff.probe.digest(evidence)
        with patch.object(handoff, "load_stage", return_value=({}, {}, profile)), patch.object(handoff, "external_evidence"), patch.object(handoff, "write") as write, self.assertRaises(ValueError):
            handoff.apply(config, evidence, external)
        write.assert_not_called()

if __name__ == "__main__": unittest.main()
