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


def probe_fixture():
    config, source, evidence = fixture()
    transport = replacement.transport
    profile = config["observation"]
    profile["legacySelector"]["fugue.io/edge-node"] = profile["node"]
    profile["candidateSelector"]["fugue.io/edge-node"] = profile["node"]
    profile["activationPath"] = "/var/lib/fugue-edge-front/activation.json"
    config.update(schema=replacement.PROBE_FOLLOW, candidateAuthority="node-worker-v1", candidateImageDigest="sha256:"+"b"*64,
                  service="probe-front", serviceUID="probe-uid", probePort=15443,
                  servingSource={"service": "public-front", "serviceUID": "service-one", "generation": 7, "podUID": "new"})
    probe = transport.desired({"namespace": profile["namespace"], "generation": 2},
                              {"name": config["service"], "address": config["address"], "port": config["probePort"]},
                              dict(profile, candidateSelector=profile["legacySelector"]))
    probe["spec"]["clusterIP"] = "10.1.0.2"
    probe["metadata"].update(uid="probe-uid", resourceVersion="12", managedFields=[{"manager": transport.MANAGER}])
    source["spec"]["selector"] = profile["candidateSelector"]
    source["metadata"]["annotations"][replacement.stage.GENERATION] = "7"
    source["spec"]["ports"][0]["name"] = "http"
    source["spec"]["ports"][1]["name"] = "https"
    source["metadata"]["annotations"][replacement.stage.DIGEST] = transport.digest(replacement.projection(source["spec"]))
    evidence.update(baseline=probe, declaration_digest=transport.digest(config),
                    serving_source={"service": source, "endpoint": {"pod_uid": "new"}})
    evidence["digest"] = transport.digest({k:v for k,v in evidence.items() if k != "digest"})
    return config, probe, source, evidence


class ReplacementTests(unittest.TestCase):
    def test_signed_publication_refresh_needs_the_same_endpoint_and_does_not_count(self):
        profile = {"node": "node-a", "group": "group-a"}
        old = {"edge": "node-a", "group": "group-a", "version": "v1", "digest": "old"}
        new = dict(old, version="v2", digest="new")
        self.assertTrue(replacement.same_publication(profile, old, old, lambda: self.fail("unneeded replay")))
        self.assertFalse(replacement.same_publication(profile, old, new, lambda: new))
        for first, second, repeat in [
            (old, new, old), (new, old, new), (old, dict(new, digest="wrong"), new),
            (old, dict(old, digest="wrong"), dict(old, digest="wrong")),
            (old, dict(new, edge="wrong-edge"), new),
        ]:
            with self.subTest(first=first, second=second, repeat=repeat), self.assertRaises(ValueError):
                replacement.same_publication(profile, first, second, lambda: repeat)

    def test_passive_front_is_only_allowed_for_probe_follow_and_remains_image_fenced(self):
        config, _, _, _ = probe_fixture()
        config["candidateAuthority"] = replacement.SERVING_READ_ONLY
        replacement.validate(config)
        public = {k: v for k, v in config.items() if k not in {"probePort", "servingSource"}}
        public["schema"] = "fugue.front-executor-replacement/v2"
        with self.assertRaisesRegex(ValueError, "allowed authority"):
            replacement.validate(public)
        image = "example.test/front@" + config["candidateImageDigest"]
        old = {"metadata": {"uid": "old"}, "spec": {"containers": []}}
        candidate = {
            "metadata": {"uid": "new"},
            "spec": {"containers": [{"name": "edge-front", "image": image}]},
            "status": {"containerStatuses": [{"name": "edge-front", "imageID": image}]},
        }
        with patch.object(replacement.front, "pod", side_effect=[old, candidate, old, candidate]) as pod, \
             patch.object(replacement.front, "node_worker_recipient") as recipient:
            self.assertEqual(replacement.executors(config), (old, candidate))
            self.assertFalse(pod.call_args.kwargs["node_worker"])
            recipient.assert_not_called()
            candidate["status"]["containerStatuses"][0]["imageID"] = "example.test/front@sha256:" + "1"*64
            with self.assertRaisesRegex(ValueError, "running image"):
                replacement.executors(config)

    def test_probe_replacement_accepts_only_an_owned_separate_listener_with_exact_baseline(self):
        config, probe, source, evidence = probe_fixture()
        transport = replacement.transport
        replacement.validate(config)
        replacement.validate_evidence(config, evidence)
        for bad in (dict(config, probePort=443), dict(config, probePort=80),
                    dict(config, service=source["metadata"]["name"]),
                    dict(config, serviceUID=source["metadata"]["uid"])):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                replacement.validate(bad)
        for fault in ["port", "internal_policy", "conflicting_writer", "annotations", "selector"]:
            changed = copy.deepcopy(probe)
            if fault == "port":
                changed["spec"]["ports"][0]["port"] = 443
            if fault == "internal_policy":
                changed["spec"]["internalTrafficPolicy"] = "Cluster"
            if fault == "conflicting_writer":
                changed["metadata"]["managedFields"].append({"manager": "other-writer", "fieldsV1": {"f:spec": {"f:selector": {}}}})
            if fault == "annotations":
                changed["metadata"]["annotations"][transport.FOLLOWS_SERVING] = "other-service"
            if fault == "selector":
                changed["spec"]["selector"] = config["observation"]["candidateSelector"]
            # A valid digest does not override the exact listener contract.
            changed["metadata"]["annotations"][transport.DIGEST] = transport.digest(replacement.projection(changed["spec"], probe=True))
            with self.subTest(fault=fault), self.assertRaises(ValueError):
                replacement.baseline(config, changed)
        spec, annotations = replacement.selected(config, probe)
        migrated = copy.deepcopy(probe)
        migrated["spec"], migrated["metadata"]["annotations"] = spec, annotations
        self.assertEqual(annotations[transport.FOLLOWS_SERVING], source["metadata"]["uid"])
        self.assertEqual(spec["ports"], probe["spec"]["ports"])
        self.assertEqual(spec["externalIPs"], probe["spec"]["externalIPs"])
        self.assertEqual(spec["clusterIP"], probe["spec"]["clusterIP"])
        legacy_intent = copy.deepcopy(probe)
        del legacy_intent["spec"]["clusterIP"]
        legacy_intent["metadata"]["annotations"][transport.GENERATION] = "8"
        with self.assertRaisesRegex(ValueError, "legacy transport"):
            transport.mutation(migrated, legacy_intent)
        with patch.object(transport, "kubectl") as kubectl:
            replacement.write(config, probe, spec, annotations, dry_run=True)
            self.assertIn("--field-manager=" + transport.MANAGER, kubectl.call_args.args)
            self.assertIn("--dry-run=server", kubectl.call_args.args)
        restored, compensated = replacement.selected(config, probe, rollback=True)
        self.assertEqual(restored, probe["spec"])
        self.assertNotIn(transport.FOLLOWS_SERVING, compensated)
        self.assertEqual(compensated[transport.GENERATION], "4")
        self.assertEqual(compensated[transport.DIGEST], probe["metadata"]["annotations"][transport.DIGEST])

    def test_probe_can_follow_a_second_handoff_of_the_same_serving_service(self):
        config, probe, _, _ = probe_fixture()
        transport = replacement.transport
        spec, annotations = replacement.selected(config, probe)
        probe["spec"], probe["metadata"]["annotations"] = spec, annotations
        previous, target = config["observation"]["candidateSelector"], config["observation"]["legacySelector"]
        config["observation"].update(legacySelector=previous, candidateSelector=target)
        config.update(expectedGeneration=3, generation=4, rollbackGeneration=5)
        config["servingSource"].update(generation=8, podUID="next")
        replacement.validate(config)
        self.assertIs(replacement.baseline(config, probe), probe)
        restored, compensated = replacement.selected(config, probe, rollback=True)
        self.assertEqual(restored, probe["spec"])
        self.assertEqual(compensated[transport.FOLLOWS_SERVING], config["servingSource"]["serviceUID"])
        self.assertEqual(compensated[replacement.stage.GENERATION], "5")
        changed_service = dict(config, servingSource=dict(config["servingSource"], serviceUID="another"))
        with self.assertRaises(ValueError):
            replacement.baseline(changed_service, probe)

    def test_probe_handoff_preserves_held_connection_and_never_writes_serving(self):
        for fault in [None, "source_before", "source_after", "worker_after", "candidate_is_not_serving"]:
            with self.subTest(fault=fault), ExitStack() as stack:
                config, probe, source, evidence = probe_fixture()
                live, writes, sockets, clock = [copy.deepcopy(probe)], [], [], [0.0]
                old = {"metadata": {"uid": "old", "name": "old"}, "status": {"podIP": "10.0.0.1"}}
                candidate = {"metadata": {"uid": "new", "name": "new"}, "status": {"podIP": "10.0.0.2"}}
                proof = {"edge": "node-a", "group": "group-a"}

                class Socket:
                    def __init__(self, address, host, port):
                        self.index = len(sockets)
                        self.closed = False
                        sockets.append((self, address, port))

                    def proof(self, _):
                        return proof

                    def close(self):
                        self.closed = True

                def read(config):
                    obj = copy.deepcopy(live[0] if config["service"] == "probe-front" else source)
                    if config["service"] != "probe-front" and (fault == "source_before" or fault == "source_after" and writes):
                        obj["metadata"]["resourceVersion"] = "drift"
                    return obj

                def write(config, observed, spec, annotations):
                    self.assertEqual(config["service"], "probe-front")
                    self.assertEqual(observed, live[0])
                    live[0]["spec"], live[0]["metadata"]["annotations"] = copy.deepcopy(spec), copy.deepcopy(annotations)
                    writes.append(annotations[replacement.stage.GENERATION])

                def endpoint(_, service, __, pod, value, *, probe=False):
                    self.assertEqual(probe, service["name"] == "probe-front")
                    self.assertEqual(value["spec"]["selector"], config["observation"]["legacySelector" if pod is old else "candidateSelector"])
                    return {"pod_uid": pod["metadata"]["uid"]}

                def route(address, _, __, port=443):
                    self.assertEqual(port, 15443 if address == config["address"] else 443)
                    return proof

                def fact(_, pod, socket):
                    return {"pod_uid": pod["metadata"]["uid"], "connection_id": str(socket.index), "slot": "b",
                            "target": "wrong-worker" if fault == "worker_after" and pod is candidate else "worker"}

                if fault == "candidate_is_not_serving":
                    candidate["metadata"]["uid"] = evidence["candidate_uid"] = "other"
                    evidence["digest"] = replacement.transport.digest({k:v for k,v in evidence.items() if k != "digest"})

                for module, name, substitute in [
                        (replacement, "current", read), (replacement, "write", write), (replacement, "executors", lambda _: (old, candidate)),
                        (replacement.stage, "selected_endpoint_witness", endpoint), (replacement.connection, "HeldTLS", Socket),
                        (replacement.connection, "fact", fact), (replacement.front, "proof", route), (replacement.front, "pod", lambda *_, **__: old),
                        (replacement.front, "state", lambda *_: {"generation": 10}), (replacement.handoff, "redirect", lambda *_: self.fail("public redirect touched")),
                        (replacement.time, "monotonic", lambda: clock[0]), (replacement.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))]:
                    stack.enter_context(patch.object(module, name, substitute))

                if fault:
                    with self.assertRaises(ValueError):
                        replacement.apply(config, evidence)
                    self.assertEqual(writes, ["3", "4"] if fault in {"source_after", "worker_after"} else [])
                    self.assertEqual(live[0]["spec"], probe["spec"])
                else:
                    result = replacement.apply(config, evidence)
                    self.assertEqual(writes, ["3"])
                    self.assertEqual(result["listener_port"], 15443)
                    self.assertEqual({(row["old_connection"]["connection_id"], row["new_connection"]["connection_id"])
                                      for row in result["observations"]}, {("0", "1")})
                self.assertTrue(all(s.closed and ip == config["address"] and port == 15443 for s, ip, port in sockets))

    def test_node_worker_front_is_explicit_and_has_one_writable_mount(self):
        config, _, _ = fixture()
        profile = config["observation"]
        config.update(schema="fugue.front-executor-replacement/v2", candidateAuthority="node-worker-v1",
                      candidateImageDigest="sha256:" + "b"*64)
        with self.assertRaises(ValueError):
            replacement.validate(config)
        profile["candidateSelector"]["fugue.io/edge-node"] = profile["node"]
        profile["activationPath"] = "/var/lib/fugue-edge-front/activation.json"
        replacement.validate(config)

        def node_front():
            image = "example.test/front@" + config["candidateImageDigest"]
            return {"metadata": {"uid": "new", "name": "candidate", "namespace": profile["namespace"],
                                 "annotations": {"fugue.pro/source-commit": profile["candidateSource"]}},
                    "spec": {
                        "nodeName": profile["node"], "automountServiceAccountToken": False,
                        "containers": [{"name": "edge-front", "image": image,
                                        "env": [{"name": "FUGUE_EDGE_FRONT_EDGE_GROUP_ID", "value": profile["group"]},
                                                {"name": "FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE", "value": profile["activationPath"]},
                                                {"name": "FUGUE_EDGE_FRONT_REQUIRE_ACTIVATION_STATE", "value": "true"}],
                                        "securityContext": {"readOnlyRootFilesystem": True, "allowPrivilegeEscalation": False,
                                                            "capabilities": {"drop": ["ALL"]}},
                                        "volumeMounts": [
                                            {"name": "activation", "mountPath": "/var/lib/fugue-edge-front"},
                                            {"name": "journal", "mountPath": "/var/run/fugue-worker-transaction", "readOnly": True}]}],
                        "volumes": [{"name": "activation", "hostPath": {
                            "path": "/var/lib/fugue/edge-owned/" + profile["group"] + "/front", "type": "Directory"}},
                            {"name": "journal", "configMap": {"name": replacement.front.node_worker_projection_name(profile["group"], profile["node"]),
                                                               "items": [{"key": "journal.json", "path": "journal.json"}], "optional": True}}]},
                    "status": {"phase": "Running", "podIP": "10.0.0.2",
                               "conditions": [{"type": "Ready", "status": "True"}],
                               "containerStatuses": [{"name": "edge-front", "ready": True, "imageID": image, "restartCount": 0}]}}

        candidate = node_front()
        with patch.object(replacement.front, "read", return_value={"items": [candidate]}):
            with self.assertRaisesRegex(ValueError, "read-only"):
                replacement.front.pod(profile, profile["candidateSelector"], True)
            self.assertIs(replacement.front.pod(profile, profile["candidateSelector"], True, node_worker=True), candidate)
        for fault in (None, "wrong_map", "wrong_key", "wrong_uid", "writable", "subpath", "wrong_path", "missing_volume"):
            candidate = node_front()
            spec = candidate["spec"]
            container = spec["containers"][0]
            path = "/var/run/fugue-worker-drain"
            grant_mount = {"name": "grant", "mountPath": path, "readOnly": True}
            grant_cm = {"name": replacement.front.node_worker_drain_name(profile["group"], profile["node"]),
                        "items": [{"key": "grant.json", "path": "grant.json"}], "optional": True}
            grant_uid = {"name": "FUGUE_EDGE_FRONT_WORKER_DRAIN_FRONT_UID",
                         "valueFrom": {"fieldRef": {"fieldPath": "metadata.uid"}}}
            grant_path = {"name": "FUGUE_EDGE_FRONT_WORKER_DRAIN_GRANT_PATH", "value": path + "/grant.json"}
            container["env"].extend([grant_uid, grant_path])
            if fault != "missing_volume":
                container["volumeMounts"].append(grant_mount)
                spec["volumes"].append({"name": "grant", "configMap": grant_cm})
            if fault == "wrong_map":
                grant_cm["name"] = replacement.front.node_worker_drain_name(profile["group"], "other-node")
            if fault == "wrong_key":
                grant_cm["items"][0]["key"] = "wrong.json"
            if fault == "wrong_uid":
                grant_uid["valueFrom"]["fieldRef"]["fieldPath"] = "metadata.name"
            if fault == "writable":
                grant_mount["readOnly"] = False
            if fault == "subpath":
                grant_mount["subPath"] = "grant.json"
            if fault == "wrong_path":
                grant_path["value"] = "/tmp/grant.json"
            with self.subTest(drain_mount=fault), patch.object(replacement.front, "read", return_value={"items": [candidate]}):
                if fault is None:
                    self.assertIs(replacement.front.pod(profile, profile["candidateSelector"], True, node_worker=True), candidate)
                else:
                    with self.assertRaises(ValueError):
                        replacement.front.pod(profile, profile["candidateSelector"], True, node_worker=True)
        for fault in ("other_mount", "other_node", "host_namespace", "extra_caps", "subpath", "required_cm"):
            candidate = node_front()
            spec = candidate["spec"]
            container = spec["containers"][0]
            if fault == "other_mount":
                spec["volumes"][0]["hostPath"]["path"] = "/var/lib/wrong"
            elif fault == "other_node":
                spec["volumes"][1]["configMap"]["name"] = replacement.front.node_worker_projection_name(profile["group"], "other-node")
            elif fault == "host_namespace":
                spec["hostPID"] = True
            elif fault == "extra_caps":
                container["securityContext"]["capabilities"]["add"] = ["NET_ADMIN"]
            elif fault == "subpath":
                container["volumeMounts"][1]["subPath"] = "journal.json"
            elif fault == "required_cm":
                spec["volumes"][1]["configMap"]["optional"] = False
            with self.subTest(fault=fault), patch.object(replacement.front, "read", return_value={"items": [candidate]}):
                with self.assertRaises(ValueError):
                    replacement.front.pod(profile, profile["candidateSelector"], True, node_worker=True)

        candidate, old = node_front(), node_front()
        old["metadata"]["uid"] = "old"
        with patch.object(replacement.front, "pod", side_effect=[old, candidate]), patch.object(
                replacement.front, "read", return_value={"pod_uid": "new", "node": profile["node"], "group": profile["group"],
                                                        "incarnation": "sha256:" + "a"*64}):
            self.assertEqual(replacement.executors(config), (old, candidate))
        candidate["status"]["containerStatuses"][0]["imageID"] = "example.test/front@sha256:" + "c"*64
        with patch.object(replacement.front, "pod", side_effect=[old, candidate]), patch.object(
                replacement.front, "node_worker_recipient") as recipient:
            with self.assertRaisesRegex(ValueError, "running image"):
                replacement.executors(config)
            recipient.assert_not_called()

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

    def test_prepare_rejects_multi_node_selector_before_any_write(self):
        config, previous, _ = fixture()
        profile = config["observation"]

        def pod(uid, node, ip):
            return {"metadata": {"name": uid, "namespace": profile["namespace"], "uid": uid,
                                 "annotations": {"fugue.pro/source-commit": profile["candidateSource"]}},
                    "spec": {"nodeName": node, "automountServiceAccountToken": False, "containers": [{
                        "name": "edge-front", "env": [
                            {"name": "FUGUE_EDGE_FRONT_EDGE_GROUP_ID", "value": profile["group"]},
                            {"name": "FUGUE_EDGE_FRONT_ACTIVE_SLOT_FILE", "value": profile["activationPath"]},
                            {"name": "FUGUE_EDGE_FRONT_REQUIRE_ACTIVATION_STATE", "value": "true"}]}]},
                    "status": {"phase": "Running", "podIP": ip,
                               "conditions": [{"type": "Ready", "status": "True"}], "containerStatuses": [
                                   {"name": "edge-front", "ready": True, "imageID": "front@sha256:" + "b"*64, "restartCount": 0}]}}

        old = pod("old", "node-a", "10.0.0.1")
        local = pod("candidate-a", "node-a", "10.0.0.2")
        remote = pod("candidate-b", "node-b", "10.0.0.3")
        for extra in [remote, dict(remote, metadata=dict(remote["metadata"], deletionTimestamp="2026-01-01T00:00:00Z"))]:
            with self.subTest(extra=extra["metadata"]), patch.object(replacement, "current", return_value=previous), \
                 patch.object(replacement.front, "read", side_effect=[{"items": [old]}, {"items": [local, extra]}]), \
                 patch.object(replacement, "write") as write, patch.object(replacement.stage, "selected_endpoint_witness") as verify:
                with self.assertRaisesRegex(ValueError, "selector must select exactly one Pod"):
                    replacement.prepare(config)
                write.assert_not_called()
                verify.assert_not_called()

    def test_handoff_preserves_connections_and_fences_recovery(self):
        for failure in [None, "held_socket", "candidate_socket", "lost_write_response", "new_writer", "changed_before_write", "executor_changed_before_write"]:
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

                def executors(_):
                    if failure == "executor_changed_before_write" and sockets:
                        return old, dict(candidate, status={"podIP": "10.0.0.3"})
                    return old, candidate

                if failure == "changed_before_write":
                    live[0]["metadata"]["resourceVersion"] = "11"
                for target, name, value in [(replacement, "current", lambda _: copy.deepcopy(live[0])), (replacement, "executors", executors), (replacement, "write", write),
                                            (replacement.front, "state", lambda *_: {"generation": 10}), (replacement.front, "proof", lambda *_: proof), (replacement.front, "pod", lambda *_, **__: old),
                                            (replacement.handoff, "redirect", lambda *_: None), (replacement.stage, "selected_endpoint_witness", lambda *_: {}),
                                            (replacement.connection, "HeldTLS", Socket), (replacement.connection, "fact", fact), (replacement.time, "monotonic", lambda: clock[0]), (replacement.time, "sleep", sleep)]:
                    stack.enter_context(patch.object(target, name, value))
                if failure:
                    with self.assertRaises((ValueError, OSError)):
                        replacement.apply(config, evidence)
                    expected = [] if failure in ["changed_before_write", "executor_changed_before_write"] else ["3"] if failure == "new_writer" else ["3", "4"]
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
