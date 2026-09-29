import copy
import datetime
import json
import unittest
from unittest.mock import patch

from scripts import bootstrap_cell_inventory as b


def config():
    return {"schema": "fugue.cell-inventory-enrollment/v1", "generation": 1, "origin": "https://api.example.test", "authority_cell_id": "cell-one", "namespace": "test-system", "release_set": {"id": "parent-one", "digest": "sha256:" + "d" * 64}, "cohort": "initial", "bootstrap_config_map": "cell-one-bootstrap", "worker": {"deployment": "worker-one", "deployment_uid": "11111111-1111-1111-1111-111111111111", "pod": "worker-one-pod", "instance_uid": "22222222-2222-2222-2222-222222222222", "node": "node-one", "slot": "a", "container": "edge", "service_account": "worker-identity", "source_sha": "a" * 40, "image_digest": "sha256:" + "b" * 64, "pvc": "cell-one-state", "activation_path": "/state/activation.json"}, "observation": {"samples": 3, "interval_seconds": 10, "timeout_seconds": 180}}


def fixtures(c):
    w = c["worker"]
    annotations = {"fugue.pro/authority-transition-role": "isolated-candidate", "fugue.pro/source-commit": w["source_sha"]}
    labels = {"fugue.io/authority-cell-id": c["authority_cell_id"], "fugue.io/edge-group-id": c["authority_cell_id"], "fugue.io/edge-slot": w["slot"], "fugue.io/fault-domain-id": w["node"]}
    image = "registry.example.test/worker@" + w["image_digest"]
    dep = {"metadata": {"uid": w["deployment_uid"], "generation": 1, "annotations": annotations}, "spec": {"replicas": 1}, "status": {"readyReplicas": 1, "observedGeneration": 1}}
    pod = {"metadata": {"uid": w["instance_uid"], "annotations": annotations, "labels": labels, "ownerReferences": [{"kind": "ReplicaSet", "name": "worker-rs", "uid": "rs-one", "controller": True}]}, "spec": {"nodeName": w["node"], "serviceAccountName": w["service_account"], "automountServiceAccountToken": False, "containers": [{"name": "edge", "image": image, "env": [{"name": "FUGUE_PLATFORM_ARTIFACT_SCOPE", "value": "authority-cell:" + c["authority_cell_id"]}, {"name": "FUGUE_EDGE_INVENTORY_ACTIVATION_STATE_FILE", "value": w["activation_path"]}, {"name": "FUGUE_EDGE_INVENTORY_BOOTSTRAP_FILE", "value": "/bootstrap/authorization.json"}], "volumeMounts": [{"name": "state", "mountPath": "/state", "subPath": "activation", "readOnly": True}, {"name": "bootstrap", "mountPath": "/bootstrap", "readOnly": True}]}], "volumes": [{"name": "state", "persistentVolumeClaim": {"claimName": w["pvc"]}}, {"name": "bootstrap", "configMap": {"name": c["bootstrap_config_map"]}}]}, "status": {"phase": "Running", "containerStatuses": [{"name": "edge", "ready": True, "restartCount": 0, "imageID": image}]}}
    rs = {"metadata": {"uid": "rs-one", "ownerReferences": [{"kind": "Deployment", "uid": w["deployment_uid"], "controller": True}]}}
    pvc = {"metadata": {"annotations": annotations}, "status": {"phase": "Bound"}}
    return {("deployment", w["deployment"]): dep, ("pod", w["pod"]): pod, ("replicaset", "worker-rs"): rs, ("pvc", w["pvc"]): pvc}


def health(c, release):
    at = b.now().isoformat()
    return {"healthy": True, "edge_id": c["worker"]["node"], "edge_group_id": c["authority_cell_id"], "route_bundle_source": "edge-control-group-authority/v1", "route_count": 5, "publication_sequence": 1, "bundle_version": "bundle-one", "caddy_applied_version": "bundle-one", "stale_cache": False, "inventory_producer_active": True, "inventory_heartbeat_at": at, "platform_serving": {"state": "serving_verified", "bundle_version": "bundle-one", "route_probes": 5, "tls_probes": 5, "verified_at": at, "reported_at": at, "traffic_release": {"release_set_id": c["release_set"]["id"], "release_set_digest": c["release_set"]["digest"], "release_id": release["id"], "fencing_token": release["fencing_token"], "scope_key": "authority-cell:" + c["authority_cell_id"], "release_channel": "gray"}}}


class EnrollmentTests(unittest.TestCase):
    def test_exact_private_executor_and_readonly_pvc(self):
        c = config()
        resources = fixtures(c)
        with patch.object(b, "resource", side_effect=lambda _, kind, name: resources[kind, name]), patch.object(b, "kubectl", return_value={"items": []}):
            worker = b.isolated_worker(c)
        self.assertTrue(worker["mount"]["readOnly"])

    def test_foreign_changed_or_public_executor_fails_closed(self):
        c = config()
        for mutation in [lambda p: p["metadata"].update(uid="foreign"), lambda p: p["metadata"]["annotations"].update({"fugue.pro/source-commit": "f" * 40}), lambda p: p["spec"].update(hostNetwork=True), lambda p: p["spec"]["containers"][0].update(ports=[{"hostPort": 443}]), lambda p: p["status"]["containerStatuses"][0].update(restartCount=1), lambda p: p["spec"]["containers"][0]["volumeMounts"][0].update(readOnly=False), lambda p: p["spec"].update(serviceAccountName="foreign")]:
            resources = fixtures(c)
            mutation(resources["pod", c["worker"]["pod"]])
            with patch.object(b, "resource", side_effect=lambda _, kind, name: resources[kind, name]), patch.object(b, "kubectl", return_value={"items": []}), self.assertRaises(ValueError):
                b.isolated_worker(c)
        resources = fixtures(c)
        for spec in [{"type": "LoadBalancer", "ports": [{"port": 443}]}, {"externalIPs": ["192.0.2.1"], "ports": [{"port": 7832}]}, {"ports": [{"port": 443, "targetPort": 18443}]}]:
            spec["selector"] = resources["pod", c["worker"]["pod"]]["metadata"]["labels"]
            with patch.object(b, "resource", side_effect=lambda _, kind, name: resources[kind, name]), patch.object(b, "kubectl", return_value={"items": [{"spec": spec}]}), self.assertRaises(ValueError):
                b.isolated_worker(c)

    def test_permission_reuse_does_not_extend_lease(self):
        c, release = config(), {"id": "gray-one"}
        fixed = b.now()
        with patch.object(b, "now", return_value=fixed):
            cm, permission = b.permission(c, release)
        with patch.object(b, "now", return_value=fixed + datetime.timedelta(minutes=5)):
            reused, authorization = b.permission(c, release, cm)
        self.assertEqual((cm, permission), (reused, authorization))
        self.assertEqual(900, (b.timestamp(permission["expires_at"]) - b.timestamp(permission["issued_at"])).total_seconds())

    def test_expired_foreign_or_mutated_permission_is_never_renewed(self):
        c, release = config(), {"id": "gray-one"}
        cm, _ = b.permission(c, release)
        for mutation in [lambda x: x.update(immutable=True), lambda x: x["metadata"]["annotations"].update({b.DECLARATION: "foreign"}), lambda x: x["data"].update({"other": "value"})]:
            bad = copy.deepcopy(cm)
            mutation(bad)
            with self.assertRaises(ValueError):
                b.permission(c, release, bad)
        with patch.object(b, "now", return_value=b.now() + datetime.timedelta(minutes=16)), self.assertRaisesRegex(ValueError, "never automatically renewed"):
            b.permission(c, release, cm)
        with self.assertRaises(ValueError):
            b.permission(c, {"id": "gray-two"}, cm)

    def test_route_only_parent_requires_exact_topology_and_digest(self):
        c = config()
        content = {"publication_role": "cell-routes", "artifact_kinds": ["edge_route_bundle", "caddy_route_config"], "artifact_ids": ["routes", "tls"], "consumer_topology": {"publication_role": "cell-routes", "schema_version": "fugue.traffic-consumer-topology/v1", "authority_cell_id": c["authority_cell_id"], "edge_node_ids": [c["worker"]["node"]], "dns_node_ids": []}, "traffic_rollout_cohorts": [{"id": c["cohort"], "edge_group_ids": [c["authority_cell_id"]]}]}
        c["release_set"]["digest"] = b.digest(content)
        a = {"content": content, "content_hash": c["release_set"]["digest"], "status": "validated", "artifact_kind": "release_set", "scope_key": "authority-cell:" + c["authority_cell_id"]}
        self.assertEqual(a, b.parent(c, lambda *args: {"artifact": a}))
        content["consumer_topology"]["dns_node_ids"] = ["dns-one"]
        with self.assertRaises(ValueError):
            b.parent(c, lambda *args: {"artifact": a})

    def test_explicit_retry_can_only_replace_exact_expired_permission(self):
        c, release = config(), {"id": "gray-one"}
        with patch.object(b, "now", return_value=b.now() - datetime.timedelta(minutes=16)):
            old, authorization = b.permission(c, release)
        old["metadata"].update(uid="33333333-3333-3333-3333-333333333333", resourceVersion="123")
        c["generation"] = 2
        c["previous_permission"] = {"uid": old["metadata"]["uid"], "declaration_digest": old["metadata"]["annotations"][b.DECLARATION], "authorization_digest": b.digest(authorization), "generation": 1}
        b.validate(c)
        options = b.expired_predecessor(c, release, old)
        self.assertEqual({"uid": old["metadata"]["uid"], "resourceVersion": "123"}, options)
        replacement = copy.deepcopy(c)
        replacement["worker"]["instance_uid"] = "44444444-4444-4444-4444-444444444444"
        replacement["worker"]["source_sha"] = "f" * 40
        self.assertEqual(options, b.expired_predecessor(replacement, release, old))
        replacement["worker"]["node"] = "foreign-node"
        with self.assertRaises(ValueError):
            b.expired_predecessor(replacement, release, old)
        for mutate in [lambda x: x["metadata"].update(uid="replacement"), lambda x: x["metadata"].update(resourceVersion=""), lambda x: x.update(immutable=True)]:
            changed = copy.deepcopy(old)
            mutate(changed)
            with self.assertRaises(ValueError):
                b.expired_predecessor(c, release, changed)
        with patch.object(b, "now", return_value=b.timestamp(authorization["issued_at"]) + datetime.timedelta(minutes=1)), self.assertRaises(ValueError):
            b.expired_predecessor(c, release, old)
        with self.assertRaises(ValueError):
            b.expired_predecessor(c, {"id": "different-gray"}, old)
        with self.assertRaises(ValueError):
            b.expired_predecessor(config(), release, old)

    def test_permission_replacement_preserves_projection_and_binds_cas(self):
        c, release = config(), {"id": "gray-one"}
        old, _ = b.permission(c, release)
        old["metadata"].update(uid="old-uid", resourceVersion="1")
        target, _ = b.permission(c, release)
        options = {"uid": "old-uid", "resourceVersion": "1"}
        with patch.object(b, "kubectl", return_value=target) as kube:
            b.replace_permission(c, options, old, target)
        args, kwargs = kube.call_args
        self.assertEqual("patch", args[0])
        self.assertEqual("json", args[args.index("-o")+1])
        operations = json.loads(kwargs["body"])
        self.assertEqual(["test"] * 4 + ["replace"] * 2, [x["op"] for x in operations])
        self.assertEqual(["old-uid", "1", old["data"], old["metadata"]["annotations"]], [x["value"] for x in operations[:4]])
        self.assertNotIn("immutable", target)

    def test_gray_publication_cannot_replace_foreign_or_full_authority(self):
        c = config()
        calls = []
        def api(method, path, body=None):
            calls.append(method)
            return {"artifact": {"id": "foreign", "artifact_kind": "release_set", "scope_key": "authority-cell:cell-one"}, "release": {"id": "full-one", "artifact_id": "foreign", "artifact_kind": "release_set", "scope_key": "authority-cell:cell-one", "release_channel": "full"}}
        with self.assertRaises(ValueError):
            b.gray(c, api, publish=True)
        self.assertEqual(calls, ["GET"])

    def test_real_health_needs_tls_routes_exact_binding_and_freshness(self):
        c, release = config(), {"id": "gray-one", "fencing_token": 1}
        good = health(c, release)
        with patch.object(b, "kubectl", return_value=good):
            self.assertEqual(good, b.health(c, release))
        for mutation in [lambda h: h.update(healthy=False), lambda h: h.update(caddy_applied_version="other"), lambda h: h.update(stale_cache=True), lambda h: h["platform_serving"].update(tls_probes=0), lambda h: h["platform_serving"]["traffic_release"].update(release_id="foreign"), lambda h: h["platform_serving"]["traffic_release"].update(release_set_digest="sha256:" + "e" * 64), lambda h: h["platform_serving"].update(verified_at=(b.now() - datetime.timedelta(minutes=5)).isoformat())]:
            bad = copy.deepcopy(good)
            mutation(bad)
            with patch.object(b, "kubectl", return_value=bad), self.assertRaises(ValueError):
                b.health(c, release)

    def test_activation_job_only_initializes_exact_observed_bundle(self):
        c, release = config(), {"id": "gray-one", "fencing_token": 1}
        _, authorization = b.permission(c, release)
        worker = {"image": "registry.example.test/worker@" + c["worker"]["image_digest"], "mount": {"name": "state", "mountPath": "/state", "subPath": "activation", "readOnly": True}}
        job = b.activation_job(c, worker, health(c, release), authorization)
        pod = job["spec"]["template"]["spec"]
        container = pod["containers"][0]
        self.assertFalse(pod["automountServiceAccountToken"])
        self.assertEqual(c["worker"]["node"], pod["nodeName"])
        self.assertEqual(worker["image"], container["image"])
        for flag, value in [("--expected-generation", "0"), ("--bundle-generation", "bundle-one"), ("--operation", "initialize")]:
            self.assertEqual(value, container["args"][container["args"].index(flag) + 1])
        self.assertIn('test "$(date +%s)" -lt "$1"', container["command"][2])
        self.assertEqual(60, job["spec"]["activeDeadlineSeconds"])
        self.assertNotIn("ports", container)
        self.assertTrue(worker["mount"]["readOnly"])

    def test_convergence_rejects_missing_or_foreign_authenticated_member(self):
        c, release = config(), {"id": "gray-one", "fencing_token": 1}
        credential = "kubernetes:" + c["namespace"] + ":" + c["worker"]["service_account"] + ":" + c["worker"]["instance_uid"]
        values = [{"artifact_kind": kind, "pass": True, "required_expected": 1, "required_passing": 1, "assessments": [{"state": "pass", "observed": {"identity_verified": True, "credential_id": credential, "release_set_id": c["release_set"]["id"], "fencing_token": 1}}]} for kind in sorted(b.KINDS)]
        self.assertEqual(values, b.convergence(c, lambda *a: {"convergence": values}, release))
        for mutate in [lambda v: v.pop(), lambda v: v[0].update(required_expected=0), lambda v: v[0]["assessments"][0]["observed"].update(credential_id="foreign"), lambda v: v[0]["assessments"][0]["observed"].update(identity_verified=False), lambda v: v[0]["assessments"][0]["observed"].update(fencing_token=2)]:
            bad = copy.deepcopy(values)
            mutate(bad)
            with self.assertRaises(ValueError):
                b.convergence(c, lambda *a: {"convergence": bad}, release)

    def test_unproven_serving_never_initializes_activation(self):
        c, release, writes = config(), {"id": "gray-one", "fencing_token": 1}, []
        def kube(*args, body=None):
            self.assertEqual("json", args[args.index("-o") + 1])
            writes.append(json.loads(body))
        with patch.object(b, "isolated_worker", return_value={}), patch.object(b, "activation", return_value=None), patch.object(b, "parent"), patch.object(b, "gray", return_value=release), patch.object(b, "resource", return_value=None), patch.object(b, "kubectl", side_effect=kube), patch.object(b, "check_projection"), patch.object(b, "health", side_effect=ValueError("no verified TLS")), patch.object(b.time, "monotonic", side_effect=[0, 1, 181]), patch.object(b.time, "sleep"):
            with self.assertRaisesRegex(ValueError, "no verified TLS"):
                b.enroll(c, lambda *a: {}, {})
        self.assertEqual(["ConfigMap"], [w["kind"] for w in writes])

    def test_old_projected_permission_cannot_satisfy_new_attempt(self):
        c = config()
        _, permission = b.permission(c, {"id": "gray-one"})
        old = dict(permission, expires_at=(b.now() - datetime.timedelta(seconds=1)).isoformat())
        worker = {"bootstrap_path": "/bootstrap/authorization.json"}
        with patch.object(b, "kubectl", return_value=old), self.assertRaisesRegex(ValueError, "different bootstrap projection"):
            b.check_projection(c, worker, permission)
        with patch.object(b, "kubectl", return_value=permission):
            b.check_projection(c, worker, permission)

    def test_changed_executor_stops_before_any_mutation(self):
        with patch.object(b, "isolated_worker", side_effect=ValueError("changed")), patch.object(b, "kubectl") as kube, patch.object(b, "API") as api:
            with self.assertRaises(ValueError):
                b.enroll(config(), api, {})
            api.assert_not_called()
            kube.assert_not_called()

    def test_existing_foreign_activation_stops_before_any_mutation(self):
        with patch.object(b, "isolated_worker", return_value={}), patch.object(b, "activation", return_value={"generation": 1}), patch.object(b, "kubectl") as kube, patch.object(b, "API") as api:
            with self.assertRaises(ValueError):
                b.enroll(config(), api, {})
            api.assert_not_called()
            kube.assert_not_called()

    def test_declaration_rejects_implicit_or_unbounded_identity(self):
        self.assertEqual(config(), b.validate(config()))
        for mutate in [lambda c: c.update(authority_cell_id="edge-group-country-aa"), lambda c: c["worker"].update(image_digest="latest"), lambda c: c["worker"].update(activation_path="/state/../activation.json"), lambda c: c["observation"].update(timeout_seconds=3600), lambda c: c.update(extra=True)]:
            c = config()
            mutate(c)
            with self.assertRaises(ValueError):
                b.validate(c)


if __name__ == "__main__":
    unittest.main()
