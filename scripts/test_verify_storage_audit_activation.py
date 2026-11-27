import copy
import json
import unittest
from unittest.mock import Mock, patch

from scripts.provision_observation_worker_database import ProvisionError
from scripts import test_verify_observation_rollback_floor as rollback_tests
from scripts.verify_observation_rollback_floor import checked_rollback, digest
from scripts import apply_storage_audit_activation as actuator
from scripts import verify_storage_audit_activation as activation


class StorageAuditActivationAdmissionTest(unittest.TestCase):
    def declaration(self):
        raw = {"schema": activation.SCHEMA, "policyDigest": "e" * 64}
        for name, (deployment, container, replicas), live, previous in (
            ("api", activation.API, "a", "b"), ("worker", activation.WORKER, "c", "d"),
        ):
            repo = f"registry.example/platform/{container}@sha256:"
            raw[name] = {"deployment": deployment, "container": container, "replicas": replicas,
                         "revision": live * 40, "image": repo + live * 64}
            raw[name + "Rollback"] = {"revision": previous * 40, "image": repo + previous * 64}
        return raw

    def test_requires_distinct_pinned_rollback_and_exact_workload_shape(self):
        raw = self.declaration()
        self.assertEqual(activation.parse_declaration(raw), raw)
        self.assertEqual(activation.declaration_digest(raw), activation.declaration_digest(dict(reversed(list(raw.items())))))
        for key, field, wrong in (
            ("api", "replicas", 1), ("worker", "deployment", "shared-worker"),
            ("apiRollback", "revision", raw["api"]["revision"]),
            ("workerRollback", "image", raw["worker"]["image"]),
            ("workerRollback", "image", "registry.example/another@sha256:" + "f" * 64),
        ):
            bad = copy.deepcopy(raw)
            bad[key][field] = wrong
            with self.subTest(key=key, field=field), self.assertRaises(ProvisionError):
                activation.parse_declaration(bad)
        raw["workerRollback"] = None
        self.assertEqual(activation.parse_declaration(raw), raw)
        raw["apiRollback"] = None
        with self.assertRaises(ProvisionError):
            activation.parse_declaration(raw)

    def test_actual_forward_and_lkg_manifests_both_need_literal_fence(self):
        provision, component, workload, objects, rollback = rollback_tests.ObservationRollbackFloorTest().fixture()
        immutable = next(v for v in objects.values() if "immutable" in v)["data"]

        def set_flag(field, definition):
            key = field + ".json"
            raw = json.loads(immutable[key])
            raw["items"][0]["spec"]["template"]["spec"]["containers"][0]["env"] = definition
            immutable[key] = json.dumps(raw)
            record = json.loads(immutable["record.json"])
            record[("forward" if field == "forward" else "lkg") + "ManifestDigest"] = digest(immutable[key])
            immutable["record.json"] = json.dumps(record)
            plan = json.loads(immutable["execution-plan.json"])
            plan[field]["manifestDigest"] = digest(immutable[key])
            immutable["execution-plan.json"] = json.dumps(plan)

        trusted = {"name": activation.FLAG, "value": "true"}
        set_flag("forward", [trusted])
        for invalid in ([], [trusted, trusted], [{"name": activation.FLAG, "valueFrom": {"configMapKeyRef": {}}}]):
            set_flag("lkg", invalid)
            with self.assertRaisesRegex(ProvisionError, "literally enable"):
                checked_rollback(provision, component, workload, verify_template=activation.verify_api_template)
        set_flag("lkg", [trusted])
        self.assertEqual(checked_rollback(provision, component, workload, verify_template=activation.verify_api_template), rollback)
        set_flag("forward", [])
        with self.assertRaisesRegex(ProvisionError, "literally enable"):
            checked_rollback(provision, component, workload, verify_template=activation.verify_api_template)

    def test_worker_only_accepts_its_required_secret_without_kubernetes_token(self):
        worker = self.declaration()["worker"]
        ref = {"name": "audit-secret", "key": "database-url"}
        template = {"spec": {"automountServiceAccountToken": False,
            "containers": [{"name": worker["container"], "env": [
                {"name": "FUGUE_STORAGE_AUDIT_DATABASE_URL", "valueFrom": {"secretKeyRef": ref}},
            ]}]}}
        activation.verify_worker_template(template, worker, "audit-secret")
        with self.assertRaisesRegex(ProvisionError, "restricted database identity"):
            activation.verify_worker_template(template, worker, "other-secret")
        ref["optional"] = True
        with self.assertRaises(ProvisionError):
            activation.verify_worker_template(template, worker, "audit-secret")
        ref["optional"] = False
        template["spec"]["automountServiceAccountToken"] = True
        with self.assertRaises(ProvisionError):
            activation.verify_worker_template(template, worker, "audit-secret")

    def test_commit_must_be_after_ownership_and_have_current_policy(self):
        state = {"failed": False, "completedPasses": 1, "policyDigest": "e" * 64, "samplePolicyDigest": "e" * 64,
                 "sampleAt": "2026-11-28T04:30:00+08:00", "activatedAt": "2026-11-27T20:29:59+00:00"}
        self.assertTrue(activation.has_committed_sample(state))
        self.assertFalse(activation.has_committed_sample({**state, "samplePolicyDigest": "f" * 64}))
        self.assertFalse(activation.has_committed_sample({**state, "failed": True}))
        for sample in ("2026-11-28T04:29:58+08:00", "2026-11-28T04:30:00"):
            with self.subTest(sample=sample), self.assertRaises(ProvisionError):
                activation.has_committed_sample({**state, "sampleAt": sample})

    def test_metrics_retirement_requires_a_real_api_response(self):
        provision = Mock(config={"namespace": "test-system"})
        pod = ("api-pod", "a0a0a0a0-a0a0-a0a0-a0a0-a0a0a0a0a0a0")
        provision.kube.return_value = '<html>upstream unavailable</html>'
        with self.assertRaisesRegex(ProvisionError, "metric identity"):
            activation.api_audit_metrics_retired(provision, pod)
        identity = 'fugue_component_info{component="api"} 1.000000\n'
        provision.kube.return_value = identity
        self.assertTrue(activation.api_audit_metrics_retired(provision, pod))
        provision.kube.return_value = identity + 'fugue_storage_lifecycle_audit_enabled 1.000000\n'
        self.assertFalse(activation.api_audit_metrics_retired(provision, pod))

    @patch.object(activation, "database_status")
    @patch.object(activation, "witness")
    @patch.object(activation, "verify_executors")
    def test_preflight_refuses_moved_primary_secret_or_worker(self, executors, witness, status):
        raw = self.declaration()
        pod = ("worker-pod", "pod-uid")
        identity = {"api": ("api-uid", 5, [("api-pod", "api-pod-uid")]), "worker": ("worker-uid", 3, [pod])}
        executors.return_value = identity
        witness.return_value = {"workerPod": pod, "prometheusPods": ["prometheus-pod"]}
        status.return_value = {"owner": "api", "completedPasses": 0}
        provision = Mock(config={"secret": "audit-secret"}, policy_digest=raw["policyDigest"])
        provision.find_primary.return_value = ("primary", "primary-uid")
        provision.credential.return_value = ("secret-uid", "never-printed")
        args = (provision, raw, {}, {}, "a" * 40, "c" * 40)
        result = activation.preflight(*args)
        self.assertEqual(result["workerPod"], pod)
        self.assertNotIn("never-printed", str(result))
        for dependency, first, second in (
            (provision.find_primary, ("primary", "uid-one"), ("primary", "uid-two")),
            (provision.credential, ("secret-uid", "omitted"), ("new-secret-uid", "omitted")),
            (executors, identity, {**identity, "worker": ("worker-uid", 4, [pod])}),
        ):
            dependency.side_effect = [first, second]
            with self.subTest(dependency=dependency), self.assertRaisesRegex(ProvisionError, "identity changed"):
                activation.preflight(*args)
            dependency.side_effect = None
        witness.return_value["workerPod"] = (pod[0], "new-pod-uid")
        with self.assertRaisesRegex(ProvisionError, "identity changed"):
            activation.preflight(*args)

    @patch.object(actuator, "verify_committed_activation", return_value={"owner": "worker"})
    @patch.object(actuator, "preflight")
    @patch.object(actuator, "activation_sql", return_value="transaction")
    def test_uncertain_reply_is_not_retried_as_a_write_after_worker_owns(self, statement, preflight, post):
        raw = self.declaration()
        provision = Mock(config={"secret": "audit-secret"}, policy_digest=raw["policyDigest"])
        provision.find_primary.return_value = ("primary", "primary-uid")
        provision.credential.return_value = ("secret-uid", "omitted")
        preflight.return_value = {"primary": "primary", "primaryUid": "primary-uid", "secretUid": "secret-uid",
                                  "declarationSha256": activation.declaration_digest(raw)}
        provision.sql.side_effect = ["1\napi", ProvisionError("response lost"), "1\nworker"]
        args = (provision, raw, {}, {}, "a" * 40, "c" * 40)
        with self.assertRaisesRegex(ProvisionError, "response lost"):
            actuator.apply(*args)
        post.assert_not_called()
        self.assertEqual(actuator.apply(*args), {"owner": "worker"})
        statement.assert_called_once_with(provision, "secret-uid", activation.declaration_digest(raw))
        preflight.assert_called_once()
        post.assert_called_once()
        self.assertEqual([call.args[1] for call in provision.sql.call_args_list].count("transaction"), 1)

    @patch.object(actuator, "preflight")
    @patch.object(actuator, "activation_sql")
    def test_refuses_database_or_secret_replacement_before_writing(self, statement, preflight):
        raw = self.declaration()
        provision = Mock(config={"secret": "audit-secret"}, policy_digest=raw["policyDigest"])
        provision.find_primary.return_value = ("primary", "primary-uid")
        provision.credential.return_value = ("new-secret-uid", "omitted")
        provision.sql.return_value = "1\napi"
        preflight.return_value = {"primary": "primary", "primaryUid": "primary-uid", "secretUid": "secret-uid"}
        with self.assertRaisesRegex(ProvisionError, "credential changed"):
            actuator.apply(provision, raw, {}, {}, "a" * 40, "c" * 40)
        statement.assert_not_called()
        self.assertEqual(provision.sql.call_count, 1)


if __name__ == "__main__":
    unittest.main()
