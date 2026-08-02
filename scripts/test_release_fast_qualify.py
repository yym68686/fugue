#!/usr/bin/env python3

import hashlib
import importlib.util
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SUBJECT = ROOT / "scripts" / "release_fast_qualify.py"
SPEC = importlib.util.spec_from_file_location("release_fast_qualify", SUBJECT)
assert SPEC and SPEC.loader
rfq = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = rfq
SPEC.loader.exec_module(rfq)


CANDIDATE = "d1e7ed9cdedbaa09db9bd78b4e433b94c7357510"
PARENT = "91184ece1c4e181ed764866c36880dc3f380692e"
GHOST_HEAD = "cac73f6da3ce714a07b181e6b7eaece2ecdf074d"
MANIFEST = b"canonical helm manifest\n"
MANIFEST_DIGEST = "sha256:" + hashlib.sha256(MANIFEST).hexdigest()


def config_document():
    return {
        "schema": rfq.SCHEMA,
        "candidate_sha": CANDIDATE,
        "parent_sha": PARENT,
        "repository": "owner/repository",
        "checks": [{"name": "ci", "run_id": 101}, {"name": "build-cli", "run_id": 102}],
        "workflows": [
            {"workflow_id": 201, "state": "disabled_manually"},
            {"workflow_id": 202, "state": "disabled_manually"},
            {"workflow_id": 203, "state": "disabled_manually"},
        ],
        "allowed_zero_job_ghost": {"run_id": 301, "head_sha": GHOST_HEAD},
        "namespace": "fugue-system",
        "release": "fugue",
        "expected_helm_revision": 806,
        "expected_helm_manifest_digest": MANIFEST_DIGEST,
        "ssh_host": "control-plane",
        "kubeconfig": "/etc/kubeconfig",
        "lease_name": "fugue-control-plane-lock",
        "expected_lease_uid": "lease-uid",
        "expected_lease_resource_version": "17",
        "runner_name": "release-runner",
        "runner_labels": ["self-hosted", "linux", "control-plane"],
        "health": [{"name": "api", "url": "https://platform.invalid/healthz"}],
        "command_deadline_seconds": 2,
        "total_deadline_seconds": 10,
    }


def load_test_config(document=None):
    document = document or config_document()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "config.json"
        path.write_bytes(rfq.canonical_json(document))
        return rfq.load_config(path)


def j(value):
    return rfq.canonical_json(value)


def saved_responses(config):
    result = {
        "git_candidate": (CANDIDATE + "\n").encode(),
        "git_parent": (PARENT + "\n").encode(),
        "main": j({"ref": "refs/heads/main", "object": {"type": "commit", "sha": CANDIDATE}}),
        "runner": j({"total_count": 1, "runners": [{"name": config.runner_name, "status": "online", "busy": False, "labels": [{"name": item} for item in config.runner_labels]}]}),
        "helm_status": j({"name": config.release, "namespace": config.namespace, "version": config.expected_helm_revision, "info": {"status": "deployed"}}),
        "helm_manifest": MANIFEST,
        "lease": j({"apiVersion": "coordination.k8s.io/v1", "kind": "Lease", "metadata": {"uid": config.expected_lease_uid, "resourceVersion": config.expected_lease_resource_version, "annotations": {}}, "spec": {"holderIdentity": ""}}),
        "recovery": j({"apiVersion": "v1", "kind": "List", "items": []}),
        "operations": j({"operations": None}),
        "ghost_jobs": j({"total_count": 0, "jobs": []}),
    }
    for check in config.checks:
        result[f"check:{check.name}"] = j({"id": check.run_id, "head_sha": CANDIDATE, "run_attempt": 1, "status": "completed", "conclusion": "success"})
    for workflow in config.workflows:
        result[f"workflow:{workflow.workflow_id}"] = j({"id": workflow.workflow_id, "state": workflow.state})
    for status in rfq.ACTIVE_STATUSES:
        runs = []
        if status == "queued":
            runs = [{"id": config.allowed_zero_job_ghost.run_id, "status": status, "head_sha": config.allowed_zero_job_ghost.head_sha, "run_attempt": 1}]
        result[f"active:{status}"] = j({"total_count": len(runs), "workflow_runs": runs})
    for health in config.health:
        result[f"health:{health.name}"] = j({"status": 200})
    return result


class FakeReader:
    def __init__(self, responses, *, delay=0.0, failure=None):
        self.responses = responses
        self.delay = delay
        self.failure = failure or {}
        self.lock = threading.Lock()
        self.calls = []

    def read(self, spec, config, deadline_ns):
        started = time.monotonic_ns()
        with self.lock:
            self.calls.append(spec.task)
        if self.delay:
            time.sleep(self.delay)
        if spec.task in self.failure:
            raise rfq.QualificationError(self.failure[spec.task], spec.task)
        return rfq.ReadResult(spec.task, self.responses[spec.task], started, time.monotonic_ns())


class ReleaseFastQualificationTests(unittest.TestCase):
    def test_saved_response_happy_path_is_parallel_and_canonical(self):
        config = load_test_config()
        reader = FakeReader(saved_responses(config), delay=0.03)
        started = time.monotonic()
        decision = rfq.qualify_once(config, reader, 1)
        elapsed = time.monotonic() - started
        self.assertEqual(decision["state"], "machine_execution_ready")
        self.assertEqual(decision["reason"], "qualification_verified")
        self.assertLess(elapsed, 0.3)
        self.assertEqual(len(reader.calls), len(rfq.command_specs(config)))
        encoded = rfq.canonical_json(decision)
        self.assertEqual(encoded, rfq.canonical_json(json.loads(encoded)))
        previous = -1
        for event in decision["timeline"]:
            self.assertGreaterEqual(event["elapsed_ns"], previous)
            previous = event["elapsed_ns"]

    def test_ten_runs_are_fresh_and_summarized(self):
        config = load_test_config()
        reader = FakeReader(saved_responses(config))
        result = rfq.run_benchmark(config, reader, 10)
        self.assertEqual(result["summary"]["count"], 10)
        self.assertEqual(result["summary"]["error_count"], 0)
        self.assertEqual(len(reader.calls), 10 * len(rfq.command_specs(config)))
        self.assertEqual(result["legacy_baseline"]["critical_path_floor_seconds"], 2070)
        self.assertEqual(result["legacy_baseline"]["build_cli"], "465s")
        self.assertEqual(result["legacy_baseline"]["monotonic_measurement"], "legacy_unmeasured")
        self.assertEqual(result["legacy_baseline"]["typed_errors"], ["pagination_rc130", "zsh_status_rc1", "curl_rc18"])

    def test_config_rejects_unknown_missing_and_extra_cli_args(self):
        document = config_document()
        document["unknown"] = True
        with self.assertRaises(rfq.QualificationError):
            load_test_config(document)
        with self.assertRaises(SystemExit):
            rfq.parse_args(["--config", "x", "--output", "y", "--runs", "1", "extra"])

    def test_bad_json_main_drift_and_each_legacy_error_fail_closed_quickly(self):
        config = load_test_config()
        for name, task, payload, failure, reason in (
            ("bad_json", "main", b"{", None, "bad_json"),
            ("main_drift", "main", j({"ref": "refs/heads/main", "object": {"type": "commit", "sha": PARENT}}), None, "main_drift"),
            ("pagination", "active:queued", None, "pagination_rc130", "pagination_rc130"),
            ("zsh_status", "main", None, "zsh_status_rc1", "zsh_status_rc1"),
            ("curl18", "health:api", None, "curl_rc18", "curl_rc18"),
            ("timeout", "lease", None, "command_timeout", "command_timeout"),
        ):
            with self.subTest(name=name):
                responses = saved_responses(config)
                if payload is not None:
                    responses[task] = payload
                reader = FakeReader(responses, failure={task: failure} if failure else {})
                started = time.monotonic()
                decision = rfq.qualify_once(config, reader, 1)
                self.assertEqual(decision["state"], "failed_closed")
                self.assertEqual(decision["reason"], reason)
                self.assertLess(time.monotonic() - started, 1.0)

    def test_all_binding_drifts_fail_closed(self):
        config = load_test_config()
        mutators = {
            "check": lambda r: r.__setitem__("check:ci", j({"id": 101, "head_sha": PARENT, "run_attempt": 1, "status": "completed", "conclusion": "success"})),
            "workflow": lambda r: r.__setitem__("workflow:201", j({"id": 201, "state": "active"})),
            "active": lambda r: r.__setitem__("active:in_progress", j({"total_count": 1, "workflow_runs": [{"id": 999, "status": "in_progress", "head_sha": CANDIDATE, "run_attempt": 1}]})),
            "ghost": lambda r: r.__setitem__("ghost_jobs", j({"total_count": 1, "jobs": [{}]})),
            "runner": lambda r: r.__setitem__("runner", j({"total_count": 1, "runners": [{"name": config.runner_name, "status": "offline", "busy": False, "labels": []}]})),
            "helm": lambda r: r.__setitem__("helm_manifest", b"drift\n"),
            "lease": lambda r: r.__setitem__("lease", j({"metadata": {"uid": config.expected_lease_uid, "resourceVersion": "18", "annotations": {}}, "spec": {"holderIdentity": "other"}})),
            "recovery": lambda r: r.__setitem__("recovery", j({"items": [{"metadata": {"name": "release-recovery", "labels": {}, "annotations": {}}}]})),
            "operations": lambda r: r.__setitem__("operations", j({"operations": [{"status": "running"}]})),
            "health": lambda r: r.__setitem__("health:api", j({"status": 503})),
        }
        for name, mutate in mutators.items():
            with self.subTest(name=name):
                responses = saved_responses(config)
                mutate(responses)
                decision = rfq.qualify_once(config, FakeReader(responses), 1)
                self.assertEqual(decision["state"], "failed_closed")

    def test_operations_empty_list_and_null_shapes_are_both_valid(self):
        config = load_test_config()
        for value in (None, []):
            responses = saved_responses(config)
            responses["operations"] = j({"operations": value})
            self.assertEqual(rfq.qualify_once(config, FakeReader(responses), 1)["state"], "machine_execution_ready")

    def test_duplicate_keys_and_multi_document_are_rejected(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":1}{"a":2}', b'{"a":NaN}'):
            with self.assertRaises(rfq.QualificationError):
                rfq.strict_json(raw, "fixture")

    def test_partial_status_or_runner_page_fails_closed(self):
        config = load_test_config()
        responses = saved_responses(config)
        responses["active:queued"] = j({"total_count": 2, "workflow_runs": [{"id": config.allowed_zero_job_ghost.run_id, "status": "queued", "head_sha": config.allowed_zero_job_ghost.head_sha, "run_attempt": 1}]})
        self.assertEqual(rfq.qualify_once(config, FakeReader(responses), 1)["state"], "failed_closed")
        responses = saved_responses(config)
        current = json.loads(responses["runner"])
        current["total_count"] = 2
        responses["runner"] = j(current)
        self.assertEqual(rfq.qualify_once(config, FakeReader(responses), 1)["state"], "failed_closed")

    def test_fixed_commands_have_no_shell_pagination_or_write_verb(self):
        config = load_test_config()
        forbidden = {"jq", "jsonpath", "--paginate", "create", "patch", "delete", "apply", "upgrade", "rollback", "dispatch", "enable", "disable", "cancel", "retry"}
        for spec in rfq.command_specs(config):
            self.assertIsInstance(spec.argv, tuple)
            self.assertNotIn("sh", spec.argv[:1])
            for argument in spec.argv:
                self.assertNotIn("\n", argument)
                self.assertNotIn("\x00", argument)
                self.assertNotIn(argument.lower(), forbidden)
        source = SUBJECT.read_text(encoding="utf-8")
        self.assertNotIn("shell=True", source)
        self.assertNotIn("--paginate", source)
        self.assertNotIn("jsonpath", source)


if __name__ == "__main__":
    unittest.main()
