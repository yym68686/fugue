#!/usr/bin/env python3
"""Bounded, read-only release qualification with canonical timing evidence."""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import datetime as dt
import hashlib
import json
import os
import re
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence


SCHEMA = "release-fast-qualification.fugue.dev/v1"
BENCHMARK_SCHEMA = "release-fast-qualification-benchmark.fugue.dev/v1"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,254}$")
REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
ACTIVE_STATUSES = ("queued", "in_progress", "waiting", "pending", "requested")
MAX_BYTES = 2 * 1024 * 1024


class QualificationError(Exception):
    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail[:256]


@dataclasses.dataclass(frozen=True)
class CheckConfig:
    name: str
    run_id: int


@dataclasses.dataclass(frozen=True)
class WorkflowConfig:
    workflow_id: int
    state: str


@dataclasses.dataclass(frozen=True)
class GhostConfig:
    run_id: int
    head_sha: str


@dataclasses.dataclass(frozen=True)
class HealthConfig:
    name: str
    url: str


@dataclasses.dataclass(frozen=True)
class Config:
    schema: str
    candidate_sha: str
    parent_sha: str
    repository: str
    checks: tuple[CheckConfig, ...]
    workflows: tuple[WorkflowConfig, ...]
    allowed_zero_job_ghost: GhostConfig
    namespace: str
    release: str
    expected_helm_revision: int
    expected_helm_manifest_digest: str
    ssh_host: str
    kubeconfig: str
    lease_name: str
    expected_lease_uid: str
    expected_lease_resource_version: str
    runner_name: str
    runner_labels: tuple[str, ...]
    health: tuple[HealthConfig, ...]
    command_deadline_seconds: float
    total_deadline_seconds: float


@dataclasses.dataclass(frozen=True)
class CommandSpec:
    task: str
    argv: tuple[str, ...]
    json_output: bool


@dataclasses.dataclass(frozen=True)
class ReadResult:
    task: str
    payload: bytes
    started_ns: int
    completed_ns: int


class Reader(Protocol):
    def read(self, spec: CommandSpec, config: Config, deadline_ns: int) -> ReadResult: ...


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise QualificationError("bad_json", f"duplicate key {key}")
        value[key] = item
    return value


def strict_json(raw: bytes, task: str) -> Any:
    if not raw or len(raw) > MAX_BYTES:
        raise QualificationError("bad_json", f"{task} size")
    try:
        text = raw.decode("utf-8")
        decoder = json.JSONDecoder(
            object_pairs_hook=_unique_object,
            parse_constant=lambda value: (_ for _ in ()).throw(
                QualificationError("bad_json", f"{task} non-finite {value}")
            ),
        )
        value, end = decoder.raw_decode(text)
        if text[end:].strip():
            raise QualificationError("bad_json", f"{task} multiple documents")
        return value
    except QualificationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise QualificationError("bad_json", f"{task}: {type(exc).__name__}") from exc


def _exact_object(value: Any, keys: set[str], location: str) -> Mapping[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise QualificationError("config_invalid", f"{location} keys")
    return value


def _positive_int(value: Any, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise QualificationError("config_invalid", location)
    return value


def load_config(path: Path) -> Config:
    raw = path.read_bytes()
    value = _exact_object(
        strict_json(raw, "config"),
        {
            "schema", "candidate_sha", "parent_sha", "repository", "checks", "workflows",
            "allowed_zero_job_ghost", "namespace", "release", "expected_helm_revision",
            "expected_helm_manifest_digest", "ssh_host", "kubeconfig", "lease_name",
            "expected_lease_uid", "expected_lease_resource_version", "runner_name",
            "runner_labels", "health", "command_deadline_seconds", "total_deadline_seconds",
        },
        "config",
    )
    checks_raw = value["checks"]
    workflows_raw = value["workflows"]
    health_raw = value["health"]
    if not isinstance(checks_raw, list) or len(checks_raw) < 2:
        raise QualificationError("config_invalid", "checks")
    if not isinstance(workflows_raw, list) or len(workflows_raw) != 3:
        raise QualificationError("config_invalid", "workflows")
    if not isinstance(health_raw, list) or not health_raw:
        raise QualificationError("config_invalid", "health")
    checks = tuple(
        CheckConfig(str(_exact_object(item, {"name", "run_id"}, "check")["name"]), _positive_int(item["run_id"], "check.run_id"))
        for item in checks_raw
    )
    workflows = tuple(
        WorkflowConfig(_positive_int(_exact_object(item, {"workflow_id", "state"}, "workflow")["workflow_id"], "workflow_id"), str(item["state"]))
        for item in workflows_raw
    )
    ghost_raw = _exact_object(value["allowed_zero_job_ghost"], {"run_id", "head_sha"}, "ghost")
    health = tuple(
        HealthConfig(str(_exact_object(item, {"name", "url"}, "health")["name"]), str(item["url"]))
        for item in health_raw
    )
    command_deadline = value["command_deadline_seconds"]
    total_deadline = value["total_deadline_seconds"]
    if (
        isinstance(command_deadline, bool)
        or not isinstance(command_deadline, (int, float))
        or isinstance(total_deadline, bool)
        or not isinstance(total_deadline, (int, float))
    ):
        raise QualificationError("config_invalid", "deadline type")
    config = Config(
        schema=str(value["schema"]), candidate_sha=str(value["candidate_sha"]), parent_sha=str(value["parent_sha"]),
        repository=str(value["repository"]), checks=checks, workflows=workflows,
        allowed_zero_job_ghost=GhostConfig(_positive_int(ghost_raw["run_id"], "ghost.run_id"), str(ghost_raw["head_sha"])),
        namespace=str(value["namespace"]), release=str(value["release"]),
        expected_helm_revision=_positive_int(value["expected_helm_revision"], "expected_helm_revision"),
        expected_helm_manifest_digest=str(value["expected_helm_manifest_digest"]), ssh_host=str(value["ssh_host"]),
        kubeconfig=str(value["kubeconfig"]), lease_name=str(value["lease_name"]),
        expected_lease_uid=str(value["expected_lease_uid"]),
        expected_lease_resource_version=str(value["expected_lease_resource_version"]), runner_name=str(value["runner_name"]),
        runner_labels=tuple(str(item) for item in value["runner_labels"]), health=health,
        command_deadline_seconds=float(command_deadline), total_deadline_seconds=float(total_deadline),
    )
    validate_config(config)
    return config


def validate_config(config: Config) -> None:
    if config.schema != SCHEMA or not SHA_RE.fullmatch(config.candidate_sha) or not SHA_RE.fullmatch(config.parent_sha):
        raise QualificationError("config_invalid", "candidate")
    if not REPO_RE.fullmatch(config.repository) or not DIGEST_RE.fullmatch(config.expected_helm_manifest_digest):
        raise QualificationError("config_invalid", "repository or digest")
    for value in (config.namespace, config.release, config.ssh_host, config.lease_name, config.runner_name, *config.runner_labels):
        if not NAME_RE.fullmatch(value):
            raise QualificationError("config_invalid", "name")
    if not config.kubeconfig.startswith("/") or "\x00" in config.kubeconfig or "\n" in config.kubeconfig:
        raise QualificationError("config_invalid", "kubeconfig")
    if len({item.name for item in config.checks}) != len(config.checks) or len({item.run_id for item in config.checks}) != len(config.checks):
        raise QualificationError("config_invalid", "duplicate checks")
    if len({item.workflow_id for item in config.workflows}) != len(config.workflows):
        raise QualificationError("config_invalid", "duplicate workflows")
    if any(item.state != "disabled_manually" for item in config.workflows):
        raise QualificationError("config_invalid", "workflow state")
    if any(not NAME_RE.fullmatch(item.name) or not item.url.startswith("https://") for item in config.health):
        raise QualificationError("config_invalid", "health")
    if not (0 < config.command_deadline_seconds <= 15 and 0 < config.total_deadline_seconds < 45):
        raise QualificationError("config_invalid", "deadline")


def command_specs(config: Config) -> list[CommandSpec]:
    repo = config.repository
    specs = [
        CommandSpec("git_candidate", ("git", "rev-parse", f"{config.candidate_sha}^{{commit}}"), False),
        CommandSpec("git_parent", ("git", "rev-parse", f"{config.candidate_sha}^"), False),
        CommandSpec("main", ("gh", "api", f"repos/{repo}/git/ref/heads/main"), True),
        CommandSpec("runner", ("gh", "api", f"repos/{repo}/actions/runners?per_page=100"), True),
        CommandSpec(
            "helm_status",
            ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ConnectionAttempts=1", config.ssh_host,
             "env", f"KUBECONFIG={config.kubeconfig}", "helm", "status", config.release, "-n", config.namespace, "-o", "json"),
            True,
        ),
        CommandSpec(
            "helm_manifest",
            ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ConnectionAttempts=1", config.ssh_host,
             "env", f"KUBECONFIG={config.kubeconfig}", "helm", "get", "manifest", config.release, "-n", config.namespace,
             "--revision", str(config.expected_helm_revision)),
            False,
        ),
        CommandSpec(
            "lease",
            ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ConnectionAttempts=1", config.ssh_host,
             "env", f"KUBECONFIG={config.kubeconfig}", "kubectl", "-n", config.namespace, "get", "lease", config.lease_name, "-o", "json"),
            True,
        ),
        CommandSpec(
            "recovery",
            ("ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ConnectionAttempts=1", config.ssh_host,
             "env", f"KUBECONFIG={config.kubeconfig}", "kubectl", "-n", config.namespace, "get", "configmaps", "-o", "json"),
            True,
        ),
        CommandSpec("operations", ("fugue", "operation", "ls", "--status", "pending", "--status", "running", "--status", "waiting", "--all", "--output", "json"), True),
    ]
    specs.extend(CommandSpec(f"check:{item.name}", ("gh", "api", f"repos/{repo}/actions/runs/{item.run_id}"), True) for item in config.checks)
    specs.extend(CommandSpec(f"workflow:{item.workflow_id}", ("gh", "api", f"repos/{repo}/actions/workflows/{item.workflow_id}"), True) for item in config.workflows)
    specs.extend(CommandSpec(f"active:{status}", ("gh", "api", f"repos/{repo}/actions/runs?status={status}&per_page=100"), True) for status in ACTIVE_STATUSES)
    specs.append(CommandSpec("ghost_jobs", ("gh", "api", f"repos/{repo}/actions/runs/{config.allowed_zero_job_ghost.run_id}/jobs"), True))
    specs.extend(CommandSpec(f"health:{item.name}", (item.url,), True) for item in config.health)
    return specs


class SubprocessReader:
    def read(self, spec: CommandSpec, config: Config, deadline_ns: int) -> ReadResult:
        started = time.monotonic_ns()
        remaining = max(0.001, min(config.command_deadline_seconds, (deadline_ns - started) / 1_000_000_000))
        if spec.task.startswith("health:"):
            request = urllib.request.Request(spec.argv[0], headers={"Connection": "close", "Accept-Encoding": "identity"})
            try:
                with urllib.request.urlopen(request, timeout=remaining) as response:
                    body = response.read(MAX_BYTES + 1)
                    if len(body) > MAX_BYTES:
                        raise QualificationError("response_too_large", spec.task)
                    payload = canonical_json({"status": response.status})
            except TimeoutError as exc:
                raise QualificationError("command_timeout", spec.task) from exc
            except Exception as exc:
                raise QualificationError("health_read_failed", f"{spec.task}:{type(exc).__name__}") from exc
        else:
            try:
                process = subprocess.run(spec.argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=remaining, check=False)
            except subprocess.TimeoutExpired as exc:
                raise QualificationError("command_timeout", spec.task) from exc
            except OSError as exc:
                raise QualificationError("command_failed", f"{spec.task}:{type(exc).__name__}") from exc
            if process.returncode != 0:
                raise QualificationError("command_failed", f"{spec.task}:rc{process.returncode}")
            payload = process.stdout
        if len(payload) > MAX_BYTES:
            raise QualificationError("response_too_large", spec.task)
        if spec.json_output:
            strict_json(payload, spec.task)
        return ReadResult(spec.task, payload, started, time.monotonic_ns())


def _required_string(value: Mapping[str, Any], key: str, task: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise QualificationError("bad_json", f"{task}.{key}")
    return item


def validate_results(config: Config, results: Mapping[str, ReadResult]) -> str:
    if results["git_candidate"].payload.decode().strip() != config.candidate_sha or results["git_parent"].payload.decode().strip() != config.parent_sha:
        raise QualificationError("git_drift", "candidate ancestry")
    main = strict_json(results["main"].payload, "main")
    if not isinstance(main, dict) or main.get("ref") != "refs/heads/main" or not isinstance(main.get("object"), dict) or main["object"].get("type") != "commit" or main["object"].get("sha") != config.candidate_sha:
        raise QualificationError("main_drift")
    for check in config.checks:
        value = strict_json(results[f"check:{check.name}"].payload, f"check:{check.name}")
        if not isinstance(value, dict) or value.get("id") != check.run_id or value.get("head_sha") != config.candidate_sha or value.get("run_attempt") != 1 or value.get("status") != "completed" or value.get("conclusion") != "success":
            raise QualificationError("check_drift", check.name)
    for workflow in config.workflows:
        value = strict_json(results[f"workflow:{workflow.workflow_id}"].payload, "workflow")
        if not isinstance(value, dict) or value.get("id") != workflow.workflow_id or value.get("state") != workflow.state:
            raise QualificationError("workflow_drift", str(workflow.workflow_id))
    active: list[Mapping[str, Any]] = []
    for status in ACTIVE_STATUSES:
        value = strict_json(results[f"active:{status}"].payload, f"active:{status}")
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("total_count"), int)
            or not isinstance(value.get("workflow_runs"), list)
            or value["total_count"] != len(value["workflow_runs"])
            or len(value["workflow_runs"]) > 100
        ):
            raise QualificationError("bad_json", f"active:{status}")
        for run in value["workflow_runs"]:
            if not isinstance(run, dict) or run.get("status") != status or not isinstance(run.get("id"), int):
                raise QualificationError("bad_json", f"active:{status}.run")
            active.append(run)
    ghost = config.allowed_zero_job_ghost
    for run in active:
        if run["id"] != ghost.run_id or run.get("head_sha") != ghost.head_sha or run.get("run_attempt") != 1:
            raise QualificationError("active_run", str(run["id"]))
    jobs = strict_json(results["ghost_jobs"].payload, "ghost_jobs")
    if not isinstance(jobs, dict) or jobs.get("total_count") != 0 or jobs.get("jobs") != []:
        raise QualificationError("ghost_drift")
    runners = strict_json(results["runner"].payload, "runner")
    if (
        not isinstance(runners, dict)
        or not isinstance(runners.get("runners"), list)
        or runners.get("total_count") != len(runners["runners"])
        or len(runners["runners"]) > 100
    ):
        raise QualificationError("bad_json", "runners")
    matches = [item for item in runners["runners"] if isinstance(item, dict) and item.get("name") == config.runner_name]
    if len(matches) != 1 or matches[0].get("status") != "online" or matches[0].get("busy") is not False:
        raise QualificationError("runner_drift")
    labels = matches[0].get("labels")
    if not isinstance(labels, list) or not set(config.runner_labels).issubset({item.get("name") for item in labels if isinstance(item, dict)}):
        raise QualificationError("runner_drift", "labels")
    helm = strict_json(results["helm_status"].payload, "helm_status")
    if not isinstance(helm, dict) or helm.get("name") != config.release or helm.get("namespace") != config.namespace or helm.get("version") != config.expected_helm_revision or not isinstance(helm.get("info"), dict) or helm["info"].get("status") != "deployed":
        raise QualificationError("helm_drift", "status")
    manifest_digest = "sha256:" + hashlib.sha256(results["helm_manifest"].payload).hexdigest()
    if manifest_digest != config.expected_helm_manifest_digest:
        raise QualificationError("helm_drift", "manifest")
    lease = strict_json(results["lease"].payload, "lease")
    metadata = lease.get("metadata") if isinstance(lease, dict) else None
    spec = lease.get("spec") if isinstance(lease, dict) else None
    if not isinstance(metadata, dict) or not isinstance(spec, dict) or metadata.get("uid") != config.expected_lease_uid or metadata.get("resourceVersion") != config.expected_lease_resource_version or metadata.get("deletionTimestamp") is not None or str(spec.get("holderIdentity") or "").strip():
        raise QualificationError("lease_drift")
    annotations = metadata.get("annotations") or {}
    if not isinstance(annotations, dict) or annotations.get("fugue.pro/recovery-required") == "true" or "fugue.pro/coordination-token" in annotations:
        raise QualificationError("lease_drift", "fence")
    recovery = strict_json(results["recovery"].payload, "recovery")
    if not isinstance(recovery, dict) or not isinstance(recovery.get("items"), list):
        raise QualificationError("bad_json", "recovery")
    for item in recovery["items"]:
        if not isinstance(item, dict) or not isinstance(item.get("metadata"), dict):
            raise QualificationError("bad_json", "recovery.item")
        meta = item["metadata"]
        labels = meta.get("labels") or {}
        anns = meta.get("annotations") or {}
        if "recovery" in str(meta.get("name") or "") or "recovery" in str(labels.get("app.kubernetes.io/component") or "") or anns.get("fugue.pro/recovery-required") == "true":
            raise QualificationError("recovery_present")
    operations = strict_json(results["operations"].payload, "operations")
    raw_operations = operations.get("operations") if isinstance(operations, dict) else object()
    if raw_operations is None:
        raw_operations = []
    if not isinstance(raw_operations, list):
        raise QualificationError("bad_json", "operations")
    if any(isinstance(item, dict) and str(item.get("status") or "").lower() in {"pending", "running", "waiting"} for item in raw_operations):
        raise QualificationError("active_operations")
    for health in config.health:
        value = strict_json(results[f"health:{health.name}"].payload, f"health:{health.name}")
        if not isinstance(value, dict) or value.get("status") != 200:
            raise QualificationError("health_failed", health.name)
    evidence = {task: "sha256:" + hashlib.sha256(result.payload).hexdigest() for task, result in sorted(results.items())}
    return "sha256:" + hashlib.sha256(canonical_json(evidence)).hexdigest()


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode("ascii")


def qualify_once(config: Config, reader: Reader, run_number: int) -> dict[str, Any]:
    start_ns = time.monotonic_ns()
    start_utc = utc_now()
    deadline_ns = start_ns + int(config.total_deadline_seconds * 1_000_000_000)
    timeline: list[dict[str, Any]] = [{"sequence": 1, "event": "code_ready", "task": "candidate", "utc": start_utc, "elapsed_ns": 0, "reason": "automatic_qualification_started"}]
    specs = command_specs(config)
    results: dict[str, ReadResult] = {}
    reason = "qualification_verified"
    state = "machine_execution_ready"
    evidence_digest = ""
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=min(32, len(specs)), thread_name_prefix="release-qualify")
    futures = {executor.submit(reader.read, spec, config, deadline_ns): spec for spec in specs}
    try:
        for future in concurrent.futures.as_completed(futures, timeout=config.total_deadline_seconds):
            spec = futures[future]
            try:
                result = future.result()
            except QualificationError:
                raise
            except Exception as exc:
                raise QualificationError("read_failed", f"{spec.task}:{type(exc).__name__}") from exc
            results[result.task] = result
        if len(results) != len(specs):
            raise QualificationError("evidence_unknown", "incomplete result set")
        for result in sorted(results.values(), key=lambda item: (item.completed_ns, item.task)):
            timeline.append({"sequence": len(timeline) + 1, "event": "evidence_read", "task": result.task, "utc": utc_now(), "elapsed_ns": result.completed_ns - start_ns, "reason": "fresh_read_complete"})
        evidence_digest = validate_results(config, results)
    except concurrent.futures.TimeoutError as exc:
        state, reason = "failed_closed", "total_deadline_exceeded"
        raise_error: QualificationError | None = QualificationError(reason)
    except QualificationError as exc:
        state, reason = "failed_closed", exc.reason
        raise_error = exc
    else:
        raise_error = None
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    end_ns = time.monotonic_ns()
    timeline.append({"sequence": len(timeline) + 1, "event": "decision", "task": "qualification", "utc": utc_now(), "elapsed_ns": end_ns - start_ns, "reason": reason})
    decision = {"schema": SCHEMA, "run": run_number, "candidate_sha": config.candidate_sha, "state": state, "reason": reason, "started_utc": start_utc, "finished_utc": timeline[-1]["utc"], "elapsed_ns": end_ns - start_ns, "evidence_digest": evidence_digest, "timeline": timeline}
    if raise_error is not None:
        decision["detail_class"] = raise_error.detail
    return decision


def percentile(values: Sequence[int], fraction: float) -> int:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int((len(ordered) - 1) * fraction + 0.999999)))
    return ordered[index]


def run_benchmark(config: Config, reader: Reader, runs: int) -> dict[str, Any]:
    if runs not in (1, 10):
        raise QualificationError("config_invalid", "runs")
    decisions = [qualify_once(config, reader, index + 1) for index in range(runs)]
    elapsed = [int(item["elapsed_ns"]) for item in decisions]
    errors = sum(item["state"] != "machine_execution_ready" for item in decisions)
    return {
        "schema": BENCHMARK_SCHEMA,
        "candidate_sha": config.candidate_sha,
        "generated_at": utc_now(),
        "runs": decisions,
        "summary": {"count": runs, "p50_ns": int(statistics.median(elapsed)), "p95_ns": percentile(elapsed, 0.95), "max_ns": max(elapsed), "error_count": errors},
        "legacy_baseline": {
            "code_ready_utc": "2026-08-02T04:53:50Z",
            "machine_run_created_utc": "2026-08-02T05:20:35Z",
            "code_ready_to_run": "26m45s",
            "qualification_wait": "23m13.421s",
            "qualification_rebuild": "3m27.710s",
            "run_materialization": "3.869s",
            "ci": "148s",
            "build_cli": "465s",
            "code_ready_to_build_complete": "34m30s",
            "critical_path_floor_seconds": 2070,
            "monotonic_measurement": "legacy_unmeasured",
            "typed_errors": ["pagination_rc130", "zsh_status_rc1", "curl_rc18"],
        },
    }


def write_output(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp.{os.getpid()}")
    with temporary.open("xb") as handle:
        os.chmod(temporary, 0o600)
        handle.write(canonical_json(value))
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--runs", required=True, type=int, choices=(1, 10))
    return parser.parse_args(argv)


def main(argv: Sequence[str]) -> int:
    try:
        args = parse_args(argv)
        config = load_config(args.config)
        result = run_benchmark(config, SubprocessReader(), args.runs)
        write_output(args.output, result)
        return 0 if result["summary"]["error_count"] == 0 else 1
    except QualificationError as exc:
        sys.stderr.write(f"release-fast-qualify: {exc.reason}\n")
        return 2
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"release-fast-qualify: local_input_error:{type(exc).__name__}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
