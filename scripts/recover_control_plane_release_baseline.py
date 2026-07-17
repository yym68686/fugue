#!/usr/bin/env python3
"""Exact-CAS recovery transaction for the control-plane release baseline tag."""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path


SHA_RE = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


class RecoveryError(RuntimeError):
    pass


_active_process: subprocess.Popen[str] | None = None


def terminate_process_group(process: subprocess.Popen[str], grace_seconds: int = 2) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=grace_seconds)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=grace_seconds)
    except subprocess.TimeoutExpired as exc:
        raise RecoveryError("bounded command process group could not be reaped") from exc


def handle_termination_signal(signum: int, _frame: object) -> None:
    process = _active_process
    if process is not None:
        terminate_process_group(process)
    raise RecoveryError(f"received termination signal {signum}")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def validate_sha(name: str, value: str) -> str:
    if not SHA_RE.fullmatch(value):
        raise RecoveryError(f"{name} must be an exact lowercase commit SHA")
    return value


def atomic_write_json(path: Path, document: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(document, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def run_bounded(argv: list[str], timeout_seconds: int) -> str:
    global _active_process
    termination_signals = {signal.SIGINT, signal.SIGTERM}
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, termination_signals)
    try:
        process = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        _active_process = process
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        terminate_process_group(process)
        raise RecoveryError(
            f"bounded command timed out after {timeout_seconds}s: {argv[0]} {argv[1]}"
        ) from exc
    finally:
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, termination_signals)
        try:
            _active_process = None
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
    if process.returncode != 0:
        detail_lines = stderr.strip().splitlines()[-8:]
        detail = " | ".join(line[:512] for line in detail_lines) or "no stderr"
        raise RecoveryError(
            f"bounded command failed with exit {process.returncode}: "
            f"{argv[0]} {argv[1]}: {detail}"
        )
    return stdout


def remote_ref_oid(remote: str, baseline_ref: str, timeout_seconds: int) -> str:
    output = run_bounded(
        ["git", "ls-remote", "--refs", "--exit-code", remote, baseline_ref],
        timeout_seconds,
    )
    lines = [line for line in output.splitlines() if line]
    if len(lines) != 1:
        raise RecoveryError("release baseline lookup did not return exactly one ref")
    fields = lines[0].split("\t")
    if len(fields) != 2 or fields[1] != baseline_ref:
        raise RecoveryError("release baseline lookup returned an unexpected ref")
    return validate_sha("release baseline object", fields[0])


def resolve_graphql_repository_id(repository: str, timeout_seconds: int) -> str:
    owner, name = repository.split("/", 1)
    query = (
        "query($owner:String!,$name:String!){"
        "repository(owner:$owner,name:$name){id}}"
    )
    output = run_bounded(
        [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={query}",
            "-f",
            f"owner={owner}",
            "-f",
            f"name={name}",
            "--jq",
            ".data.repository.id",
        ],
        timeout_seconds,
    )
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if len(lines) != 1 or not re.fullmatch(r"[A-Za-z0-9_=-]+", lines[0]):
        raise RecoveryError("GitHub GraphQL repository lookup returned an invalid node ID")
    return lines[0]


def push_exact_ref_graphql(
    repository: str,
    baseline_ref: str,
    expected_oid: str,
    desired_oid: str,
    timeout_seconds: int,
) -> None:
    repository_id = resolve_graphql_repository_id(repository, timeout_seconds)
    client_mutation_id = (
        f"fugue-release-policy-recovery-{expected_oid[:12]}-{desired_oid[:12]}"
    )
    mutation = (
        "mutation($repositoryId:ID!,$name:GitRefname!,"
        "$beforeOid:GitObjectID!,$afterOid:GitObjectID!,"
        "$force:Boolean!,$clientMutationId:String!){"
        "updateRefs(input:{repositoryId:$repositoryId,"
        "refUpdates:[{name:$name,beforeOid:$beforeOid,"
        "afterOid:$afterOid,force:$force}],"
        "clientMutationId:$clientMutationId}){clientMutationId}}"
    )
    output = run_bounded(
        [
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={mutation}",
            "-f",
            f"repositoryId={repository_id}",
            "-f",
            f"name={baseline_ref}",
            "-f",
            f"beforeOid={expected_oid}",
            "-f",
            f"afterOid={desired_oid}",
            "-F",
            "force=true",
            "-f",
            f"clientMutationId={client_mutation_id}",
            "--jq",
            ".data.updateRefs.clientMutationId",
        ],
        timeout_seconds,
    )
    if output.strip() != client_mutation_id:
        raise RecoveryError("GitHub GraphQL ref CAS did not echo its mutation ID")


def push_exact_ref(
    cas_backend: str,
    github_repository: str,
    remote: str,
    baseline_ref: str,
    expected_oid: str,
    desired_oid: str,
    timeout_seconds: int,
) -> None:
    if cas_backend == "github-graphql":
        push_exact_ref_graphql(
            github_repository,
            baseline_ref,
            expected_oid,
            desired_oid,
            timeout_seconds,
        )
        return
    run_bounded(
        [
            "git",
            "push",
            "--no-follow-tags",
            "--recurse-submodules=no",
            f"--force-with-lease={baseline_ref}:{expected_oid}",
            remote,
            f"{desired_oid}:{baseline_ref}",
        ],
        timeout_seconds,
    )


def base_document(args: argparse.Namespace) -> dict[str, object]:
    document: dict[str, object] = {
        "schema_version": 1,
        "operation": args.operation,
        "baseline_ref": args.baseline_ref,
        "base_sha": args.base_sha,
        "target_sha": args.target_sha,
        "started_at": utc_now(),
        "cluster_mutation_attempted": False,
        "cas_backend": args.cas_backend,
    }
    if args.cas_backend == "github-graphql":
        document["github_repository"] = args.github_repository
    return document


def transact(args: argparse.Namespace, evidence: dict[str, object]) -> None:
    evidence["phase"] = "observe-pre-state"
    pre_oid = remote_ref_oid(args.remote, args.baseline_ref, args.timeout_seconds)
    evidence["pre_oid"] = pre_oid
    if pre_oid != args.base_sha:
        raise RecoveryError("release baseline pre-state is not the authorized base SHA")

    evidence["phase"] = "advance"
    push_exact_ref(
        args.cas_backend,
        args.github_repository,
        args.remote,
        args.baseline_ref,
        args.base_sha,
        args.target_sha,
        args.timeout_seconds,
    )
    forward_oid = remote_ref_oid(args.remote, args.baseline_ref, args.timeout_seconds)
    evidence["forward_oid"] = forward_oid
    if forward_oid != args.target_sha:
        raise RecoveryError("release baseline advance was not observed at the target SHA")

    evidence["phase"] = "rollback"
    push_exact_ref(
        args.cas_backend,
        args.github_repository,
        args.remote,
        args.baseline_ref,
        args.target_sha,
        args.base_sha,
        args.timeout_seconds,
    )
    rollback_oid = remote_ref_oid(args.remote, args.baseline_ref, args.timeout_seconds)
    evidence["rollback_oid"] = rollback_oid
    if rollback_oid != args.base_sha:
        raise RecoveryError("release baseline rollback was not observed at the base SHA")

    evidence["phase"] = "re-advance"
    push_exact_ref(
        args.cas_backend,
        args.github_repository,
        args.remote,
        args.baseline_ref,
        args.base_sha,
        args.target_sha,
        args.timeout_seconds,
    )
    final_oid = remote_ref_oid(args.remote, args.baseline_ref, args.timeout_seconds)
    evidence["final_oid"] = final_oid
    if final_oid != args.target_sha:
        raise RecoveryError("release baseline final state is not the target SHA")
    evidence["phase"] = "complete"
    evidence["rollback_verification"] = "succeeded"
    evidence["outcome"] = "recovered"


def compensate(args: argparse.Namespace, evidence: dict[str, object]) -> None:
    evidence["phase"] = "observe-failed-transaction"
    observed_oid = remote_ref_oid(args.remote, args.baseline_ref, args.timeout_seconds)
    evidence["observed_oid"] = observed_oid
    if observed_oid == args.target_sha:
        evidence["phase"] = "compensate-target-to-base"
        push_exact_ref(
            args.cas_backend,
            args.github_repository,
            args.remote,
            args.baseline_ref,
            args.target_sha,
            args.base_sha,
            args.timeout_seconds,
        )
        disposition = "restored-target-to-base"
    elif observed_oid == args.base_sha:
        disposition = "already-at-base"
    else:
        evidence["compensation_disposition"] = "unexpected-oid-refused"
        raise RecoveryError(
            "release baseline is neither the authorized base nor target; refusing overwrite"
        )

    final_oid = remote_ref_oid(args.remote, args.baseline_ref, args.timeout_seconds)
    evidence["final_oid"] = final_oid
    evidence["compensation_disposition"] = disposition
    if final_oid != args.base_sha:
        raise RecoveryError("release baseline compensation did not finish at the base SHA")
    evidence["phase"] = "complete"
    evidence["outcome"] = "rolled-back"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=("transact", "compensate"))
    parser.add_argument("--remote", default="origin")
    parser.add_argument(
        "--cas-backend", choices=("git", "github-graphql"), default="git"
    )
    parser.add_argument(
        "--github-repository", default=os.environ.get("GITHUB_REPOSITORY", "")
    )
    parser.add_argument(
        "--baseline-ref", default="refs/tags/fugue-control-plane-release-baseline"
    )
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--target-sha", required=True)
    parser.add_argument("--evidence", required=True, type=Path)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    args = parser.parse_args()
    validate_sha("base SHA", args.base_sha)
    validate_sha("target SHA", args.target_sha)
    if args.base_sha == args.target_sha:
        raise RecoveryError("base and target SHA must differ")
    if args.cas_backend == "github-graphql":
        if not REPOSITORY_RE.fullmatch(args.github_repository):
            raise RecoveryError(
                "github-repository must be an exact owner/name for GraphQL CAS"
            )
        if not os.environ.get("GH_TOKEN"):
            raise RecoveryError("GH_TOKEN is required for GitHub GraphQL CAS")
    if not re.fullmatch(r"refs/tags/[A-Za-z0-9._/-]+", args.baseline_ref):
        raise RecoveryError("baseline ref must be a canonical tag ref")
    if not 1 <= args.timeout_seconds <= 300:
        raise RecoveryError("command timeout must be between 1 and 300 seconds")
    return args


def main() -> int:
    signal.signal(signal.SIGTERM, handle_termination_signal)
    signal.signal(signal.SIGINT, handle_termination_signal)
    try:
        args = parse_args()
    except RecoveryError as exc:
        print(f"release baseline recovery input error: {exc}", file=sys.stderr)
        return 2

    evidence = base_document(args)
    status = 0
    try:
        if args.operation == "transact":
            transact(args, evidence)
        else:
            compensate(args, evidence)
    except (RecoveryError, OSError) as exc:
        evidence["outcome"] = "failed"
        evidence["error"] = str(exc)
        try:
            evidence["failure_observed_oid"] = remote_ref_oid(
                args.remote, args.baseline_ref, args.timeout_seconds
            )
        except (RecoveryError, OSError) as observe_exc:
            evidence["failure_observation_error"] = str(observe_exc)
        print(f"release baseline recovery failed: {exc}", file=sys.stderr)
        status = 1
    evidence["finished_at"] = utc_now()
    evidence["elapsed_seconds"] = round(
        time.time() - datetime.fromisoformat(str(evidence["started_at"])).timestamp(), 3
    )
    atomic_write_json(args.evidence, evidence)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
