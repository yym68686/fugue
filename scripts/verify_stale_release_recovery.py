#!/usr/bin/env python3
"""Build a fail-closed proof for one explicitly authorized stale release Lease.

The proof contains no credential or Lease fencing token. It is bound to the
current Actions run and is consumed by the upgrade script immediately before
an atomic old-owner -> new-owner Lease CAS.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.parse import quote, urlsplit


class RecoveryProofError(RuntimeError):
    pass


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RecoveryProofError(message)


def parse_release_holder(holder: str) -> tuple[int, int]:
    match = re.fullmatch(r"release/([1-9][0-9]*)-([1-9][0-9]*)", holder)
    _require(match is not None, "expected holder is not release/<run-id>-<attempt>")
    return int(match.group(1)), int(match.group(2))


def git_blob_sha1(payload: bytes) -> str:
    prefix = f"blob {len(payload)}\0".encode()
    return hashlib.sha1(prefix + payload, usedforsecurity=False).hexdigest()


def validate_release_source(
    payload: bytes,
    *,
    expected_blob: str,
    expected_failure: str,
) -> None:
    _require(
        re.fullmatch(r"[0-9a-f]{40}", expected_blob) is not None,
        "expected script blob must be a 40-character SHA-1",
    )
    _require(git_blob_sha1(payload) == expected_blob, "historical release script blob changed")
    source = payload.decode("utf-8")
    fail_start = source.find("\nfail() {")
    _require(fail_start >= 0, "historical release script has no fail function")
    fail_end = source.find("\n}", fail_start)
    _require(fail_end > fail_start, "historical fail function is truncated")
    _require("exit 1" in source[fail_start:fail_end], "historical fail function is not terminal")

    failure_call = f'fail "{expected_failure}"'
    _require(source.count(failure_call) == 1, "expected pre-Helm failure call is not unique")
    failure_position = source.index(failure_call)
    mutation_marker = 'CONTROL_PLANE_RELEASE_HELM_MUTATION_STARTED="true"'
    _require(source.count(mutation_marker) == 1, "historical Helm mutation marker is not unique")
    mutation_position = source.index(mutation_marker)
    helm_position = source.find('helm upgrade "${FUGUE_RELEASE_NAME}"', mutation_position)
    _require(helm_position > mutation_position, "historical Helm command is missing or out of order")
    _require(
        failure_position < mutation_position < helm_position,
        "expected failure is not provably before the historical Helm mutation",
    )
    _require(
        '{"op": "add", "path": "/spec/leaseDurationSeconds", "value": 0}' in source,
        "historical source does not contain the diagnosed Lease release defect",
    )


def _repository_name(payload: dict[str, Any]) -> str:
    repository = payload.get("repository") or {}
    return str(repository.get("full_name") or "") if isinstance(repository, dict) else ""


def validate_github_evidence(
    attempt_payload: dict[str, Any],
    run_payload: dict[str, Any],
    jobs_payload: dict[str, Any],
    *,
    repository: str,
    workflow_path: str,
    expected_run_id: int,
    expected_attempt: int,
    expected_head_sha: str,
    expected_job_name: str,
    expected_runner_name: str,
) -> int:
    for label, payload in (("attempt", attempt_payload), ("run", run_payload)):
        _require(int(payload.get("id") or 0) == expected_run_id, f"{label} run id differs")
        _require(int(payload.get("run_attempt") or 0) == expected_attempt, f"{label} attempt differs")
        _require(payload.get("status") == "completed", f"{label} is not completed")
        _require(payload.get("conclusion") == "failure", f"{label} did not fail")
        _require(payload.get("path") == workflow_path, f"{label} workflow path differs")
        _require(payload.get("head_sha") == expected_head_sha, f"{label} head SHA differs")
        _require(_repository_name(payload) == repository, f"{label} repository differs")

    jobs = jobs_payload.get("jobs") or []
    _require(isinstance(jobs, list), "GitHub jobs response is malformed")
    matches = [job for job in jobs if job.get("name") == expected_job_name]
    _require(len(matches) == 1, "expected deploy job is not unique")
    job = matches[0]
    _require(int(job.get("run_attempt") or 0) == expected_attempt, "deploy job attempt differs")
    _require(job.get("status") == "completed", "deploy job is not completed")
    _require(job.get("conclusion") == "failure", "deploy job did not fail")
    _require(job.get("runner_name") == expected_runner_name, "deploy job ran on another runner")
    job_id = int(job.get("id") or 0)
    _require(job_id > 0, "deploy job id is missing")
    return job_id


def validate_job_log(log_text: str, *, expected_failure: str, expected_revision: int) -> None:
    required_once = (
        f"ERROR: {expected_failure}",
        f"previous Helm revision: {expected_revision}",
        "spec.leaseDurationSeconds: Invalid value: 0: must be greater than 0",
        "owner-CAS release of the control-plane backup coordination Lease failed; holder was not overwritten",
    )
    for marker in required_once:
        _require(log_text.count(marker) == 1, f"historical job log marker is not unique: {marker}")
    _require("Helm upgrade" not in log_text, "historical job log reached the Helm mutation command")


def validate_helm_history(history: Any, *, expected_revision: int) -> None:
    _require(isinstance(history, list) and history, "Helm history is empty or malformed")
    revisions: list[tuple[int, str]] = []
    for entry in history:
        _require(isinstance(entry, dict), "Helm history entry is malformed")
        try:
            revision = int(entry.get("revision"))
        except (TypeError, ValueError) as exc:
            raise RecoveryProofError("Helm history has a non-numeric revision") from exc
        revisions.append((revision, str(entry.get("status") or "")))
    _require(max(revision for revision, _ in revisions) == expected_revision, "Helm has a newer revision")
    expected = [status for revision, status in revisions if revision == expected_revision]
    _require(expected == ["deployed"], "expected Helm revision is not uniquely deployed")


def assert_old_run_process_absent(old_run_id: int, *, proc_root: Path = Path("/proc")) -> None:
    _require(proc_root.is_dir(), "Linux procfs is unavailable")
    own_uid = os.geteuid()
    needle = f"GITHUB_RUN_ID={old_run_id}".encode()
    for process_dir in proc_root.iterdir():
        if not process_dir.name.isdigit():
            continue
        try:
            if process_dir.stat().st_uid != own_uid:
                continue
            environ = (process_dir / "environ").read_bytes()
        except (FileNotFoundError, ProcessLookupError):
            continue
        except PermissionError as exc:
            raise RecoveryProofError("cannot inspect a same-user process environment") from exc
        except OSError as exc:
            raise RecoveryProofError("cannot inspect a same-user process environment") from exc
        if needle in environ.split(b"\0"):
            raise RecoveryProofError(f"old Actions run still has a live process: pid={process_dir.name}")


def _curl_download(url: str, token: str, output: Path, *, limit: int) -> None:
    _require(not any(character in token for character in '\r\n"'), "GITHUB_TOKEN is malformed")
    _require(not any(character in url for character in '\r\n"'), "GitHub evidence URL is malformed")
    descriptor, header_name = tempfile.mkstemp(prefix="fugue-github-headers.")
    os.close(descriptor)
    headers = Path(header_name)
    os.chmod(headers, stat.S_IRUSR | stat.S_IWUSR)
    config = "\n".join(
        (
            "silent",
            "show-error",
            "fail",
            "connect-timeout = 10",
            "max-time = 30",
            f'url = "{url}"',
            'header = "Accept: application/vnd.github+json"',
            f'header = "Authorization: Bearer {token}"',
            'header = "X-GitHub-Api-Version: 2022-11-28"',
            "",
        )
    ).encode()
    try:
        completed = subprocess.run(
            [
                "curl",
                "--config",
                "-",
                "--proto",
                "=https",
                "--proto-redir",
                "=https",
                "--max-filesize",
                str(limit),
                "--dump-header",
                str(headers),
                "--write-out",
                "%{http_code}",
                "--output",
                str(output),
            ],
            input=config,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            timeout=35,
        )
        _require(completed.returncode == 0, "GitHub recovery evidence request failed")
        try:
            status = int(completed.stdout.decode("ascii"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise RecoveryProofError("GitHub recovery evidence returned no HTTP status") from exc
        if 200 <= status < 300:
            return
        _require(300 <= status < 400, "GitHub recovery evidence request failed")
        header_text = headers.read_text(encoding="iso-8859-1")
        locations = re.findall(r"(?im)^location:[ \t]*(\S+)[ \t]*$", header_text)
        _require(len(locations) == 1, "GitHub recovery redirect is missing or ambiguous")
        redirected_url = locations[0]
        redirected = urlsplit(redirected_url)
        _require(redirected.scheme == "https" and bool(redirected.netloc), "GitHub recovery redirect is not HTTPS")
        _require(not any(character in redirected_url for character in '\r\n"'), "GitHub recovery redirect is malformed")
        output.write_bytes(b"")
        # The signed log URL is deliberately fetched in a second curl process
        # with no Authorization header. Keeping --location confined to this
        # credential-free request prevents a custom header from crossing hosts.
        redirected_config = "\n".join(
            (
                "silent",
                "show-error",
                "fail",
                "location",
                "connect-timeout = 10",
                "max-time = 30",
                f'url = "{redirected_url}"',
                "",
            )
        ).encode()
        redirected_result = subprocess.run(
            [
                "curl",
                "--config",
                "-",
                "--proto",
                "=https",
                "--proto-redir",
                "=https",
                "--max-filesize",
                str(limit),
                "--output",
                str(output),
            ],
            input=redirected_config,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=False,
            timeout=35,
        )
        _require(redirected_result.returncode == 0, "GitHub recovery redirected evidence request failed")
    except (OSError, subprocess.SubprocessError) as exc:
        raise RecoveryProofError("GitHub recovery evidence request failed") from exc
    finally:
        headers.unlink(missing_ok=True)


def github_get(url: str, token: str, *, binary: bool = False) -> Any:
    _require(urlsplit(url).scheme == "https", "GitHub recovery evidence URL must use HTTPS")
    descriptor, temporary_name = tempfile.mkstemp(prefix="fugue-github-evidence.")
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        os.chmod(temporary, stat.S_IRUSR | stat.S_IWUSR)
        _curl_download(url, token, temporary, limit=32 * 1024 * 1024)
        _require(temporary.stat().st_size <= 32 * 1024 * 1024, "GitHub recovery evidence exceeds 32 MiB")
        payload = temporary.read_bytes()
    finally:
        temporary.unlink(missing_ok=True)
    if binary:
        return payload
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecoveryProofError("GitHub recovery evidence is not valid JSON") from exc
    _require(isinstance(decoded, dict), "GitHub recovery evidence is not an object")
    return decoded


def _run_checked(command: list[str], *, cwd: Path) -> bytes:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RecoveryProofError(f"evidence command failed: {command[0]}") from exc
    return completed.stdout


def write_proof(path: Path, payload: dict[str, Any]) -> None:
    _require(path.is_absolute(), "proof output path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True)
    _require(not path.is_symlink(), "proof output path must not be a symlink")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, stat.S_IRUSR | stat.S_IWUSR)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    finally:
        temporary.unlink(missing_ok=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--workflow-path", required=True)
    parser.add_argument("--job-name", default="deploy")
    parser.add_argument("--runner-name", required=True)
    parser.add_argument("--current-run-id", type=int, required=True)
    parser.add_argument("--current-run-attempt", type=int, required=True)
    parser.add_argument("--current-head-sha", required=True)
    parser.add_argument("--expected-holder", required=True)
    parser.add_argument("--expected-head-sha", required=True)
    parser.add_argument("--expected-script-blob", required=True)
    parser.add_argument("--expected-helm-revision", type=int, required=True)
    parser.add_argument("--expected-failure", required=True)
    parser.add_argument("--release-name", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def run(args: argparse.Namespace) -> None:
    _require(args.mode == "legacy-pre-helm", "unsupported stale release recovery mode")
    _require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", args.repository) is not None, "repository is invalid")
    _require(args.api_url.startswith("https://"), "GitHub API URL must use HTTPS")
    _require(args.workflow_path == ".github/workflows/deploy-control-plane.yml", "workflow path is not the production deploy workflow")
    _require(re.fullmatch(r"[0-9a-f]{40}", args.expected_head_sha) is not None, "expected head SHA is invalid")
    _require(re.fullmatch(r"[0-9a-f]{40}", args.current_head_sha) is not None, "current head SHA is invalid")
    _require(args.expected_helm_revision > 0, "expected Helm revision must be positive")
    _require(args.expected_failure.strip() == args.expected_failure and args.expected_failure, "expected failure is invalid")
    old_run_id, old_attempt = parse_release_holder(args.expected_holder)
    _require(old_run_id != args.current_run_id, "current run cannot recover its own Lease")
    _require(args.current_run_id > 0 and args.current_run_attempt > 0, "current Actions identity is invalid")
    token = os.environ.get("GITHUB_TOKEN", "")
    _require(bool(token), "GITHUB_TOKEN is unavailable")

    repository_path = "/".join(quote(part, safe="") for part in args.repository.split("/"))
    base = args.api_url.rstrip("/") + f"/repos/{repository_path}/actions/runs/{old_run_id}"
    attempt_payload = github_get(f"{base}/attempts/{old_attempt}", token)
    run_payload = github_get(base, token)
    jobs_payload = github_get(f"{base}/attempts/{old_attempt}/jobs?per_page=100", token)
    job_id = validate_github_evidence(
        attempt_payload,
        run_payload,
        jobs_payload,
        repository=args.repository,
        workflow_path=args.workflow_path,
        expected_run_id=old_run_id,
        expected_attempt=old_attempt,
        expected_head_sha=args.expected_head_sha,
        expected_job_name=args.job_name,
        expected_runner_name=args.runner_name,
    )
    log_payload = github_get(
        args.api_url.rstrip("/") + f"/repos/{repository_path}/actions/jobs/{job_id}/logs",
        token,
        binary=True,
    )
    try:
        log_text = log_payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RecoveryProofError("historical job log is not UTF-8") from exc
    validate_job_log(
        log_text,
        expected_failure=args.expected_failure,
        expected_revision=args.expected_helm_revision,
    )

    repo_root = Path(__file__).resolve().parents[1]
    historical_source = _run_checked(
        ["git", "show", f"{args.expected_head_sha}:scripts/upgrade_fugue_control_plane.sh"],
        cwd=repo_root,
    )
    validate_release_source(
        historical_source,
        expected_blob=args.expected_script_blob,
        expected_failure=args.expected_failure,
    )
    history_payload = _run_checked(
        [
            "helm",
            "history",
            args.release_name,
            "-n",
            args.namespace,
            "--max",
            "256",
            "-o",
            "json",
        ],
        cwd=repo_root,
    )
    try:
        history = json.loads(history_payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecoveryProofError("Helm history is not valid JSON") from exc
    validate_helm_history(history, expected_revision=args.expected_helm_revision)
    assert_old_run_process_absent(old_run_id)

    proof = {
        "version": 1,
        "mode": args.mode,
        "repository": args.repository,
        "workflow_path": args.workflow_path,
        "old_holder": args.expected_holder,
        "old_run_id": old_run_id,
        "old_run_attempt": old_attempt,
        "old_head_sha": args.expected_head_sha,
        "old_script_blob": args.expected_script_blob,
        "old_job_id": job_id,
        "expected_helm_revision": args.expected_helm_revision,
        "expected_failure_sha256": hashlib.sha256(args.expected_failure.encode()).hexdigest(),
        "runner_name": args.runner_name,
        "authorized_run_id": args.current_run_id,
        "authorized_run_attempt": args.current_run_attempt,
        "authorized_head_sha": args.current_head_sha,
        "generated_at_epoch": int(time.time()),
    }
    write_proof(args.output, proof)


def main() -> int:
    try:
        run(build_parser().parse_args())
    except RecoveryProofError as exc:
        print(f"stale release recovery proof failed: {exc}", file=sys.stderr)
        return 1
    print("stale pre-Helm release recovery proof verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
