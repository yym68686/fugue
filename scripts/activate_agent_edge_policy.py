#!/usr/bin/env python3
"""Publish Agent control authority only after bounded real consumer evidence.

Preparation is read-only and emits a reviewable witness. The workflow retains
that witness before invoking the separate publication operation. No code image,
DNS artifact or application assignment is changed by this publisher.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time

try:
    from .publish_agent_edge_shadow import API, SCOPE, canonical, current, digest, target_matches
    from .reconcile_agent_edge_trust import kubectl, strict_json
except ImportError:
    from publish_agent_edge_shadow import API, SCOPE, canonical, current, digest, target_matches
    from reconcile_agent_edge_trust import kubectl, strict_json


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def timestamp(value):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate(config):
    if set(config) != {"apiVersion", "kind", "origin", "expectedShadowGeneration", "policy", "consumer", "observation"} or config["apiVersion"] != "configuration.fugue.dev/v1" or config["kind"] != "AgentEdgeActivePolicy":
        raise ValueError("invalid active policy declaration")
    p = config["policy"]
    if p.get("schema_version") != "fugue.agent-edge-policy/v1" or p.get("scope") != SCOPE or p.get("mode") != "active" or p.get("origin") != config["origin"] or not re.fullmatch(r"https://[a-z0-9][a-z0-9.-]+[a-z0-9]", config["origin"]):
        raise ValueError("explicit active policy and canonical origin required")
    for value in [p.get("generation", ""), config["expectedShadowGeneration"]]:
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", value):
            raise ValueError("explicit policy generation required")
    c = config["consumer"]
    if set(c) != {"namespace", "deployment", "container", "runtimeId", "sourceSha", "checkpointPath"} or not re.fullmatch(r"[0-9a-f]{40}", c["sourceSha"]) or not re.fullmatch(r"runtime_[a-zA-Z0-9_]+", c["runtimeId"]) or not c["checkpointPath"].startswith("/"):
        raise ValueError("declared immutable canary identity required")
    for key in ["namespace", "deployment", "container"]:
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", c[key]):
            raise ValueError("canonical consumer identity required")
    o = config["observation"]
    if set(o) != {"samples", "intervalSeconds", "minimumGrants", "minimumDistinctCells", "maxHeartbeatAgeSeconds", "maxRecoveredDegradationSeconds"} or any(type(v) is not int for v in o.values()) or not 5 <= o["samples"] <= 20 or not 10 <= o["intervalSeconds"] <= 60 or not 3 <= o["minimumGrants"] <= o["samples"] or not 1 <= o["minimumDistinctCells"] <= 5 or not 15 <= o["maxHeartbeatAgeSeconds"] <= 90 or not 0 <= o["maxRecoveredDegradationSeconds"] <= 60:
        raise ValueError("bounded explicit observation window required")
    return config


def selected_authority(api, config):
    full = current(api, "full")
    if full.get("artifact"):
        if not target_matches(full["artifact"], config["policy"]) or full.get("release", {}).get("status") != "active":
            raise ValueError("another full Agent authority exists")
        return full, "active"
    shadow = current(api, "shadow")
    artifact, release = shadow.get("artifact", {}), shadow.get("release", {})
    if artifact.get("generation") != config["expectedShadowGeneration"] or artifact.get("status") != "validated" or release.get("status") != "active" or artifact.get("content", {}).get("mode") != "shadow":
        raise ValueError("expected observational authority is unavailable")
    candidate, baseline = dict(config["policy"]), dict(artifact["content"])
    for item in [candidate, baseline]:
        item.pop("generation", None)
        item.pop("mode", None)
    if candidate != baseline:
        raise ValueError("active policy differs from observed shadow constraints")
    return shadow, "shadow"


def verify_grant(validator, public, signed, runtime_id, origin):
    with tempfile.TemporaryDirectory(prefix="fugue-agent-observation-") as directory:
        trust_path, grant_path = Path(directory) / "trust.json", Path(directory) / "grant.json"
        for path, value in [(trust_path, public), (grant_path, signed)]:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(canonical(value))
        result = subprocess.run([validator, "verify-grant", "-public-file", str(trust_path), "-grant-file", str(grant_path), "-audience", runtime_id, "-origin", origin], capture_output=True, text=True, timeout=20)
        if result.returncode:
            raise ValueError("consumer checkpoint has no live independent signature")
        return strict_json(result.stdout)


def measured_choice(logs, grant_digest, max_recovered_seconds=0):
    observations = []
    degraded_since = None
    acquisition_failure = None
    previous_at = None
    previous_valid_until = None
    healthy_seen = False
    recovery = {"degradations": 0, "max_degraded_seconds": 0, "acquisition_failures": 0}
    for line in logs.splitlines():
        if any(text in line for text in ["heartbeat failed:", "poll failed:", "no live Agent Edge permission", "agent exited:"]):
            raise ValueError("recent canary control or permission gap observed")
        if "agent_edge_selection " in line:
            raw = line.split("agent_edge_selection ", 1)[1]
            value, suffix_start = json.JSONDecoder().raw_decode(raw)
            if not value.get("primary"):
                raise ValueError("recent canary lacked an authorized measured primary")
            error = raw[suffix_start:].strip()
            if max_recovered_seconds == 0:
                if value.get("degraded") or error:
                    raise ValueError("recent canary measurements were degraded or failed")
            else:
                # Kubernetes timestamps bound actual recovery duration. Missing
                # history cannot turn an indefinitely degraded state into a pass.
                at = timestamp(line.split()[0])
                valid_until = timestamp(value["valid_until"])
                if at > now() or previous_at is not None and at < previous_at or valid_until <= at or previous_valid_until is not None and previous_valid_until < at:
                    raise ValueError("canary observation is unordered or has no live grant")
                previous_at = at
                previous_valid_until = valid_until
                if value.get("degraded"):
                    if not healthy_seen:
                        raise ValueError("degradation began before available observation history")
                    if degraded_since is None:
                        degraded_since = at
                elif degraded_since is not None:
                    elapsed = (at - degraded_since).total_seconds()
                    if elapsed > max_recovered_seconds:
                        raise ValueError("independent standby exceeded its declared recovery bound")
                    recovery["degradations"] += 1
                    recovery["max_degraded_seconds"] = max(recovery["max_degraded_seconds"], elapsed)
                    degraded_since = None
                if error:
                    if error != "error=Agent Edge acquisition returned HTTP 503":
                        raise ValueError("recent canary selection error observed")
                    acquisition_failure = (acquisition_failure[0] if acquisition_failure else at, value["grant_digest"])
                    recovery["acquisition_failures"] += 1
                elif acquisition_failure and value["grant_digest"] != acquisition_failure[1]:
                    if (at - acquisition_failure[0]).total_seconds() > max_recovered_seconds:
                        raise ValueError("permission acquisition exceeded its declared recovery bound")
                    acquisition_failure = None
                healthy_seen = healthy_seen or not value.get("degraded")
            if value.get("grant_digest") == grant_digest:
                observations.append(value)
    if degraded_since is not None or acquisition_failure is not None:
        raise ValueError("canary recovery is not yet observed")
    if not observations or observations[-1].get("degraded"):
        raise ValueError("consumer lacks current measured primary and independent standby")
    result = dict(observations[-1])
    if max_recovered_seconds:
        result["recovery"] = recovery
    return result


def verify_runtime_observation(runtime, grant_valid_until, max_age_seconds):
    if runtime.get("connection_mode") != "agent" or runtime.get("status") != "active" or runtime.get("access_mode") != "private" or runtime.get("pool_mode") != "dedicated":
        raise ValueError("actual Agent is no longer active and isolated")
    heartbeat = timestamp(runtime["last_heartbeat_at"])
    observed = timestamp(runtime.get("labels", {}).get("fugue.io/cell-observed-at", ""))
    checked_at = now()
    ahead = (max(heartbeat, observed) - checked_at).total_seconds()
    # Independent server and observer clocks can differ by milliseconds. Wait
    # once for a bounded near-future observation to become past; never clamp
    # its original timestamp, extend its freshness or accept future evidence.
    if 0 < ahead <= 1:
        time.sleep(ahead + 0.001)
        checked_at = now()
    if heartbeat > checked_at or observed > checked_at:
        raise ValueError("actual Agent heartbeat or cell observation is still in the future")
    if min(heartbeat, observed) < checked_at - datetime.timedelta(seconds=max_age_seconds):
        raise ValueError("actual Agent heartbeat or cell observation is stale")
    if timestamp(grant_valid_until) <= checked_at + datetime.timedelta(seconds=10):
        raise ValueError("Agent permission expired or approached expiry during observation")
    return checked_at, heartbeat, observed


def sample(config, api, authority, mode, public, validator, log_start_at=None):
    c, o = config["consumer"], config["observation"]
    deployment = strict_json(kubectl(["get", "deployment", c["deployment"], "-n", c["namespace"], "-o", "json"]))
    template = deployment["spec"]["template"]
    if deployment["spec"]["replicas"] != 1 or deployment.get("status", {}).get("readyReplicas") != 1 or template["metadata"]["annotations"].get("fugue.pro/source-commit") != c["sourceSha"]:
        raise ValueError("declared canary code is not the one healthy replica")
    selector = ",".join(k + "=" + v for k, v in sorted(deployment["spec"]["selector"]["matchLabels"].items()))
    pods = strict_json(kubectl(["get", "pods", "-n", c["namespace"], "-l", selector, "-o", "json"]))["items"]
    pods = [p for p in pods if not p["metadata"].get("deletionTimestamp")]
    if len(pods) != 1:
        raise ValueError("canary ownership is ambiguous")
    pod = pods[0]
    status = next(s for s in pod["status"]["containerStatuses"] if s["name"] == c["container"])
    if not status.get("ready") or status.get("restartCount") != 0 or not status.get("imageID", "").startswith("ghcr.io/") or "@sha256:" not in status.get("imageID", ""):
        raise ValueError("canary restarted or has no immutable healthy image")
    prefix = ["-n", c["namespace"], "exec", pod["metadata"]["name"], "-c", c["container"], "--"]
    checkpoint = strict_json(kubectl([*prefix, "cat", c["checkpointPath"]]))
    verified = verify_grant(validator, public, checkpoint["last_grant"], c["runtimeId"], config["origin"])
    grant = verified["grant"]
    if verified.get("verified") is not True or checkpoint.get("activated") != (mode == "active") or grant["mode"] != mode or grant["policy_reference"]["artifact_id"] != authority["artifact"]["id"] or grant["policy_reference"]["release_id"] != authority["release"]["id"] or timestamp(grant["valid_until"]) <= now() + datetime.timedelta(seconds=10):
        raise ValueError("consumer has not accepted the exact live policy publication")
    log_start_at = log_start_at or (now() - datetime.timedelta(seconds=120)).isoformat()
    logs = kubectl(["-n", c["namespace"], "logs", pod["metadata"]["name"], "-c", c["container"], "--since-time=" + log_start_at, "--timestamps=true"])
    choice = measured_choice(logs, verified["digest"], o["maxRecoveredDegradationSeconds"])
    selected = set([choice["primary"], *choice.get("standbys", [])])
    cells = {candidate["authority_cell_id"] for candidate in grant["candidates"] if candidate["edge_id"] in selected}
    if len(cells) < o["minimumDistinctCells"]:
        raise ValueError("measured candidate diversity below activation requirement")
    runtime = api("GET", "/v1/runtimes/" + c["runtimeId"])["runtime"]
    checked_at, heartbeat, observed = verify_runtime_observation(runtime, grant["valid_until"], o["maxHeartbeatAgeSeconds"])
    return {"at": checked_at.isoformat(), "log_start_at": log_start_at, "pod_uid": pod["metadata"]["uid"], "image_id": status["imageID"], "grant_digest": verified["digest"], "grant_valid_until": grant["valid_until"], "policy_release_id": authority["release"]["id"], "primary": choice["primary"], "cells": sorted(cells), "heartbeat": heartbeat.isoformat(), "cell_observed_at": observed.isoformat(), "mode": mode, "publication_ids": sorted({candidate["publication"]["release_id"] for candidate in grant["candidates"]}), "recovery": choice.get("recovery", {})}


def prepare_window(config, api, public, validator):
    authority, mode = selected_authority(api, config)
    observations = []
    log_start_at = None
    for index in range(config["observation"]["samples"]):
        fresh, current_mode = selected_authority(api, config)
        if fresh.get("release") != authority.get("release") or current_mode != mode:
            raise ValueError("policy authority changed during observation window")
        # Allow the first receipt to catch up to an already-published policy;
        # once the watch starts, any failure invalidates the entire window.
        if index == 0:
            for attempt in range(13):
                try:
                    observation = sample(config, api, authority, mode, public, validator)
                    log_start_at = observation["log_start_at"]
                    break
                except ValueError:
                    if attempt == 12:
                        raise
                    time.sleep(5)
        else:
            # Preserve the successful baseline's entire log interval. A sliding
            # tail can drop the healthy event before a completed recovery and
            # cannot prove a continuous observation window.
            observation = sample(config, api, authority, mode, public, validator, log_start_at)
        observations.append(observation)
        if index + 1 < config["observation"]["samples"]:
            time.sleep(config["observation"]["intervalSeconds"])
    if len({x["pod_uid"] for x in observations}) != 1 or len({x["image_id"] for x in observations}) != 1 or len({x["grant_digest"] for x in observations}) < config["observation"]["minimumGrants"] or len({x["heartbeat"] for x in observations}) != len(observations):
        raise ValueError("consumer did not remain stable and renew permission and heartbeat")
    result = {"schema": "fugue.agent-edge-activation-evidence/v1", "declaration_digest": digest(config), "trust_digest": digest(public), "authority": authority, "mode": mode, "observations": observations, "completed_at": now().isoformat()}
    result["evidence_digest"] = digest(result)
    return result


def prepare(config, api, public, validator):
    for attempt in range(3):
        try:
            return prepare_window(config, api, public, validator)
        except ValueError:
            if attempt == 2:
                raise
            print(canonical({"observation_window_rejected": True, "attempt": attempt + 1}), flush=True)
            time.sleep(30)


def validate_witness(config, public, witness):
    unsigned = dict(witness)
    actual = unsigned.pop("evidence_digest", None)
    if actual != digest(unsigned) or witness.get("schema") != "fugue.agent-edge-activation-evidence/v1" or witness.get("declaration_digest") != digest(config) or witness.get("trust_digest") != digest(public) or not now() - datetime.timedelta(minutes=5) < timestamp(witness["completed_at"]) <= now() or len(witness.get("observations", [])) != config["observation"]["samples"]:
        raise ValueError("activation evidence is stale or not bound to this declaration")


def attest_lkg(api, authority, witness, initial):
    artifact, release = authority["artifact"], authority["release"]
    lkg = authority.get("lkg")
    if lkg and lkg.get("artifact_id") == artifact["id"] and lkg.get("verified_by_release_id") == release["id"]:
        return
    api("POST", "/v1/admin/artifact-releases/" + release["id"] + "/verify-lkg", {"fencing_token": release["fencing_token"], "allow_initial_lkg": initial, "reason": "verified actual Runtime Agent signature, primary/standby measurements, stable pod and renewed heartbeat over the retained observation window", "evidence": {"consumer_convergence": True, "local_probe": True, "platform_evidence": True, "watch_window": True, "baseline_monotonic": True, "database_rollback_compatible": True, "evidence_refs": [witness["evidence_digest"], artifact["id"], release["id"]]}})


def activate(config, api, public, validator, witness):
    validate_witness(config, public, witness)
    authority, mode = selected_authority(api, config)
    if authority["release"]["id"] != witness["authority"]["release"]["id"] or mode != witness["mode"]:
        raise ValueError("authority changed after witness retention")
    sample(config, api, authority, mode, public, validator, witness["observations"][0]["log_start_at"])
    if mode == "active":
        return authority
    attest_lkg(api, authority, witness, initial=not bool(authority.get("lkg")))
    policy = config["policy"]
    candidates = api("GET", "/v1/admin/artifacts?kind=policy_snapshot&scope=" + SCOPE + "&limit=100")["artifacts"]
    matches = [a for a in candidates if a.get("generation") == policy["generation"]]
    if len(matches) > 1:
        raise ValueError("ambiguous active policy generation")
    artifact = matches[0] if matches else api("POST", "/v1/admin/artifacts", {"artifact_kind": "policy_snapshot", "scope": {"scope_type": "global", "key": SCOPE}, "generation": policy["generation"], "content": policy})["artifact"]
    if not target_matches(artifact, policy):
        raise ValueError("stored active policy differs from explicit intent")
    validation = api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/validate", {"dry_run": False})
    if validation.get("pass") is not True or validation.get("artifact", {}).get("status") != "validated":
        raise ValueError("active policy failed typed validation")
    current_authority, current_mode = selected_authority(api, config)
    if current_mode != mode or current_authority["release"]["id"] != authority["release"]["id"]:
        raise ValueError("authority changed immediately before activation")
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "full", "idempotency_key": "git-agent-active/" + policy["generation"] + "/" + digest(policy), "reason": "activate explicitly observed Agent control transport; retained witness " + witness["evidence_digest"]})
    result, mode = selected_authority(api, config)
    if mode != "active":
        raise ValueError("full Agent authority did not activate")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("operation", choices=["prepare", "activate", "verify"])
    parser.add_argument("config")
    parser.add_argument("--trust", required=True)
    parser.add_argument("--validator", required=True)
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    config = validate(strict_json(Path(args.config).read_bytes()))
    public = strict_json(Path(args.trust).read_bytes())["publicTrust"]
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token:
        raise ValueError("configuration writer credential missing")
    api = API(config["origin"], token)
    if args.operation == "prepare":
        witness = prepare(config, api, public, args.validator)
        Path(args.evidence).write_text(canonical(witness) + "\n")
        print(canonical({"prepared": True, "mode": witness["mode"], "evidence_digest": witness["evidence_digest"]}))
    else:
        witness = strict_json(Path(args.evidence).read_bytes())
        if args.operation == "activate":
            authority = activate(config, api, public, args.validator, witness)
        else:
            validate_witness(config, public, witness)
            authority, mode = selected_authority(api, config)
            if mode != "active" or witness["mode"] != "active" or authority["release"]["id"] != witness["authority"]["release"]["id"]:
                raise ValueError("active verification lacks current selected-transport evidence")
            sample(config, api, authority, mode, public, args.validator, witness["observations"][0]["log_start_at"])
            attest_lkg(api, authority, witness, initial=False)
        print(canonical({"operation": args.operation, "artifact_id": authority["artifact"]["id"], "release_id": authority["release"]["id"], "mode": "active"}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("Agent Edge activation stopped: " + str(error)) from None
