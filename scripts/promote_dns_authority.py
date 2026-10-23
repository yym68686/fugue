#!/usr/bin/env python3
"""Promote an exact private DNS-only shadow with fresh per-reader evidence.

This lane changes only the declared authority's publication channels and LKG.
Public Services, workloads, routing sources and the global producer are untouched.
"""
import argparse
import base64
from concurrent.futures import ThreadPoolExecutor
import datetime
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import time
import urllib.parse

from scripts.bootstrap_cell_producer import digest, selected
from scripts.observe_front_candidate import now, timestamp
from scripts.reconcile_dns_transport import read_json, ready_pod
from scripts.stage_dns_authority_transition import API, artifact

ID = re.compile(r"[a-zA-Z0-9_.-]{1,256}")
NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
HASH = re.compile(r"sha256:[a-f0-9]{64}")


def validate(c):
    fields = {"schema", "generation", "origin", "authority_cell_id", "namespace", "release_set", "dns_artifact", "shadow_release", "readers", "observation", "verifier_revision"}
    if set(c) != fields or c["schema"] != "fugue.private-dns-promotion/v1" or type(c["generation"]) is not int or c["generation"] < 1:
        raise ValueError("explicit private DNS promotion declaration required")
    origin = urllib.parse.urlsplit(c["origin"])
    if origin.scheme != "https" or c["origin"] != "https://" + str(origin.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(origin.hostname)):
        raise ValueError("canonical HTTPS origin required")
    if not NAME.fullmatch(c["namespace"]) or not re.fullmatch(r"cell-[a-z0-9]+(?:-[a-z0-9]+)*", c["authority_cell_id"]) or not re.fullmatch(r"[a-f0-9]{40}", c["verifier_revision"]):
        raise ValueError("explicit namespace, authority and verified reader revision required")
    for key in ["release_set", "dns_artifact"]:
        ref = c[key]
        if set(ref) != {"id", "digest"} or not ID.fullmatch(ref["id"]) or not HASH.fullmatch(ref["digest"]):
            raise ValueError("exact immutable DNS artifacts required")
    r = c["shadow_release"]
    if set(r) != {"id", "fencing_token"} or not ID.fullmatch(r["id"]) or type(r["fencing_token"]) is not int or r["fencing_token"] < 1:
        raise ValueError("exact shadow predecessor required")
    readers = c["readers"]
    if not isinstance(readers, list) or not 1 <= len(readers) <= 16 or [r["node_id"] for r in readers] != sorted({r["node_id"] for r in readers}):
        raise ValueError("sorted unique DNS reader membership required")
    for r in readers:
        if set(r) != {"node_id", "pod", "uid", "container", "image", "service_account", "port"} or not ID.fullmatch(r["node_id"]) or any(not NAME.fullmatch(r[k]) for k in ["pod", "container", "service_account"]) or not re.fullmatch(r"[a-f0-9-]{36}", r["uid"]) or not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", r["image"]) or type(r["port"]) is not int or not 1 <= r["port"] <= 65535:
            raise ValueError("exact physical reader and immutable image required")
    o = c["observation"]
    if set(o) != {"samples", "interval_seconds", "timeout_seconds"} or any(type(v) is not int for v in o.values()) or not 3 <= o["samples"] <= 6 or not 20 <= o["interval_seconds"] <= 30 or not 180 <= o["timeout_seconds"] <= 600:
        raise ValueError("bounded independent DNS observation window required")
    return c


def scope(c):
    return "authority-cell:" + c["authority_cell_id"]


def inputs(c, api):
    parent = artifact(api, c["release_set"]["id"], "release_set", scope(c))
    dns = artifact(api, c["dns_artifact"]["id"], "dns_answer_bundle", scope(c))
    if parent["content_hash"] != c["release_set"]["digest"] or dns["content_hash"] != c["dns_artifact"]["digest"] or parent["content"].get("publication_role") != "cell-dns" or parent["content"].get("artifact_kinds") != ["dns_answer_bundle"] or parent["content"].get("artifact_ids") != [dns["id"]]:
        raise ValueError("promotion is not the exact independent DNS-only composition")
    topology = parent["content"].get("consumer_topology", {})
    if topology != {"schema_version": "fugue.traffic-consumer-topology/v1", "publication_role": "cell-dns", "authority_cell_id": c["authority_cell_id"], "edge_node_ids": [], "dns_node_ids": [r["node_id"] for r in c["readers"]]}:
        raise ValueError("promotion readers differ from signed execution membership")
    expected_consumers(c, api, None)
    return parent


def publication(c, api, channel):
    state = selected(api, scope(c), channel, "release_set")
    a, r = state.get("artifact"), state.get("release")
    if not a and not r:
        return None
    if a["id"] != c["release_set"]["id"] or a["content_hash"] != c["release_set"]["digest"] or r.get("status") != "active" or r.get("fencing_token", 0) <= 0:
        raise ValueError("private DNS publication differs from declaration")
    if channel == "shadow":
        if {"id": r["id"], "fencing_token": r["fencing_token"]} != c["shadow_release"]:
            raise ValueError("private DNS shadow predecessor changed")
    elif r.get("idempotency_key") != "git-private-dns/" + digest(c) + "/" + channel or r.get("canary_rule_ref", "") != ("cohort=complete" if channel == "gray" else ""):
        raise ValueError("private DNS serving lane belongs to another operation")
    return r


def expected_consumers(c, api, release):
    query = urllib.parse.urlencode({"release_set_id": c["release_set"]["id"], "artifact_kind": "dns_answer_bundle", "scope_key": scope(c), "limit": 20})
    result = api("GET", "/v1/admin/expected-consumer-sets?" + query)
    sets = result.get("expected_consumer_sets", [])
    if not sets or len(sets) > 20:
        raise ValueError("private DNS expected consumer sets are unavailable")
    revision = max(value.get("revision", 0) for value in sets)
    latest = [value for value in sets if value.get("revision") == revision]
    if len(latest) != 1:
        raise ValueError("private DNS expected consumer set is ambiguous")
    selected = latest[0]
    if selected.get("release_set_id") != c["release_set"]["id"] or selected.get("artifact_kind") != "dns_answer_bundle" or selected.get("scope_key") != scope(c) or selected.get("requires_consumers") is not True or selected.get("required_cardinality") != len(c["readers"]):
        raise ValueError("private DNS expected consumer set has a different authority")
    dns = artifact(api, c["dns_artifact"]["id"], "dns_answer_bundle", scope(c))
    if selected.get("expected_generation") != dns.get("generation"):
        raise ValueError("private DNS expected generation differs from its signed artifact")
    if release is not None and selected.get("artifact_release_id") != release["id"]:
        raise ValueError("private DNS consumer set is for another publication")
    members = selected.get("consumers", [])
    if len(members) != len(c["readers"]):
        raise ValueError("private DNS expected member count differs")
    readers = {value["node_id"]: value for value in c["readers"]}
    if {member.get("node_id") for member in members} != set(readers):
        raise ValueError("private DNS expected member identities differ or repeat")
    for member in members:
        reader = readers.get(member.get("node_id"))
        if reader is None or member.get("required") is not True or member.get("component") != "dns-server" or member.get("authority_id") != c["authority_cell_id"] or member.get("scope_key") != scope(c) or member.get("artifact_kind") != "dns_answer_bundle" or member.get("expected_generation") != selected.get("expected_generation") or member.get("consumer_id") != "dns-server:" + c["authority_cell_id"] + ":" + reader["node_id"]:
            raise ValueError("private DNS expected consumer identity differs")
    return selected


def convergence(c, api, release):
    expected_consumers(c, api, release)
    query = urllib.parse.urlencode({"release_set_id": c["release_set"]["id"], "artifact_release_id": release["id"]})
    values = api("GET", "/v1/admin/platform-state/convergence?" + query)["convergence"]
    if len(values) != 1 or values[0].get("artifact_kind") != "dns_answer_bundle":
        raise ValueError("private DNS convergence composition differs")
    value = values[0]
    readers = {r["node_id"]: r for r in c["readers"]}
    assessments = value.get("assessments", [])
    if value.get("pass") is not True or value.get("required_expected") != len(readers) or value.get("required_passing") != len(readers) or len(assessments) != len(readers):
        raise ValueError("private DNS convergence is not complete and positive")
    seen = set()
    for a in assessments:
        observed = a.get("observed", {})
        node = observed.get("node_id")
        reader = readers.get(node)
        if reader is None or node in seen or a.get("state") != "pass" or observed.get("identity_verified") is not True or observed.get("credential_id") != "kubernetes:" + c["namespace"] + ":" + reader["service_account"] + ":" + reader["uid"] or observed.get("release_set_id") != c["release_set"]["id"] or observed.get("fencing_token") != release["fencing_token"]:
            raise ValueError("private DNS receipt does not belong to the pinned executor")
        seen.add(node)


def shadow_health(c, reader):
    # A shadow-only process correctly returns 503 until it has a serving
    # publication. Read that JSON without requiring an HTTP success status.
    command = ["kubectl", "exec", "-i", "-n", c["namespace"], reader["pod"], "-c", reader["container"], "--", "nc", "-w", "10", "127.0.0.1", str(reader["port"])]
    result = subprocess.run(command, input=b"GET /healthz HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n", capture_output=True, timeout=30)
    if result.returncode or len(result.stdout) > 8 << 20:
        raise ValueError("private DNS shadow health transport failed")
    head, separator, body = result.stdout.partition(b"\r\n\r\n")
    if not separator or head.split(b"\r\n", 1)[0] not in [b"HTTP/1.0 200 OK", b"HTTP/1.0 503 Service Unavailable"]:
        raise ValueError("private DNS shadow health HTTP response is invalid")
    return json.loads(body)


def observe_shadow_reader(c, reader):
    pod = read_json("get", "pod", reader["pod"], "-n", c["namespace"], "-o", "json")
    meta, spec = pod["metadata"], pod["spec"]
    if meta.get("uid") != reader["uid"] or not meta.get("resourceVersion") or spec.get("nodeName") != reader["node_id"] or spec.get("serviceAccountName") != reader["service_account"] or not ready_pod(pod) or any(spec.get(k) for k in ["hostNetwork", "hostPID", "hostIPC"]) or any(port.get("hostPort", 0) for container in spec.get("containers", []) + spec.get("initContainers", []) for port in container.get("ports", [])):
        raise ValueError("private DNS shadow reader identity or isolation changed")
    annotations = meta.get("annotations", {})
    identity = json.loads(annotations.get("fugue.pro/consumer-identity", "{}"))
    if annotations.get("fugue.pro/authority-transition-role") != "isolated-candidate" or identity != {"version": "v1", "component": "dns-server", "scope_key": scope(c), "authority_id": c["authority_cell_id"], "artifact_kinds": ["dns_answer_bundle"]}:
        raise ValueError("private DNS shadow executor authorization differs")
    containers = [v for v in spec["containers"] if v["name"] == reader["container"] and v["image"] == reader["image"]]
    if len(containers) != 1:
        raise ValueError("private DNS shadow executable changed")
    probe = containers[0].get("readinessProbe", {}).get("httpGet", {})
    port = probe.get("port")
    if isinstance(port, str):
        ports = [v["containerPort"] for v in containers[0].get("ports", []) if v.get("name") == port and v.get("protocol", "TCP") == "TCP"]
        port = ports[0] if len(ports) == 1 else None
    if port != reader["port"] or probe.get("host") or probe.get("scheme", "HTTP") != "HTTP":
        raise ValueError("private DNS shadow observation port differs")
    health = shadow_health(c, reader)
    candidate = health.get("platform_candidate", {})
    readiness, query = candidate.get("readiness", {}), candidate.get("query", {})
    # Shadow means a recent successful dry run, not a serving lease. The
    # shortest individual proof may expire before this read even when another
    # endpoint still satisfies its record. Check the dry-run evaluation time;
    # gray/full use the independent signed, current-time serving verifier below.
    if (candidate.get("state") != "shadow_verified" or candidate.get("artifact_id") != c["dns_artifact"]["id"] or candidate.get("digest") != c["dns_artifact"]["digest"] or candidate.get("release_set_id") != c["release_set"]["id"] or candidate.get("last_error") or
            type(readiness.get("records")) is not int or readiness.get("records") < 1 or readiness.get("ready_records") != readiness.get("records") or not HASH.fullmatch(readiness.get("plan_digest", "")) or timestamp(readiness["checked_at"]) > now() or timestamp(readiness["fresh_until"]) <= timestamp(query["evaluated_at"]) or
            type(query.get("records")) is not int or query.get("records") < 1 or query.get("eligible_records") != query.get("records") or query.get("answers", 0) < 1 or query.get("serving") is not False or timestamp(query["evaluated_at"]) < timestamp(readiness["checked_at"]) or not datetime.timedelta(0) <= now() - timestamp(query["evaluated_at"]) <= datetime.timedelta(seconds=90)):
        raise ValueError("private DNS signed shadow has no fresh complete answer proof")
    current = read_json("get", "pod", reader["pod"], "-n", c["namespace"], "-o", "json")
    if current["metadata"].get("uid") != meta["uid"] or current["metadata"].get("resourceVersion") != meta["resourceVersion"]:
        raise ValueError("private DNS shadow reader changed during observation")
    return {"node_id": reader["node_id"], "uid": reader["uid"], "readiness_plan_digest": readiness["plan_digest"], "readiness_checked_at": readiness["checked_at"], "readiness_fresh_until": readiness["fresh_until"], "query_view_digest": query["view_digest"], "query_evaluated_at": query["evaluated_at"], "ready_records": readiness["ready_records"], "records": readiness["records"], "answers": query["answers"]}


def shadow_convergence(c, api, release):
    expected = expected_consumers(c, api, release)
    query = urllib.parse.urlencode({"release_set_id": c["release_set"]["id"], "artifact_release_id": release["id"]})
    values = api("GET", "/v1/admin/platform-state/convergence?" + query).get("convergence", [])
    if len(values) != 1 or values[0].get("artifact_kind") != "dns_answer_bundle":
        raise ValueError("private DNS shadow consumer assessment is missing")
    value = values[0]
    readers = {r["node_id"]: r for r in c["readers"]}
    assessments = value.get("assessments", [])
    if value.get("expected_consumer_set_id") != expected.get("id") or value.get("required_expected") != len(readers) or len(assessments) != len(readers):
        raise ValueError("private DNS shadow membership is incomplete")
    observed_nodes = set()
    fresh_after = now() - datetime.timedelta(seconds=90)
    required_caps = {"cell_dns_v1", "dns_authority_transition_v1", "dns_availability_v1", "dns_route_sources_v1", "dns_retained_routes_v1", "traffic_release_v1"}
    for assessment in assessments:
        observed = assessment.get("observed", {})
        node = observed.get("node_id")
        reader = readers.get(node)
        issued = timestamp(observed["issued_at"]) if observed.get("issued_at") else fresh_after - datetime.timedelta(seconds=1)
        received = timestamp(assessment["observed_at"]) if assessment.get("observed_at") else fresh_after - datetime.timedelta(seconds=1)
        if (reader is None or node in observed_nodes or observed.get("identity_verified") is not True or observed.get("credential_id") != "kubernetes:" + c["namespace"] + ":" + reader["service_account"] + ":" + reader["uid"] or
                observed.get("release_set_id") != c["release_set"]["id"] or observed.get("expected_consumer_set_id") != expected["id"] or observed.get("fencing_token") != release["fencing_token"] or observed.get("desired_generation") != expected["expected_generation"] or
                observed.get("apply_status") != "staged" or observed.get("probe_status") != "shadow_validated" or issued < fresh_after or issued > now() or received < issued or received > now() or timestamp(assessment["fresh_until"]) <= now() or not HASH.fullmatch(observed.get("evidence_hash", "")) or observed.get("generation_sequence") != release["fencing_token"] or
                not required_caps.issubset(set(observed.get("compatibility_capabilities", [])))):
            raise ValueError("private DNS shadow capability receipt is stale or belongs to another executor")
        observed_nodes.add(node)
    with ThreadPoolExecutor(max_workers=len(c["readers"])) as pool:
        return list(pool.map(lambda reader: observe_shadow_reader(c, reader), c["readers"]))


def observe_shadow(c, api, release):
    deadline, samples, error = time.monotonic() + c["observation"]["timeout_seconds"], [], "awaiting private DNS shadow capability and answers"
    while time.monotonic() < deadline:
        current = publication(c, api, "shadow")
        if current is None or (current["id"], current["fencing_token"]) != (release["id"], release["fencing_token"]):
            raise ValueError("private DNS shadow publication changed during observation")
        try:
            samples.append({"at": now().isoformat(), "readers": shadow_convergence(c, api, release)})
        except (ValueError, RuntimeError, subprocess.TimeoutExpired, KeyError) as exc:
            samples, error = [], str(exc)
        if len(samples) >= c["observation"]["samples"]:
            for i, reader in enumerate(c["readers"]):
                first, last = samples[0]["readers"][i], samples[-1]["readers"][i]
                if last["node_id"] != reader["node_id"] or timestamp(last["readiness_checked_at"]) <= timestamp(first["readiness_checked_at"]) or timestamp(last["query_evaluated_at"]) <= timestamp(first["query_evaluated_at"]):
                    raise ValueError("private DNS shadow observation did not advance real probes")
            return {"declaration_digest": digest(c), "release_id": release["id"], "fencing_token": release["fencing_token"], "samples": samples, "completed_at": now().isoformat()}
        time.sleep(c["observation"]["interval_seconds"])
    raise ValueError(error)


def observe_reader(c, reader, release):
    pod = read_json("get", "pod", reader["pod"], "-n", c["namespace"], "-o", "json")
    meta, spec = pod["metadata"], pod["spec"]
    if meta.get("uid") != reader["uid"] or not meta.get("resourceVersion") or spec.get("nodeName") != reader["node_id"] or spec.get("serviceAccountName") != reader["service_account"] or not ready_pod(pod) or any(spec.get(k) for k in ["hostNetwork", "hostPID", "hostIPC"]) or any(p.get("hostPort", 0) for container in spec.get("containers", []) + spec.get("initContainers", []) for p in container.get("ports", [])):
        raise ValueError("private DNS reader identity or isolation changed")
    annotations = meta.get("annotations", {})
    identity = json.loads(annotations.get("fugue.pro/consumer-identity", "{}"))
    if annotations.get("fugue.pro/authority-transition-role") != "isolated-candidate" or identity != {"version": "v1", "component": "dns-server", "scope_key": scope(c), "authority_id": c["authority_cell_id"], "artifact_kinds": ["dns_answer_bundle"]}:
        raise ValueError("private DNS executor authorization differs")
    containers = [v for v in spec["containers"] if v["name"] == reader["container"] and v["image"] == reader["image"]]
    if len(containers) != 1:
        raise ValueError("private DNS executable changed")
    container = containers[0]
    health = container.get("readinessProbe", {}).get("httpGet", {})
    health_port = health.get("port")
    if isinstance(health_port, str):
        ports = [port["containerPort"] for port in container.get("ports", []) if port.get("name") == health_port and port.get("protocol", "TCP") == "TCP"]
        health_port = ports[0] if len(ports) == 1 else None
    if health_port != reader["port"] or health.get("host") or health.get("scheme", "HTTP") != "HTTP":
        raise ValueError("private DNS observation port differs from declared process")
    environment = {e["name"]: e for e in container.get("env", [])}
    if len(environment) != len(container.get("env", [])) or container.get("envFrom"):
        raise ValueError("private DNS verifier settings are ambiguous")
    def literal(name, default=""):
        e = environment.get(name, {})
        if e.get("valueFrom"):
            raise ValueError("explicit DNS verifier setting required")
        return e.get("value", default)
    def secret(name):
        e = environment.get(name, {})
        ref = e.get("valueFrom", {}).get("secretKeyRef")
        if not ref:
            if e.get("value"):
                raise ValueError("inline signing material is forbidden")
            return ""
        value = read_json("get", "secret", ref["name"], "-n", c["namespace"], "-o", "json").get("data", {}).get(ref["key"])
        if value is None and ref.get("optional"):
            return ""
        if not value:
            raise ValueError("DNS verification material unavailable")
        return base64.b64decode(value, validate=True).decode()
    if literal("FUGUE_EDGE_GROUP_ID") != c["authority_cell_id"] or literal("FUGUE_PLATFORM_ARTIFACT_SCOPE") != scope(c):
        raise ValueError("private DNS configured authority differs")
    cache = literal("FUGUE_DNS_CACHE_PATH")
    if not PurePosixPath(cache).is_absolute() or ".." in PurePosixPath(cache).parts or str(PurePosixPath(cache)) != cache:
        raise ValueError("canonical DNS checkpoint path required")
    command = ["exec", "-n", c["namespace"], reader["pod"], "-c", reader["container"], "--"]
    checkpoint = read_json(*command, "cat", cache + ".platform-serving.json")
    # Loopback reads the claimed process even when the private network policy
    # intentionally rejects direct connections from the release runner.
    snapshot = read_json(*command, "wget", "-qO-", "http://127.0.0.1:" + str(reader["port"]) + "/runtime-facts")
    assignment = snapshot.get("assignment", {})
    if snapshot.get("node_id") != reader["node_id"] or snapshot.get("parent_digest") != c["release_set"]["digest"] or assignment.get("artifact_id") != c["dns_artifact"]["id"] or assignment.get("content_hash") != c["dns_artifact"]["digest"] or assignment.get("release_set_id") != c["release_set"]["id"] or assignment.get("artifact_release_id") != release["id"] or assignment.get("fencing_token") != release["fencing_token"] or assignment.get("release_channel") != release["release_channel"] or assignment.get("scope_key") != scope(c):
        raise ValueError("private DNS snapshot publication differs")
    request = {"node_id": reader["node_id"], "group_id": c["authority_cell_id"], "scope": scope(c), "zone": literal("FUGUE_DNS_ZONE"), "checkpoint": checkpoint, "snapshot": snapshot,
               "signing_key": secret("FUGUE_BUNDLE_SIGNING_KEY"), "signing_key_id": literal("FUGUE_BUNDLE_SIGNING_KEY_ID"), "previous_key": secret("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY"), "previous_key_id": literal("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID"), "revoked_key_ids": [v.strip() for v in literal("FUGUE_BUNDLE_REVOKED_KEY_IDS").split(",") if v.strip()]}
    verifier = os.environ.get("FUGUE_DNS_OBSERVATION_VERIFIER", "")
    if not os.path.isabs(verifier):
        raise ValueError("pinned DNS observation verifier unavailable")
    result = subprocess.run([verifier], input=json.dumps(request), text=True, capture_output=True, timeout=30)
    if result.returncode or result.stdout.strip() != '{"verified":true}':
        raise ValueError("private DNS checkpoint or fresh answer verification failed")
    current = read_json("get", "pod", reader["pod"], "-n", c["namespace"], "-o", "json")
    if current["metadata"].get("uid") != meta["uid"] or current["metadata"].get("resourceVersion") != meta["resourceVersion"]:
        raise ValueError("private DNS reader changed during observation")
    return {"node_id": reader["node_id"], "uid": reader["uid"], "observed_at": snapshot["observed_at"], "evaluated_at": snapshot["evaluated_at"], "snapshot_digest": digest(snapshot)}


def observe(c, api, release):
    deadline, samples, error = time.monotonic() + c["observation"]["timeout_seconds"], [], "awaiting private DNS convergence"
    while time.monotonic() < deadline:
        current = publication(c, api, release["release_channel"])
        if current is None or (current["id"], current["fencing_token"]) != (release["id"], release["fencing_token"]):
            raise ValueError("private DNS publication changed during observation")
        try:
            convergence(c, api, release)
            with ThreadPoolExecutor(max_workers=len(c["readers"])) as pool:
                readers = list(pool.map(lambda reader: observe_reader(c, reader, release), c["readers"]))
            samples.append({"at": now().isoformat(), "readers": readers})
        except (ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
            samples, error = [], str(exc)
        if len(samples) >= c["observation"]["samples"]:
            for i in range(len(c["readers"])):
                if timestamp(samples[-1]["readers"][i]["observed_at"]) <= timestamp(samples[0]["readers"][i]["observed_at"]):
                    raise ValueError("private DNS observation did not renew actual probes")
            if publication(c, api, release["release_channel"])["id"] != release["id"]:
                raise ValueError("private DNS publication changed after observation")
            return {"declaration_digest": digest(c), "release_id": release["id"], "fencing_token": release["fencing_token"], "samples": samples, "completed_at": now().isoformat()}
        time.sleep(c["observation"]["interval_seconds"])
    raise ValueError(error)


def retained_lkg(c, api, accepted):
    value = api("GET", "/v1/admin/artifacts/" + c["release_set"]["id"] + "/lkg").get("lkg")
    if value is not None and (value.get("artifact_id") != c["release_set"]["id"] or value.get("content_hash") != c["release_set"]["digest"] or value.get("artifact_kind") != "release_set" or value.get("scope_key") != scope(c) or not HASH.fullmatch(value.get("verification_evidence_hash", "")) or value.get("verified_by_release_id") not in accepted or timestamp(value["expires_at"]) <= now()):
        raise ValueError("private DNS LKG is foreign or expired")
    return value


def promote(c, api, save):
    validate(c)
    inputs(c, api)
    if publication(c, api, "shadow") is None:
        raise ValueError("private DNS shadow is missing")
    gray, full = publication(c, api, "gray"), publication(c, api, "full")
    if full and not gray:
        raise ValueError("initial private DNS full has no declared gray predecessor")
    evidence = {"schema": "fugue.private-dns-promotion-result/v1", "declaration_digest": digest(c), "public_transport_changed": False, "route_configuration_changed": False}
    save(evidence)
    for channel in ["gray", "full"]:
        release = gray if channel == "gray" else full
        if channel == "gray" and full:
            if retained_lkg(c, api, [gray["id"], full["id"]]) is None:
                raise ValueError("resumed full DNS publication lacks positive LKG")
            continue
        if release is None:
            if channel == "gray":
                shadow = publication(c, api, "shadow")
                evidence["shadow"] = observe_shadow(c, api, shadow)
                save(evidence)
            elif retained_lkg(c, api, [gray["id"]]) is None:
                raise ValueError("private DNS full requires verified gray LKG")
            body = {"release_channel": channel, "idempotency_key": "git-private-dns/" + digest(c) + "/" + channel, "reason": "explicit private DNS configuration promotion with independent reader observations"}
            if channel == "gray":
                body["canary_rule_ref"] = "cohort=complete"
            api("POST", "/v1/admin/artifacts/" + c["release_set"]["id"] + "/release", body)
            release = publication(c, api, channel)
            if release is None:
                raise ValueError("private DNS promotion did not select declared release")
        if channel == "gray":
            gray = release
        api("POST", "/v1/admin/platform-config/release-set/prepare-consumers", {"release_set_id": c["release_set"]["id"], "artifact_release_id": release["id"]})
        witness = observe(c, api, release)
        evidence[channel] = {"release": {"id": release["id"], "fencing_token": release["fencing_token"]}, "witness": witness}
        save(evidence)
        existing = retained_lkg(c, api, [gray["id"], release["id"]])
        if existing is None or existing["verified_by_release_id"] != release["id"]:
            # Recheck immediately before committing the receipt. The API repeats
            # immutable membership, live identity, current fence and convergence.
            current = publication(c, api, channel)
            if current is None or current["id"] != release["id"] or not datetime.timedelta(0) <= now() - timestamp(witness["completed_at"]) < datetime.timedelta(seconds=60):
                raise ValueError("private DNS verification witness is no longer current")
            convergence(c, api, release)
            api("POST", "/v1/admin/artifact-releases/" + release["id"] + "/verify-lkg", {"fencing_token": release["fencing_token"], "allow_initial_lkg": channel == "gray" and existing is None, "reason": "verified private DNS answers with authenticated checkpoints and advancing bounded probe observations", "evidence": {"consumer_convergence": True, "local_probe": True, "platform_evidence": True, "watch_window": True, "baseline_monotonic": True, "database_rollback_compatible": True, "evidence_refs": [digest(witness), c["release_set"]["id"], release["id"]]}})
        evidence[channel]["lkg"] = retained_lkg(c, api, [release["id"]])
        if evidence[channel]["lkg"] is None:
            raise ValueError("private DNS verified LKG was not retained")
        save(evidence)
    evidence["completed_at"] = now().isoformat()
    save(evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--evidence")
    args = parser.parse_args()
    c = validate(json.loads(Path(args.declaration).read_text()))
    if args.validate_only:
        print("private DNS promotion declaration validated")
        return
    if not args.evidence or not os.environ.get("FUGUE_API_KEY"):
        raise ValueError("private DNS promotion credential and evidence path required")
    def save(value):
        Path(args.evidence).write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")
    promote(c, API(c["origin"], os.environ["FUGUE_API_KEY"]), save)


if __name__ == "__main__":
    main()
