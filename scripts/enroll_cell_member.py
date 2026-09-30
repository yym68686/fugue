#!/usr/bin/env python3
"""Admit one privately verified member of an established signed Cell release.

This lane publishes no traffic artifact, grants no bootstrap health, and changes
no public listener. Only a worker already proving the exact verified full may
initialize its own absent inventory activation file at the current Cell fence.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import time
import urllib.parse

from scripts import bootstrap_cell_inventory as initial
from scripts.bootstrap_cell_producer import authority_identity, digest, selected
from scripts.publish_agent_edge_shadow import API, canonical
from scripts.reconfigure_cell_producer import validate_ref

MANAGER = "fugue-cell-member-enrollment"


def initial_projection(c):
    out = {k: v for k, v in c.items() if k in {"generation", "origin", "authority_cell_id", "namespace", "release_set", "cohort", "bootstrap_config_map", "worker", "observation"}}
    return {**out, "schema": "fugue.cell-inventory-enrollment/v1"}


def validate(c):
    extra = {"full_publication", "verification_evidence_hash", "member_node_ids", "serving_epoch", "control", "admission_window"}
    if c.get("schema") != "fugue.cell-member-enrollment/v1" or set(c) != set(initial_projection(c)) | extra:
        raise ValueError("complete established Cell member declaration required")
    initial.validate(initial_projection(c))
    validate_ref(c["full_publication"])
    ref, control, epoch = c["full_publication"], c["control"], c["serving_epoch"]
    if c["release_set"] != {"id": ref["artifact_id"], "digest": ref["content_hash"]} or not re.fullmatch(r"sha256:[a-f0-9]{64}", c["verification_evidence_hash"]):
        raise ValueError("exact full publication and positive LKG required")
    members = c["member_node_ids"]
    if not isinstance(members, list) or not 2 <= len(members) <= 256 or any(not isinstance(x, str) or not re.fullmatch(r"[a-z0-9]+(?:[-.][a-z0-9]+)*", x) for x in members) or members != sorted(set(members)) or c["worker"]["node"] not in members:
        raise ValueError("complete established physical membership required")
    if set(epoch) != {"slot", "fence_sequence", "min_healthy_instances"} or epoch["slot"] != c["worker"]["slot"] or any(type(epoch[k]) is not int for k in ["fence_sequence", "min_healthy_instances"]) or not 1 <= epoch["fence_sequence"] < (1 << 64) - 1 or not 1 <= epoch["min_healthy_instances"] <= len(members):
        raise ValueError("exact current Cell serving fence required")
    if set(control) != {"deployment", "pod", "instance_uid", "source_sha", "image_digest", "state_file"} or any(not isinstance(control[k], str) or not re.fullmatch(r"[a-z0-9]+(?:[-.][a-z0-9]+)*", control[k]) for k in ["deployment", "pod"]) or not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", control["instance_uid"]) or not re.fullmatch(r"[a-f0-9]{40}", control["source_sha"]) or not re.fullmatch(r"sha256:[a-f0-9]{64}", control["image_digest"]):
        raise ValueError("exact serving control executor required")
    path = control["state_file"]
    if not path.startswith("/") or str(Path(path)) != path or ".." in Path(path).parts:
        raise ValueError("clean absolute control state path required")
    window = c["admission_window"]
    if set(window) != {"not_before", "expires_at"} or not datetime.timedelta(0) < initial.timestamp(window["expires_at"]) - initial.timestamp(window["not_before"]) <= datetime.timedelta(minutes=15):
        raise ValueError("explicit absolute admission window required")
    return c


def window_open(c):
    window = c["admission_window"]
    if not initial.timestamp(window["not_before"]) <= initial.now() < initial.timestamp(window["expires_at"]):
        raise ValueError("member admission window is not active")


def full(c, api):
    scope = "authority-cell:" + c["authority_cell_id"]
    state = selected(api, scope, "full", "release_set")
    if authority_identity(state) != c["full_publication"]:
        raise ValueError("verified full publication changed")
    a, release = state["artifact"], state["release"]
    topology = {"publication_role": "cell-routes", "schema_version": "fugue.traffic-consumer-topology/v1", "authority_cell_id": c["authority_cell_id"], "edge_node_ids": c["member_node_ids"], "dns_node_ids": []}
    if digest(a["content"]) != a["content_hash"] or a["content"].get("publication_role") != "cell-routes" or a["content"].get("consumer_topology") != topology or set(a["content"].get("artifact_kinds", [])) != initial.KINDS or release.get("verification_state") != "verified":
        raise ValueError("member is not authorized by the verified route-only full")
    lkg = api("GET", "/v1/admin/artifacts/" + a["id"] + "/lkg").get("lkg", {})
    if any(lkg.get(k) != v for k, v in {"artifact_id": a["id"], "content_hash": a["content_hash"], "scope_key": scope, "artifact_kind": "release_set", "verified_by_release_id": release["id"], "verification_evidence_hash": c["verification_evidence_hash"]}.items()) or initial.timestamp(lkg["expires_at"]) <= initial.now():
        raise ValueError("positive full LKG changed or expired")
    return release


def control_epoch(c):
    control = c["control"]
    pod = initial.resource(c, "pod", control["pod"])
    deployment = initial.resource(c, "deployment", control["deployment"])
    if not pod or not deployment or pod["metadata"].get("uid") != control["instance_uid"] or pod["metadata"].get("deletionTimestamp") or deployment["metadata"].get("annotations", {}).get("fugue.pro/production-config-sha") != control["source_sha"]:
        raise ValueError("Cell control executor changed")
    owners = pod["metadata"].get("ownerReferences", [])
    if len(owners) != 1 or owners[0].get("kind") != "ReplicaSet" or owners[0].get("controller") is not True:
        raise ValueError("Cell control owner is ambiguous")
    rs = initial.resource(c, "replicaset", owners[0]["name"])
    if not rs or rs["metadata"]["uid"] != owners[0]["uid"] or not any(o.get("kind") == "Deployment" and o.get("uid") == deployment["metadata"]["uid"] and o.get("controller") is True for o in rs["metadata"].get("ownerReferences", [])):
        raise ValueError("Cell control owner differs")
    spec = next(x for x in pod["spec"]["containers"] if x["name"] == "edge-control")
    runtime = next(x for x in pod["status"]["containerStatuses"] if x["name"] == "edge-control")
    if pod["status"].get("phase") != "Running" or runtime.get("ready") is not True or runtime.get("restartCount") != 0 or not spec["image"].endswith("@" + control["image_digest"]) or not runtime.get("imageID", "").endswith("@" + control["image_digest"]):
        raise ValueError("Cell control executable differs")
    state = initial.kubectl("-n", c["namespace"], "exec", control["pod"], "-c", "edge-control", "--", "cat", control["state_file"])
    inventory = state.get("inventory", {})
    epoch = inventory.get("active_epoch", {})
    if state.get("edge_group_id") != c["authority_cell_id"] or inventory.get("edge_group_id") != c["authority_cell_id"] or epoch.get("edge_group_id") != c["authority_cell_id"] or any(epoch.get(k) != v for k, v in c["serving_epoch"].items()) or not datetime.timedelta(0) <= initial.now() - initial.timestamp(inventory["observed_at"]) < datetime.timedelta(seconds=90):
        raise ValueError("Cell serving fence is missing, stale or changed")
    return c["serving_epoch"]


def health(c, release, admitted):
    w = c["worker"]
    h = initial.kubectl("-n", c["namespace"], "exec", w["pod"], "-c", w["container"], "--", "wget", "-qO-", "http://127.0.0.1:7832/healthz")
    serving = h.get("platform_serving", {})
    binding = serving.get("traffic_release", {})
    if h.get("healthy") is not True or h.get("edge_id") != w["node"] or h.get("edge_group_id") != c["authority_cell_id"] or h.get("route_bundle_source") != "edge-control-group-authority/v1" or h.get("route_count", 0) < 1 or h.get("publication_sequence", 0) < 1 or h.get("stale_cache") or h.get("candidate_bundle_loaded") or not h.get("bundle_version") or h.get("caddy_applied_version") != h["bundle_version"]:
        raise ValueError("private member has not loaded a real positive group bundle")
    if serving.get("state") != "serving_verified" or serving.get("bundle_version") != h["bundle_version"] or serving.get("route_probes", 0) < 1 or serving.get("tls_probes", 0) < 1 or any(binding.get(k) != v for k, v in {"release_set_id": c["release_set"]["id"], "release_set_digest": c["release_set"]["digest"], "release_id": release["id"], "fencing_token": release["fencing_token"], "scope_key": "authority-cell:" + c["authority_cell_id"], "release_channel": "full"}.items()):
        raise ValueError("member route and TLS proof belongs to another publication")
    if any(not datetime.timedelta(0) <= initial.now() - initial.timestamp(serving[k]) < datetime.timedelta(seconds=90) for k in ["verified_at", "reported_at"]):
        raise ValueError("member serving proofs are stale")
    if admitted and (h.get("inventory_producer_active") is not True or not datetime.timedelta(0) <= initial.now() - initial.timestamp(h["inventory_heartbeat_at"]) < datetime.timedelta(seconds=90)):
        raise ValueError("admitted member has not reported fresh real inventory")
    if not admitted and h.get("inventory_producer_active"):
        raise ValueError("uninitialized member unexpectedly reports inventory")
    return h


def convergence(c, api, release):
    query = urllib.parse.urlencode({"release_set_id": c["release_set"]["id"], "artifact_release_id": release["id"]})
    results = api("GET", "/v1/admin/platform-state/convergence?" + query)["convergence"]
    if len(results) != 2 or {r["artifact_kind"] for r in results} != initial.KINDS:
        raise ValueError("member convergence is incomplete")
    for result in results:
        if result.get("pass") is not True or result.get("required_expected") != len(c["member_node_ids"]) or result.get("required_passing") != len(c["member_node_ids"]):
            raise ValueError("all signed Cell members must prove the full publication")
        assessments = result.get("assessments", [])
        if len(assessments) != len(c["member_node_ids"]) or sorted(x.get("node_id", "") for x in assessments) != c["member_node_ids"]:
            raise ValueError("consumer set differs from exact physical membership")
        for a in assessments:
            observed = a.get("observed", {})
            if a.get("state") != "pass" or observed.get("identity_verified") is not True or observed.get("release_set_id") != c["release_set"]["id"] or observed.get("fencing_token") != release["fencing_token"]:
                raise ValueError("member convergence evidence differs")
            if a["node_id"] == c["worker"]["node"] and observed.get("credential_id") != "kubernetes:" + c["namespace"] + ":" + c["worker"]["service_account"] + ":" + c["worker"]["instance_uid"]:
                raise ValueError("new member proof belongs to another Pod")


def check_activation(c, actual, bundle=None):
    w = c["worker"]
    expected = {"schema": "edge-front-group-activation/v1", "edge_group_id": c["authority_cell_id"], "generation": c["serving_epoch"]["fence_sequence"], "active_slot": w["slot"], "worker_source_commit": w["source_sha"], "worker_image_digest": w["image_digest"], "authority": "edge-control", "operation": "initialize", "reason": "Verified isolated Cell member " + digest(c)}
    if not isinstance(actual, dict) or any(actual.get(k) != v for k, v in expected.items()) or not actual.get("bundle_generation") or bundle is not None and actual["bundle_generation"] != bundle:
        raise ValueError("member activation differs from this declaration")


def observe(c, api, admitted):
    window_open(c)
    worker = initial.isolated_worker(initial_projection(c))
    # Established admission never supplies the first-publication permission.
    if initial.resource(c, "configmap", c["bootstrap_config_map"]) is not None:
        raise ValueError("established member cannot carry a bootstrap permission")
    release = full(c, api)
    control_epoch(c)
    facts = health(c, release, admitted)
    convergence(c, api, release)
    # Proof collection can span a publication or serving-epoch transition.
    # Re-read both authorities before the caller can initialize local state.
    full(c, api)
    control_epoch(c)
    window_open(c)
    return worker, facts


def enrollment_job(c, worker, facts):
    job = initial.activation_job(c, worker, facts, {"expires_at": c["admission_window"]["expires_at"]})
    job["metadata"]["name"] = "cell-member-" + digest(c)[7:23]
    job["metadata"]["labels"]["app.kubernetes.io/managed-by"] = MANAGER
    args = job["spec"]["template"]["spec"]["containers"][0]["args"]
    args[args.index("--reason") + 1] = "Verified isolated Cell member " + digest(c)
    args.extend(["--initial-generation", str(c["serving_epoch"]["fence_sequence"])])
    return job


def enroll(c, api, save):
    validate(c)
    actual = initial.activation(c)
    if actual is not None:
        check_activation(c, actual)
    evidence = {"schema": "fugue.cell-member-enrollment-result/v1", "declaration_digest": digest(c), "public_transport_changed": False, "artifacts_published": False, "observations": []}
    for index in range(c["observation"]["samples"]):
        worker, facts = observe(c, api, actual is not None)
        if initial.activation(c) != actual:
            raise ValueError("member activation changed during verification")
        evidence["observations"].append({"at": initial.now().isoformat(), "bundle_version": facts["bundle_version"], "route_probes": facts["platform_serving"]["route_probes"], "tls_probes": facts["platform_serving"]["tls_probes"]})
        save(evidence)
        if index + 1 < c["observation"]["samples"]:
            time.sleep(c["observation"]["interval_seconds"])
    if actual is None:
        worker, facts = observe(c, api, False)
        if initial.activation(c) is not None:
            raise ValueError("activation appeared before member CAS")
        initial.kubectl("create", "-f", "-", "-o", "json", "--field-manager=" + MANAGER, body=canonical(enrollment_job(c, worker, facts)))
        for _ in range(12):
            actual = initial.activation(c)
            if actual is not None:
                check_activation(c, actual, facts["bundle_version"])
                break
            time.sleep(5)
        else:
            raise ValueError("member activation CAS did not complete")
    for _ in range(3):
        time.sleep(30)
        _, facts = observe(c, api, True)
        if initial.activation(c) != actual or initial.timestamp(facts["inventory_heartbeat_at"]) <= initial.timestamp(actual["updated_at"]):
            raise ValueError("fresh inventory did not follow exact member activation")
    evidence.update(activation=actual, completed_at=initial.now().isoformat())
    save(evidence)
    return evidence


def main():
    p = argparse.ArgumentParser()
    p.add_argument("declaration")
    p.add_argument("--validate-only", action="store_true")
    p.add_argument("--evidence")
    args = p.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 1 << 20:
        raise ValueError("member declaration exceeds bound")
    c = validate(json.loads(raw))
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(c)}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token or not args.evidence:
        raise ValueError("configuration credential and retained evidence required")
    def save(value):
        Path(args.evidence).write_text(canonical(value) + "\n")
    enroll(c, API(c["origin"], token), save)
    print(canonical({"completed": True, "declaration_digest": digest(c), "public_transport_changed": False}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("Cell member enrollment stopped: " + str(error)) from None
