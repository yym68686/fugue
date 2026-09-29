#!/usr/bin/env python3
"""Initialize one isolated cell from real gray serving observations.

This configuration operation cannot select public transport, publish full, or
attest artifact LKG. Its optional inventory permission has a fixed deadline.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.parse

from scripts.bootstrap_cell_producer import digest, selected
from scripts.observe_front_candidate import now, timestamp
from scripts.publish_agent_edge_shadow import API, canonical
from scripts.reconcile_front_probe_transport import kubectl

MANAGER = "fugue-cell-inventory-bootstrap"
DECLARATION = "fugue.pro/bootstrap-declaration"
KINDS = {"edge_route_bundle", "caddy_route_config"}


def validate(c):
    if set(c) - {"previous_permission"} != {"schema", "generation", "origin", "authority_cell_id", "namespace", "release_set", "cohort", "bootstrap_config_map", "worker", "observation"} or c["schema"] != "fugue.cell-inventory-enrollment/v1" or type(c["generation"]) is not int or c["generation"] < 1:
        raise ValueError("explicit cell inventory declaration required")
    url = urllib.parse.urlsplit(c["origin"])
    if url.scheme != "https" or c["origin"] != "https://" + str(url.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(url.hostname)):
        raise ValueError("canonical HTTPS origin required")
    if not re.fullmatch(r"cell-[a-z0-9]+(?:-[a-z0-9]+)*", c["authority_cell_id"]):
        raise ValueError("neutral cell identity required")
    w, r, o = c["worker"], c["release_set"], c["observation"]
    if set(w) != {"deployment", "deployment_uid", "pod", "instance_uid", "node", "slot", "container", "service_account", "source_sha", "image_digest", "pvc", "activation_path"} or set(r) != {"id", "digest"} or set(o) != {"samples", "interval_seconds", "timeout_seconds"}:
        raise ValueError("complete exact worker, parent and observation pins required")
    for value in [c["namespace"], c["bootstrap_config_map"], w["deployment"], w["pod"], w["node"], w["container"], w["service_account"], w["pvc"]]:
        if not isinstance(value, str) or len(value) > 253 or not re.fullmatch(r"[a-z0-9]+(?:[-.][a-z0-9]+)*", value):
            raise ValueError("canonical Kubernetes identity required")
    for value in [w["instance_uid"], w["deployment_uid"]]:
        if not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", value):
            raise ValueError("exact Kubernetes UID required")
    if not re.fullmatch(r"[a-f0-9]{40}", w["source_sha"]) or any(not re.fullmatch(r"sha256:[a-f0-9]{64}", value) for value in [w["image_digest"], r["digest"]]) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,256}", r["id"]) or not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", c["cohort"]):
        raise ValueError("immutable executable and gray release pins required")
    if w["slot"] not in ["a", "b"] or not w["activation_path"].startswith("/") or str(Path(w["activation_path"])) != w["activation_path"] or ".." in Path(w["activation_path"]).parts:
        raise ValueError("explicit slot and clean activation path required")
    if any(type(o[k]) is not int for k in o) or not 3 <= o["samples"] <= 6 or not 10 <= o["interval_seconds"] <= 30 or not 180 <= o["timeout_seconds"] <= 600:
        raise ValueError("bounded initial observation window required")
    previous = c.get("previous_permission")
    if "previous_permission" in c and (not isinstance(previous, dict) or set(previous) != {"uid", "declaration_digest", "authorization_digest", "generation"} or type(previous["generation"]) is not int or previous["generation"] != c["generation"] - 1 or previous["generation"] < 1 or not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", previous["uid"]) or any(not re.fullmatch(r"sha256:[a-f0-9]{64}", previous[k]) for k in ["declaration_digest", "authorization_digest"])):
        raise ValueError("explicit expired permission predecessor required")
    return c


def resource(c, kind, name):
    return kubectl("get", kind, name, "-n", c["namespace"], "-o", "json", "--ignore-not-found")


def isolated_worker(c):
    w, cell = c["worker"], c["authority_cell_id"]
    dep, pod = resource(c, "deployment", w["deployment"]), resource(c, "pod", w["pod"])
    if not dep or not pod:
        raise ValueError("declared executor is absent")
    dm, pm, spec, status = dep["metadata"], pod["metadata"], pod["spec"], pod["status"]
    if dm.get("uid") != w["deployment_uid"] or pm.get("uid") != w["instance_uid"] or any(m.get("deletionTimestamp") or m.get("annotations", {}).get("fugue.pro/authority-transition-role") != "isolated-candidate" for m in [dm, pm]) or dep["spec"].get("replicas") != 1 or dep.get("status", {}).get("readyReplicas") != 1 or dep.get("status", {}).get("observedGeneration") != dm.get("generation"):
        raise ValueError("isolated executor identity or readiness changed")
    labels, annotations = pm.get("labels", {}), pm.get("annotations", {})
    expected = {"fugue.io/authority-cell-id": cell, "fugue.io/edge-group-id": cell, "fugue.io/edge-slot": w["slot"], "fugue.io/fault-domain-id": w["node"]}
    if any(labels.get(k) != v for k, v in expected.items()) or annotations.get("fugue.pro/source-commit") != w["source_sha"] or spec.get("nodeName") != w["node"] or spec.get("serviceAccountName") != w["service_account"] or spec.get("hostNetwork") or spec.get("automountServiceAccountToken") is not False or any(p.get("hostPort") for container in spec["containers"] for p in container.get("ports", [])):
        raise ValueError("executor cell, source or isolation changed")
    owners = pm.get("ownerReferences", [])
    if len(owners) != 1 or owners[0].get("kind") != "ReplicaSet" or owners[0].get("controller") is not True:
        raise ValueError("executor controller is ambiguous")
    rs = resource(c, "replicaset", owners[0]["name"])
    if not rs or rs["metadata"]["uid"] != owners[0]["uid"] or not any(x.get("uid") == dm["uid"] and x.get("kind") == "Deployment" and x.get("controller") is True for x in rs["metadata"].get("ownerReferences", [])):
        raise ValueError("executor does not belong to declared deployment")
    container = next(x for x in spec["containers"] if x["name"] == w["container"])
    runtime = next(x for x in status["containerStatuses"] if x["name"] == w["container"])
    if status.get("phase") != "Running" or runtime.get("ready") is not True or runtime.get("restartCount") != 0 or not runtime.get("imageID", "").endswith("@" + w["image_digest"]) or not container["image"].endswith("@" + w["image_digest"]):
        raise ValueError("immutable executor image or process changed")
    env = {e["name"]: e.get("value") for e in container["env"]}
    if env.get("FUGUE_PLATFORM_ARTIFACT_SCOPE") != "authority-cell:" + cell or env.get("FUGUE_EDGE_INVENTORY_ACTIVATION_STATE_FILE") != w["activation_path"]:
        raise ValueError("executor activation or consumer scope differs")
    mount = next(m for m in container["volumeMounts"] if str(Path(m["mountPath"]) / Path(w["activation_path"]).name) == w["activation_path"])
    volumes = {v["name"]: v for v in spec["volumes"]}
    if mount.get("readOnly") is not True or not mount.get("subPath") or volumes[mount["name"]].get("persistentVolumeClaim", {}).get("claimName") != w["pvc"]:
        raise ValueError("executor activation is not isolated on its declared PVC")
    pvc = resource(c, "pvc", w["pvc"])
    if not pvc or pvc["metadata"].get("deletionTimestamp") or pvc["metadata"].get("annotations", {}).get("fugue.pro/authority-transition-role") != "isolated-candidate" or pvc.get("status", {}).get("phase") != "Bound":
        raise ValueError("initial state PVC is not a bound isolated candidate")
    permission_path = env.get("FUGUE_EDGE_INVENTORY_BOOTSTRAP_FILE", "")
    projections = [m for m in container["volumeMounts"] if m.get("readOnly") is True and str(Path(m["mountPath"]) / "authorization.json") == permission_path and volumes[m["name"]].get("configMap", {}).get("name") == c["bootstrap_config_map"]]
    if len(projections) != 1:
        raise ValueError("bootstrap projection is not independently bound")
    services = kubectl("get", "services", "-n", c["namespace"], "-o", "json")
    if services.get("metadata", {}).get("continue"):
        raise ValueError("transport observation is incomplete")
    for service in services["items"]:
        s = service["spec"]
        if s.get("selector") and all(labels.get(k) == v for k, v in s["selector"].items()):
            if s.get("type", "ClusterIP") != "ClusterIP" or s.get("externalIPs") or any(p.get("port") != 7832 or p.get("targetPort") not in [7832, "health"] for p in s.get("ports", [])):
                raise ValueError("initial executor is already selected by serving transport")
    return {"pod": pod, "image": container["image"], "mount": mount, "bootstrap_path": permission_path}


def activation(c):
    w = c["worker"]
    # Absence must be distinguished from permission, parsing and transport errors.
    result = subprocess.run(["kubectl", "-n", c["namespace"], "exec", w["pod"], "-c", w["container"], "--", "sh", "-ec", 'if test -e "$1"; then cat "$1"; else test -d "$(dirname "$1")"; printf null; fi', "sh", w["activation_path"]], capture_output=True, text=True, timeout=20)
    if result.returncode or len(result.stdout) > 16384:
        raise ValueError("activation observation unavailable")
    return json.loads(result.stdout)


def parent(c, api):
    a = api("GET", "/v1/admin/artifacts/" + c["release_set"]["id"])["artifact"]
    content, cell = a.get("content", {}), c["authority_cell_id"]
    topology = content.get("consumer_topology", {})
    if a.get("content_hash") != c["release_set"]["digest"] or digest(content) != a.get("content_hash") or a.get("status") != "validated" or a.get("artifact_kind") != "release_set" or a.get("scope_key") != "authority-cell:" + cell or content.get("publication_role") != "cell-routes" or set(content.get("artifact_kinds", [])) != KINDS or len(content.get("artifact_ids", [])) != 2 or len(set(content["artifact_ids"])) != 2:
        raise ValueError("declared parent is not the validated route-only release")
    if topology != {"publication_role": "cell-routes", "schema_version": "fugue.traffic-consumer-topology/v1", "authority_cell_id": cell, "edge_node_ids": [c["worker"]["node"]], "dns_node_ids": []} or {"id": c["cohort"], "edge_group_ids": [cell]} not in content.get("traffic_rollout_cohorts", []):
        raise ValueError("initial route-only cell membership or signed cohort differs")
    return a


def gray(c, api, publish=False):
    scope = "authority-cell:" + c["authority_cell_id"]
    if selected(api, scope, "full", "release_set").get("artifact"):
        raise ValueError("initial enrollment cannot alter established full authority")
    state = selected(api, scope, "gray", "release_set")
    a, r = state.get("artifact"), state.get("release")
    if not a and publish:
        parent(c, api)
        api("POST", "/v1/admin/artifacts/" + c["release_set"]["id"] + "/release", {"release_channel": "gray", "canary_rule_ref": "cohort=" + c["cohort"], "idempotency_key": "git-cell-inventory/" + digest(c), "reason": "isolated first cell inventory and private serving verification"})
        return gray(c, api)
    if not a or a["id"] != c["release_set"]["id"] or a["content_hash"] != c["release_set"]["digest"] or r.get("status") != "active" or r.get("canary_rule_ref") != "cohort=" + c["cohort"]:
        raise ValueError("gray authority differs from declared initial parent")
    return r


def permission(c, release, existing=None):
    w = c["worker"]
    fields = {"schema": "fugue.cell-inventory-bootstrap/v1", "authority_cell_id": c["authority_cell_id"], "edge_id": w["node"], "instance_uid": w["instance_uid"], "slot": w["slot"], "source_sha": w["source_sha"], "release_set_id": c["release_set"]["id"], "release_set_digest": c["release_set"]["digest"], "release_id": release["id"]}
    if existing is not None:
        meta = existing.get("metadata", {})
        if existing.get("immutable") is True or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or meta.get("annotations", {}).get(DECLARATION) != digest(c) or meta.get("deletionTimestamp") or set(existing.get("data", {})) != {"authorization.json"}:
            raise ValueError("bootstrap permission is foreign or changed")
        a = json.loads(existing["data"]["authorization.json"])
        if set(a) != set(fields) | {"issued_at", "expires_at"} or any(a.get(k) != v for k, v in fields.items()) or not timestamp(a["issued_at"]) <= now() < timestamp(a["expires_at"]) or not datetime.timedelta(0) < timestamp(a["expires_at"]) - timestamp(a["issued_at"]) <= datetime.timedelta(minutes=15):
            raise ValueError("bootstrap permission expired or changed; it is never automatically renewed")
        return existing, a
    issued = now().replace(microsecond=0)
    fields.update(issued_at=issued.isoformat(), expires_at=(issued + datetime.timedelta(minutes=15)).isoformat())
    return {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": c["bootstrap_config_map"], "namespace": c["namespace"], "labels": {"app.kubernetes.io/managed-by": MANAGER}, "annotations": {DECLARATION: digest(c)}}, "data": {"authorization.json": canonical(fields)}}, fields


def expired_predecessor(c, release, existing):
    """Only a new explicit declaration may replace its exact expired permission."""
    previous, meta = c.get("previous_permission"), existing.get("metadata", {})
    if not previous or meta.get("uid") != previous["uid"] or not meta.get("resourceVersion") or meta.get("annotations", {}).get(DECLARATION) != previous["declaration_digest"] or meta.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or meta.get("deletionTimestamp") or existing.get("immutable") is True or set(existing.get("data", {})) != {"authorization.json"}:
        raise ValueError("expired permission predecessor differs from explicit retry")
    a = json.loads(existing["data"]["authorization.json"])
    _, target = permission(c, release)
    # A new explicitly pinned executor may recover this still-unactivated
    # cell after a code rollout. Its Pod/source are rechecked independently;
    # the exact old permission digest, cell, node, slot and publication stay
    # bound, and only an already expired permission is replaceable.
    fixed = set(target) - {"issued_at", "expires_at", "instance_uid", "source_sha"}
    if digest(a) != previous["authorization_digest"] or set(a) != set(target) or any(a.get(k) != target[k] for k in fixed) or timestamp(a["expires_at"]) > now() or not datetime.timedelta(0) < timestamp(a["expires_at"]) - timestamp(a["issued_at"]) <= datetime.timedelta(minutes=15):
        raise ValueError("only an expired exact permission for this cell and publication can be replaced")
    return {"uid": meta["uid"], "resourceVersion": meta["resourceVersion"]}


def replace_permission(c, options, existing, target):
    # Patch the same mutable projection object. Immutable ConfigMaps stop
    # kubelet watches and cannot provide independently recoverable configuration.
    patch = [{"op": "test", "path": "/metadata/uid", "value": options["uid"]}, {"op": "test", "path": "/metadata/resourceVersion", "value": options["resourceVersion"]}, {"op": "test", "path": "/data", "value": existing["data"]}, {"op": "test", "path": "/metadata/annotations", "value": existing["metadata"]["annotations"]}, {"op": "replace", "path": "/data", "value": target["data"]}, {"op": "replace", "path": "/metadata/annotations/" + DECLARATION.replace("/", "~1"), "value": target["metadata"]["annotations"][DECLARATION]}]
    return kubectl("patch", "configmap", c["bootstrap_config_map"], "-n", c["namespace"], "--type=json", "--patch-file", "-", "-o", "json", "--field-manager=" + MANAGER, body=canonical(patch))


def check_projection(c, worker, authorization):
    w = c["worker"]
    projected = kubectl("-n", c["namespace"], "exec", w["pod"], "-c", w["container"], "--", "cat", worker["bootstrap_path"])
    if projected != authorization:
        raise ValueError("Worker still observes a different bootstrap projection")


def health(c, release):
    w = c["worker"]
    h = kubectl("-n", c["namespace"], "exec", w["pod"], "-c", w["container"], "--", "wget", "-qO-", "http://127.0.0.1:7832/healthz")
    serving, binding = h.get("platform_serving", {}), h.get("platform_serving", {}).get("traffic_release", {})
    if h.get("healthy") is not True or h.get("edge_id") != w["node"] or h.get("edge_group_id") != c["authority_cell_id"] or h.get("route_bundle_source") != "edge-control-group-authority/v1" or h.get("route_count", 0) < 1 or h.get("publication_sequence", 0) < 1 or h.get("stale_cache") or not h.get("bundle_version") or h.get("caddy_applied_version") != h["bundle_version"] or h.get("inventory_producer_active") is not True:
        raise ValueError("initial cell has not loaded a healthy real group bundle")
    if serving.get("state") != "serving_verified" or serving.get("bundle_version") != h["bundle_version"] or serving.get("route_probes", 0) < 1 or serving.get("tls_probes", 0) < 1 or binding.get("release_set_id") != c["release_set"]["id"] or binding.get("release_set_digest") != c["release_set"]["digest"] or binding.get("release_id") != release["id"] or binding.get("fencing_token") != release["fencing_token"] or binding.get("scope_key") != "authority-cell:" + c["authority_cell_id"] or binding.get("release_channel") != release.get("release_channel", "gray"):
        raise ValueError("initial route and TLS proof is not bound to this publication")
    for value in [h.get("inventory_heartbeat_at", ""), serving.get("verified_at", ""), serving.get("reported_at", "")]:
        if not datetime.timedelta(0) <= now() - timestamp(value) < datetime.timedelta(seconds=90):
            raise ValueError("initial serving proof is stale")
    return h


def convergence(c, api, release):
    query = urllib.parse.urlencode({"release_set_id": c["release_set"]["id"], "artifact_release_id": release["id"]})
    values = api("GET", "/v1/admin/platform-state/convergence?" + query)["convergence"]
    if len(values) != 2 or {v["artifact_kind"] for v in values} != KINDS:
        raise ValueError("initial expected membership is incomplete")
    for value in values:
        if value.get("pass") is not True or value.get("required_expected") != 1 or value.get("required_passing") != 1 or len(value.get("assessments", [])) != 1:
            raise ValueError("initial authenticated consumer convergence is not positive")
        a = value["assessments"][0]
        observed = a.get("observed", {})
        if a.get("state") != "pass" or observed.get("identity_verified") is not True or observed.get("credential_id") != "kubernetes:" + c["namespace"] + ":" + c["worker"]["service_account"] + ":" + c["worker"]["instance_uid"] or observed.get("release_set_id") != c["release_set"]["id"] or observed.get("fencing_token") != release["fencing_token"]:
            raise ValueError("initial convergence belongs to a different executor or authority")
    return values


def activation_job(c, worker, observed, authorization):
    w = c["worker"]
    args = ["/usr/local/bin/fugue-edge-front-cas", "--state-file", w["activation_path"], "--group", c["authority_cell_id"], "--expected-generation", "0", "--expected-slot", w["slot"], "--target-slot", w["slot"], "--bundle-generation", observed["bundle_version"], "--worker-source-commit", w["source_sha"], "--worker-image-digest", w["image_digest"], "--operation", "initialize", "--reason", "Verified isolated initial cell publication " + digest(c)]
    mount = dict(worker["mount"], readOnly=False)
    spec = {"restartPolicy": "Never", "automountServiceAccountToken": False, "nodeName": w["node"], "containers": [{"name": "activation-cas", "image": worker["image"], "imagePullPolicy": "IfNotPresent", "command": ["sh", "-ec", 'test "$(date +%s)" -lt "$1"; shift; exec "$@"', "sh"], "args": [str(int(timestamp(authorization["expires_at"]).timestamp())), *args], "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}, "resources": {"requests": {"cpu": "10m", "memory": "16Mi"}, "limits": {"memory": "64Mi"}}, "volumeMounts": [mount]}], "volumes": [{"name": mount["name"], "persistentVolumeClaim": {"claimName": w["pvc"]}}]}
    return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {"name": "cell-activate-" + digest(c)[7:23], "namespace": c["namespace"], "labels": {"app.kubernetes.io/managed-by": MANAGER}, "annotations": {DECLARATION: digest(c)}}, "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 60, "ttlSecondsAfterFinished": 3600, "template": {"spec": spec}}}


def check_activation(c, a, bundle=None):
    w = c["worker"]
    fields = {"schema": "edge-front-group-activation/v1", "edge_group_id": c["authority_cell_id"], "generation": 1, "active_slot": w["slot"], "worker_source_commit": w["source_sha"], "worker_image_digest": w["image_digest"], "authority": "edge-control", "operation": "initialize", "reason": "Verified isolated initial cell publication " + digest(c)}
    if not isinstance(a, dict) or any(a.get(k) != v for k, v in fields.items()) or not a.get("bundle_generation") or bundle is not None and a["bundle_generation"] != bundle:
        raise ValueError("real activation differs from this observed initialization")


def enroll(c, api, evidence):
    validate(c)
    initial = isolated_worker(c)
    previous = activation(c)
    if previous is not None:
        check_activation(c, previous)
        release = gray(c, api)
    else:
        parent(c, api)
        release = gray(c, api, publish=True)
        api("POST", "/v1/admin/platform-config/release-set/prepare-consumers", {"release_set_id": c["release_set"]["id"], "artifact_release_id": release["id"]})
        isolated_worker(c)
        if activation(c) is not None or gray(c, api)["id"] != release["id"]:
            raise ValueError("initial authority changed before enrollment")
        existing = resource(c, "configmap", c["bootstrap_config_map"])
        if existing is not None and existing.get("metadata", {}).get("annotations", {}).get(DECLARATION) != digest(c):
            options = expired_predecessor(c, release, existing)
            isolated_worker(c)
            if activation(c) is not None or gray(c, api)["id"] != release["id"]:
                raise ValueError("authority changed before explicit expired permission replacement")
            target, _ = permission(c, release)
            replace_permission(c, options, existing, target)
            evidence["replaced_expired_permission_uid"] = options["uid"]
            existing = resource(c, "configmap", c["bootstrap_config_map"])
            if existing is None or existing["metadata"]["uid"] != options["uid"]:
                raise ValueError("permission projection object changed during explicit retry")
        cm, authorization = permission(c, release, existing)
        if existing is None:
            kubectl("create", "-f", "-", "-o", "json", "--field-manager=" + MANAGER, body=canonical(cm))
        evidence["authorization"] = authorization
    evidence["release_id"] = release["id"]
    deadline = time.monotonic() + c["observation"]["timeout_seconds"]
    observations, last_error = [], "awaiting initial serving"
    while time.monotonic() < deadline:
        isolated_worker(c)
        if gray(c, api)["id"] != release["id"]:
            raise ValueError("initial gray authority changed during observation")
        try:
            if previous is None:
                check_projection(c, initial, authorization)
            observed = health(c, release)
            convergence(c, api, release)
            observations.append({"at": now().isoformat(), "bundle_version": observed["bundle_version"], "route_probes": observed["platform_serving"]["route_probes"], "tls_probes": observed["platform_serving"]["tls_probes"]})
        except ValueError as error:
            observations, last_error = [], str(error)
            evidence["last_observation_error"] = last_error
        evidence["observations"] = observations
        if len(observations) >= c["observation"]["samples"]:
            break
        time.sleep(c["observation"]["interval_seconds"])
    else:
        raise ValueError(last_error)
    if previous is None:
        permission(c, release, resource(c, "configmap", c["bootstrap_config_map"]))
        isolated_worker(c)
        observed = health(c, release)
        convergence(c, api, release)
        if activation(c) is not None:
            raise ValueError("activation appeared before initial CAS")
        job = activation_job(c, initial, observed, authorization)
        kubectl("create", "-f", "-", "-o", "json", "--field-manager=" + MANAGER, body=canonical(job))
        # The job can only initialize an absent file on the isolated PVC.
        for _ in range(12):
            current = activation(c)
            if current is not None:
                check_activation(c, current, observed["bundle_version"])
                break
            time.sleep(5)
        else:
            raise ValueError("initial activation CAS did not complete")
    actual = activation(c)
    check_activation(c, actual)
    for _ in range(3):
        time.sleep(30)
        isolated_worker(c)
        if activation(c) != actual or gray(c, api)["id"] != release["id"]:
            raise ValueError("initialized cell authority changed during final observation")
        observed = health(c, release)
        convergence(c, api, release)
        if timestamp(observed["inventory_heartbeat_at"]) <= timestamp(actual["updated_at"]):
            raise ValueError("inventory has not advanced after real activation")
    evidence["activation"] = actual
    evidence["completed_at"] = now().isoformat()
    evidence.pop("last_observation_error", None)
    return evidence


def main():
    p = argparse.ArgumentParser()
    p.add_argument("declaration")
    p.add_argument("--validate-only", action="store_true")
    p.add_argument("--evidence")
    args = p.parse_args()
    c = validate(json.loads(Path(args.declaration).read_text()))
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(c)}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token or not args.evidence:
        raise ValueError("configuration credential and evidence path required")
    evidence = {"schema": "fugue.cell-inventory-enrollment-result/v1", "declaration_digest": digest(c), "public_transport_changed": False, "full_published": False, "artifact_lkg_attested": False}
    try:
        enroll(c, API(c["origin"], token), evidence)
    except Exception as error:
        evidence["failure"] = str(error)
        evidence["failed_at"] = now().isoformat()
        raise
    finally:
        Path(args.evidence).write_text(canonical(evidence) + "\n")
    print(canonical(evidence))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("initial cell inventory stopped: " + str(error)) from None
