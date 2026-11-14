"""Select configuration-only inputs through the existing fenced release API.

Only representation changes. No traffic publication, history deletion, renewal
of observations, relaxed LKG gate, or live-workload mutation is performed here.
"""
import copy
import json
import re
import time
from pathlib import Path

from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.compiler_input_storage import command, readers
from scripts.observe_front_candidate import now, timestamp
from scripts.reconfigure_cell_producer import DIGEST, IDENTITY, validate_ref

SCHEMA = "fugue.runtime-input-reconfiguration/v1"


class BaselineNotVerified(ValueError):
    pass


def compatible_revision(revision):
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("runtime input reader revision missing")
    requirements = {
        "internal/platformconfig/runtime_input.go": (
            'ConfigurationInputCompilerVersion = "platform-config-compiler/v37"',
            "func ArtifactRuntimeInputDigest", "lineage.RuntimeInputDigest != runtime"),
        "internal/store/platform_runtime_input.go": (
            "GetPlatformArtifactRuntimeInputContext", "VerifyPlatformArtifactIntegrity",
            "platformconfig.ArtifactRuntimeInputDigest(a)", "compactDigest != digest"),
        "internal/store/platform_input_hot_dependencies.go": ("retainOriginal", "runtime_input_digest"),
        "internal/api/platform_compiler_input.go": ("runtime_input_digest", "GetPlatformArtifactRuntimeInputContext"),
        "internal/platformproducer/runtime_input_update.go": ("ValidateRuntimeInputUpdate", "reflect.DeepEqual"),
    }
    for path, markers in requirements.items():
        body = command(["git", "show", revision + ":" + path])
        if any(marker not in body for marker in markers):
            raise ValueError("current or rollback source lacks configuration input support")


def check_readers():
    population = readers(json.loads(Path("deploy/releases/components.json").read_text()), archive_required=True)
    for revision in sorted({r[k] for r in population for k in ("revision", "rollback_revision")}):
        compatible_revision(revision)
    return population


def validate(c):
    fields = {"schema", "generation", "origin", "target_scope", "producer_generation", "projection_generation", "precondition", "baseline_mode"}
    if not isinstance(c, dict) or set(c) != fields or c["schema"] != SCHEMA or type(c["generation"]) is not int or c["generation"] < 1:
        raise ValueError("explicit runtime input declaration required")
    if not re.fullmatch(r"https://[a-z0-9][a-z0-9.-]+[a-z0-9]", c["origin"]) or not re.fullmatch(r"global|authority-cell:cell-[a-z0-9]+(?:-[a-z0-9]+)*", c["target_scope"]):
        raise ValueError("canonical origin and scope required")
    if c["baseline_mode"] not in ("declared", "latest_verified_same_policy") or not IDENTITY.fullmatch(c["producer_generation"]):
        raise ValueError("explicit baseline mode and generation required")
    r = c["precondition"]
    if not IDENTITY.fullmatch(c["projection_generation"]):
        raise ValueError("new projection generation required")
    if set(r) != {"operation", "previous_policy", "serving_full", "verification_evidence_hash"} or r["operation"] != "runtime_input" or not DIGEST.fullmatch(r["verification_evidence_hash"]):
        raise ValueError("exact runtime input preconditions required")
    for field in ("previous_policy", "serving_full"):
        validate_ref(r[field])
    return c


def producer_scope(c):
    return "platform-config-producer" + (":" + c["target_scope"].split(":", 1)[1] if c["target_scope"] != "global" else "")


def baseline(c, api):
    full = selected(api, c["target_scope"], "full", "release_set")
    if authority_identity(full) != c["precondition"]["serving_full"]:
        raise ValueError("serving baseline changed")
    a, r = full["artifact"], full["release"]
    role = "cell-routes" if c["target_scope"] != "global" else ""
    if a.get("content", {}).get("publication_role", "") != role or a.get("metadata", {}).get("producer_policy_release_id") != c["precondition"]["previous_policy"]["release_id"] or r.get("released_by_type") != "bootstrap" or r.get("released_by_id") != "platform-config-producer":
        raise ValueError("baseline is not a verified predecessor-owned publication")
    if r.get("verification_state") != "verified" or r.get("verified_lkg_generation") != a["generation"]:
        raise BaselineNotVerified("predecessor publication is not yet verified")
    lkg = api("GET", "/v1/admin/artifacts/" + a["id"] + "/lkg").get("lkg") or {}
    if any(lkg.get(k) != v for k, v in {"artifact_id": a["id"], "content_hash": a["content_hash"], "artifact_kind": "release_set", "scope_key": c["target_scope"], "verified_by_release_id": r["id"], "verification_evidence_hash": c["precondition"]["verification_evidence_hash"]}.items()) or timestamp(lkg["expires_at"]) <= now():
        raise ValueError("exact unexpired positive LKG required")
    gray = selected(api, c["target_scope"], "gray", "release_set").get("release")
    if gray and gray.get("verification_state") != "failed" and timestamp(gray["released_at"]) > timestamp(r["released_at"]):
        raise BaselineNotVerified("a newer gray publication is still in progress")
    return full


def resolve_baseline(c, api):
    if c["baseline_mode"] == "declared":
        baseline(c, api)
        return c
    state = selected(api, c["target_scope"], "full", "release_set")
    ref = authority_identity(state)
    declared = c["precondition"]["serving_full"]
    if ref == declared:
        baseline(c, api)
        return c
    if not ref or ref["fencing_token"] <= declared["fencing_token"]:
        raise ValueError("serving fence regressed")
    lkg = api("GET", "/v1/admin/artifacts/" + ref["artifact_id"] + "/lkg").get("lkg") or {}
    if not DIGEST.fullmatch(lkg.get("verification_evidence_hash", "")):
        raise ValueError("renewed baseline lacks positive evidence")
    resolved = copy.deepcopy(c)
    resolved["precondition"].update(serving_full=ref, verification_evidence_hash=lkg["verification_evidence_hash"])
    baseline(resolved, api)  # same declared producer; never an unverified rollback fence
    return resolved


def wait_baseline(c, api):
    # Only read-only positive-verification waiting is retried. Never retry a
    # release/CAS error, foreign producer, corrupt source or expired LKG.
    deadline = time.monotonic() + 240
    for attempt in range(25):
        try:
            return resolve_baseline(c, api)
        except BaselineNotVerified:
            if c["baseline_mode"] != "latest_verified_same_policy" or attempt == 24 or time.monotonic() + 10 > deadline:
                raise
            time.sleep(10)


def publish(declaration, api, save):
    validate(declaration)
    c = wait_baseline(declaration, api)
    scope, pre = producer_scope(c), c["precondition"]
    previous = api("GET", "/v1/admin/artifacts/" + pre["previous_policy"]["artifact_id"])["artifact"]
    p = previous["content"]
    if not exact(previous, "policy_snapshot", scope, p) or previous.get("status") != "validated" or previous["content_hash"] != pre["previous_policy"]["content_hash"] or p.get("target_scope") != c["target_scope"] or p.get("mode") != "serving" or not p.get("serving") or p["serving"].get("single_publication", False) or p.get("input_source") != "business-static-intent" or p["generation"] == c["producer_generation"]:
        raise ValueError("exact continuously serving predecessor required")
    source = api("GET", "/v1/admin/artifacts/" + p["dns_policy_artifact_id"])["artifact"]
    if not exact(source, "policy_snapshot", c["target_scope"], source["content"]) or source.get("status") != "validated" or source["content_hash"] != p["dns_policy_digest"]:
        raise ValueError("pinned projection source differs")
    projection = copy.deepcopy(source["content"])
    if projection.get("runtime_input_mode", "") != "" or projection["generation"] == c["projection_generation"]:
        raise ValueError("new opt-in projection generation required")
    # Derive from the exact immutable source: declarations cannot inject any
    # new operational fields, schedules, placement or serving controls.
    projection.update(generation=c["projection_generation"], runtime_input_mode="configuration-v1")
    current = selected(api, scope, "shadow")
    # Preparing immutable drafts is allowed only under the reviewed predecessor.
    # An exact successor is accepted later solely for lost-response retries.
    if authority_identity(current) != pre["previous_policy"] and current.get("artifact", {}).get("generation") != c["producer_generation"]:
        raise ValueError("producer changed")
    source = ensure_input(api, "policy_snapshot", c["target_scope"], projection)
    successor = copy.deepcopy(p)
    successor.update(generation=c["producer_generation"], dns_policy_artifact_id=source["id"], dns_policy_digest=source["content_hash"])
    if authority_identity(current) != pre["previous_policy"] and not exact(current.get("artifact", {}), "policy_snapshot", scope, successor):
        raise ValueError("selected successor differs")
    artifact = ensure_input(api, "policy_snapshot", scope, successor)
    preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + artifact["id"] + "&view=configuration")
    if preview.get("policy", {}).get("scope") != c["target_scope"] or preview["policy"].get("runtime_input_mode") != "configuration-v1" or not preview.get("business_snapshot_revision"):
        raise ValueError("configuration preview did not preserve runtime input mode")
    baseline(c, api)
    evidence = {"schema": "fugue.runtime-input-reconfiguration-result/v1", "declaration_digest": digest(declaration), "declared_precondition": declaration["precondition"], "resolved_precondition": pre, "target_scope": c["target_scope"], "artifact_id": artifact["id"], "public_transport_changed": False, "audit_inputs_deleted": 0, "producer_activated": False}
    save(evidence)
    key = "producer-reconfiguration/" + digest({"artifact_id": artifact["id"], "content_hash": artifact["content_hash"], "precondition": pre})
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "shadow", "producer_reconfiguration": pre, "idempotency_key": key, "reason": "explicit configuration input representation; declaration " + digest(declaration)})
    after = selected(api, scope, "shadow")
    if not exact(after.get("artifact", {}), "policy_snapshot", scope, successor) or after.get("release", {}).get("idempotency_key") != key:
        raise ValueError("runtime input successor not confirmed")
    evidence.update(authority=authority_identity(after), producer_activated=True, completed_at=now().isoformat())
    save(evidence)
    return evidence
