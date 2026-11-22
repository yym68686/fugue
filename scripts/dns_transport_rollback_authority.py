"""Read-only checks for restoring transport to exact, previously verified DNS content."""
import datetime
import re


def validate_reference(reference):
    if (not isinstance(reference, dict) or set(reference) != {
        "artifact_id", "parent_digest", "release_id", "fencing_token", "lkg_release_id", "lkg_evidence_hash"
    } or any(not isinstance(reference[k], str) or not re.fullmatch(r"[a-z0-9_]{1,128}", reference[k])
             for k in ("artifact_id", "release_id", "lkg_release_id")) or
            any(not isinstance(reference[k], str) or not re.fullmatch(r"sha256:[a-f0-9]{64}", reference[k])
                for k in ("parent_digest", "lkg_evidence_hash")) or
            type(reference["fencing_token"]) is not int or reference["fencing_token"] < 1):
        raise ValueError("exact DNS transport rollback authority reference required")


def utc_timestamp(value):
    if not isinstance(value, str):
        raise ValueError("DNS transport rollback timestamp is missing")
    timestamp = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.utcoffset() != datetime.timedelta(0):
        raise ValueError("DNS transport rollback timestamp must be UTC")
    return timestamp


def validate_rollback(snapshot, publication, lkg, reference, now=None):
    """Recovery requires BOTH live readiness and the exact historical binding."""
    if snapshot.get("ready") is not True:
        raise ValueError("DNS transport backend is not ready for the exact current full rollback")
    validate_rollback_binding(snapshot, publication, lkg, reference, now)


def validate_rollback_binding(snapshot, publication, lkg, reference, now=None):
    """Read-only identity check. It does not establish runtime readiness."""
    validate_reference(reference)
    now = now or datetime.datetime.now(datetime.timezone.utc)
    artifact, release = publication.get("artifact", {}), publication.get("release", {})
    assignment = snapshot.get("assignment", {})
    generation = artifact.get("generation")
    identity = {"artifact_id": reference["artifact_id"], "artifact_kind": "release_set", "scope_key": "global",
                "generation": generation}
    if (not generation or
            artifact.get("status") != "validated" or artifact.get("id") != reference["artifact_id"] or
            artifact.get("artifact_kind") != "release_set" or artifact.get("scope_key") != "global" or
            artifact.get("content_hash") != reference["parent_digest"] or
            snapshot.get("parent_digest") != reference["parent_digest"] or
            any(assignment.get(key) != value for key, value in {
                "release_set_id": reference["artifact_id"], "artifact_release_id": reference["release_id"],
                "fencing_token": reference["fencing_token"], "release_channel": "full", "scope_key": "global",
                "artifact_kind": "dns_answer_bundle"
            }.items()) or
            any(release.get(key) != value for key, value in identity.items()) or
            release.get("id") != reference["release_id"] or release.get("fencing_token") != reference["fencing_token"] or
            release.get("status") != "active" or release.get("release_channel") != "full" or
            release.get("released_by_type") != "bootstrap" or release.get("released_by_id") != "platform-config-producer" or
            release.get("override_mode") or release.get("bypassed_invariants") or
            any(release.get(key) != generation for key in (
                "verified_lkg_generation", "pinned_rollback_generation", "rollback_target_generation"))):
        raise ValueError("DNS transport backend does not match the exact current full rollback")
    released_at = utc_timestamp(release.get("released_at"))
    # The feed is a recent display window, not an identity proof. Only the
    # server can exclude duplicates hidden outside that window in its snapshot.
    message = publication.get("current_rollback_message")
    if (released_at > now or not isinstance(message, dict) or not message.get("id") or
            message.get("message_type") != "rollback" or message.get("release_id") != reference["release_id"] or
            message.get("release_channel") != "full" or any(message.get(k) != v for k, v in identity.items()) or
            not isinstance(release.get("scope"), dict) or message.get("scope") != release["scope"] or
            message.get("created_at") != release.get("released_at")):
        raise ValueError("DNS transport lacks the exact current rollback ledger message")
    if (any(lkg.get(key) != value for key, value in identity.items()) or
            lkg.get("content_hash") != reference["parent_digest"] or utc_timestamp(lkg.get("expires_at")) <= now):
        raise ValueError("DNS transport rollback lacks an unexpired LKG for identical content")
    by, evidence = lkg.get("verified_by_release_id"), lkg.get("verification_evidence_hash")
    if (release.get("verification_state") == "verified" and by == reference["release_id"] and
            isinstance(evidence, str) and re.fullmatch(r"sha256:[a-f0-9]{64}", evidence)):
        return  # This exact activation has independently reached verification.
    if (release.get("verification_state") != "serving_unverified" or by == reference["release_id"] or
            by != reference["lkg_release_id"] or evidence != reference["lkg_evidence_hash"]):
        raise ValueError("DNS transport rollback does not match the reviewed historical verification")


def require_current_rollback(snapshot, failure):
    from scripts.recover_dns_transport import read_recovery_api

    ref = failure["rollback"]
    validate_reference(ref)
    lkg = read_recovery_api(failure["api"], "/v1/admin/artifacts/" + ref["artifact_id"] + "/lkg")
    # Read the selected full last. A historic, but still retained, rollback
    # record cannot substitute for the publication currently being served.
    full = read_recovery_api(failure["api"], "/v1/platform-state/artifacts/release_set?"
                             "scope_key=global&channel=full&include_current_rollback_message=true")
    validate_rollback(snapshot, full, lkg["lkg"], ref)
