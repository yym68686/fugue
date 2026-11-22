import copy
import datetime
import unittest

from scripts import dns_transport_rollback_authority as authority


class DNSTransportRollbackAuthorityTests(unittest.TestCase):
    def inputs(self):
        ref = {"artifact_id": "parent", "parent_digest": "sha256:" + "a" * 64, "release_id": "rollback",
               "fencing_token": 9, "lkg_release_id": "historical", "lkg_evidence_hash": "sha256:" + "b" * 64}
        identity = {"artifact_id": "parent", "generation": "same-content", "artifact_kind": "release_set", "scope_key": "global"}
        artifact = dict(identity, id="parent", status="validated", content_hash=ref["parent_digest"])
        release = dict(identity, id="rollback", scope={"scope_type": "global"}, fencing_token=9, status="active", release_channel="full",
                       verification_state="serving_unverified", released_at="2026-01-01T00:00:00Z",
                       released_by_type="bootstrap", released_by_id="platform-config-producer",
                       verified_lkg_generation="same-content", rollback_target_generation="same-content",
                       pinned_rollback_generation="same-content")
        message = dict(identity, id="original", scope={"scope_type": "global"}, release_id="rollback", release_channel="full", message_type="rollback",
                       created_at=release["released_at"])
        lkg = dict(identity, content_hash=ref["parent_digest"], expires_at="2026-01-01T01:00:00Z",
                   verified_by_release_id="historical", verification_evidence_hash=ref["lkg_evidence_hash"])
        snapshot = {"ready": True, "parent_digest": ref["parent_digest"], "assignment": {
            "release_set_id": "parent", "artifact_release_id": "rollback", "fencing_token": 9,
            "artifact_kind": "dns_answer_bundle", "scope_key": "global", "release_channel": "full"}}
        return snapshot, {"artifact": artifact, "release": release, "current_rollback_message": message}, lkg, ref

    def test_only_reviewed_full_rollback_with_unexpired_lkg_is_admitted(self):
        now = datetime.datetime(2026, 1, 1, 0, 30, tzinfo=datetime.timezone.utc)
        original = self.inputs()
        authority.validate_rollback(*original, now=now)
        for key, mutate in {
            "unready": lambda s, p, l, r: s.update(ready=False),
            "other_fence": lambda s, p, l, r: s["assignment"].update(fencing_token=10),
            "other_release": lambda s, p, l, r: p["release"].update(id="other"),
            "superseded": lambda s, p, l, r: p["release"].update(status="superseded"),
            "unsigned_origin": lambda s, p, l, r: p["release"].update(released_by_id="other"),
            "override": lambda s, p, l, r: p["release"].update(override_mode="force"),
            "regular_publication": lambda s, p, l, r: p["release"].update(rollback_target_generation=""),
            "failed": lambda s, p, l, r: p["release"].update(verification_state="failed"),
            "legacy_feed_cannot_authorize": lambda s, p, l, r: p.update(messages=[p.pop("current_rollback_message")]),
            "array_not_unique_proof": lambda s, p, l, r: p.update(current_rollback_message=[p["current_rollback_message"]]*2),
            "earlier_message": lambda s, p, l, r: p["current_rollback_message"].update(created_at="2025-12-31T23:59:59Z"),
            "other_message_release": lambda s, p, l, r: p["current_rollback_message"].update(release_id="other"),
            "other_message_scope": lambda s, p, l, r: p["current_rollback_message"].update(scope={"key": "other"}),
            "expired_lkg": lambda s, p, l, r: l.update(expires_at="2026-01-01T00:29:59Z"),
            "other_lkg": lambda s, p, l, r: l.update(content_hash="sha256:" + "d" * 64),
            "unproven_lkg": lambda s, p, l, r: l.update(verification_evidence_hash="sha256:" + "d" * 64),
            "unverified_self_reference": lambda s, p, l, r: l.update(verified_by_release_id="rollback"),
            "not_yet_confirmed": lambda s, p, l, r: p["release"].update(verification_state="verified"),
            "changed_reference": lambda s, p, l, r: r.update(parent_digest="sha256:" + "d" * 64),
        }.items():
            with self.subTest(case=key), self.assertRaises(ValueError):
                current = copy.deepcopy(original)
                mutate(*current)
                authority.validate_rollback(*current, now=now)

    def test_same_rollback_can_be_independently_verified_during_handoff(self):
        now = datetime.datetime(2026, 1, 1, 0, 30, tzinfo=datetime.timezone.utc)
        snapshot, publication, lkg, ref = self.inputs()
        publication["release"]["verification_state"] = "verified"
        lkg.update(verified_by_release_id="rollback", verification_evidence_hash="sha256:" + "c" * 64)
        authority.validate_rollback(snapshot, publication, lkg, ref, now=now)
        del publication["current_rollback_message"]
        with self.assertRaises(ValueError):
            authority.validate_rollback(snapshot, publication, lkg, ref, now=now)
