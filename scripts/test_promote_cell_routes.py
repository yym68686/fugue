import copy
import datetime
import unittest
from unittest.mock import patch

from scripts import promote_cell_routes as p
from scripts.test_bootstrap_cell_inventory import config as enrollment


def config():
    e = enrollment()
    return {"schema": "fugue.private-cell-route-promotion/v1", "generation": 1, "enrollment": "deploy/environments/production/cell-inventory/cell-one.json", "enrollment_digest": p.digest(e), "gray_release": {"id": "gray-one", "fencing_token": 1}, "observation": {"samples": 3, "interval_seconds": 15, "timeout_seconds": 180}}


class PromotionTests(unittest.TestCase):
    def test_requires_explicit_pins_and_bounded_observation(self):
        self.assertEqual(config(), p.validate(config()))
        for mutate in [lambda c: c.update(enrollment="/tmp/untrusted.json"), lambda c: c.update(enrollment_digest="latest"), lambda c: c.update(generation=True), lambda c: c["gray_release"].update(fencing_token=0), lambda c: c["observation"].update(samples=1), lambda c: c["observation"].update(timeout_seconds=3600)]:
            c = config()
            mutate(c)
            with self.assertRaises(ValueError):
                p.validate(c)

    def test_lkg_rejects_foreign_or_expired_recovery_state(self):
        e = enrollment()
        good = {"artifact_id": e["release_set"]["id"], "content_hash": e["release_set"]["digest"], "artifact_kind": "release_set", "scope_key": "authority-cell:" + e["authority_cell_id"], "verified_by_release_id": "gray-one", "verification_evidence_hash": "evidence", "expires_at": (p.now() + datetime.timedelta(minutes=5)).isoformat()}
        self.assertEqual(good, p.lkg(e, lambda *a: {"lkg": good}, ["gray-one"]))
        for update in [{"artifact_id": "foreign"}, {"verified_by_release_id": "unrelated"}, {"verification_evidence_hash": ""}, {"scope_key": "global"}, {"expires_at": (p.now() - datetime.timedelta(seconds=1)).isoformat()}]:
            changed = dict(good, **update)
            with self.assertRaises(ValueError):
                p.lkg(e, lambda *a: {"lkg": changed}, ["gray-one"])

    def test_serving_window_requires_fresh_advancing_probes_and_inventory(self):
        c, e = config(), enrollment()
        baseline = {"updated_at": (p.now() - datetime.timedelta(minutes=5)).isoformat()}
        release = {"id": "gray-one", "fencing_token": 1, "release_channel": "gray"}
        def h(i):
            at = (p.now() - datetime.timedelta(seconds=30-i)).isoformat()
            return {"bundle_version": "bundle-" + str(i), "inventory_heartbeat_at": at, "platform_serving": {"verified_at": at, "route_probes": 4, "tls_probes": 3}}
        for advancing in [True, False]:
            values = [h(i) for i in range(3)] if advancing else [h(0)] * 3
            with patch.object(p.cell, "isolated_worker"), patch.object(p.cell, "activation", return_value=baseline), patch.object(p, "active_publication"), patch.object(p.cell, "health", side_effect=values), patch.object(p.cell, "convergence"), patch.object(p.time, "sleep"):
                if advancing:
                    witness = p.observe(c, e, None, release, baseline)
                    self.assertEqual(3, len(witness["observations"]))
                else:
                    with self.assertRaisesRegex(ValueError, "did not advance"):
                        p.observe(c, e, None, release, baseline)

    def test_failed_or_stale_witness_cannot_attest_lkg(self):
        c, e = config(), enrollment()
        release = {"id": "gray-one", "fencing_token": 1, "release_channel": "gray"}
        baseline = {"generation": 1}
        witness = {"declaration_digest": p.digest(c), "enrollment_digest": p.digest(e), "release_id": release["id"], "fencing_token": 1, "activation": baseline, "worker_uid": e["worker"]["instance_uid"], "observations": [{}] * 3, "completed_at": p.now().isoformat()}
        for mutate in [lambda w: w.update(completed_at=(p.now()-datetime.timedelta(minutes=5)).isoformat()), lambda w: w.update(worker_uid="foreign"), lambda w: w.update(fencing_token=2), lambda w: w.update(observations=[])]:
            writes = []
            changed = copy.deepcopy(witness)
            mutate(changed)
            with self.assertRaises(ValueError):
                p.verify(c, e, lambda *args: writes.append(args), release, changed, baseline, True)
            self.assertEqual([], writes)
        with patch.object(p.cell, "isolated_worker"), patch.object(p.cell, "activation", return_value=baseline), patch.object(p, "active_publication"), patch.object(p.cell, "health", side_effect=ValueError("TLS failed")):
            writes = []
            with self.assertRaisesRegex(ValueError, "TLS failed"):
                p.verify(c, e, lambda *args: writes.append(args), release, witness, baseline, True)
            self.assertEqual([], writes)

    def test_full_waits_for_gray_lkg_and_uses_a_new_window(self):
        c, e = config(), enrollment()
        gray = {"id": "gray-one", "fencing_token": 1, "release_channel": "gray"}
        full = {"id": "full-one", "fencing_token": 1, "release_channel": "full", "idempotency_key": "git-cell-full/" + p.digest(c)}
        events, published = [], [False]
        def api(method, path, body=None):
            events.append((method, path))
            if path.endswith("/release"):
                self.assertIn(("verify", "gray"), events)
                self.assertIn(("save", "gray_lkg"), events)
                published[0] = True
            return {}
        def publication(e, api, channel):
            return gray if channel == "gray" else full if published[0] else None
        def observe(c, e, api, release, baseline):
            events.append(("observe", release["release_channel"]))
            return {"channel": release["release_channel"]}
        def verify(c, e, api, release, witness, baseline, initial):
            self.assertEqual(release["release_channel"], witness["channel"])
            self.assertEqual(release == gray, initial)
            events.append(("verify", release["release_channel"]))
            return {"verified_by_release_id": release["id"]}
        def save(evidence):
            events.append(("save", "full_witness" if "full_witness" in evidence else "gray_lkg" if "gray_lkg" in evidence else "gray_witness" if "gray_witness" in evidence else "start"))
        with patch.object(p.cell, "parent"), patch.object(p.cell, "isolated_worker"), patch.object(p.cell, "activation", return_value={"generation": 1}), patch.object(p.cell, "check_activation"), patch.object(p, "publication", side_effect=publication), patch.object(p, "lkg", return_value=None), patch.object(p, "observe", side_effect=observe), patch.object(p, "verify", side_effect=verify), patch.object(p, "active_publication"):
            result = p.promote(c, e, api, save)
        self.assertEqual([("observe", "gray"), ("observe", "full")], [x for x in events if x[0] == "observe"])
        self.assertFalse(result["dns_published"])
        self.assertFalse(result["public_transport_changed"])
        self.assertEqual("full-one", result["full_lkg"]["verified_by_release_id"])

    def test_existing_foreign_full_is_not_replaced(self):
        c, e, writes = config(), enrollment(), []
        gray = {"id": "gray-one", "fencing_token": 1, "release_channel": "gray"}
        with patch.object(p.cell, "parent"), patch.object(p.cell, "isolated_worker"), patch.object(p.cell, "activation", return_value={}), patch.object(p.cell, "check_activation"), patch.object(p, "publication", side_effect=[gray, {"id": "foreign-full", "idempotency_key": "someone-else"}]):
            with self.assertRaises(ValueError):
                p.promote(c, e, lambda *args: writes.append(args), lambda _: None)
        self.assertEqual([], writes)


if __name__ == "__main__":
    unittest.main()
