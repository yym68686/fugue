from copy import deepcopy
from datetime import datetime, timezone
import unittest
from unittest.mock import patch

from scripts import release_edge_origin_relay_policy as release
from scripts.test_stage_edge_origin_relay_trust import fixture, uid


class RelayPolicyReleaseTest(unittest.TestCase):
    def setUp(self):
        self.trust = fixture()
        self.d = release.validate_declaration({
            "schema": "fugue.edge.origin-relay-policy-release/v1", "worker_config_sha": "1" * 40,
            "monitor": "worker-monitor", "front": "edge-front", "front_uid": uid(30),
            "host": "app.example.test", "path": "/v1/stream", "source": "node-1", "peer": "node-2",
            "public_ips": {"node-1": "11.0.0.1", "node-2": "11.0.0.2"},
            "mode": "shadow", "generation": 2, "ca_sha256": self.trust["ca_sha256"],
        }, self.trust)
        self.w = {
            t["edge"]: {
                "endpoint": {"edge": t["edge"], "slot": "b", "tls_name": t["tls_name"], "address": f"10.0.0.{i}:8443"},
                "fence": {"generation": 1, "payload_sha256": "8" * 64},
                "target": t, "pod": (f"pod-{i}", uid(50 + i), f"containerd://{i}"),
            } for i, t in enumerate(self.trust["targets"], 1)
        }
        self.proofs = {edge: {"version": "current", "proof": "sha256:" + str(i) * 64, "app_traffic_proof": "sha256:" + "5" * 64}
                       for i, edge in enumerate(self.w, 1)}
        self.v = {"payload_sha256": "a" * 64,
                  "policy": {"mode": "shadow", "generation": 2, "max_concurrent": 1, "relay_percent": 0,
                             "endpoints": [w["endpoint"] for w in self.w.values()],
                             "bindings": [{"source": self.d["source"], "peer": self.d["peer"], "host": self.d["host"],
                                           "path": self.d["path"], "source_digest": self.proofs["node-1"]["proof"],
                                           "peer_digest": self.proofs["node-2"]["proof"], "max_body_bytes": 4096}]}}

    def test_policy_respects_full_live_proof_and_exact_scope(self):
        release.admit_policy(self.d, self.v, self.w, self.proofs)
        for name, val in (("max_concurrent", 2), ("relay_percent", 1), ("generation", 3)):
            with self.subTest(name=name), self.assertRaises(ValueError):
                release.admit_policy(self.d, {**self.v, "policy": {**self.v["policy"], name: val}}, self.w, self.proofs)
        for name, val in (("host", "other.example.test"), ("path", "/v1/other"), ("peer", "node-1"),
                          ("source_digest", self.proofs["node-2"]["proof"]), ("max_body_bytes", 2**20)):
            with self.subTest(name=name), self.assertRaises(ValueError):
                bad = deepcopy(self.v)
                bad["policy"]["bindings"][0][name] = val
                release.admit_policy(self.d, bad, self.w, self.proofs)
        other_session = deepcopy(self.w)
        other_session["node-1"]["endpoint"]["address"] = "10.0.0.9:8443"
        with self.assertRaises(ValueError):
            release.admit_policy(self.d, self.v, other_session, self.proofs)

    def test_off_independent_of_peer_and_public_proof(self):
        self.d.update(mode="off", generation=3)
        off = {"payload_sha256": "b" * 64, "policy": {"mode": "off", "generation": 3, "relay_percent": 0, "max_concurrent": 1,
                                                     "endpoints": [], "bindings": []}}
        with (
            patch.object(release, "release_image"), patch.object(release, "verify_envelope", return_value=off),
            patch.object(release, "slot_witness", side_effect=[self.w["node-1"], ValueError("unreachable peer")]),
            patch.object(release, "route_proof") as route, patch.object(release, "deliver") as deliver,
        ):
            result = release.execute(self.d, self.trust, b"signed", "verifier")
        route.assert_not_called()
        deliver.assert_called_once()
        self.assertEqual("node-1", deliver.call_args.args[2])
        self.assertEqual(result["verified_fences"], ["node-1"])
        self.assertEqual(result["unavailable"], ["node-2"])
        self.assertFalse(result["lease_published"])

    def test_preview_does_not_write_and_peer_failure_never_enables_ingress(self):
        def witness(_d, _t, edge, _image):
            return self.w[edge]

        def proof(_d, _t, edge, _now):
            return self.proofs[edge]

        with (
            patch.object(release, "release_image"), patch.object(release, "verify_envelope", return_value=self.v),
            patch.object(release, "slot_witness", side_effect=witness), patch.object(release, "route_proof", side_effect=proof),
            patch.object(release, "deliver", side_effect=ValueError("egress changed")) as deliver,
        ):
            preview = release.execute(self.d, self.trust, b"", "verifier")
            deliver.assert_not_called()
            result = release.execute(self.d, self.trust, b"signed", "verifier")
        deliver.assert_called_once()
        self.assertEqual("node-2", deliver.call_args.args[2])
        self.assertFalse(preview["lease_published"])
        self.assertFalse(result["lease_published"])
        self.assertEqual(result["unavailable"], ["node-2"])

    def test_active_requires_exact_shadow_loaded_on_both(self):
        for w in self.w.values():
            w["fence"] = {"generation": 2, "payload_sha256": self.v["payload_sha256"]}
        active = {**self.d, "mode": "active", "generation": 3}
        with patch.object(release.trust, "worker_exec"), patch.object(release, "verify_envelope", return_value=self.v) as verify:
            release.require_shadow(active, self.trust, "verifier", self.w, self.proofs)
        self.assertEqual(verify.call_count, 2)
        self.w["node-1"]["fence"]["payload_sha256"] = "b" * 64
        with patch.object(release.trust, "worker_exec"), patch.object(release, "verify_envelope", return_value=self.v):
            with self.assertRaises(ValueError):
                release.require_shadow(active, self.trust, "verifier", self.w, self.proofs)

    def test_route_proof_rejects_missing_wrong_and_retained_evidence(self):
        now = datetime.fromisoformat("2026-01-01T00:00:00+00:00").astimezone(timezone.utc)
        nonce = "c" * 32
        lines = [
            "HTTP/2 204", f"x-fugue-route-probe-nonce: {nonce}", "x-fugue-route-edge-id: node-1",
            "x-fugue-route-edge-group: edge-group-test", "x-fugue-route-bundle-version: on",
            "x-fugue-route-valid-until: 2026-01-01T00:02:00.123456789Z",
            "x-fugue-route-proof: sha256:" + "1" * 64, "x-fugue-app-traffic-proof: sha256:" + "5" * 64,
        ]
        for changed in (lines, lines + ["x-fugue-route-probe-mode: retained-lkg"], lines[:-1], lines + [lines[-1]]):
            with (
                patch.object(release.secrets, "token_hex", return_value=nonce),
                patch.object(release.trust, "run", return_value="\r\n".join(changed).encode()),
            ):
                if changed == lines:
                    self.assertEqual(release.route_proof(self.d, self.trust, "node-1", now)["proof"], self.proofs["node-1"]["proof"])
                else:
                    with self.assertRaises(ValueError):
                        release.route_proof(self.d, self.trust, "node-1", now)


if __name__ == "__main__":
    unittest.main()
