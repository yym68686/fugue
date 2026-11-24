import datetime
import unittest

from scripts import node_worker_handoff as worker


PROFILE = {"node": "edge-pilot", "group": "edge-group-fixture"}
ADDRESS = "192.0.2.7"
PROBE = {"host": "probe.example.test", "path": "/healthz"}


class Fixture:
    def __init__(self, failure=None):
        self.clock = datetime.datetime(2026, 11, 25, tzinfo=datetime.timezone.utc) + datetime.timedelta(seconds=1)
        self.before = {"schema": "fugue.edge-front-activation/v1", "edge_group_id": PROFILE["group"], "generation": 912,
                       "active_slot": "b", "bundle_generation": "bundle.p45.r1", "worker_source_commit": "b"*40,
                       "worker_image_digest": "sha256:" + "b"*64, "authority": "edge-control", "operation": "init",
                       "reason": "observed predecessor", "updated_at": "2026-11-24T23:58:00Z"}
        self.journal = {"expected": self.before, "candidate": {"slot": "a", "bundle_generation": "bundle.p45.r1",
                        "worker_source_commit": "a"*40, "worker_image_digest": "sha256:" + "a"*64},
                        "digest": "sha256:" + "c"*64, "recipient": {"pod_uid": "front-uid"},
                        "created_at": "2026-11-25T00:00:00Z"}
        self.activation = dict(self.before, active_slot="a", previous_slot="b", generation=913,
                               worker_source_commit="a"*40, worker_image_digest="sha256:" + "a"*64,
                               operation="promote", reason="advance node Worker using journal " + self.journal["digest"],
                               updated_at="2026-11-25T00:00:04Z")
        self.arm_input, self.witness_input = None, None
        self.reads = 0
        self.sockets = []
        self.failure = failure

    def now(self):
        return self.clock

    def pause(self, seconds):
        self.clock += datetime.timedelta(seconds=seconds)

    def arm(self, obj):
        self.arm_input = obj

    def witness(self, obj):
        assert all(not s.closed for s in self.sockets[:2]), "witnessed sockets must remain open"
        self.witness_input = obj

    def result(self):
        assert all(not s.closed for s in self.sockets[:2]), "sockets closed before Guardian receipt"
        self.reads += 1
        if self.reads <= 2:
            return None
        return {"schema": "edge-node-worker-result/v1", "journal_digest": self.journal["digest"],
                "outcome": "verified", "activation": dict(self.activation),
                "witness": dict(self.witness_input, observed_at=self.witness_input["observed_at"].replace("Z", "+00:00"))}

    def selected_activation(self):
        if not self.arm_input or self.clock.second < 4 and self.clock.minute == 0 or self.failure == "never_moves":
            return self.before
        if self.failure == "external":
            return dict(self.activation, reason="some other writer")
        if self.failure == "reverted":
            return dict(self.before, generation=914, previous_slot="a", updated_at=self.activation["updated_at"],
                        operation="rollback", rollback_of_generation=913,
                        reason="restore node Worker using journal " + self.journal["digest"])
        return self.activation

    def open(self, address, host):
        assert (address, host) == (ADDRESS, PROBE["host"])
        socket = Socket(self, len(self.sockets), worker.wire_time(self.now()))
        self.sockets.append(socket)
        return socket

    def connection_fact(self, profile, pod, sock):
        assert profile == PROFILE and pod == {"pod": "existing-front"}
        assert not sock.closed
        old = sock.number == 0
        connection = "original" if old else f"new-{sock.number}"
        if self.failure == "replaced_old" and old and self.selected_activation() != self.before:
            connection = "different-original"
        target = ADDRESS + (":28443" if old or self.failure == "wrong_new_listener" else ":38443")
        return {"pod_uid": "front-uid", "connection_id": connection, "started_at": sock.started_at,
                "slot": "b" if old else "a", "target": target, "source_evidence": "socket"}


class Socket:
    def __init__(self, fixture, number, started_at):
        self.closed, self.fixture, self.number, self.started_at = False, fixture, number, started_at
        self.requests = 0

    def proof(self, path):
        assert not self.closed and path == PROBE["path"]
        self.requests += 1
        version = "expected"
        if self.fixture.failure == "different_route" and self.number != 0:
            version = "unexpected"
        return {"edge": PROFILE["node"], "group": PROFILE["group"], "version": version}

    def close(self):
        self.closed = True


def simulate(fixture):
    return worker.handoff(fixture, PROFILE, {"pod": "existing-front"}, ADDRESS, PROBE, fixture.journal,
                          now=fixture.now, pause=fixture.pause, open_tls=fixture.open)


class NodeWorkerHandoffTests(unittest.TestCase):
    def test_one_original_socket_and_one_fresh_socket_survive_to_guardian_readback(self):
        f = Fixture()
        result = simulate(f)
        self.assertEqual(result["outcome"], "verified")
        self.assertEqual(len(f.sockets), 2)
        self.assertTrue(all(s.closed and s.requests > 1 for s in f.sockets))
        self.assertEqual(f.arm_input["held"], f.witness_input["held"])
        self.assertEqual(f.arm_input["held"]["id"], "original")
        self.assertEqual(f.witness_input["fresh"]["target"], ADDRESS + ":38443")
        self.assertEqual(f.witness_input["fresh"]["id"], "new-1")

    def test_forged_continuity_or_unrelated_activation_never_produces_witness(self):
        for failure in ("never_moves", "replaced_old", "external", "wrong_new_listener", "reverted", "different_route"):
            with self.subTest(failure=failure):
                f = Fixture(failure)
                with self.assertRaises(ValueError):
                    simulate(f)
                self.assertIsNone(f.witness_input)
                self.assertTrue(all(s.closed for s in f.sockets))
                if failure != "wrong_new_listener":
                    self.assertLessEqual(len(f.sockets), 2)

    def test_result_must_bind_the_two_original_refs(self):
        f = Fixture()
        result = simulate(f)
        result["witness"] = dict(result["witness"], held=dict(result["witness"]["held"], id="reopened"))
        with self.assertRaisesRegex(ValueError, "exact connection identities"):
            worker.require_verified(result, f.journal, f.activation, f.witness_input)
