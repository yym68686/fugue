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
        result = {"schema": "edge-node-worker-result/v1", "journal_digest": self.journal["digest"],
                "outcome": "verified", "activation": dict(self.activation),
                "witness": dict(self.witness_input, observed_at=self.witness_input["observed_at"].replace("Z", "+00:00"))}
        if self.journal.get("probe_service_uid"):
            result["drain_start"] = worker.wire_time(self.now())
        return result

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

    def open(self, address, host, port=443):
        assert (address, host) == (ADDRESS, PROBE["host"])
        assert port in (443, 15443)
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
        target = ADDRESS + (":28443" if old or self.failure == "wrong_new_listener" else
                            (":18443" if self.journal.get("fixed_transport") else ":38443"))
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
                          probe_port=15443 if fixture.journal.get("probe_service_uid") else None,
                          now=fixture.now, pause=fixture.pause, open_tls=fixture.open)


class DrainFixture(Fixture):
    def __init__(self, failure=None):
        super().__init__(failure)
        self.journal.update(fixed_transport=True, probe_service_uid="probe-uid")
        self.journal["recipient"]["node"] = PROFILE["node"]
        self.evidence = {}

    def result(self):
        result = super().result()
        if result is not None:
            self.start = worker.front.timestamp(result["drain_start"])
            self.deadline = self.start + datetime.timedelta(seconds=600)
        return result

    def record(self, **data):
        self.evidence.update(data)

    def drain_grant(self):
        return {"schema": "edge-worker-bounded-drain/v1", "front_uid": "front-uid", "node": PROFILE["node"],
                "previous": self.before, "serving": self.activation, "start": worker.wire_time(self.start),
                "deadline": worker.wire_time(self.deadline), "healthy_at": worker.wire_time(self.now())}

    def drain_metrics(self):
        expired = self.now() >= self.deadline
        return {"capability": 1, "last_generation": 913, "last_deadline_seconds": int(self.deadline.timestamp()),
                "last_forced": int(expired), "last_remaining": int(not expired), "last_completed": int(expired)}

    def forced_connection(self, conn_id, generation, deadline):
        assert (conn_id, generation, deadline) == ("original", 913, self.deadline)
        return None if self.failure == "unattributed" else self.deadline

    def open(self, address, host, port=443):
        sock = super().open(address, host, port)
        proof = sock.proof

        def observed(path):
            if sock.number == 0 and hasattr(self, "deadline"):
                deadline = self.deadline - datetime.timedelta(seconds=30 if self.failure == "early_close" else 0)
                if self.now() >= deadline:
                    assert not sock.closed, "must observe peer close before closing our side"
                    raise ConnectionResetError("closed by remote Front")
            return proof(path)

        sock.proof = observed
        return sock

    def verify(self):
        return worker.handoff(self, PROFILE, {"pod": "existing-front"}, ADDRESS, PROBE, self.journal, probe_port=15443,
                              now=self.now, pause=self.pause, open_tls=self.open, drain_record=self.record)


class NodeWorkerHandoffTests(unittest.TestCase):
    def test_deadline_proof_holds_original_until_its_exact_front_receipt(self):
        f = DrainFixture()
        self.assertEqual(f.verify()["outcome"], "verified")
        self.assertGreater(f.evidence["drain_started"]["remaining_seconds"], 580)
        complete = f.evidence["drain_completed"]
        self.assertLess(worker.front.timestamp(complete["last_alive_at"]), f.deadline)
        self.assertGreaterEqual(worker.front.timestamp(complete["peer_closed_at"]), f.deadline)
        self.assertEqual(worker.front.timestamp(complete["front_forced_at"]), f.deadline)
        self.assertEqual(complete["metrics"]["last_forced"], 1)

    def test_early_or_unattributed_disconnect_cannot_pass(self):
        for case, error in (("early_close", "without a verified"), ("unattributed", "no witnessed")):
            f = DrainFixture(case)
            with self.subTest(case=case), self.assertRaisesRegex(ValueError, error):
                f.verify()
            self.assertNotIn("drain_completed", f.evidence)

    def test_fixed_transport_has_distinct_fresh_public_and_probe_connections(self):
        fixture = Fixture()
        fixture.journal.update(fixed_transport=True, probe_service_uid="probe-uid")
        result = simulate(fixture)
        witness = result["witness"]
        self.assertEqual(witness["fresh"]["target"], ADDRESS + ":18443")
        self.assertEqual(witness["probe_fresh"]["target"], witness["fresh"]["target"])
        self.assertEqual(len({witness[k]["id"] for k in ("held", "fresh", "probe_fresh")}), 3)
        self.assertTrue(all(s.closed for s in fixture.sockets))

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
