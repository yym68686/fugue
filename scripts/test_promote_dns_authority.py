import copy
import datetime
import json
import os
import subprocess
import ssl
from unittest.mock import MagicMock
import unittest
from unittest.mock import patch

from scripts import promote_dns_authority as p
from scripts.stage_dns_authority_transition import digest as artifact_digest


def config():
    return {"schema": "fugue.private-dns-promotion/v1", "generation": 1, "origin": "https://api.example.test", "namespace": "system", "authority_cell_id": "cell-dns-one", "verifier_revision": "a" * 40,
            "release_set": {"id": "parent", "digest": "sha256:" + "b" * 64}, "dns_artifact": {"id": "dns", "digest": "sha256:" + "c" * 64}, "shadow_release": {"id": "shadow", "fencing_token": 4},
            "readers": [{"node_id": "node-one", "pod": "dns-one", "uid": "11111111-1111-1111-1111-111111111111", "container": "dns", "image": "registry.example/dns@sha256:" + "d" * 64, "service_account": "dns-one", "port": 8080}],
            "observation": {"samples": 3, "interval_seconds": 20, "timeout_seconds": 180}}


def release(c, channel):
    return {"id": channel, "fencing_token": 4, "status": "active", "release_channel": channel, "idempotency_key": "git-private-dns/" + p.digest(c) + "/" + channel, "canary_rule_ref": "cohort=complete" if channel == "gray" else ""}


def lkg(c, channel):
    return {"artifact_id": c["release_set"]["id"], "content_hash": c["release_set"]["digest"], "artifact_kind": "release_set", "scope_key": p.scope(c), "verified_by_release_id": channel, "verification_evidence_hash": "sha256:" + "e" * 64, "expires_at": (p.now() + datetime.timedelta(minutes=10)).isoformat()}


class PrivateDNSPromotionTests(unittest.TestCase):
    def successor_config(self, channel="gray"):
        c = config()
        pin = {"artifact_id": "previous-parent", "content_hash": "sha256:" + "f" * 64, "release_id": "previous-full", "fencing_token": 2}
        lanes = {lane: dict(pin, release_id="previous-" + lane) for lane in ["gray", "full"]}
        lanes["shadow"] = {"artifact_id": c["release_set"]["id"], "content_hash": c["release_set"]["digest"], "release_id": c["shadow_release"]["id"], "fencing_token": c["shadow_release"]["fencing_token"]}
        if channel == "full":
            lanes["gray"] = dict(lanes["shadow"], release_id="observed-gray", fencing_token=3)
        c.update(schema="fugue.private-dns-promotion/v3", release_channel=channel, publication_precondition={"lanes": lanes, "lkg_id": "positive", "lkg_publication": pin, "verification_evidence_hash": "sha256:" + "e" * 64})
        return c

    def test_successor_promotes_one_channel_only_with_atomic_guard_and_keeps_initial_lkg_disabled(self):
        from scripts import dns_publication_precondition as guard
        for channel in ["gray", "full"]:
            with self.subTest(channel=channel):
                c = self.successor_config(channel)
                p.validate(c)
                candidate = release(c, channel)
                before = c["publication_precondition"]
                calls, receipts = [], []
                selected = {"shadow": release(c, "shadow"), channel: None}
                if channel == "full": selected["gray"] = release(c, "gray")
                def api(method, path, body=None):
                    calls.append((method, path, body))
                    if path.endswith("/release"):
                        self.assertEqual(before, body["dns_publication_precondition"])
                        self.assertEqual(channel, body["release_channel"])
                        self.assertEqual(guard.key({"id": c["release_set"]["id"], "content_hash": c["release_set"]["digest"]}, channel, "cohort=complete" if channel == "gray" else "", before), body["idempotency_key"])
                        selected[channel] = candidate
                    if path.endswith("/lkg"):
                        return {"lkg": {"verified_by_release_id": "previous-full"}}
                    if path.endswith("/verify-lkg"):
                        self.assertFalse(body["allow_initial_lkg"])
                    return {}
                witness = {"completed_at": p.now().isoformat()}
                with patch.object(p, "inputs"), patch.object(p, "publication", side_effect=lambda _c, _api, lane: selected[lane]), patch.object(guard, "check") as check, patch.object(p, "observe_shadow", return_value=witness), patch.object(p, "observe", return_value=witness), patch.object(p, "convergence"), patch.object(p, "retained_lkg", return_value={"verified_by_release_id": candidate["id"]}):
                    p.promote(c, api, receipts.append)
                    self.assertEqual(3, check.call_count)
                self.assertEqual(1, sum(path.endswith("/release") for _, path, _ in calls))
                self.assertEqual(1 if channel == "full" else 0, sum(path.endswith("/verify-lkg") for _, path, _ in calls))

    def test_successor_cannot_promote_full_from_previous_artifacts_gray(self):
        c = self.successor_config("gray")
        c["release_channel"] = "full"
        with self.assertRaisesRegex(ValueError, "own exact gray"):
            p.validate(c)
        c = self.successor_config("gray")
        with patch.object(p, "inputs"), patch.object(p, "publication", return_value=None), self.assertRaisesRegex(ValueError, "shadow"):
            p.promote(c, lambda *args: self.fail("missing shadow mutated API"), lambda _: None)

    def test_failed_successor_observation_never_verifies_lkg(self):
        from scripts import dns_publication_precondition as guard
        c = self.successor_config()
        calls = []
        with patch.object(p, "inputs"), patch.object(p, "publication", return_value=release(c, "gray")), patch.object(guard, "check"), patch.object(p, "observe", side_effect=ValueError("fresh proof missing")), self.assertRaisesRegex(ValueError, "fresh proof"):
            p.promote(c, lambda *args: calls.append(args), lambda _: None)
        self.assertTrue(all(not args[1].endswith("/verify-lkg") for args in calls))

    def test_pins_and_bounded_watch_are_mandatory(self):
        p.validate(config())
        for mutate in [lambda c: c.update(scope="global"), lambda c: c.update(generation=True), lambda c: c.update(origin="http://api.example.test"), lambda c: c["shadow_release"].update(fencing_token=0), lambda c: c["readers"][0].update(image="registry.example/dns:latest"), lambda c: c["observation"].update(samples=1), lambda c: c["observation"].update(interval_seconds=0), lambda c: c.update(readers=[]), lambda c: c["readers"].append(c["readers"][0])]:
            c = config()
            mutate(c)
            with self.assertRaises(ValueError):
                p.validate(c)

    def test_resume_replaces_only_physical_executors_with_original_operation_pins(self):
        previous = config()
        c = copy.deepcopy(previous)
        c.update(schema="fugue.private-dns-promotion/v2", generation=2, verifier_revision="f" * 40, api_connect_address="8.8.8.8",
                 resume={"declaration": previous, "gray_release": {"id": "gray", "fencing_token": 4}})
        c["readers"][0].update(pod="replacement", uid="22222222-2222-2222-2222-222222222222", image="registry.example/dns@sha256:" + "f" * 64)
        p.validate(c)
        state = {"artifact": {"id": "parent", "content_hash": c["release_set"]["digest"]}, "release": release(previous, "gray")}
        with patch.object(p, "selected", return_value=state):
            self.assertEqual("gray", p.publication(c, None, "gray")["id"])
        for mutate in [lambda v: v.update(api_connect_address="127.0.0.1"), lambda v: v.update(generation=1), lambda v: v["release_set"].update(id="foreign"), lambda v: v["dns_artifact"].update(id="foreign"), lambda v: v["readers"][0].update(node_id="foreign"), lambda v: v["readers"][0].update(service_account="foreign"), lambda v: v["observation"].update(samples=4), lambda v: v["resume"]["declaration"].update(schema="fugue.private-dns-promotion/v2")]:
            changed = copy.deepcopy(c)
            mutate(changed)
            with self.assertRaises(ValueError):
                p.validate(changed)
        for update in [{"id": "foreign"}, {"fencing_token": 5}, {"idempotency_key": "foreign"}]:
            changed = copy.deepcopy(state)
            changed["release"].update(update)
            with patch.object(p, "selected", return_value=changed), self.assertRaises(ValueError):
                p.publication(c, None, "gray")
        with patch.object(p, "selected", return_value={}), self.assertRaisesRegex(ValueError, "no longer selected"):
            p.publication(c, None, "gray")
        with patch.object(p, "inputs"), patch.object(p, "publication", side_effect=lambda c, api, ch: state["release"] if ch in ["gray", "shadow"] else None), patch.object(p, "observe", side_effect=ValueError("replacement not ready")):
            writes = []
            with self.assertRaisesRegex(ValueError, "not ready"):
                p.promote(c, lambda *args: writes.append(args), lambda value: None)
            self.assertEqual(1, len(writes))
            self.assertEqual("/v1/admin/platform-config/release-set/prepare-consumers", writes[0][1])

    def test_explicit_recovery_transport_preserves_https_identity(self):
        from scripts.stage_dns_authority_transition import ExplicitAddressHTTPSHandler
        handler = ExplicitAddressHTTPSHandler("https://api.example.test", "8.8.8.8")
        self.assertTrue(handler._context.check_hostname)
        self.assertEqual(ssl.CERT_REQUIRED, handler._context.verify_mode)
        peer = MagicMock()
        with patch("socket.create_connection", return_value=peer) as dial, patch.object(handler._context, "wrap_socket", return_value=peer) as tls:
            connection = handler.connection("api.example.test", context=handler._context, timeout=5)
            connection.connect()
            self.assertEqual(("8.8.8.8", 443), dial.call_args.args[0])
            self.assertEqual("api.example.test", tls.call_args.kwargs["server_hostname"])
            self.assertEqual("api.example.test", connection.host)
            with self.assertRaises(ValueError):
                connection._create_connection(("foreign.example.test", 443), 5)
        with patch("socket.create_connection", return_value=peer), patch.object(handler._context, "wrap_socket", side_effect=ssl.SSLCertVerificationError("identity rejected")):
            with self.assertRaises(ssl.SSLCertVerificationError):
                handler.connection("api.example.test", context=handler._context).connect()
        for host in ["foreign.example.test", "api.example.test:8443"]:
            with self.assertRaises(ValueError):
                handler.connection(host, context=handler._context)
        for address in ["127.0.0.1", "10.0.0.1", "169.254.169.254", "localhost", "8.8.8.8:443"]:
            with self.assertRaises(ValueError):
                ExplicitAddressHTTPSHandler("https://api.example.test", address)

    def test_composition_cannot_enroll_workers_or_other_dns_readers(self):
        c = config()
        parent = {"content_hash": c["release_set"]["digest"], "content": {"publication_role": "cell-dns", "artifact_kinds": ["dns_answer_bundle"], "artifact_ids": ["dns"], "consumer_topology": {"schema_version": "fugue.traffic-consumer-topology/v1", "publication_role": "cell-dns", "authority_cell_id": c["authority_cell_id"], "edge_node_ids": [], "dns_node_ids": ["node-one"]}}}
        dns = {"id": "dns", "content_hash": c["dns_artifact"]["digest"]}
        member = {"node_id": "node-one", "required": True, "component": "dns-server", "authority_id": c["authority_cell_id"], "scope_key": p.scope(c), "artifact_kind": "dns_answer_bundle", "expected_generation": "generation", "consumer_id": "dns-server:cell-dns-one:node-one"}
        expected = {"revision": 1, "release_set_id": "parent", "artifact_kind": "dns_answer_bundle", "scope_key": p.scope(c), "requires_consumers": True, "required_cardinality": 1, "consumers": [member]}
        with patch.object(p, "artifact", side_effect=[parent, dns]), patch.object(p, "expected_consumers", return_value=expected):
            p.inputs(c, None)
        for change in [{"publication_role": "cell-routes"}, {"artifact_ids": ["foreign"]}, {"consumer_topology": {"dns_node_ids": ["foreign"]}}]:
            altered = copy.deepcopy(parent)
            altered["content"].update(change)
            with patch.object(p, "artifact", side_effect=[altered, dns]), patch.object(p, "expected_consumers", return_value=expected), self.assertRaises(ValueError):
                p.inputs(c, None)

    def test_expected_members_are_the_exact_latest_dns_only_set(self):
        c = config()
        c["readers"].append({"node_id": "node-two", "pod": "dns-two", "uid": "22222222-2222-2222-2222-222222222222", "container": "dns", "image": "registry.example/dns@sha256:" + "d" * 64, "service_account": "dns-two", "port": 8080})
        c["readers"].sort(key=lambda r: r["node_id"])
        def member(node):
            return {"node_id": node, "required": True, "component": "dns-server", "authority_id": c["authority_cell_id"], "scope_key": p.scope(c), "artifact_kind": "dns_answer_bundle", "expected_generation": "generation", "consumer_id": "dns-server:" + c["authority_cell_id"] + ":" + node}
        current = {"release_set_id": "parent", "artifact_kind": "dns_answer_bundle", "scope_key": p.scope(c), "requires_consumers": True, "required_cardinality": 2, "revision": 2, "expected_generation": "generation", "artifact_release_id": "gray", "consumers": [member("node-one"), member("node-two")]}
        older = dict(current, revision=1, consumers=[])
        def api_with_artifact(method, path, body=None):
            if method == "GET" and path.startswith("/v1/admin/artifacts/"):
                content = {"fixture": True}
                return {"artifact": {"id": "dns", "artifact_kind": "dns_answer_bundle", "scope_key": p.scope(c), "generation": "generation", "status": "validated", "content_hash": artifact_digest(content), "content": content}}
            if method == "GET" and path.startswith("/v1/admin/expected-consumer-sets?"):
                return {"expected_consumer_sets": [older, current]}
            raise AssertionError((method, path))
        self.assertEqual(current, p.expected_consumers(c, api_with_artifact, {"id": "gray"}))
        for mutation in [lambda s: s.update(required_cardinality=1), lambda s: s.update(consumers=[member("node-one")]), lambda s: s["consumers"][0].update(component="edge-worker"), lambda s: s["consumers"][0].update(required=False), lambda s: s["consumers"][0].update(node_id="foreign"), lambda s: s.update(consumers=[member("node-one"), member("node-one")])]:
            changed = copy.deepcopy(current)
            mutation(changed)
            def changed_api(method, path, body=None):
                if method == "GET" and path.startswith("/v1/admin/artifacts/"):
                    return {"artifact": {"id": "dns", "artifact_kind": "dns_answer_bundle", "scope_key": p.scope(c), "generation": "generation", "status": "validated", "content_hash": artifact_digest({"fixture": True}), "content": {"fixture": True}}}
                return {"expected_consumer_sets": [changed]}
            with self.assertRaises(ValueError):
                p.expected_consumers(c, changed_api, {"id": "gray"})

    def test_publication_cannot_adopt_a_foreign_operation(self):
        c = config()
        state = {"artifact": {"id": "parent", "content_hash": c["release_set"]["digest"]}, "release": release(c, "gray")}
        with patch.object(p, "selected", return_value=state):
            p.publication(c, None, "gray")
        for update in [{"idempotency_key": "other"}, {"canary_rule_ref": "cohort=partial"}, {"status": "failed"}, {"fencing_token": 0}]:
            changed = copy.deepcopy(state)
            changed["release"].update(update)
            with patch.object(p, "selected", return_value=changed), self.assertRaises(ValueError):
                p.publication(c, None, "gray")

    def test_lkg_requires_exact_fresh_authority_and_verified_evidence(self):
        c, value = config(), lkg(config(), "gray")
        p.retained_lkg(c, lambda *args: {"lkg": value}, ["gray"])
        for update in [{"artifact_id": "foreign"}, {"scope_key": "global"}, {"verified_by_release_id": "shadow"}, {"verification_evidence_hash": ""}, {"expires_at": (p.now() - datetime.timedelta(seconds=1)).isoformat()}]:
            with self.assertRaises(ValueError):
                p.retained_lkg(c, lambda *args: {"lkg": dict(value, **update)}, ["gray"])

    def test_watch_cannot_count_repeated_snapshots(self):
        c, r = config(), release(config(), "gray")
        for advance in [True, False]:
            values = [{"node_id": "node-one", "observed_at": (p.now() - datetime.timedelta(seconds=20-i if advance else 20)).isoformat()} for i in range(3)]
            if not advance:
                values = [values[0]] * 3
            with patch.object(p, "publication", return_value=r), patch.object(p, "convergence"), patch.object(p, "observe_reader", side_effect=values), patch.object(p.time, "sleep"), patch.object(p.time, "monotonic", side_effect=[0, 0, 1, 2, 181]):
                if advance:
                    self.assertEqual(3, len(p.observe(c, None, r)["samples"]))
                else:
                    with self.assertRaisesRegex(ValueError, "did not renew"):
                        p.observe(c, None, r)

    def test_watch_waits_for_real_renewal_within_original_deadline(self):
        c, r, at = config(), release(config(), "full"), p.now()
        values = [{"node_id": "node-one", "observed_at": (at + datetime.timedelta(seconds=i)).isoformat()} for i in [0, 0, 0, 1, 2]]
        with patch.object(p, "publication", return_value=r), patch.object(p, "convergence"), patch.object(p, "observe_reader", side_effect=values), patch.object(p.time, "sleep"), patch.object(p.time, "monotonic", side_effect=[0, 0, 1, 2, 3, 4]):
            witness = p.observe(c, None, r)
        self.assertEqual(3, len(witness["samples"]))
        self.assertGreater(p.timestamp(witness["samples"][-1]["readers"][0]["observed_at"]), p.timestamp(witness["samples"][0]["readers"][0]["observed_at"]))

    def test_failed_shadow_cannot_publish_gray(self):
        c, writes = config(), []
        with patch.object(p, "inputs"), patch.object(p, "publication", side_effect=lambda c, api, ch: release(c, ch) if ch == "shadow" else None), patch.object(p, "observe_shadow", side_effect=ValueError("no positive DNS answer")), self.assertRaisesRegex(ValueError, "positive DNS"):
            p.promote(c, lambda *args: writes.append(args), lambda value: None)
        self.assertEqual([], writes)

    def test_shadow_evidence_is_a_recent_dry_run_and_not_a_serving_lease(self):
        c, t = config(), p.now()
        reader = c["readers"][0]
        pod = {"metadata": {"uid": reader["uid"], "resourceVersion": "12", "annotations": {"fugue.pro/authority-transition-role": "isolated-candidate", "fugue.pro/consumer-identity": json.dumps({"version": "v1", "component": "dns-server", "scope_key": p.scope(c), "authority_id": c["authority_cell_id"], "artifact_kinds": ["dns_answer_bundle"]})}}, "spec": {"nodeName": reader["node_id"], "serviceAccountName": reader["service_account"], "containers": [{"name": "dns", "image": reader["image"], "readinessProbe": {"httpGet": {"port": 8080}}}]}, "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}
        candidate = {"state": "shadow_verified", "artifact_id": "dns", "digest": c["dns_artifact"]["digest"], "release_set_id": "parent", "readiness": {"records": 2, "ready_records": 2, "plan_digest": "sha256:" + "f" * 64, "checked_at": (t - datetime.timedelta(seconds=20)).isoformat(), "fresh_until": (t - datetime.timedelta(seconds=1)).isoformat()}, "query": {"records": 3, "eligible_records": 3, "answers": 3, "view_digest": "sha256:" + "a" * 64, "serving": False, "evaluated_at": (t - datetime.timedelta(seconds=19)).isoformat()}}
        with patch.object(p, "read_json", return_value=pod), patch.object(p, "shadow_health", return_value={"platform_candidate": candidate}):
            self.assertEqual(2, p.observe_shadow_reader(c, reader)["ready_records"])
        mutations = [lambda v: v.update(artifact_id="foreign"), lambda v: v["readiness"].update(ready_records=1), lambda v: v["query"].update(eligible_records=2), lambda v: v["query"].update(serving=True), lambda v: v["query"].update(evaluated_at=(t - datetime.timedelta(seconds=100)).isoformat()), lambda v: v["query"].update(evaluated_at=(t + datetime.timedelta(seconds=10)).isoformat()), lambda v: v["readiness"].update(fresh_until=(t - datetime.timedelta(seconds=20)).isoformat())]
        for mutate in mutations:
            value = copy.deepcopy(candidate)
            mutate(value)
            with patch.object(p, "read_json", return_value=pod), patch.object(p, "shadow_health", return_value={"platform_candidate": value}), self.assertRaisesRegex(ValueError, "complete answer proof"):
                p.observe_shadow_reader(c, reader)

    def test_shadow_receipts_bind_fresh_physical_executors_without_claiming_serving(self):
        c, r, t = config(), release(config(), "shadow"), p.now()
        expected = {"id": "set-one", "expected_generation": "g-one"}
        observed = {"node_id": "node-one", "identity_verified": True, "credential_id": "kubernetes:system:dns-one:11111111-1111-1111-1111-111111111111", "release_set_id": "parent", "expected_consumer_set_id": "set-one", "fencing_token": 4, "generation_sequence": 4, "desired_generation": "g-one", "apply_status": "staged", "probe_status": "shadow_validated", "issued_at": (t - datetime.timedelta(seconds=3)).isoformat(), "evidence_hash": "sha256:" + "f" * 64, "compatibility_capabilities": ["cell_dns_v1", "dns_authority_transition_v1", "dns_availability_v1", "dns_route_sources_v1", "dns_retained_routes_v1", "traffic_release_v1"]}
        assessment = {"observed": observed, "observed_at": (t - datetime.timedelta(seconds=2)).isoformat(), "fresh_until": (t + datetime.timedelta(seconds=87)).isoformat()}
        value = {"artifact_kind": "dns_answer_bundle", "expected_consumer_set_id": "set-one", "required_expected": 1, "assessments": [assessment], "pass": False}
        with patch.object(p, "expected_consumers", return_value=expected), patch.object(p, "observe_shadow_reader", return_value={"node_id": "node-one"}):
            self.assertEqual([{"node_id": "node-one"}], p.shadow_convergence(c, lambda *a: {"convergence": [value]}, r))
            for update in [{"credential_id": "foreign"}, {"apply_status": "applied"}, {"expected_consumer_set_id": "old-set"}, {"fencing_token": 3}, {"generation_sequence": 3}, {"issued_at": (t - datetime.timedelta(seconds=100)).isoformat()}, {"issued_at": (t + datetime.timedelta(seconds=100)).isoformat()}, {"compatibility_capabilities": []}, {"evidence_hash": ""}]:
                altered = copy.deepcopy(value)
                altered["assessments"][0]["observed"].update(update)
                with self.assertRaisesRegex(ValueError, "capability receipt"):
                    p.shadow_convergence(c, lambda *a: {"convergence": [altered]}, r)

    def test_shadow_observation_requires_new_probes_not_repeated_status(self):
        c, r, t = config(), release(config(), "shadow"), p.now()
        for advances in [True, False]:
            samples = [[{"node_id": "node-one", "readiness_checked_at": (t + datetime.timedelta(seconds=i if advances else 0)).isoformat(), "query_evaluated_at": (t + datetime.timedelta(seconds=i if advances else 0)).isoformat()}] for i in range(3)]
            with patch.object(p, "publication", return_value=r), patch.object(p, "shadow_convergence", side_effect=samples), patch.object(p.time, "sleep"):
                if advances:
                    self.assertEqual(3, len(p.observe_shadow(c, None, r)["samples"]))
                else:
                    with self.assertRaisesRegex(ValueError, "did not advance"):
                        p.observe_shadow(c, None, r)

    def test_shadow_health_reads_candidate_json_even_before_serving(self):
        c = config()
        for status in [b"200 OK", b"503 Service Unavailable"]:
            result = subprocess.CompletedProcess([], 0, b'HTTP/1.0 ' + status + b'\r\nContent-Type: application/json\r\n\r\n{"platform_candidate":{"state":"shadow_verified"}}', b"")
            with patch.object(p.subprocess, "run", return_value=result) as run:
                self.assertEqual("shadow_verified", p.shadow_health(c, c["readers"][0])["platform_candidate"]["state"])
                self.assertEqual(b"GET /healthz HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n", run.call_args.kwargs["input"])
        for raw in [b'HTTP/1.0 403 Forbidden\r\n\r\n{}', b'{}']:
            with patch.object(p.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, raw, b"")), self.assertRaisesRegex(ValueError, "HTTP response"):
                p.shadow_health(c, c["readers"][0])

    def test_reader_binds_loopback_checkpoint_verifier_and_reread_identity(self):
        c, r = config(), release(config(), "gray")
        reader = c["readers"][0]
        env = [{"name": k, "value": v} for k, v in {"FUGUE_EDGE_GROUP_ID": c["authority_cell_id"], "FUGUE_PLATFORM_ARTIFACT_SCOPE": p.scope(c), "FUGUE_DNS_CACHE_PATH": "/state/cache", "FUGUE_DNS_ZONE": "example.test"}.items()]
        env.append({"name": "FUGUE_BUNDLE_SIGNING_KEY", "valueFrom": {"secretKeyRef": {"name": "signing", "key": "key"}}})
        env.append({"name": "FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY", "valueFrom": {"secretKeyRef": {"name": "signing", "key": "previous", "optional": True}}})
        pod = {"metadata": {"name": reader["pod"], "uid": reader["uid"], "resourceVersion": "12", "annotations": {"fugue.pro/authority-transition-role": "isolated-candidate", "fugue.pro/consumer-identity": json.dumps({"version": "v1", "component": "dns-server", "scope_key": p.scope(c), "authority_id": c["authority_cell_id"], "artifact_kinds": ["dns_answer_bundle"]})}},
               "spec": {"nodeName": reader["node_id"], "serviceAccountName": reader["service_account"], "containers": [{"name": "dns", "image": reader["image"], "env": env, "readinessProbe": {"httpGet": {"port": reader["port"]}}}]}, "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}
        snapshot = {"node_id": reader["node_id"], "parent_digest": c["release_set"]["digest"], "assignment": {"artifact_id": "dns", "content_hash": c["dns_artifact"]["digest"], "release_set_id": "parent", "artifact_release_id": r["id"], "fencing_token": r["fencing_token"], "release_channel": "gray", "scope_key": p.scope(c)}, "observed_at": p.now().isoformat(), "evaluated_at": p.now().isoformat()}
        snapshot.update(ready=True, checkpoint_valid_until=(p.now()+datetime.timedelta(hours=1)).isoformat())
        checkpoint = {"candidate":{"assignment":copy.deepcopy(snapshot["assignment"]),"artifact":{"content":{"policy":{"max_stale_seconds":3600}}}},"applied_at":(p.timestamp(snapshot["checkpoint_valid_until"])-datetime.timedelta(hours=1)).isoformat()}

        for scenario in ["valid", "replaced", "wrong image", "wrong authority", "host network", "wrong port", "stale fence", "crypto rejection", "required key empty"]:
            local, facts, calls = copy.deepcopy(pod), copy.deepcopy(snapshot), []
            if scenario == "wrong image":
                local["spec"]["containers"][0]["image"] = "other"
            if scenario == "wrong authority":
                local["metadata"]["annotations"]["fugue.pro/consumer-identity"] = "{}"
            if scenario == "host network":
                local["spec"]["hostNetwork"] = True
            if scenario == "wrong port":
                local["spec"]["containers"][0]["readinessProbe"]["httpGet"]["port"] += 1
            if scenario == "stale fence":
                facts["assignment"]["fencing_token"] -= 1
            def read(*args):
                calls.append(args)
                if args[:2] == ("get", "pod"):
                    result = copy.deepcopy(local)
                    if scenario == "replaced" and len(calls) > 1:
                        result["metadata"]["uid"] = "replacement"
                    return result
                if args[:2] == ("get", "secret"):
                    return {"data": {"key": "" if scenario == "required key empty" else "Zml4dHVyZS1rZXk=", "previous": ""}}
                self.assertEqual("exec", args[0])
                self.assertEqual(reader["pod"], args[3])
                if args[-2:] == ("cat", "/state/cache.platform-serving.json"):
                    return copy.deepcopy(checkpoint)
                self.assertEqual(("wget", "-qO-", "http://127.0.0.1:8080/runtime-facts"), args[-3:])
                return facts
            result = subprocess.CompletedProcess(["/verified/reader"], 1 if scenario == "crypto rejection" else 0, '{"verified":true}', "")
            with patch.object(p, "read_json", side_effect=read), patch.object(p.subprocess, "run", return_value=result) as verify, patch.dict(os.environ, {"FUGUE_DNS_OBSERVATION_VERIFIER": "/verified/reader"}):
                if scenario == "valid":
                    observed = p.observe_reader(c, reader, r)
                    self.assertEqual(reader["uid"], observed["uid"])
                    self.assertNotIn("fixture-key", json.dumps(observed))
                    body = json.loads(verify.call_args.kwargs["input"])
                    self.assertEqual("fixture-key", body["signing_key"])
                    self.assertEqual(p.scope(c), body["scope"])
                else:
                    with self.assertRaises(ValueError, msg=scenario):
                        p.observe_reader(c, reader, r)

    def test_coherent_capture_retries_only_reads_and_never_renews_authority(self):
        at = p.now()
        checkpoint = {"candidate": {"assignment": {"fence": 7}, "artifact": {"content": {"policy": {"max_stale_seconds": 3600}}}}, "applied_at": at.isoformat(), "route_sources": {"release": "current"}}
        facts = {"assignment": {"fence": 7}, "ready": True, "checkpoint_valid_until": (at + datetime.timedelta(hours=1)).isoformat(), "route_sources": {"release": "current"}, "observed_at": at.isoformat()}
        mixed = dict(facts, route_sources={"release": "new"})
        with patch.object(p.time, "monotonic", return_value=1):
            capture = MagicMock(side_effect=[RuntimeError("503 state_replaced"), (checkpoint, mixed), (checkpoint, facts)])
            verify = MagicMock()
            result = p.coherent_reader_observation(capture, verify, deadline=10)
            self.assertIs(result, facts)
            self.assertEqual(3, capture.call_count)
            verify.assert_called_once_with(checkpoint, facts)
            for scenario in ["assignment", "negative", "expired", "crypto", "exhausted", "deadline"]:
                with self.subTest(scenario=scenario):
                    value = copy.deepcopy(facts)
                    if scenario == "assignment": value["assignment"]["fence"] += 1
                    if scenario == "negative": value["ready"] = False
                    if scenario == "expired": value["checkpoint_valid_until"] = (at - datetime.timedelta(seconds=1)).isoformat()
                    capture = MagicMock(return_value=(checkpoint, value))
                    if scenario == "exhausted": capture.side_effect = RuntimeError("503")
                    verify = MagicMock(side_effect=ValueError("signature rejected") if scenario == "crypto" else None)
                    with self.assertRaises(ValueError):
                        p.coherent_reader_observation(capture, verify, deadline=0 if scenario == "deadline" else 10)
                    self.assertEqual(3 if scenario == "exhausted" else 0 if scenario == "deadline" else 1, capture.call_count)
                    self.assertEqual(1 if scenario == "crypto" else 0, verify.call_count)

    def test_capture_cannot_finish_outside_original_watch_deadline(self):
        checkpoint = {"candidate": {"assignment": {}}, "applied_at": p.now().isoformat()}
        with patch.object(p.time, "monotonic", side_effect=[9, 11]):
            verify = MagicMock()
            with self.assertRaisesRegex(ValueError, "deadline"):
                p.coherent_reader_observation(lambda: (checkpoint, {"assignment": {}, "ready": True}), verify, deadline=10)
            verify.assert_not_called()

    def test_full_waits_for_gray_lkg_and_a_separate_observation_window(self):
        c, events, published, verified = config(), [], {"shadow"}, []
        def current(c, api, channel):
            return release(c, channel) if channel in published else None
        def api(method, path, body=None):
            events.append((method, path))
            if path.endswith("/release"):
                if body["release_channel"] == "full":
                    self.assertEqual(["gray"], verified)
                published.add(body["release_channel"])
            if path.endswith("/verify-lkg"):
                channel = path.split("/")[-2]
                self.assertIn(("observe", channel), events)
                verified.append(channel)
            if path.endswith("/lkg"):
                return {"lkg": lkg(c, verified[-1]) if verified else None}
            return {}
        def observe(c, api, r):
            events.append(("observe", r["release_channel"]))
            return {"completed_at": p.now().isoformat(), "release_id": r["id"], "fencing_token": r["fencing_token"]}
        def shadow(c, api, r):
            self.assertEqual({"shadow"}, published)
            self.assertEqual("shadow", r["release_channel"])
            events.append(("shadow", r["id"]))
            return {"release_id": r["id"]}
        with patch.object(p, "inputs"), patch.object(p, "publication", side_effect=current), patch.object(p, "convergence"), patch.object(p, "observe_shadow", side_effect=shadow), patch.object(p, "observe", side_effect=observe):
            result = p.promote(c, api, lambda value: None)
        self.assertEqual(["gray", "full"], verified)
        self.assertFalse(result["public_transport_changed"])
        self.assertFalse(result["route_configuration_changed"])
        self.assertEqual([("observe", "gray"), ("observe", "full")], [e for e in events if e[0] == "observe"])

    def test_failed_gray_or_changed_fence_never_publishes_full_or_attests_lkg(self):
        c = config()
        for scenario in ["failed answers", "changed fence"]:
            writes = []
            def api(*args):
                writes.append(args)
                if args[1].endswith("/lkg"):
                    return {"lkg": None}
                return {}
            reads = [0]
            def current(c, api, channel):
                if channel == "full":
                    return None
                if channel == "gray":
                    reads[0] += 1
                    if scenario == "changed fence" and reads[0] > 1:
                        return dict(release(c, channel), id="foreign", fencing_token=5)
                return release(c, channel)
            def observe(c, api, r):
                if scenario == "failed answers":
                    raise ValueError("unavailable answers")
                return {"completed_at": p.now().isoformat()}
            with patch.object(p, "inputs"), patch.object(p, "publication", side_effect=current), patch.object(p, "convergence"), patch.object(p, "observe", side_effect=observe), self.assertRaises(ValueError):
                p.promote(c, api, lambda value: None)
            self.assertFalse(any(args[1].endswith(("/release", "/verify-lkg")) for args in writes))


if __name__ == "__main__":
    unittest.main()
