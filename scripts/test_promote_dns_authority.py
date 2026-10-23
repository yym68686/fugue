import copy
import datetime
import json
import os
import subprocess
import unittest
from unittest.mock import patch

from scripts import promote_dns_authority as p


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
    def test_pins_and_bounded_watch_are_mandatory(self):
        p.validate(config())
        for mutate in [lambda c: c.update(scope="global"), lambda c: c.update(generation=True), lambda c: c.update(origin="http://api.example.test"), lambda c: c["shadow_release"].update(fencing_token=0), lambda c: c["readers"][0].update(image="registry.example/dns:latest"), lambda c: c["observation"].update(samples=1), lambda c: c["observation"].update(interval_seconds=0), lambda c: c.update(readers=[]), lambda c: c["readers"].append(c["readers"][0])]:
            c = config()
            mutate(c)
            with self.assertRaises(ValueError):
                p.validate(c)

    def test_composition_cannot_enroll_workers_or_other_dns_readers(self):
        c = config()
        parent = {"content_hash": c["release_set"]["digest"], "content": {"publication_role": "cell-dns", "artifact_kinds": ["dns_answer_bundle"], "artifact_ids": ["dns"], "consumer_topology": {"dns_node_ids": ["node-one"], "edge_node_ids": []}}}
        dns = {"id": "dns", "content_hash": c["dns_artifact"]["digest"]}
        with patch.object(p, "artifact", side_effect=[parent, dns]):
            p.inputs(c, None)
        for change in [{"publication_role": "cell-routes"}, {"artifact_ids": ["foreign"]}, {"consumer_topology": {"dns_node_ids": ["node-one"], "edge_node_ids": ["edge-one"]}}, {"consumer_topology": {"dns_node_ids": ["other"], "edge_node_ids": []}}]:
            altered = copy.deepcopy(parent)
            altered["content"].update(change)
            with patch.object(p, "artifact", side_effect=[altered, dns]), self.assertRaises(ValueError):
                p.inputs(c, None)

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
            with patch.object(p, "publication", return_value=r), patch.object(p, "convergence"), patch.object(p, "observe_reader", side_effect=values), patch.object(p.time, "sleep"):
                if advance:
                    self.assertEqual(3, len(p.observe(c, None, r)["samples"]))
                else:
                    with self.assertRaisesRegex(ValueError, "did not renew"):
                        p.observe(c, None, r)

    def test_failed_shadow_cannot_publish_gray(self):
        c, writes = config(), []
        with patch.object(p, "inputs"), patch.object(p, "publication", side_effect=lambda c, api, ch: release(c, ch) if ch == "shadow" else None), patch.object(p, "convergence", side_effect=ValueError("no positive DNS answer")), self.assertRaisesRegex(ValueError, "positive DNS"):
            p.promote(c, lambda *args: writes.append(args), lambda value: None)
        self.assertEqual([], writes)

    def test_reader_binds_loopback_checkpoint_verifier_and_reread_identity(self):
        c, r = config(), release(config(), "gray")
        reader = c["readers"][0]
        env = [{"name": k, "value": v} for k, v in {"FUGUE_EDGE_GROUP_ID": c["authority_cell_id"], "FUGUE_PLATFORM_ARTIFACT_SCOPE": p.scope(c), "FUGUE_DNS_CACHE_PATH": "/state/cache", "FUGUE_DNS_ZONE": "example.test"}.items()]
        env.append({"name": "FUGUE_BUNDLE_SIGNING_KEY", "valueFrom": {"secretKeyRef": {"name": "signing", "key": "key"}}})
        pod = {"metadata": {"name": reader["pod"], "uid": reader["uid"], "resourceVersion": "12", "annotations": {"fugue.pro/authority-transition-role": "isolated-candidate", "fugue.pro/consumer-identity": json.dumps({"version": "v1", "component": "dns-server", "scope_key": p.scope(c), "authority_id": c["authority_cell_id"], "artifact_kinds": ["dns_answer_bundle"]})}},
               "spec": {"nodeName": reader["node_id"], "serviceAccountName": reader["service_account"], "containers": [{"name": "dns", "image": reader["image"], "env": env, "readinessProbe": {"httpGet": {"port": reader["port"]}}}]}, "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}
        snapshot = {"node_id": reader["node_id"], "parent_digest": c["release_set"]["digest"], "assignment": {"artifact_id": "dns", "content_hash": c["dns_artifact"]["digest"], "release_set_id": "parent", "artifact_release_id": r["id"], "fencing_token": r["fencing_token"], "release_channel": "gray", "scope_key": p.scope(c)}, "observed_at": p.now().isoformat(), "evaluated_at": p.now().isoformat()}
        for scenario in ["valid", "replaced", "wrong image", "wrong authority", "host network", "wrong port", "stale fence", "crypto rejection"]:
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
                    return {"data": {"key": "Zml4dHVyZS1rZXk="}}
                self.assertEqual("exec", args[0])
                self.assertEqual(reader["pod"], args[3])
                if args[-2:] == ("cat", "/state/cache.platform-serving.json"):
                    return {"signed": "fixture"}
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
        with patch.object(p, "inputs"), patch.object(p, "publication", side_effect=current), patch.object(p, "convergence"), patch.object(p, "observe", side_effect=observe):
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
