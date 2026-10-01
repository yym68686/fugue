import copy
import unittest

from scripts import reconcile_dns_probe_egress as p


def fixture():
    spec = {"type": "ClusterIP", "selector": {"role": "public-front", "node": "edge-a"}, "ports": [{"name": "http", "protocol": "TCP", "port": 80, "targetPort": 80}, {"name": "https", "protocol": "TCP", "port": 443, "targetPort": 443}], "externalIPs": ["8.8.8.8"], "externalTrafficPolicy": "Local", "publishNotReadyAddresses": False}
    service = {"metadata": {"name": "public-front-a", "namespace": "platform-system", "uid": "service-one", "resourceVersion": "12", "labels": {"app.kubernetes.io/managed-by": p.FRONT_MANAGER}, "annotations": {"transport.fugue.dev/phase": "serving", "transport.fugue.dev/generation": "2", "transport.fugue.dev/digest": p.digest(spec)}}, "spec": spec}
    ref = {"name": "public-front-a", "uid": "service-one", "generation": 2, "digest": p.digest(spec)}
    c = {"schema": "fugue.dns-probe-egress/v1", "generation": 1, "namespace": "platform-system", "policy_name": "dns-public-probes", "authority_cell_id": "cell-dns", "services": [ref]}
    return c, service


class ProbeEgressTests(unittest.TestCase):
    def test_only_pinned_public_https_backend_is_allowed(self):
        c, service = fixture()
        before = copy.deepcopy((c, service))
        target = p.desired(p.validate(c), [service])
        self.assertEqual(before, (c, service))
        self.assertEqual(["Egress"], target["spec"]["policyTypes"])
        self.assertEqual({"matchLabels": {"app.kubernetes.io/component": "dns-server", "fugue.io/authority-cell-id": "cell-dns"}}, target["spec"]["podSelector"])
        self.assertEqual([{"ports": [{"protocol": "TCP", "port": 443}], "to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "platform-system"}}, "podSelector": {"matchLabels": {"role": "public-front", "node": "edge-a"}}}]}], target["spec"]["egress"])
        self.assertNotIn("ingress", target["spec"])

    def test_private_ports_foreign_services_and_unpinned_changes_are_rejected(self):
        for change in [lambda s: s["metadata"].update(uid="replaced"), lambda s: s["metadata"]["labels"].clear(), lambda s: s["metadata"]["annotations"].update({"transport.fugue.dev/phase": "staged"}), lambda s: s["spec"]["ports"][1].update(targetPort=7831), lambda s: s["spec"].update(externalIPs=["10.0.0.1"]), lambda s: s["spec"].update(selector={}), lambda s: s["spec"]["selector"].update(role="private-control"), lambda s: s["spec"].update(publishNotReadyAddresses=True), lambda s: s["spec"].update(externalIPs=[])]:
            c, service = fixture()
            change(service)
            with self.assertRaises(ValueError): p.desired(c, [service])

    def test_successor_cas_preserves_other_writers_and_monotonic_generations(self):
        c, service = fixture()
        old = p.desired(c, [service])
        old["metadata"].update(uid="policy-one", resourceVersion="20")
        old["metadata"]["managedFields"] = [{"manager": p.MANAGER, "fieldsV1": {"f:spec": {"f:egress": {}}}}]
        self.assertEqual((None, None), p.mutation(old, p.desired(c, [service])))
        c["generation"] = 2
        target = p.desired(c, [service])
        action, patch = p.mutation(old, target)
        self.assertEqual("patch", action)
        tests = {x["path"]: x["value"] for x in patch if x["op"] == "test"}
        self.assertEqual("policy-one", tests["/metadata/uid"])
        self.assertEqual("20", tests["/metadata/resourceVersion"])
        self.assertEqual(old["spec"], tests["/spec"])
        for mode in ["foreign-owner", "drift", "replay", "same-generation-change", "other-writer"]:
            value, want = copy.deepcopy(old), copy.deepcopy(target)
            if mode == "foreign-owner": value["metadata"]["labels"]["app.kubernetes.io/managed-by"] = "operator"
            if mode == "drift": value["spec"]["egress"] = [{}]
            if mode == "replay": value["metadata"]["annotations"][p.GENERATION] = "3"
            if mode == "same-generation-change": want["metadata"]["annotations"][p.GENERATION] = "1"
            if mode == "other-writer": value["metadata"]["managedFields"][0]["manager"] = "operator"
            with self.subTest(mode=mode), self.assertRaises(ValueError): p.mutation(value, want)

    def test_source_race_aborts_before_any_write(self):
        c, service = fixture()
        reads, writes = [], []
        def kube(*args, body=None):
            if args[:2] == ("get", "networkpolicy"): return None
            if args[:2] == ("get", "service"):
                reads.append(args)
                value = copy.deepcopy(service)
                if len(reads) > 1: value["metadata"]["resourceVersion"] = "13"
                return value
            writes.append(args)
        with self.assertRaisesRegex(ValueError, "changed during"):
            p.reconcile(c, True, lambda _: None, kube)
        self.assertEqual([], writes)

    def test_configuration_create_and_replay_only_touch_one_network_policy(self):
        c, service = fixture()
        current, writes, saved = None, [], []
        def kube(*args, body=None):
            nonlocal current
            if args[:2] == ("get", "service"): return copy.deepcopy(service)
            if args[:2] == ("get", "networkpolicy"): return copy.deepcopy(current)
            self.assertTrue(saved)
            self.assertEqual("create", args[0])
            import json
            value = json.loads(body)
            self.assertEqual("NetworkPolicy", value["kind"])
            self.assertEqual(c["policy_name"], value["metadata"]["name"])
            writes.append(args)
            if "--dry-run=server" not in args:
                current = value
                current["metadata"].update(uid="new-policy", resourceVersion="30")
            return value
        result = p.reconcile(c, True, lambda v: saved.append(copy.deepcopy(v)), kube)
        self.assertTrue(result["applied"])
        self.assertFalse(result["public_transport_changed"])
        self.assertEqual(2, len(writes))
        p.reconcile(c, True, lambda _: None, kube)
        self.assertEqual(2, len(writes))

    def test_declaration_requires_explicit_bounded_pins(self):
        for change in [lambda c: c.update(generation=True), lambda c: c.update(namespace="*"), lambda c: c.update(services=[]), lambda c: c["services"][0].pop("uid"), lambda c: c["services"][0].update(generation=True), lambda c: c.update(allow_private=True)]:
            c, _ = fixture()
            change(c)
            with self.assertRaises(ValueError): p.validate(c)


if __name__ == "__main__":
    unittest.main()
