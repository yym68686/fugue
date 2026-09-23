import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import reconcile_dns_transport as transport


class TransportTests(unittest.TestCase):
    def config(self):
        return {"apiVersion": "transport.fugue.dev/v1", "kind": "DNSLocalTransport", "generation": 1,
                "namespace": "system", "listeners": [{"name": "listener", "address": "8.8.8.8", "port": 15353,
                "targetPort": 53, "selector": {"app": "dns"}}]}

    def load(self, config):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "policy.json"
            file.write_text(json.dumps(config))
            return transport.load_config(file)

    def test_transport_preserves_source_and_filters_unready(self):
        config = self.load(self.config())
        svc = transport.service(config, config["listeners"][0])
        self.assertEqual(svc["spec"]["externalTrafficPolicy"], "Local")
        self.assertEqual(svc["spec"]["internalTrafficPolicy"], "Local")
        self.assertFalse(svc["spec"]["publishNotReadyAddresses"])
        self.assertEqual([p["protocol"] for p in svc["spec"]["ports"]], ["UDP", "TCP"])
        transport.validate_existing(svc, copy.deepcopy(svc))
        defaulted = copy.deepcopy(svc)
        del defaulted["spec"]["publishNotReadyAddresses"]
        transport.validate_existing(defaulted, svc)
        for mutate in [lambda c: c["metadata"]["labels"].clear(), lambda c: c["spec"].update(externalTrafficPolicy="Cluster"), lambda c: c["metadata"]["annotations"].update({transport.GENERATION: "2"}), lambda c: c["metadata"].update(deletionTimestamp="now")]:
            changed = copy.deepcopy(svc)
            mutate(changed)
            with self.assertRaises(ValueError):
                transport.validate_existing(changed, svc)

    def test_unknown_fields_and_ambiguous_listeners_fail(self):
        for mutate in [lambda c: c.update(script="run"), lambda c: c.update(generation=True), lambda c: c["listeners"][0].update(address="127.0.0.1"), lambda c: c["listeners"][0].update(selector={}), lambda c: c["listeners"].append(copy.deepcopy(c["listeners"][0]))]:
            config = self.config()
            mutate(config)
            with self.assertRaises(ValueError): self.load(config)

    def test_preflight_all_listeners_before_any_write(self):
        config = self.config()
        calls = []
        def fake(*args, body=None):
            calls.append(args)
            if args[:2] == ("get", "services"): return '{"items": []}'
            if args[:2] == ("get", "service"): return ""
            if args[:2] == ("get", "pods"):
                return json.dumps({"items": [{"metadata": {"name": "backend"}, "spec": {"nodeName": "node"}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]})
            if args[:2] == ("get", "node"): return json.dumps({"status": {"addresses": [{"address": "8.8.8.8"}]}})
            self.assertEqual(json.loads(body)["kind"], "Service")
            return "{}"
        with patch.object(transport, "kubectl", fake):
            transport.reconcile(config)
            self.assertFalse(any(c[0] == "create" and "--dry-run=server" not in c for c in calls))
            calls.clear()
            transport.reconcile(config, apply=True)
            self.assertEqual(sum(c[0] == "create" and "--dry-run=server" not in c for c in calls), 1)
            calls.clear()
            config["listeners"].append({**config["listeners"][0], "name": "other", "address": "9.9.9.9"})
            with self.assertRaises(ValueError): transport.reconcile(config, apply=True)
            self.assertFalse(any(c[0] == "create" and "--dry-run=server" not in c for c in calls))


if __name__ == "__main__":
    unittest.main()
