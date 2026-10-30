import copy
import unittest
from unittest.mock import patch

from scripts import reconcile_app_tcp_transport as tcp


class TCPTransportTests(unittest.TestCase):
    def fixture(self):
        return {"schema": "fugue.app-tcp-transport/v1", "generation": 1, "listeners": [{"name": "mail-tcp", "namespace": "tenant-demo", "appId": "app_demo", "tenantId": "tenant_demo", "managedName": "app-demo", "managedUID": "uid-demo", "deployment": "app-demo", "node": "worker-demo", "address": "8.8.4.4", "port": 25, "targetPort": 25, "enabled": True}]}

    def owner(self):
        return {"apiVersion": "fugue.pro/v1alpha1", "kind": "ManagedApp", "name": "app-demo", "uid": "uid-demo", "controller": False, "blockOwnerDeletion": True}

    def current(self, target):
        current = copy.deepcopy(target)
        current["metadata"].update(uid="uid-resource", resourceVersion="7")
        if current["kind"] == "Service":
            current["spec"].update(clusterIP="10.43.1.9", clusterIPs=["10.43.1.9"], ipFamilies=["IPv4"], ipFamilyPolicy="SingleStack", sessionAffinity="None", internalTrafficPolicy="Cluster")
        return current

    def test_admin_declaration_is_explicit_bounded_and_socket_unique(self):
        config = self.fixture()
        self.assertEqual(tcp.validate(config), config)
        for key, value in [("managedUID", ""), ("address", "127.0.0.1"), ("port", 443), ("port", True), ("targetPort", 0), ("enabled", "true"), ("namespace", "../other")]:
            invalid = copy.deepcopy(config)
            invalid["listeners"][0][key] = value
            with self.assertRaises(ValueError):
                tcp.validate(invalid)
        duplicate = copy.deepcopy(config)
        duplicate["listeners"].append(copy.deepcopy(duplicate["listeners"][0]))
        with self.assertRaises(ValueError):
            tcp.validate(duplicate)

    def test_transport_keeps_source_ip_and_allows_only_selected_app_port(self):
        config = self.fixture()
        service, policy = tcp.targets(config, config["listeners"][0], self.owner())
        self.assertEqual(service["spec"]["externalTrafficPolicy"], "Local")
        self.assertEqual(service["spec"]["ports"], [{"name": "tcp", "protocol": "TCP", "port": 25, "targetPort": 25}])
        self.assertEqual(policy["spec"]["podSelector"]["matchLabels"], service["spec"]["selector"])
        self.assertEqual(policy["spec"]["ingress"][0]["ports"], [{"protocol": "TCP", "port": 25}])
        for target in [service, policy]:
            self.assertNotIn(tcp.APP, target["metadata"]["labels"])
            self.assertNotIn("fugue.pro/owner-app-id", target["metadata"]["labels"])
            self.assertEqual(target["metadata"]["labels"][tcp.TRANSPORT_APP], "app_demo")
            self.assertEqual(target["metadata"]["ownerReferences"], [self.owner()])
            self.assertEqual(tcp.mutation(self.current(target), target), (None, None))

    def test_legacy_transport_labels_migrate_under_exact_owner_cas(self):
        config = self.fixture()
        for target in tcp.targets(config, config["listeners"][0], self.owner()):
            current = self.current(target)
            labels = current["metadata"]["labels"]
            labels[tcp.APP] = labels.pop(tcp.TRANSPORT_APP)
            labels["custom-label"] = "retained"
            command, body = tcp.mutation(current, target)
            self.assertEqual(command[0], "patch")
            self.assertIn({"op": "test", "path": "/metadata/labels", "value": labels}, body)
            migrated = next(p["value"] for p in body if p["path"] == "/metadata/labels" and p["op"] == "replace")
            self.assertNotIn(tcp.APP, migrated)
            self.assertEqual(migrated[tcp.TRANSPORT_APP], "app_demo")
            self.assertEqual(migrated["custom-label"], "retained")
            current["metadata"]["ownerReferences"][0]["uid"] = "foreign"
            with self.assertRaises(ValueError):
                tcp.mutation(current, target)

    def test_settle_observation_rejects_pruned_or_replaced_resources(self):
        config = self.fixture()
        targets = tcp.targets(config, config["listeners"][0], self.owner())
        before = [self.current(t) for t in targets]
        for after in [None, dict(before[0], metadata=dict(before[0]["metadata"], uid="replacement"))]:
            with patch.object(tcp, "observe", return_value=self.owner()), patch.object(tcp, "current_resource", side_effect=[*before, after]), patch.object(tcp.time, "sleep") as sleep:
                with self.assertRaisesRegex(ValueError, "remain converged"):
                    tcp.reconcile(config, apply=True, settle_seconds=30)
                sleep.assert_called_once_with(30)

    def test_foreign_uid_drift_and_generation_replay_never_adopted(self):
        config = self.fixture()
        target = tcp.targets(config, config["listeners"][0], self.owner())[0]
        for alter in [lambda c: c["metadata"].pop("uid"), lambda c: c["metadata"]["labels"].update({"app.kubernetes.io/managed-by": "foreign"}), lambda c: c["metadata"]["ownerReferences"][0].update(uid="another-owner"), lambda c: c["spec"].update(selector={tcp.APP: "other-app"}), lambda c: c["metadata"]["annotations"].update({tcp.GENERATION: "2"})]:
            current = self.current(target)
            alter(current)
            with self.assertRaises(ValueError):
                tcp.mutation(current, target)

    def test_disable_withdraws_external_address_and_ingress_without_deleting_spool(self):
        config = self.fixture()
        old = tcp.targets(config, config["listeners"][0], self.owner())
        config["generation"] = 2
        config["listeners"][0]["enabled"] = False
        new = tcp.targets(config, config["listeners"][0], self.owner())
        for before, target in zip(old, new):
            command, body = tcp.mutation(self.current(before), target)
            self.assertEqual(command[0], "patch")
            self.assertTrue(all(any(p["path"] == "/metadata/" + key and p["op"] == "test" for p in body) for key in ["uid", "resourceVersion", "ownerReferences"]))
            spec = next(p["value"] for p in body if p["path"] == "/spec" and p["op"] == "replace")
            if target["kind"] == "Service":
                self.assertEqual(spec["externalIPs"], [])
                self.assertNotIn("externalTrafficPolicy", spec)
                self.assertEqual(spec["clusterIP"], "10.43.1.9")
            else:
                self.assertEqual(spec["ingress"], [])
            self.assertEqual(tcp.mutation(self.current(target), target), (None, None))

    def test_all_preflight_completes_before_first_write_and_cas_rechecks(self):
        config = self.fixture()
        with patch.object(tcp, "observe", return_value=self.owner()), patch.object(tcp, "current_resource", return_value=None), patch.object(tcp, "write", return_value={}) as write:
            result = tcp.reconcile(config, apply=True)
            self.assertEqual([c.kwargs.get("dry_run", False) for c in write.call_args_list], [True, True, False, False])
            self.assertEqual(len(result["resources"]), 2)
        with patch.object(tcp, "observe", return_value=self.owner()), patch.object(tcp, "current_resource", side_effect=[None, None, {"metadata": {"uid": "racing-owner"}}]), patch.object(tcp, "write", return_value={}) as write:
            with self.assertRaises(ValueError):
                tcp.reconcile(config, apply=True)
            self.assertEqual(write.call_count, 2)

    def test_absent_resource_uses_json_list_and_incomplete_observation_is_rejected(self):
        config = self.fixture()
        target = tcp.targets(config, config["listeners"][0], self.owner())[0]
        with patch.object(tcp, "read", return_value={"metadata": {}, "items": []}) as read:
            self.assertIsNone(tcp.current_resource(target))
            self.assertIn("--field-selector=metadata.name=mail-tcp", read.call_args.args)
        current = self.current(target)
        with patch.object(tcp, "read", return_value={"items": [current]}):
            self.assertEqual(tcp.current_resource(target), current)
        for result in [{}, {"metadata": {"continue": "next"}, "items": []}, {"items": [current, current]}]:
            with patch.object(tcp, "read", return_value=result), self.assertRaises(ValueError):
                tcp.current_resource(target)


if __name__ == "__main__":
    unittest.main()
