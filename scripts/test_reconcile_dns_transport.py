import copy
import datetime
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

    def test_update_binds_identity_and_preserves_allocated_and_foreign_fields(self):
        config = self.config()
        old = transport.service(config, config["listeners"][0])
        old["metadata"].update(uid="fixed-uid", resourceVersion="17")
        old["metadata"]["annotations"]["unrelated"] = "keep"
        old["spec"]["clusterIP"] = "10.0.0.1"
        config["generation"] = 2
        config["listeners"][0]["port"] = 53
        desired = transport.service(config, config["listeners"][0])
        patch = transport.update_patch(old, desired)
        self.assertEqual(patch[:2], [{"op": "test", "path": "/metadata/uid", "value": "fixed-uid"}, {"op": "test", "path": "/metadata/resourceVersion", "value": "17"}])
        self.assertEqual(patch[2], {"op": "test", "path": "/spec", "value": old["spec"]})
        changes = [p for p in patch if p["op"] != "test"]
        self.assertEqual({p["path"] for p in changes}, {"/spec/ports", "/metadata/annotations/transport.fugue.dev~1generation", "/metadata/annotations/transport.fugue.dev~1digest"})
        self.assertEqual(old["metadata"]["annotations"]["unrelated"], "keep")
        self.assertEqual(old["spec"]["clusterIP"], "10.0.0.1")
        old["metadata"]["managedFields"] = [{"manager": "other", "fieldsV1": {"f:spec": {"f:ports": {}}}}]
        with self.assertRaises(ValueError): transport.update_patch(old, desired)

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

    def snapshot(self):
        now = datetime.datetime.now(datetime.timezone.utc)
        stamp = lambda delta: (now + datetime.timedelta(seconds=delta)).isoformat()
        digest = "sha256:" + "a" * 64
        assignment = {"artifact_id":"dns", "artifact_kind":"dns_answer_bundle", "scope_key":"global",
                      "release_channel":"full", "release_set_id":"parent", "artifact_release_id":"release",
                      "expected_consumer_set_id":"set", "expected_generation":"generation", "fencing_token":7,
                      "generation_sequence":12, "content_hash":digest}
        binding = {"release_set_id":"parent", "release_set_digest":digest, "route_artifact_id":"route",
                   "release_id":"release", "release_channel":"full", "fencing_token":7, "scope_key":"global"}
        return {"schema":"fugue.dns.runtime-facts/v1", "node_id":"node", "edge_group_id":"group",
                "assignment":assignment, "parent_digest":digest, "route_artifact_id":"route", "plan_digest":digest,
                "observed_at":stamp(-2), "evaluated_at":stamp(-1), "checkpoint_valid_until":stamp(300), "ready":True,
                "facts":[{"probe_id":digest, "ready":True, "proof":{"checked_at":stamp(-3), "valid_until":stamp(60), "traffic_release":binding}}]}

    def test_handoff_requires_same_fresh_artifact_and_proof_membership(self):
        old = self.snapshot()
        transport.validate_handoff_snapshots(old, copy.deepcopy(old), "node")
        mutations = [
            lambda x: x.update(ready=False), lambda x: x.update(node_id="other"),
            lambda x: x.update(plan_digest="sha256:"+"b"*64),
            lambda x: x["assignment"].update(expected_consumer_set_id="next"),
            lambda x: x.update(checkpoint_valid_until=x["observed_at"]),
            lambda x: x.update(evaluated_at=x["checkpoint_valid_until"]),
            lambda x: x["facts"][0].update(ready=False),
            lambda x: x["facts"][0]["proof"].update(valid_until=x["observed_at"]),
            lambda x: x["facts"][0]["proof"]["traffic_release"].update(fencing_token=6),
            lambda x: x["facts"].append(copy.deepcopy(x["facts"][0])),
            lambda x: x.update(facts=[]),
        ]
        for change in mutations:
            candidate = copy.deepcopy(old)
            change(candidate)
            with self.assertRaises(ValueError):
                transport.validate_handoff_snapshots(old, candidate, "node")

    def backend(self):
        return {"metadata":{"name":"candidate", "namespace":"system", "uid":"candidate-uid", "resourceVersion":"20",
                            "annotations":{"fugue.pro/consumer-identity":json.dumps({"version":"v1","component":"dns-server","scope_key":"global","artifact_kinds":["dns_answer_bundle"]})}},
                "spec":{"nodeName":"node","serviceAccountName":"candidate-sa","containers":[{"name":"dns","ports":[{"name":"udp","protocol":"UDP","containerPort":53},{"name":"tcp","protocol":"TCP","containerPort":53},{"name":"http","protocol":"TCP","containerPort":8080}],"readinessProbe":{"httpGet":{"path":"/healthz","port":"http"}}}]},
                "status":{"phase":"Running","podIPs":[{"ip":"10.0.0.2"}],"conditions":[{"type":"Ready","status":"True"}]}}

    def test_backend_is_unique_authorized_and_reread_after_proxy(self):
        for scenario in ["valid","duplicate","unready","wrong-auth","changed","port"]:
            pod=self.backend()
            if scenario=="unready":pod['status']['conditions'][0]['status']='False'
            if scenario=="wrong-auth":pod['metadata']['annotations'].clear()
            if scenario=="port":pod['spec']['containers'][0]['readinessProbe']['httpGet']['host']='outside'
            def fake(*args):
                if args[:2]==('get','pods'):return {'items':[pod,pod] if scenario=='duplicate' else [pod]}
                if args[:2]==('get','--raw'):
                    self.assertEqual(args[2],'/api/v1/namespaces/system/pods/candidate:8080/proxy/runtime-facts')
                    return self.snapshot()
                if args[:2]==('get','pod'):
                    current=copy.deepcopy(pod)
                    if scenario=='changed':current['metadata']['resourceVersion']='21'
                    return current
                self.fail(args)
            with patch.object(transport,'read_json',fake):
                if scenario=='valid':self.assertEqual(transport.handoff_backend('system',{'app':'dns'},'node',53)['pod'],pod)
                else:
                    with self.assertRaises(ValueError):transport.handoff_backend('system',{'app':'dns'},'node',53)

    def test_current_endpoint_cannot_be_foreign_unready_or_duplicate(self):
        pod=self.backend()
        service={'metadata':{'name':'listener','uid':'service-uid'}}
        slices=[{'metadata':{'namespace':'system','ownerReferences':[{'kind':'Service','name':'listener','uid':'service-uid','controller':True}]},
                 'ports':[{'protocol':'UDP','port':53},{'protocol':'TCP','port':53}],
                 'endpoints':[{'nodeName':'node','targetRef':{'kind':'Pod','name':'candidate','namespace':'system','uid':'candidate-uid'},'addresses':['10.0.0.2'],'conditions':{'ready':True}}]}]
        transport.validate_handoff_endpoints(service,pod,slices,53)
        for change in [lambda x:x[0]['metadata']['ownerReferences'][0].update(uid='other'),
                       lambda x:x[0]['endpoints'][0]['targetRef'].update(uid='other'),
                       lambda x:x[0]['endpoints'][0]['conditions'].update(ready=False),
                       lambda x:x[0]['endpoints'][0]['conditions'].update(terminating=True),
                       lambda x:x[0]['endpoints'].append(copy.deepcopy(x[0]['endpoints'][0])),
                       lambda x:x[0].update(ports=[]),lambda x:x.clear()]:
            changed=copy.deepcopy(slices);change(changed)
            with self.assertRaises(ValueError):transport.validate_handoff_endpoints(service,pod,changed,53)

    def test_changed_handoff_observation_prevents_selector_write(self):
        config = self.config()
        old = transport.service(config, config['listeners'][0])
        old['metadata'].update(uid='service-uid', resourceVersion='10')
        config['generation'] = 2
        config['listeners'][0]['selector'] = {'app':'candidate'}
        writes = []
        def fake(*args, body=None):
            if args[:2] == ('get','services'):return json.dumps({'items':[old]})
            if args[:2] == ('get','service'):return json.dumps(old)
            if args[:2] == ('get','pods'):return json.dumps({'items':[self.backend()]})
            if args[:2] == ('get','node'):return json.dumps({'status':{'addresses':[{'address':'8.8.8.8'}]}})
            if args[0] == 'patch':
                if '--dry-run=server' not in args:writes.append(json.loads(body))
                return '{}'
            self.fail(args)
        for drift in [False,True]:
            writes.clear()
            observations=[{'candidate_uid':'first'},{'candidate_uid':'changed' if drift else 'first'}]
            with patch.object(transport,'kubectl',fake),patch.object(transport,'handoff_witness',side_effect=observations):
                if drift:
                    with self.assertRaises(ValueError):transport.reconcile(config,apply=True)
                    self.assertEqual(writes,[])
                else:
                    transport.reconcile(config,apply=True)
                    self.assertEqual(len(writes),1)
                    self.assertIn({'op':'test','path':'/metadata/resourceVersion','value':'10'},writes[0])
                    self.assertIn({'op':'add','path':'/spec/selector','value':{'app':'candidate'}},writes[0])


if __name__ == "__main__":
    unittest.main()
