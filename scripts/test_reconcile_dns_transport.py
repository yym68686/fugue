import copy
import datetime
import json
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import reconcile_dns_transport as transport


class TransportTests(unittest.TestCase):
    def test_checkpoint_transport_failure_is_retryable_only_for_read_commands(self):
        failure = RuntimeError('error sending request: Post "https://node/exec": EOF')
        for args in [('get', 'pod', 'dns'), ('exec', 'dns', '--', 'cat', '/state/checkpoint')]:
            with patch.object(transport, 'kubectl', side_effect=failure):
                with self.assertRaises(transport.ObservationUnavailable):
                    transport.read_json(*args)
        for args in [('patch', 'daemonset', 'dns'), ('exec', 'dns', '--', 'sh', '-c', 'mutation')]:
            with patch.object(transport, 'kubectl', side_effect=failure):
                with self.assertRaises(RuntimeError) as error:
                    transport.read_json(*args)
                self.assertNotIsInstance(error.exception, transport.ObservationUnavailable)

    def test_production_listeners_select_declared_dns_workloads(self):
        root = Path(__file__).resolve().parents[1]
        config = transport.load_config(root / 'deploy/environments/production/dns-transport/transport.json')
        workloads = []
        for path in (root / 'deploy/releases').glob('*/resources.json'):
            for item in json.loads(path.read_text()).get('items', []):
                if item.get('kind') != 'DaemonSet':
                    continue
                pod = item['spec']['template']
                if any(c.get('name') == 'dns' for c in pod['spec']['containers']):
                    workloads.append((item['metadata'], pod))
        for listener in config['listeners']:
            matching = [(meta, pod) for meta, pod in workloads
                        if meta.get('namespace') == config['namespace'] and
                        all(pod['metadata']['labels'].get(key) == value
                            for key, value in listener['selector'].items())]
            self.assertEqual(len(matching), 1, listener['name'])
            ports = [port for container in matching[0][1]['spec']['containers']
                     for port in container.get('ports', [])]
            for protocol in ('UDP', 'TCP'):
                self.assertEqual(sum(p.get('containerPort') == listener['targetPort'] and
                                     p.get('protocol', 'TCP') == protocol for p in ports), 1)

    def test_staged_dns_recovery_executor_has_no_public_listener_or_shared_checkpoint(self):
        root = Path(__file__).resolve().parents[1]
        resources = root / 'deploy/releases'
        config = transport.load_config(root / 'deploy/environments/production/dns-transport/transport.json')

        def placement(slot):
            manifest = json.loads((resources / ('edge-client-dns-de-' + slot) / 'resources.json').read_text())
            self.assertEqual({item['kind'] for item in manifest['items']}, {'DaemonSet', 'ServiceAccount'})
            return next(item['spec']['template'] for item in manifest['items'] if item['kind'] == 'DaemonSet')

        def cache(template):
            return next(v['hostPath']['path'] for v in template['spec']['volumes'] if v['name'] == 'dns-cache')

        spare = placement('g')
        self.assertFalse(spare['spec'].get('hostNetwork'))
        self.assertTrue(spare['spec']['nodeSelector'].get('kubernetes.io/hostname'))
        self.assertFalse(any(port.get('hostPort') for c in spare['spec']['containers'] for port in c['ports']))
        self.assertEqual(len({cache(spare), cache(placement('e')), cache(placement('f'))}), 3)
        self.assertFalse(any(all(spare['metadata']['labels'].get(k) == v for k, v in listener['selector'].items())
                             for listener in config['listeners'] if listener['port'] == 53))
        # The independent rollback executor must not share any slot identity or
        # persistent state with the staged candidate or the failed incumbent.
        rollback = placement('h')
        cohort = [placement(slot) for slot in ('e', 'f', 'g', 'h')]
        self.assertEqual(len({cache(slot) for slot in cohort}), len(cohort))
        self.assertEqual(len({slot['spec']['serviceAccountName'] for slot in cohort}), len(cohort))
        self.assertEqual(rollback['spec']['nodeSelector'], spare['spec']['nodeSelector'])
        self.assertFalse(rollback['spec'].get('hostNetwork'))
        self.assertFalse(any(port.get('hostPort') for c in rollback['spec']['containers'] for port in c['ports']))
        self.assertFalse(any(all(rollback['metadata']['labels'].get(k) == v for k, v in listener['selector'].items())
                             for listener in config['listeners']))

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

    def test_empty_staged_declaration_is_a_read_only_transport_hold(self):
        config = self.load(dict(self.config(), apiVersion="transport.fugue.dev/v4", handoffs=[]))
        old = transport.service(config, config["listeners"][0])
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory) / "evidence.json"
            with patch.object(transport, "kubectl", side_effect=['{"items":[]}', json.dumps(old)]) as kubectl:
                self.assertEqual(transport.reconcile(config, apply=True, evidence_path=evidence), 0)
                self.assertEqual(json.loads(evidence.read_text())["operations"], [])
                self.assertTrue(all(call.args[0] == "get" for call in kubectl.call_args_list))
            drift = copy.deepcopy(config)
            drift["generation"] += 1
            drift["listeners"][0]["selector"] = {"app": "candidate"}
            with patch.object(transport, "kubectl", side_effect=['{"items":[]}', json.dumps(old)]) as kubectl:
                with self.assertRaisesRegex(ValueError, "only update declared sites"):
                    transport.reconcile(drift, apply=True, evidence_path=evidence)
                self.assertTrue(all(call.args[0] == "get" for call in kubectl.call_args_list))

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

    def test_rejected_preflight_retains_safe_evidence_without_applying_partial_plan(self):
        from scripts.recover_dns_transport import DNSObservationRejected

        config = self.config()
        config["listeners"].append(dict(config["listeners"][0], name="public", port=53))
        rejected = DNSObservationRejected("wire_materialization")
        rejected.backend_role = "predecessor"
        rejected.add_note("private subprocess details")
        pod = {"metadata": {"name": "backend"}, "spec": {"nodeName": "node"},
               "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        reads = ['{"items":[]}', "", json.dumps({"items": [pod]}),
                 '{"status":{"addresses":[{"address":"8.8.8.8"}]}}', "{}",
                 "", json.dumps({"items": [pod]}), '{"status":{"addresses":[{"address":"8.8.8.8"}]}}']
        with tempfile.TemporaryDirectory() as directory, \
             patch.object(transport, "kubectl", side_effect=reads) as kubectl, \
             patch.object(transport, "handoff_witness", side_effect=[None, rejected]):
            path = Path(directory) / "receipt.json"
            path.write_text("stale successful receipt")
            path.chmod(0o644)
            with self.assertRaises(DNSObservationRejected):
                transport.reconcile(config, apply=True, evidence_path=path)
            raw = path.read_text()
            self.assertNotIn("private subprocess details", raw)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            receipt = json.loads(raw)
            self.assertEqual(receipt["preflight"], {"status": "rejected", "gate": "handoff_witness",
                                                   "listener": "public", "backend_role": "predecessor",
                                                   "reason": "wire_materialization"})
            self.assertEqual([operation["listener"] for operation in receipt["operations"]], ["listener"])
            self.assertEqual(receipt["applied"], [])
            self.assertFalse(any(call.args[0] != "get" and "--dry-run=server" not in call.args
                                 for call in kubectl.call_args_list))

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
        old_with_transient_negative = copy.deepcopy(old)
        old_with_transient_negative["facts"][0]["ready"] = False
        transport.validate_handoff_snapshots(old_with_transient_negative, copy.deepcopy(old), "node")
        mutations = [
            lambda x: x.update(ready=False), lambda x: x.update(node_id="other"),
            lambda x: x.update(plan_digest="sha256:"+"b"*64),
            lambda x: x["assignment"].update(expected_consumer_set_id="next"),
            lambda x: x.update(checkpoint_valid_until=x["observed_at"]),
            lambda x: x.update(evaluated_at=x["checkpoint_valid_until"]),
            lambda x: x.update(ready=False),
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
            observations=[{'node_uid':'node','old_uid':'old','candidate_uid':'first'},
                          {'node_uid':'node','old_uid':'old','candidate_uid':'changed' if drift else 'first'}]
            with patch.object(transport,'kubectl',fake),patch.object(transport,'handoff_witness',side_effect=observations):
                if drift:
                    with self.assertRaises(ValueError):transport.reconcile(config,apply=True)
                    self.assertEqual(writes,[])
                else:
                    transport.reconcile(config,apply=True)
                    self.assertEqual(len(writes),1)
                    self.assertIn({'op':'test','path':'/metadata/resourceVersion','value':'10'},writes[0])
                    self.assertIn({'op':'add','path':'/spec/selector','value':{'app':'candidate'}},writes[0])

    def test_handoff_validates_each_backends_own_dns_port(self):
        config = self.config()
        current = transport.service(config, config["listeners"][0])
        listener = dict(config["listeners"][0], selector={"app": "candidate"}, targetPort=5353)
        node = {"metadata": {"name": "node", "uid": "node-uid"}}
        old = {"pod": {"metadata": {"uid": "old"}}}
        new = {"pod": {"metadata": {"uid": "new"}}, "snapshot": self.snapshot()}
        old["snapshot"] = copy.deepcopy(new["snapshot"])
        with patch.object(transport, "listener_node", return_value=node), \
             patch.object(transport, "read_json", return_value={"items": []}), \
             patch.object(transport, "handoff_backend", side_effect=[old, new]) as backends, \
             patch.object(transport, "validate_handoff_endpoints") as endpoints, \
             patch.object(transport, "validate_handoff_snapshots"):
            transport.handoff_witness(config, listener, current)
            self.assertEqual([c.args[-1] for c in backends.call_args_list], [53, 5353])
            endpoints.assert_called_once_with(current, old["pod"], [], 53)
        with self.assertRaisesRegex(ValueError, "different verified backend"):
            transport.handoff_witness(config, dict(config["listeners"][0], targetPort=5353), current)

    def test_staged_handoff_observes_both_backends_in_one_fresh_window(self):
        from scripts.recover_dns_transport import DNSObservationRejected

        config = self.config()
        current = transport.service(config, config["listeners"][0])
        listener = dict(config["listeners"][0], selector={"app": "candidate"})
        config["handoffs"] = [{"address": listener["address"], "previous_revision": "1"*40,
                               "candidate_revision": "2"*40}]
        node = {"metadata": {"name": "node", "uid": "node-uid"}}
        parallel = threading.Barrier(2, timeout=2)
        snapshot = self.snapshot()

        def backend(namespace, selector, node_name, port):
            parallel.wait()
            candidate = selector == listener["selector"]
            return {"pod": {"metadata": {"uid": "new" if candidate else "old", "annotations":
                        {"fugue.pro/oci-revision": ("2" if candidate else "1")*40}}},
                    "snapshot": copy.deepcopy(snapshot)}

        with patch.object(transport, "listener_node", return_value=node), \
             patch.object(transport, "read_json", return_value={"items": []}), \
             patch.object(transport, "handoff_backend", side_effect=backend), \
             patch.object(transport, "verify_staged_backend") as verify, \
             patch.object(transport, "validate_handoff_endpoints") as endpoints:
            witness = transport.handoff_witness(config, listener, current)
            self.assertEqual((witness["old_uid"], witness["candidate_uid"]), ("old", "new"))
            self.assertEqual(witness["candidate_revision"], "2"*40)
            endpoints.assert_called_once()
            self.assertEqual([call.args[0]["pod"]["metadata"]["uid"] for call in verify.call_args_list], ["old", "new"])
            self.assertTrue(all(call.args[1:] == (config["namespace"], listener["targetPort"])
                                for call in verify.call_args_list))
            with patch.object(transport, "HANDOFF_ATTEMPTS", 1):
                verify.side_effect = ValueError("DNS recovery observation failed cryptographic or readiness verification: record_readiness")
                with self.assertRaisesRegex(ValueError, "record_readiness"):
                    transport.handoff_witness(config, listener, current)
            with patch.object(transport, "HANDOFF_ATTEMPTS", 2), patch.object(transport.time, "sleep") as wait:
                for category in ("wire_materialization", "expired", "record_readiness"):
                    pending = DNSObservationRejected(category)
                    verify.side_effect = [None, pending, None, None]
                    self.assertEqual(transport.handoff_witness(config, listener, current), witness)
                    self.assertIn("candidate", pending.__notes__[0])
                    wait.assert_called_once()
                    wait.reset_mock()
                for category in ("artifact_authentication", "authority_mismatch", "temporal_ordering"):
                    rejected = DNSObservationRejected(category)
                    verify.side_effect = rejected
                    with self.assertRaisesRegex(ValueError, category):
                        transport.handoff_witness(config, listener, current)
                    self.assertIn("predecessor", rejected.__notes__[0])
                    wait.assert_not_called()
            verify.side_effect = None
            now = datetime.datetime.now(datetime.timezone.utc)
            snapshot["facts"][0]["proof"]["valid_until"] = (now - datetime.timedelta(seconds=1)).isoformat()
            with patch.object(transport, "HANDOFF_ATTEMPTS", 1):
                with self.assertRaisesRegex(ValueError, "proof is expired"):
                    transport.handoff_witness(config, listener, current)

    def test_staged_backend_verifies_the_dns_listener_owner_and_rechecks_pod(self):
        from scripts import recover_dns_transport

        port = 5353
        image = "registry.example/dns@sha256:" + "a"*64
        pod = {"metadata": {"name": "new", "uid": "uid", "resourceVersion": "11"},
               "spec": {"containers": [{"image": "other", "ports": [{"containerPort": port}]},
                                        {"image": image, "ports": [{"containerPort": port, "protocol": protocol}
                                                                   for protocol in ("UDP", "TCP")]}]},
               "status": {"phase": "Running", "conditions": [{"type": "Ready", "status": "True"}]}}
        candidate = {"pod": pod}
        with patch.object(recover_dns_transport, "verify_candidate") as verify, \
             patch.object(transport, "read_json", return_value=copy.deepcopy(pod)) as read:
            transport.verify_staged_backend(candidate, "system", port, "/approved/verifier")
            verify.assert_called_once_with(candidate, {"namespace": "system", "recovery":
                {"candidate_image": image, "candidate_pod_uid": "uid"}}, "/approved/verifier")
            read.return_value["metadata"]["resourceVersion"] = "12"
            with self.assertRaisesRegex(ValueError, "backend changed"):
                transport.verify_staged_backend(candidate, "system", port)
            with self.assertRaisesRegex(ValueError, "immutable"):
                transport.verify_staged_backend(candidate, "system", port + 1)

    def test_only_recovery_predecessor_may_lack_complete_materialization(self):
        from scripts.recover_dns_transport import DNSObservationRejected

        port = 53
        pod = self.backend()
        pod["spec"]["containers"][0]["image"] = "registry.example/dns@sha256:" + "a"*64
        for category in ("wire_materialization", "positive_proof", "artifact_authentication",
                         "proof_membership", "record_readiness", "expired"):
            for role, recovery in (("predecessor", True), ("predecessor", False),
                                   ("candidate", True), ("fallback", True)):
                with self.subTest(category=category, role=role, recovery=recovery), \
                     patch("scripts.recover_dns_transport.verify_candidate",
                           side_effect=DNSObservationRejected(category)), \
                     patch.object(transport, "read_json", return_value=pod) as read:
                    if category == "wire_materialization" and role == "predecessor" and recovery:
                        transport.verify_handoff_backend(role, {"pod": pod}, "system", port, recovery=recovery)
                        read.assert_called_once_with("get", "pod", pod["metadata"]["name"], "-n", "system", "-o", "json")
                    else:
                        with self.assertRaises(DNSObservationRejected) as caught:
                            transport.verify_handoff_backend(role, {"pod": pod}, "system", port, recovery=recovery)
                        self.assertEqual(caught.exception.backend_role, role)
                        read.assert_not_called()

    def recovery_fixture(self):
        config = self.config()
        public = dict(config["listeners"][0], name="public", port=53, selector={"app": "new"})
        diagnostic = dict(public, name="diagnostic", port=15353)
        fallback = {"selector": {"app": "fallback"}, "revision": "3"*40,
                    "pod_uid": "00000000-0000-0000-0000-000000000003"}
        stage = {"address": public["address"], "previous_revision": "1"*40,
                 "candidate_revision": "2"*40, "recovery_fallback": fallback}
        config.update(apiVersion="transport.fugue.dev/v4", generation=2, rollback_generation=3,
                      listeners=[diagnostic, public], handoffs=[stage])
        return self.load(config), stage, public

    def test_recovery_declaration_rejects_ambiguous_fallbacks(self):
        config, _, public = self.recovery_fixture()
        for change in ({"pod_uid": "unknown"}, {"revision": "main"}, {"selector": {}},
                       {"selector": public["selector"]}, {"port": 53}):
            changed = copy.deepcopy(config)
            changed["handoffs"][0]["recovery_fallback"].update(change)
            with self.assertRaisesRegex(ValueError, "fallback"):
                self.load(changed)

    def test_recovery_rolls_back_public_only_to_independent_verified_fallback(self):
        config, stage, public = self.recovery_fixture()
        fallback = stage["recovery_fallback"]
        before = transport.service(dict(config, generation=1), dict(public, selector={"app": "old"}))
        before["metadata"].update(uid="service", resourceVersion="11")
        before["spec"]["clusterIP"] = "10.0.0.1"
        witness = {"node_uid":"node", "old_uid":"old", "candidate_uid":"new", "fallback_uid":fallback["pod_uid"],
                   "previous_revision":stage["previous_revision"], "assignment":{"fencing_token":1},
                   "parent_digest":"parent", "plan_digest":"plan"}
        operation = (["patch", "public"], "{}", public, before, witness)

        def live(listener, generation, rv):
            service = copy.deepcopy(before)
            declaration = transport.service(dict(config, generation=generation), listener)
            service["metadata"]["annotations"].update(declaration["metadata"]["annotations"])
            service["metadata"]["resourceVersion"] = rv
            service["spec"].update(declaration["spec"])
            return service

        current = live(public, 2, "12")
        recovered = live(dict(public, selector=fallback["selector"]), 3, "13")
        verified = dict(witness, candidate_uid=fallback["pod_uid"])
        evidence = {"applied": []}
        observations = []

        def observe(_, reported_stage, binding, port):
            observations.append((reported_stage["candidate_revision"], port))
            if port != 53:
                return {"verified_backend": witness}
            if binding["candidate_uid"] == witness["old_uid"]:
                self.assertTrue(reported_stage["require_observed_predecessor"])
                return {"verified_backend": dict(witness, candidate_uid=witness["old_uid"])}
            if binding["candidate_uid"] == witness["candidate_uid"]:
                raise transport.PublicDNSVerificationFailed([])
            self.assertEqual(binding["candidate_uid"], fallback["pod_uid"])
            return {"passed": True}

        with patch.object(transport, "revalidate_handoff_witness", return_value=witness), \
             patch.object(transport, "read_json", side_effect=[current, recovered]), \
             patch.object(transport, "kubectl") as write, \
             patch.object(transport, "verify_staged_backend") as old_check:
            fallback_observations = []
            def verify_fallback(_, spec, listener, service, proof):
                self.assertEqual((spec, listener, proof), (stage, public, witness))
                fallback_observations.append(service["metadata"]["resourceVersion"])
                return verified
            with self.assertRaises(transport.PublicDNSVerificationFailed):
                transport.apply_transport_steps(config, [operation], evidence, lambda: None, observe, verify_fallback)
            self.assertEqual(fallback_observations, ["11", "12"])
            self.assertEqual(observations, [(stage["candidate_revision"], 15353), (stage["previous_revision"], 53),
                                            (stage["candidate_revision"], 53), (fallback["revision"], 53)])
            self.assertEqual(evidence["compensations"], [
                {"listener":"public", "result":"restored", "public_verification":{"passed":True}}])
            self.assertEqual(evidence["fallback_verifications"], [verified])
            self.assertEqual(write.call_count, 2)
            self.assertIn({"op":"add", "path":"/spec/selector", "value":fallback["selector"]},
                          json.loads(write.call_args.kwargs["body"]))
            old_check.assert_not_called()

        for relaxation in ({"candidate_uid": "unreviewed"}, {"assignment": {"fencing_token": 2}},
                           {"parent_digest": "different"}, {"plan_digest": "different"}):
            with self.subTest(relaxation=relaxation), patch.object(transport, "kubectl") as write:
                with self.assertRaisesRegex(ValueError, "assignment changed"):
                    transport.apply_transport_steps(config, [operation], {"applied": []}, lambda: None, observe,
                                                     lambda *_: dict(verified, **relaxation))
                write.assert_not_called()

    def test_staged_handoff_compensates_only_attempted_stage_and_reverifies_on_resume(self):
        one = dict(self.config()["listeners"][0], name="public-one", port=53)
        two = dict(one, name="public-two", address="9.9.9.9")
        diagnose = dict(one, port=15353, name="diagnostic-one")
        diagnose_two = dict(two, port=15353, name="diagnostic-two")
        config = dict(self.config(), apiVersion="transport.fugue.dev/v4", rollback_generation=2,
                      listeners=[one, two, diagnose, diagnose_two],
                      handoffs=[{"address": x["address"], "previous_revision": "1"*40,
                                 "candidate_revision": "2"*40} for x in (one, two)])
        config = self.load(config)
        with self.assertRaisesRegex(ValueError, "reserved next generation"):
            self.load(dict(config, rollback_generation=3))
        with self.assertRaisesRegex(ValueError, "reserved compensation generation"):
            old_config = dict(config)
            del old_config["rollback_generation"]
            transport.reconcile(self.load(old_config), apply=True, evidence_path=Path("unused"), verify_stage=object())
        witness = {"candidate_uid": "candidate", "old_uid": "old", "node_uid": "node",
                   "previous_revision": "1"*40, "assignment": {"fencing_token": 1},
                   "parent_digest": "parent", "plan_digest": "plan"}
        predecessor_binding = {"candidate_uid": "old", "node_uid": "node"}
        def op(listener):
            old = transport.service(config, dict(listener, selector={"app": "predecessor"}))
            return (["patch", listener["name"]], "{}", listener, old, witness)
        def save():
            pass
        def run(remaining, fail_port=None, fail_address=None, changed_diagnostic=False,
                write_failure=None, restore_failure=False, predecessor_failure=False, changed_predecessor=None):
            evidence, history = {"applied": []}, []
            def verify(_, stage, binding, port):
                history.append((stage["address"], port, binding))
                if port == fail_port and (fail_address is None or stage["address"] == fail_address) and binding == witness:
                    raise ValueError("UDP failed")
                if binding == predecessor_binding and predecessor_failure:
                    raise transport.PublicDNSVerificationFailed([{"hostname": "app.example.test",
                                                                  "transport": "udp", "response_code": 2}])
                if binding == predecessor_binding:
                    result = dict(witness, candidate_uid=witness["old_uid"])
                    if changed_predecessor:
                        result.update(changed_predecessor)
                else:
                    result = dict(witness, assignment={"fencing_token": 2}) if changed_diagnostic else witness
                return {"passed": True, "verified_backend": result}
            def write(_, name, **kwargs):
                if name == write_failure:
                    raise RuntimeError("write response lost")
            def restore(_, operation, observed, acknowledged):
                if operation[2]["name"] == write_failure:
                    self.assertFalse(acknowledged)
                    if restore_failure:
                        raise ValueError("foreign CAS")
                else:
                    self.assertTrue(acknowledged)
                return config
            with patch.object(transport, "revalidate_handoff_witness", return_value=witness), \
                 patch.object(transport, "kubectl", side_effect=write), \
                 patch.object(transport, "restore_staged_listener", side_effect=restore):
                if changed_diagnostic:
                    with self.assertRaisesRegex(ValueError, "assignment changed after diagnostic"):
                        transport.apply_transport_steps(config, remaining, evidence, save, verify)
                elif changed_predecessor:
                    with self.assertRaisesRegex(ValueError, "assignment changed after predecessor"):
                        transport.apply_transport_steps(config, remaining, evidence, save, verify)
                elif write_failure:
                    with self.assertRaisesRegex(RuntimeError, "could not be fully verified" if restore_failure else "response lost"):
                        transport.apply_transport_steps(config, remaining, evidence, save, verify)
                elif predecessor_failure:
                    with self.assertRaises(transport.PublicDNSVerificationFailed):
                        transport.apply_transport_steps(config, remaining, evidence, save, verify)
                elif fail_port is None:
                    transport.apply_transport_steps(config, remaining, evidence, save, verify)
                else:
                    with self.assertRaisesRegex(ValueError, "UDP failed"):
                        transport.apply_transport_steps(config, remaining, evidence, save, verify)
            return evidence, history
        operations = [op(one), op(two), op(diagnose), op(diagnose_two)]
        evidence, history = run(operations, fail_port=15353)
        self.assertEqual(evidence["applied"], ["diagnostic-one"])
        self.assertEqual([x["listener"] for x in evidence["compensations"]], ["diagnostic-one"])
        self.assertEqual(history, [(one["address"], 15353, witness)])
        evidence, history = run(operations, fail_port=53)
        self.assertEqual(evidence["applied"], ["diagnostic-one", "public-one"])
        self.assertEqual(evidence["public_verifications"], [{"address": one["address"], "port": 53, "passed": False}])
        self.assertEqual([x["listener"] for x in evidence["compensations"]], ["public-one", "diagnostic-one"])
        self.assertEqual(history, [(one["address"], 15353, witness), (one["address"], 53, predecessor_binding),
                                   (one["address"], 53, witness), (one["address"], 53, predecessor_binding)])
        evidence, history = run(operations, predecessor_failure=True)
        self.assertEqual(evidence["applied"], ["diagnostic-one"])
        self.assertEqual(evidence["compensations"], [{"listener": "diagnostic-one", "result": "retained_diagnostic_stage"}])
        self.assertEqual(evidence["predecessor_verifications"][0]["failed_samples"][0]["response_code"], 2)
        self.assertEqual(history, [(one["address"], 15353, witness), (one["address"], 53, predecessor_binding)])
        evidence, _ = run(operations, fail_port=53, fail_address=two["address"])
        self.assertEqual([x["listener"] for x in evidence["compensations"]], ["public-two", "diagnostic-two"])
        evidence, _ = run(operations, write_failure="public-one")
        self.assertEqual(evidence["applied"], ["diagnostic-one"])
        self.assertEqual([x["listener"] for x in evidence["compensations"]], ["public-one", "diagnostic-one"])
        evidence, _ = run(operations, write_failure="public-one", restore_failure=True)
        self.assertEqual([x["result"] for x in evidence["compensations"]], ["unverified", "restored"])
        evidence, history = run(operations, changed_diagnostic=True)
        self.assertEqual(evidence["applied"], ["diagnostic-one"])
        self.assertEqual(history, [(one["address"], 15353, witness), (one["address"], 53, predecessor_binding)])
        for drift in ({"assignment": {"fencing_token": 2}}, {"parent_digest": "other"},
                      {"plan_digest": "other"}, {"candidate_uid": "replacement"}):
            with self.subTest(predecessor=drift):
                evidence, history = run(operations, changed_predecessor=drift)
                self.assertEqual(evidence["applied"], ["diagnostic-one"])
                self.assertEqual(evidence["compensations"], [{"listener": "diagnostic-one", "result": "retained_diagnostic_stage"}])
                self.assertEqual(history, [(one["address"], 15353, witness), (one["address"], 53, predecessor_binding)])
        evidence, history = run([op(one)])
        self.assertEqual(evidence["applied"], ["public-one"])
        self.assertEqual(history[:3], [(one["address"], 15353, None), (one["address"], 53, predecessor_binding),
                                      (one["address"], 53, witness)])
        evidence, history = run([op(two), op(diagnose_two)])
        self.assertEqual(evidence["applied"], ["diagnostic-two", "public-two"])
        self.assertEqual(history, [(one["address"], 15353, None), (one["address"], 53, None),
                                   (two["address"], 15353, witness), (two["address"], 53, predecessor_binding),
                                   (two["address"], 53, witness)])

    def test_staged_compensation_cas_restores_exact_predecessor_and_rejects_drift(self):
        prior = self.config()
        old_listener = prior["listeners"][0]
        previous = transport.service(prior, old_listener)
        previous["metadata"].update(uid="service-uid", resourceVersion="11")
        previous["metadata"]["annotations"]["unrelated"] = "keep"
        previous["spec"]["clusterIP"] = "10.0.0.1"
        listener = dict(old_listener, selector={"app": "new"}, targetPort=5353)
        config = dict(prior, generation=2, rollback_generation=3, listeners=[listener])
        witness = {"node_uid": "node-uid", "old_uid": "old-uid", "previous_revision": "1"*40}
        operation = (None, None, listener, previous, witness)
        desired = transport.service(config, listener)

        def applied(declaration, revision):
            state = copy.deepcopy(previous)
            state["metadata"]["resourceVersion"] = revision
            state["metadata"]["annotations"].update(declaration["metadata"]["annotations"])
            state["spec"].update(declaration["spec"])
            return state

        live = applied(desired, "12")
        restored = applied(transport.service(dict(prior, generation=3), old_listener), "13")
        node = {"metadata": {"name": "node", "uid": witness["node_uid"]}}
        backend = {"pod": {"metadata": {"uid": witness["old_uid"], "annotations":
                                       {"fugue.pro/oci-revision": witness["previous_revision"]}}}}
        with patch.object(transport, "listener_node", return_value=node), \
             patch.object(transport, "handoff_backend", return_value=backend) as read_backend, \
             patch.object(transport, "verify_staged_backend") as verify, \
             patch.object(transport, "read_json") as read, patch.object(transport, "kubectl") as write:
            for observed in (live, previous):
                # The first case restored the candidate. The second fences a
                # still-pending forward CAS on the original resourceVersion.
                read.side_effect = [observed, restored]
                write.side_effect = RuntimeError("lost compensation response")
                result = transport.restore_staged_listener(config, operation, witness, observed is live)
                self.assertEqual((result["generation"], result["listeners"]), (3, [old_listener]))
                read_backend.assert_called_with("system", old_listener["selector"], "node", 53)
                verify.assert_called_with(backend, "system", 53)
                operations = json.loads(write.call_args.kwargs["body"])
                self.assertEqual(operations[1]["value"], observed["metadata"]["resourceVersion"])
                self.assertEqual(operations[2]["value"], observed["spec"])
                self.assertEqual(operations[3]["value"], observed["metadata"]["annotations"])

            write.reset_mock()
            read.side_effect = [previous, live, live, restored]
            self.assertIsNotNone(transport.restore_staged_listener(config, operation, witness, False))
            self.assertEqual([json.loads(call.kwargs["body"])[1]["value"] for call in write.call_args_list], ["11", "12"])

            rejected = copy.deepcopy(previous)
            rejected["metadata"]["resourceVersion"] = "12"
            read.side_effect = [rejected]
            write.reset_mock()
            self.assertIsNone(transport.restore_staged_listener(config, operation, witness, False))
            write.assert_not_called()
            for drift in ("uid", "annotations", "spec", "pod"):
                current = copy.deepcopy(live)
                old = copy.deepcopy(backend)
                if drift == "uid":
                    current["metadata"]["uid"] = "foreign"
                if drift == "annotations":
                    current["metadata"]["annotations"]["unrelated"] = "foreign"
                if drift == "spec":
                    current["spec"]["clusterIP"] = "10.0.0.2"
                if drift == "pod":
                    old["pod"]["metadata"]["uid"] = "foreign"
                read.side_effect = [current]
                read_backend.return_value = old
                with self.assertRaises(ValueError):
                    transport.restore_staged_listener(config, operation, witness, True)
                write.assert_not_called()

    def test_assignment_refresh_requires_stable_complete_witness_and_same_identity(self):
        initial = {'node_uid':'node','old_uid':'old','candidate_uid':'candidate','assignment':{'fencing_token':1}}
        advanced = dict(initial, assignment={'fencing_token':2})
        with patch.object(transport, 'handoff_witness', side_effect=[advanced, advanced]) as read, patch.object(transport.time, 'sleep'):
            transport.revalidate_handoff_witness({}, {}, {}, initial)
            self.assertEqual(read.call_count, 2)
        changing = [dict(initial, assignment={'fencing_token':i+2}) for i in range(transport.HANDOFF_ATTEMPTS)]
        with patch.object(transport, 'handoff_witness', side_effect=changing), patch.object(transport.time, 'sleep'):
            with self.assertRaisesRegex(ValueError, 'did not stabilize'):
                transport.revalidate_handoff_witness({}, {}, {}, initial)
        with patch.object(transport, 'handoff_witness', return_value=dict(advanced, old_uid='replacement')):
            with self.assertRaisesRegex(ValueError, 'identity changed'):
                transport.revalidate_handoff_witness({}, {}, {}, initial)

    def test_runtime_fact_identity_race_is_retried(self):
        config = self.config()
        current = transport.service(config, config['listeners'][0])
        current['metadata'].update(uid='service-uid', resourceVersion='10')
        candidate_listener = copy.deepcopy(config['listeners'][0])
        candidate_listener['selector'] = {'app': 'candidate'}
        observations = [
            ValueError('DNS handoff backend changed while reading runtime facts'),
            {'pod': {'metadata': {'uid': 'old'}}, 'snapshot': {}},
            {'pod': {'metadata': {'uid': 'new'}}, 'snapshot': {'assignment': {},
             'parent_digest': 'sha256:' + 'a' * 64, 'plan_digest': 'sha256:' + 'b' * 64}},
        ]
        with patch.object(transport, 'handoff_backend', side_effect=observations), \
             patch.object(transport, 'read_json', return_value={'items': [{'metadata': {'name': 'node', 'uid': 'node-uid'}, 'status': {'addresses': [{'address': config['listeners'][0]['address']}]}}]}), \
             patch.object(transport, 'validate_handoff_endpoints'), \
             patch.object(transport, 'validate_handoff_snapshots'), \
             patch.object(transport.time, 'sleep') as sleep:
            result = transport.handoff_witness(config, candidate_listener, current)
            self.assertEqual(result['candidate_uid'], 'new')
        sleep.assert_called_once_with(transport.HANDOFF_RETRY_SECONDS)

    def test_proof_expiry_during_observation_requires_a_fresh_bounded_retry(self):
        config = self.config()
        current = transport.service(config, config['listeners'][0])
        candidate_listener = copy.deepcopy(config['listeners'][0])
        candidate_listener['selector'] = {'app': 'candidate'}
        old = {'pod': {'metadata': {'uid': 'old'}}, 'snapshot': self.snapshot()}
        candidate = {'pod': {'metadata': {'uid': 'new'}}, 'snapshot': copy.deepcopy(old['snapshot'])}
        expired = ValueError('DNS handoff proof is expired or from the future')
        nodes = {'items': [{'metadata': {'name': 'node', 'uid': 'node-uid'},
                           'status': {'addresses': [{'address': config['listeners'][0]['address']}]}}]}
        for persistent in [False, True]:
            with self.subTest(persistent=persistent), \
                 patch.object(transport, 'handoff_backend', side_effect=[old, candidate] * transport.HANDOFF_ATTEMPTS) as backend, \
                 patch.object(transport, 'read_json', return_value=nodes), \
                 patch.object(transport, 'validate_handoff_endpoints'), \
                 patch.object(transport, 'validate_handoff_snapshots', side_effect=([expired] * transport.HANDOFF_ATTEMPTS if persistent else [expired, None])), \
                 patch.object(transport.time, 'sleep') as sleep:
                if persistent:
                    with self.assertRaisesRegex(ValueError, 'proof is expired'):
                        transport.handoff_witness(config, candidate_listener, current)
                    self.assertEqual(backend.call_count, 2 * transport.HANDOFF_ATTEMPTS)
                    self.assertEqual(sleep.call_count, transport.HANDOFF_ATTEMPTS - 1)
                else:
                    result = transport.handoff_witness(config, candidate_listener, current)
                    self.assertEqual(result['candidate_uid'], 'new')
                    self.assertEqual(backend.call_count, 4)
                    sleep.assert_called_once_with(transport.HANDOFF_RETRY_SECONDS)

    def test_only_read_unavailability_is_retryable(self):
        for command, error, expected in [
            ('get', 'Error from server (ServiceUnavailable): try again', transport.ObservationUnavailable),
            ('patch', 'Error from server (ServiceUnavailable): try again', RuntimeError),
            ('get', 'Error from server (Forbidden): denied', RuntimeError),
        ]:
            result = subprocess.CompletedProcess([], 1, stdout='', stderr=error)
            with patch.object(transport.subprocess, 'run', return_value=result):
                with self.assertRaises(expected) as caught:
                    transport.kubectl(command, 'service', 'listener')
                self.assertIs(type(caught.exception), expected)

    def test_temporarily_unavailable_snapshot_is_read_again(self):
        config = self.config()
        current = transport.service(config, config['listeners'][0])
        listener = copy.deepcopy(config['listeners'][0])
        listener['selector'] = {'app': 'candidate'}
        snapshot = self.snapshot()
        observations = [transport.ObservationUnavailable('temporarily unavailable'),
                        {'pod': {'metadata': {'uid': 'old'}}, 'snapshot': snapshot},
                        {'pod': {'metadata': {'uid': 'new'}}, 'snapshot': copy.deepcopy(snapshot)}]
        nodes = {'items': [{'metadata': {'name': 'node', 'uid': 'node-uid'},
                           'status': {'addresses': [{'address': listener['address']}]}}]}
        with patch.object(transport, 'handoff_backend', side_effect=observations) as backend, \
             patch.object(transport, 'read_json', return_value=nodes), \
             patch.object(transport, 'validate_handoff_endpoints'), \
             patch.object(transport.time, 'sleep') as sleep:
            self.assertEqual(transport.handoff_witness(config, listener, current)['candidate_uid'], 'new')
            self.assertEqual(backend.call_count, 3)
            sleep.assert_called_once_with(transport.HANDOFF_RETRY_SECONDS)


if __name__ == "__main__":
    unittest.main()
