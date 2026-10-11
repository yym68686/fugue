import copy
import datetime
import http.client
import json
import unittest
from unittest import mock
import urllib.error

from scripts import observe_managed_ssh_front as observe
from scripts import stage_managed_ssh_fronts as stage


class SSHFrontObservationTest(unittest.TestCase):
    def test_network_diagnostics_classify_failure_without_response_or_credentials(self):
        failures=[(http.client.IncompleteRead(b'private-response-body'),'response truncated: received 21 bytes'),(urllib.error.HTTPError('https://control.example?token=hidden',403,'hidden-header',{},None),'HTTP 403'),(TimeoutError('hidden-credential'),'TimeoutError')]
        for error,expected in failures:
            opener=mock.Mock()
            opener.open.side_effect=error
            with mock.patch.object(observe.urllib.request,'build_opener',return_value=opener):
                with self.assertRaises(ValueError) as raised:observe.read_json('https://control.example','private-api-token')
            self.assertIn(expected,str(raised.exception))
            self.assertNotIn('hidden',str(raised.exception))
            self.assertNotIn('private-',str(raised.exception))

    def fixture(self):
        node = {'id': 'worker-a', 'group': 'group-a', 'publicAddress': '8.8.8.8'}
        config = {'generation': 2, 'namespace': 'test-system', 'stateConfigMap': 'front-stage', 'image': 'registry.example/front@sha256:'+'a'*64, 'nodes': [node]}
        slot = stage.slot_name(config, node)
        selector = {'fugue.pro/ssh-front-slot': slot}
        meta = {'name': slot, 'namespace': 'test-system', 'uid': 'deployment-a', 'labels': {'app.kubernetes.io/managed-by': stage.MANAGER}, 'annotations': {stage.DIGEST: stage.digest(config)}}
        template = {'nodeName': node['id'], 'hostNetwork': False, 'containers': [{'image': config['image']}]}
        deployment = {'metadata': meta, 'spec': {'replicas': 1, 'selector': {'matchLabels': selector}, 'template': {'spec': template}}}
        pod = {'metadata': {'name': 'front-a', 'namespace': 'test-system', 'uid': 'pod-a', 'ownerReferences': [{'kind': 'ReplicaSet', 'name': 'replica-a', 'uid': 'replica-a', 'controller': True}]}, 'spec': template, 'status': {'podIP': '10.1.1.2', 'containerStatuses': [{'ready': True, 'restartCount': 0, 'imageID': config['image']}]}}
        replica = {'metadata': {'uid': 'replica-a', 'ownerReferences': [{'kind': 'Deployment', 'uid': 'deployment-a', 'controller': True}]}}
        state = {'metadata': {'uid': 'stage-a', 'labels': {'app.kubernetes.io/managed-by': stage.MANAGER}, 'annotations': {stage.DIGEST: stage.digest(config), stage.GENERATION: '2'}}, 'data': {'declaration.json': stage.canonical(config)}}
        observed_node = {'status': {'addresses': [{'address': node['publicAddress']}], 'conditions': [{'type': 'Ready', 'status': 'True'}]}}
        records = {'configmap': state, 'deployment': deployment, 'pods': {'items': [pod]}, 'pod': pod, 'replicaset': replica, 'node': observed_node}
        requests = []
        def kube(args, value=None):
            if args[0] != 'get' or value is not None:
                self.fail('read-only observer attempted mutation')
            requests.append(args)
            return json.dumps(records[args[1]])
        identity = {'app_id': 'app_fixture', 'tenant_id': 'tenant_fixture', 'id': 'identity-a', 'public_key': 'fixture-public', 'fingerprint': 'fixture-fingerprint'}
        route = {'public_port': 24000, 'app_id': identity['app_id'], 'tenant_id': identity['tenant_id'], 'target_host': 'slot-a.test-backends.svc.cluster.local', 'target_port': 2222, 'status': 'pending'}
        timestamp = observe.now()
        ack = {'edge_id': node['id'], 'edge_group_id': node['group'], 'instance_uid': 'pod-a', 'persisted': True, 'loaded_at': timestamp.isoformat(), 'observed_at': timestamp.isoformat(), 'expires_at': (timestamp+datetime.timedelta(seconds=90)).isoformat(), 'routes': [route], 'connections': [], 'route_digest': 'sha256:'+'b'*64, 'bundle_version': 'route-a'}
        health = {'status': 'ok', 'draining': False, 'instance_uid': 'pod-a', 'route_digest': ack['route_digest'], 'bundle_version': ack['bundle_version'], 'last_ack_at': timestamp.isoformat()}
        acknowledgement = {'fresh': True, 'current_routes_match': True, 'acknowledgement': ack}
        binding = {'app_id': identity['app_id'], 'tenant_id': identity['tenant_id'], 'identity_id': identity['id'], 'id': 'binding-a', 'public_port': 24000}
        status = {'intent': {'app_id': identity['app_id'], 'tenant_id': identity['tenant_id'], 'spec': {'enabled': True, 'identity_id': identity['id']}}, 'binding': binding, 'control': {'identity_id': identity['id'], 'route_generation': 1, 'active': {'id': 'slot-a', 'namespace': 'test-backends', 'verified': True, 'evidence': {'fingerprint': identity['fingerprint'], 'identity_id': identity['id'], 'expires_at': ack['expires_at']}}}}
        def api(path):
            if '/front-acknowledgements/' in path:return acknowledgement
            if path.endswith('/identity'):return {'identity': identity}
            return status
        probes = []
        def probe(target, key):
            probes.append(target)
            return {'verified': True}
        return config, records, identity, status, acknowledgement, health, kube, api, probe, probes

    def test_exact_candidate_requires_stored_ack_and_current_managed_binding(self):
        config, records, identity, status, ack, health, kube, api, probe, probes = self.fixture()
        result = observe.observe(config, 'worker-a', ['app_fixture'], api, probe, kube, lambda url: health, public=True)
        self.assertTrue(result['verified'])
        self.assertFalse(result['authorizes_traffic'])
        self.assertEqual(probes, ['10.1.1.2:24000', '8.8.8.8:24000'])
        self.assertEqual(result['pod']['metadata']['uid'], 'pod-a')

    def test_rejects_stale_foreign_and_incomplete_acknowledgements(self):
        for change in ('stale', 'old-routes', 'foreign-instance', 'missing-connections', 'expired', 'draining', 'live-digest'):
            with self.subTest(change=change):
                config, records, identity, status, ack, health, kube, api, probe, probes = self.fixture()
                if change == 'stale':ack['fresh'] = False
                if change == 'old-routes':ack['current_routes_match'] = False
                if change == 'foreign-instance':ack['acknowledgement']['instance_uid'] = 'other-pod'
                if change == 'missing-connections':ack['acknowledgement']['connections'] = None
                if change == 'expired':ack['acknowledgement']['expires_at'] = observe.now().isoformat()
                if change == 'draining':health['draining'] = True
                if change == 'live-digest':health['route_digest'] = 'other-digest'
                with self.assertRaises(ValueError):observe.observe(config, 'worker-a', ['app_fixture'], api, probe, kube, lambda url: health)
                self.assertEqual(probes, [])

    def test_rejects_wrong_workload_and_executor_ownership(self):
        for change in ('foreign-route', 'tenant', 'inactive', 'unverified', 'image', 'replica-owner', 'restart', 'ambiguous'):
            with self.subTest(change=change):
                config, records, identity, status, ack, health, kube, api, probe, probes = self.fixture()
                if change == 'foreign-route':ack['acknowledgement']['routes'][0]['app_id'] = 'app_other'
                if change == 'tenant':identity['tenant_id'] = 'another-tenant'
                if change == 'inactive':status['control']['active'] = None
                if change == 'unverified':status['control']['active']['verified'] = False
                if change == 'image':records['pod']['status']['containerStatuses'][0]['imageID'] = 'other-image'
                if change == 'replica-owner':records['replicaset']['metadata']['ownerReferences'][0]['uid'] = 'other-deployment'
                if change == 'restart':records['pod']['status']['containerStatuses'][0]['restartCount'] = 1
                if change == 'ambiguous':records['pods']['items'].append(copy.deepcopy(records['pod']))
                with self.assertRaises(ValueError):observe.observe(config, 'worker-a', ['app_fixture'], api, probe, kube, lambda url: health)

    def test_changed_binding_after_probe_cannot_produce_positive_evidence(self):
        config, records, identity, status, ack, health, kube, api, probe, probes = self.fixture()
        def racing_probe(target, key):
            status['binding']['public_port'] = 24001
            return probe(target, key)
        with self.assertRaises(ValueError):observe.observe(config, 'worker-a', ['app_fixture'], api, racing_probe, kube, lambda url: health)

    def test_endpoint_selection_requires_exact_service_pod_node_and_ports(self):
        config, records, identity, status, ack, health, kube, api, probe, probes = self.fixture()
        pod = records['pod']
        service = {'metadata': {'uid': 'service-a'}, 'spec': {}}
        endpoint = {'nodeName': 'worker-a', 'addresses': ['10.1.1.2'], 'targetRef': {'uid': 'pod-a', 'kind': 'Pod', 'name': 'front-a', 'namespace': 'test-system'}, 'conditions': {'ready': True}}
        slices = {'items': [{'metadata': {'ownerReferences': [{'kind': 'Service', 'uid': 'service-a', 'controller': True}]}, 'ports': [{'name': 'ssh-24000', 'protocol': 'TCP', 'port': 24000}], 'endpoints': [endpoint]}]}
        self.assertEqual(observe.endpoint_witness(service, pod, slices, [24000])['pod_uid'], 'pod-a')
        for change in ('owner', 'pod', 'node', 'terminating', 'ports', 'ambiguous'):
            candidate = copy.deepcopy(slices)
            if change == 'owner':candidate['items'][0]['metadata']['ownerReferences'][0]['uid'] = 'other-service'
            if change == 'pod':candidate['items'][0]['endpoints'][0]['targetRef']['uid'] = 'other-pod'
            if change == 'node':candidate['items'][0]['endpoints'][0]['nodeName'] = 'other-node'
            if change == 'terminating':candidate['items'][0]['endpoints'][0]['conditions']['terminating'] = True
            if change == 'ports':candidate['items'][0]['ports'][0]['port'] = 22
            if change == 'ambiguous':candidate['items'][0]['endpoints'].append(copy.deepcopy(endpoint))
            with self.assertRaises(ValueError):observe.endpoint_witness(service, pod, candidate, [24000])


if __name__ == '__main__':
    unittest.main()
