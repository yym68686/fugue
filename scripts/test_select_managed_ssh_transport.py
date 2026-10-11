import copy
import json
import unittest

from scripts import select_managed_ssh_transport as transport
from scripts.run_managed_ssh_transport import api_origin, validate_operation


class SSHTransportSelectionTest(unittest.TestCase):
    def test_host_runner_resolves_only_declared_internal_service_and_port(self):
        stage = {'namespace': 'test-system', 'apiURL': 'http://api.test-system.svc.cluster.local:80'}
        service = {'metadata': {'uid': 'api-service'}, 'spec': {'type': 'ClusterIP', 'clusterIP': '10.2.2.10', 'ports': [{'port': 80}]}}
        def kube(args):
            self.assertEqual(args, ['get', 'service', 'api', '-n', 'test-system', '-o', 'json'])
            return json.dumps(service)
        self.assertEqual(api_origin(stage, kube), 'http://10.2.2.10:80')
        service['spec']['ports'][0]['port'] = 8080
        with self.assertRaises(ValueError):api_origin(stage, kube)
        stage['apiURL'] = 'https://api.example'
        self.assertEqual(api_origin(stage, kube), 'https://api.example')
        stage['apiURL'] = 'https://api.example?token=secret'
        with self.assertRaises(ValueError):api_origin(stage, kube)

    def test_operation_cannot_escape_reviewed_configuration_or_select_implicit_action(self):
        operation = {'schema': 'fugue.ssh-transport-operation/v1', 'stage': 'deploy/environments/production/managed-ssh/front-stage.json', 'selection': 'deploy/environments/production/managed-ssh/transport/fixture.json', 'action': 'stage'}
        self.assertEqual(validate_operation(operation), operation)
        for field, value in [('stage', '/tmp/stage.json'), ('selection', 'deploy/environments/production/managed-ssh/transport/../fixture.json'), ('action', 'automatic')]:
            candidate = dict(operation)
            candidate[field] = value
            with self.assertRaises(ValueError):validate_operation(candidate)

    def fixture(self):
        stage = {'namespace': 'test-system', 'nodes': [{'id': 'worker-a', 'publicAddress': '8.8.8.8'}]}
        config = {'schema': 'fugue.ssh-transport-selection/v1', 'namespace': 'test-system', 'node': 'worker-a', 'service': 'ssh-public', 'stageDigest': transport.digest(stage), 'applications': ['app_fixture'], 'stagedGeneration': 1, 'servingGeneration': 2, 'compensationGeneration': 3}
        pod = {'metadata': {'uid': 'pod-a', 'namespace': 'test-system', 'name': 'front-a'}, 'spec': {'nodeName': 'worker-a'}, 'status': {'podIP': '10.1.1.2'}}
        evidence = {'observed_at': 'fixture-time', 'selector': {'slot': 'front-a'}, 'pod': pod, 'route_digest': 'routes-a', 'bindings': [{'public_port': 24000, 'route': {'app_id': 'app_fixture', 'target_host': 'backend'}}]}
        state = {}
        writes = []
        def kube(args, value=None):
            if args[0] == 'get':
                if args[1] == 'service':return json.dumps(state['service']) if 'service' in state else ''
                if args[1] == 'services':return json.dumps({'items': [state['service']]+state.get('foreign', [])})
                if args[1] == 'endpointslices':return json.dumps({'items': [{'metadata': {'ownerReferences': [{'kind': 'Service', 'uid': state['service']['metadata']['uid'], 'controller': True}]}, 'ports': [{'name': 'ssh-24000', 'protocol': 'TCP', 'port': 24000}], 'endpoints': [{'nodeName': 'worker-a', 'addresses': ['10.1.1.2'], 'targetRef': {'kind': 'Pod', **pod['metadata']}, 'conditions': {'ready': True}}]}]})
                self.fail(args)
            if args[0] == 'create':
                if state.get('service'):raise ValueError('already exists')
                state['service'] = copy.deepcopy(value)
                state['service']['metadata'].update(uid='service-a', resourceVersion='1')
                state['service']['spec']['clusterIP'] = '10.2.2.2'
            elif args[0] == 'patch':
                patch = json.loads(args[args.index('-p')+1])
                candidate = copy.deepcopy(state['service'])
                for operation in patch:
                    keys = operation['path'].strip('/').split('/')
                    parent = candidate
                    for key in keys[:-1]:parent = parent[key]
                    key = keys[-1]
                    if operation['op'] == 'test':
                        if parent[key] != operation['value']:raise ValueError('CAS conflict')
                    elif operation['op'] == 'remove':del parent[key]
                    else:parent[key] = operation['value']
                candidate['metadata']['resourceVersion'] = str(int(candidate['metadata']['resourceVersion'])+1)
                state['service'] = candidate
            else:self.fail('unexpected mutation '+str(args))
            writes.append(copy.deepcopy(state['service']))
            return ''
        observed = []
        def observe(public):
            observed.append(public)
            return copy.deepcopy(evidence)
        witnesses = []
        class Witness:
            def __init__(self):
                self.exchanges = 0
                self.closed = False
            def exchange(self):self.exchanges += 1
            def close(self):self.closed = True
        def witness(address, binding):
            value = Witness()
            witnesses.append(value)
            return value
        return config, stage, evidence, state, writes, kube, observe, witness, witnesses

    def test_initial_selection_and_compensation_preserve_both_witnesses(self):
        config, stage, evidence, state, writes, kube, observe, witness, witnesses = self.fixture()
        staged = transport.select(config, stage, 'stage', observe, kube=kube)
        self.assertFalse(staged['public_changed'])
        self.assertNotIn('externalIPs', state['service']['spec'])
        self.assertEqual(transport.select(config, stage, 'stage', observe, kube=kube), staged)
        selected = transport.select(config, stage, 'activate', observe, witness, kube, lambda seconds: None)
        self.assertTrue(selected['held_sessions_preserved'])
        self.assertEqual(state['service']['spec']['externalIPs'], ['8.8.8.8'])
        self.assertEqual(state['service']['spec']['clusterIP'], '10.2.2.2')
        restored = transport.select(config, stage, 'compensate', observe, witness, kube, lambda seconds: None)
        self.assertEqual(restored['phase'], 'compensated')
        self.assertNotIn('externalIPs', state['service']['spec'])
        self.assertEqual(state['service']['spec']['selector'], evidence['selector'])
        self.assertTrue(all(value.closed and value.exchanges >= 3 for value in witnesses))
        with self.assertRaises(ValueError):transport.select(config, stage, 'activate', observe, witness, kube)
        self.assertEqual(len(writes), 3)

    def test_failed_public_verification_compensates_only_its_selection(self):
        config, stage, evidence, state, writes, kube, observe, witness, witnesses = self.fixture()
        transport.select(config, stage, 'stage', observe, kube=kube)
        def failing(public):
            if state['service']['metadata']['annotations'][transport.PHASE] == 'serving':
                raise ValueError('public path failed')
            return observe(public)
        with self.assertRaisesRegex(ValueError, 'exact selection compensated'):
            transport.select(config, stage, 'activate', failing, witness, kube, lambda seconds: None)
        self.assertEqual(state['service']['metadata']['annotations'][transport.GENERATION], '3')
        self.assertNotIn('externalIPs', state['service']['spec'])
        self.assertTrue(all(value.closed for value in witnesses))

    def test_newer_or_foreign_selection_cannot_be_overwritten_by_compensation(self):
        config, stage, evidence, state, writes, kube, observe, witness, witnesses = self.fixture()
        transport.select(config, stage, 'stage', observe, kube=kube)
        def superseded(public):
            if state['service']['metadata']['annotations'][transport.PHASE] == 'serving':
                state['service']['metadata']['annotations'][transport.GENERATION] = '4'
                state['service']['metadata']['resourceVersion'] = '4'
                raise ValueError('superseded')
            return observe(public)
        with self.assertRaises(ValueError):transport.select(config, stage, 'activate', superseded, witness, kube)
        self.assertEqual(len(writes), 2)
        self.assertEqual(state['service']['metadata']['annotations'][transport.GENERATION], '4')
        self.assertEqual(state['service']['spec']['externalIPs'], ['8.8.8.8'])

    def test_capture_missing_witness_and_foreign_service_fail_before_public_write(self):
        for change in ('capture', 'no-witness', 'foreign-owner', 'drift'):
            with self.subTest(change=change):
                config, stage, evidence, state, writes, kube, observe, witness, witnesses = self.fixture()
                transport.select(config, stage, 'stage', observe, kube=kube)
                if change == 'capture':state['foreign'] = [{'metadata': {'uid': 'another-service'}, 'spec': {'externalIPs': ['8.8.8.8'], 'ports': [{'port': 24000}]}}]
                if change == 'no-witness':witness = None
                if change == 'foreign-owner':state['service']['metadata']['labels']['app.kubernetes.io/managed-by'] = 'another-manager'
                if change == 'drift':state['service']['spec']['selector'] = {'slot': 'unexpected'}
                with self.assertRaises(ValueError):transport.select(config, stage, 'activate', observe, witness, kube)
                self.assertEqual(len(writes), 1)


if __name__ == '__main__':
    unittest.main()
