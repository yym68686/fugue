import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import retire_dns_hostport as retirement


class RetirementTests(unittest.TestCase):
    def config(self):
        return {'schema': 'fugue.dns-hostport-retirement/v1', 'generation': 1, 'namespace': 'system', 'node': 'node-a',
                'node_uid': '11111111-1111-1111-1111-111111111111', 'workload': 'old-dns',
                'workload_uid': '22222222-2222-2222-2222-222222222222', 'image': 'registry.example/dns@sha256:' + 'a' * 64,
                'service': 'public-dns', 'service_uid': '33333333-3333-3333-3333-333333333333',
                'candidate_uid': '44444444-4444-4444-4444-444444444444', 'candidate_image': 'registry.example/dns@sha256:' + 'b' * 64,
                'origin': 'https://api.example.test', 'api_address': '8.8.8.8'}

    def workload(self):
        config = self.config()
        return {'metadata': {'uid': config['workload_uid'], 'resourceVersion': '10', 'annotations': {'unrelated': 'keep'}},
                'spec': {'updateStrategy': {'type': 'RollingUpdate'}, 'template': {'spec': {
                    'volumes': [{'name': 'cache', 'hostPath': {'path': '/state/retained'}}],
                    'containers': [{'name': 'dns', 'image': config['image'], 'ports': [
                        {'containerPort': 7834, 'protocol': 'TCP'},
                        {'containerPort': 53, 'hostPort': 53, 'hostIP': '8.8.8.8', 'protocol': 'UDP'},
                        {'containerPort': 53, 'hostPort': 53, 'hostIP': '8.8.8.8', 'protocol': 'TCP'}]}]}}}}

    def test_retirement_only_releases_dns_ports_with_exact_workload_cas(self):
        config, workload = self.config(), self.workload()
        retirement.validate(config)
        before = copy.deepcopy(workload)
        operations = retirement.port_patch(workload, config)
        self.assertEqual(workload, before)
        self.assertEqual(operations[:3], [{'op': 'test', 'path': '/metadata/uid', 'value': config['workload_uid']},
                                          {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '10'},
                                          {'op': 'test', 'path': '/spec', 'value': workload['spec']}])
        self.assertEqual({entry['path'] for entry in operations[3:]}, {'/metadata/annotations', '/spec/template/spec/containers/0/ports'})
        self.assertEqual(operations[3]['value']['unrelated'], 'keep')
        self.assertEqual(operations[4]['value'], [{'containerPort': 7834, 'protocol': 'TCP'}, {'containerPort': 53, 'protocol': 'UDP'}, {'containerPort': 53, 'protocol': 'TCP'}])

    def test_foreign_or_ambiguous_workloads_cannot_be_retired(self):
        for mutation in [lambda value: value['metadata'].update(uid='other'),
                         lambda value: value['metadata'].update(deletionTimestamp='now'),
                         lambda value: value['spec']['template']['spec'].update(hostNetwork=True),
                         lambda value: value['spec']['template']['spec']['containers'][0].update(image='other'),
                         lambda value: value['spec']['template']['spec']['containers'][0]['ports'].append({'hostPort': 443, 'protocol': 'TCP'}),
                         lambda value: value['metadata']['annotations'].update({retirement.RETIRED: '2'})]:
            workload = self.workload()
            mutation(workload)
            with self.assertRaises(ValueError):
                retirement.port_patch(workload, self.config())

    def test_active_dns_connections_block_retirement(self):
        header = 'sl local_address rem_address st\n'
        listening = '0: 00000000:0035 00000000:0000 0A 00000000\n'
        established = '1: 00000000:0035 00000000:1000 01 00000000\n'
        closing = '2: 00000000:0035 00000000:1000 08 00000000\n'
        unrelated = '3: 00000000:1E9A 00000000:1000 01 00000000\n'
        self.assertEqual(retirement.active_dns_connections(header + listening + unrelated), 0)
        self.assertEqual(retirement.active_dns_connections(header + listening + established + closing), 2)
        with self.assertRaises(ValueError):
            retirement.active_dns_connections('malformed')

    def test_changed_prewrite_witness_prevents_any_mutation(self):
        config, workload = self.config(), self.workload()
        operations = retirement.port_patch(workload, config)
        witness = {'old_pod_uid': 'old', 'service_resource_version': '10'}
        changed = dict(witness, service_resource_version='11')
        with tempfile.TemporaryDirectory() as directory, patch.object(retirement, 'inspect', side_effect=[(workload, operations, witness), (workload, operations, changed)]), patch.object(retirement.transport, 'kubectl') as kube:
            evidence = Path(directory) / 'evidence.json'
            with self.assertRaisesRegex(ValueError, 'changed before CAS'):
                retirement.reconcile(config, evidence, apply=True)
            self.assertEqual(kube.call_count, 1)
            self.assertIn('--dry-run=server', kube.call_args.args)
            self.assertFalse(json.loads(evidence.read_text())['applied'])
            self.assertEqual(evidence.stat().st_mode & 0o777, 0o600)
