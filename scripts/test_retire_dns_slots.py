import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import retire_dns_slots as retirement


class RetirementTests(unittest.TestCase):
    def fixture(self, kind='DaemonSet'):
        workload = {'apiVersion': 'apps/v1', 'kind': kind,
                    'metadata': {'name': 'obsolete-dns', 'uid': '11111111-1111-1111-1111-111111111111', 'resourceVersion': '12'},
                    'spec': {'selector': {'matchLabels': {'app': 'obsolete'}},
                             'template': {'metadata': {'labels': {'app': 'obsolete'}}, 'spec': {
                                 'containers': [{'name': 'dns', 'image': 'registry.example/dns@sha256:' + 'a' * 64,
                                                 'ports': [{'containerPort': 53, 'protocol': 'UDP'}, {'containerPort': 53, 'protocol': 'TCP'}]}],
                                 'volumes': [{'name': 'positive-lkg', 'hostPath': {'path': '/retained/checkpoint'}}]}}}}
        if kind == 'Deployment':
            workload['spec']['replicas'] = 1
        item = {'kind': kind, 'name': 'obsolete-dns', 'uid': workload['metadata']['uid'],
                'image': workload['spec']['template']['spec']['containers'][0]['image'],
                'before_spec_digest': retirement.digest(workload['spec']),
                'retired_spec_digest': retirement.digest(retirement.retired_spec(workload))}
        config = {'schema': 'fugue.dns-slot-retirement/v1', 'generation': 1, 'namespace': 'system',
                  'origin': 'https://api.example.test', 'api_address': '8.8.8.8', 'workloads': [item], 'quiescent_services': [], 'verifier_revision': 'a' * 40,
                  'replacements': [{'node': 'dns-node', 'service': 'public-dns', 'service_uid': '22222222-2222-2222-2222-222222222222',
                                    'active_uid': '33333333-3333-3333-3333-333333333333', 'active_image': 'registry.example/dns@sha256:' + 'b' * 64,
                                    'standby_selector': {'app': 'standby'}, 'standby_uid': '44444444-4444-4444-4444-444444444444',
                                    'standby_image': 'registry.example/dns@sha256:' + 'c' * 64}]}
        return config, workload, item

    def test_stop_is_fenced_replayable_and_preserves_images_and_lkg(self):
        for kind in ['DaemonSet', 'Deployment']:
            config, workload, item = self.fixture(kind)
            retirement.validate(config)
            before = copy.deepcopy(workload)
            operations = retirement.retirement_patch(workload, item, config)
            self.assertEqual(workload, before)
            self.assertEqual(operations[:3], [{'op': 'test', 'path': '/metadata/uid', 'value': item['uid']},
                                             {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '12'},
                                             {'op': 'test', 'path': '/spec', 'value': before['spec']}])
            workload['metadata']['annotations'] = operations[3]['value']
            workload['spec'] = operations[4]['value']
            self.assertEqual(workload['spec']['template']['spec']['containers'], before['spec']['template']['spec']['containers'])
            self.assertEqual(workload['spec']['template']['spec']['volumes'], before['spec']['template']['spec']['volumes'])
            self.assertEqual(retirement.retirement_patch(workload, item, config), [])
            if kind == 'Deployment':
                self.assertEqual(workload['spec']['replicas'], 0)
            else:
                terms = workload['spec']['template']['spec']['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
                self.assertEqual(len(terms), 1)
                expressions = terms[0]['matchExpressions']
                # Every possible node value is excluded, including future nodes.
                for value in [None, '', 'false', 'true', 'other']:
                    matches = [(value in e['values'] if e['operator'] == 'In' else value not in e['values']) for e in expressions]
                    self.assertFalse(all(matches))

    def test_changed_identity_image_spec_and_host_listeners_block_retirement(self):
        for mutate in [lambda w: w['metadata'].update(uid='foreign'),
                       lambda w: w['metadata'].update(deletionTimestamp='now'),
                       lambda w: w['spec']['template']['spec']['containers'][0].update(image='foreign'),
                       lambda w: w['spec']['template']['spec'].update(hostNetwork=True),
                       lambda w: w['spec']['template']['spec']['containers'][0]['ports'][0].update(hostPort=53),
                       lambda w: w['spec']['template']['spec'].update(nodeSelector={'changed': 'true'}),
                       lambda w: w['metadata'].update(annotations={retirement.RETIRED: 'different'})]:
            config, workload, item = self.fixture()
            mutate(workload)
            with self.assertRaises(ValueError):
                retirement.retirement_patch(workload, item, config)

    def test_any_service_selecting_old_slot_blocks_retirement(self):
        _, workload, _ = self.fixture()
        for service_type in ['ClusterIP', 'LoadBalancer', 'NodePort']:
            services = [{'metadata': {'name': 'still-in-use'}, 'spec': {'type': service_type, 'selector': {'app': 'obsolete'}}}]
            with self.assertRaisesRegex(ValueError, 'still selected'):
                retirement.ensure_unselected(workload, services)
        retirement.ensure_unselected(workload, [{'metadata': {'name': 'other'}, 'spec': {'selector': {'app': 'active'}}}])

    def test_changed_prewrite_workload_never_applies(self):
        config, workload, item = self.fixture()
        operations = retirement.retirement_patch(workload, item, config)
        changed = copy.deepcopy(operations)
        changed[1]['value'] = '13'
        with tempfile.TemporaryDirectory() as directory, patch.object(retirement, 'replacements', return_value=[]), \
                patch.object(retirement, 'inspect_workload', side_effect=[(workload, operations, []), (workload, changed, [])]), \
                patch.object(retirement.transport, 'kubectl') as kube:
            evidence = Path(directory) / 'retirement.json'
            with self.assertRaisesRegex(ValueError, 'changed before CAS'):
                retirement.reconcile(config, evidence, apply=True)
            self.assertEqual(kube.call_count, 1)
            self.assertIn('--dry-run=server', kube.call_args.args)
            self.assertFalse(json.loads(evidence.read_text())['workloads'][0]['applied'])
            self.assertEqual(evidence.stat().st_mode & 0o777, 0o600)

    def test_only_exact_internal_nonpublic_services_can_be_quiesced(self):
        _, workload, _ = self.fixture()
        service = {'metadata': {'name': 'validation', 'uid': 'uid'},
                   'spec': {'selector': {'app': 'obsolete'}, 'type': 'ClusterIP', 'ports': [{'port': 5353}]}}
        quiescent = [{'name': 'validation', 'uid': 'uid', 'spec_digest': retirement.digest(service['spec'])}]
        retirement.ensure_unselected(workload, [service], quiescent)
        for mutation in [lambda s: s['metadata'].update(uid='changed'),
                         lambda s: s['spec'].update(type='LoadBalancer'),
                         lambda s: s['spec'].update(externalIPs=['8.8.8.8']),
                         lambda s: s['spec']['ports'][0].update(port=53)]:
            changed = copy.deepcopy(service)
            mutation(changed)
            with self.assertRaises(ValueError):
                retirement.ensure_unselected(workload, [changed], quiescent)
        self.assertEqual(retirement.active_dns_connections('0: 00000000:14E9 00000000:1000 01 rest\n', {5353}), 1)

    def test_even_explicit_quiescence_never_allows_a_public_dns_service(self):
        _, workload, _ = self.fixture()
        for spec in [
            {'type': 'ClusterIP', 'ports': [{'port': 53}]},
            {'type': 'ClusterIP', 'externalIPs': ['8.8.8.8'], 'ports': [{'port': 5353}]},
            {'type': 'NodePort', 'ports': [{'port': 5353, 'nodePort': 30053}]},
        ]:
            spec['selector'] = {'app': 'obsolete'}
            service = {'metadata': {'name': 'listener', 'uid': 'uid'}, 'spec': spec}
            quiescent = [{'name': 'listener', 'uid': 'uid', 'spec_digest': retirement.digest(spec)}]
            with self.assertRaises(ValueError):
                retirement.ensure_unselected(workload, [service], quiescent)

    def test_no_write_without_both_verified_replacements(self):
        config, _, _ = self.fixture()
        with tempfile.TemporaryDirectory() as directory, patch.object(retirement, 'replacements', side_effect=ValueError('standby unavailable')), \
                patch.object(retirement, 'inspect_workload') as inspect, patch.object(retirement.transport, 'kubectl') as kube:
            with self.assertRaisesRegex(ValueError, 'standby unavailable'):
                retirement.reconcile(config, Path(directory) / 'retirement.json', apply=True)
            inspect.assert_not_called()
            kube.assert_not_called()


if __name__ == '__main__':
    unittest.main()
