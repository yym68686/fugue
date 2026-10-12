import copy
import json
from pathlib import Path
import tempfile
import unittest

from scripts import reconcile_fixed_node_retention as retention


class RetentionTests(unittest.TestCase):
    def fixture(self):
        selector = {'app.kubernetes.io/component': 'fixed-front', 'app.kubernetes.io/managed-by': 'front-owner'}
        config = {'schema': 'fugue.fixed-node-retention/v1', 'generation': 1, 'expected_previous_digest': '',
                  'namespace': 'system', 'state_configmap': 'retention', 'owner_manager': 'front-owner', 'pod_selector': selector,
                  'nodes': [{'id': 'node-a', 'uid': '11111111-1111-1111-1111-111111111111', 'reason': 'quota_exhausted', 'expected_recovery_date': '2020-01-01'}]}
        pod = {'metadata': {'name': 'pending', 'uid': 'pending-uid', 'resourceVersion': '10', 'labels': selector,
                            'ownerReferences': [{'kind': 'ReplicaSet', 'name': 'replica', 'uid': 'replica-uid', 'controller': True}]},
               'spec': {'nodeName': 'node-a', 'tolerations': [{'key': key, 'operator': 'Exists', 'effect': 'NoExecute', 'tolerationSeconds': 300} for key in sorted(retention.TAINTS)]},
               'status': {'phase': 'Pending'}}
        state = {'pod': pod, 'ready': False, 'configmap': None}
        writes = []
        def kube(arguments, value=None):
            if arguments[0] == 'get':
                kind = arguments[1]
                if kind == 'configmap':
                    return '' if state['configmap'] is None else json.dumps(state['configmap'])
                if kind == 'node':
                    return json.dumps({'metadata': {'uid': config['nodes'][0]['uid']}, 'status': {'conditions': [{'type': 'Ready', 'status': 'True' if state['ready'] else 'Unknown'}]}})
                if kind == 'pods':
                    terminating = copy.deepcopy(pod)
                    terminating['metadata'].update(name='old', uid='old', deletionTimestamp='yesterday')
                    return json.dumps({'metadata': {}, 'items': [state['pod'], terminating]})
                if kind == 'replicaset':
                    return json.dumps({'metadata': {'uid': 'replica-uid', 'labels': selector}, 'spec': {'template': {'spec': {'nodeName': 'node-a'}}}})
                self.fail(arguments)
            if '--dry-run=server' in arguments:
                return '{}'
            writes.append((arguments, value))
            if arguments[0] == 'patch':
                state['pod']['spec']['tolerations'] = copy.deepcopy(value[-1]['value'])
            else:
                state['configmap'] = copy.deepcopy(value)
                state['configmap']['metadata'].update(uid='configuration-uid', resourceVersion='1')
            return '{}'
        return config, state, writes, kube

    def test_retention_is_idempotent_and_preserves_terminating_materials(self):
        config, state, writes, kube = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'evidence.json'
            planned = retention.reconcile(config, path, kube=kube)
            self.assertEqual(writes, [])
            self.assertEqual(len(planned['patches']), 1)
            applied = retention.reconcile(config, path, apply=True, kube=kube)
            self.assertEqual(applied['applied'], ['pending-uid'])
            self.assertEqual(applied['observations'][0]['terminating_materials_retained'], 1)
            self.assertFalse(applied['observations'][0]['automatic_readmission'])
            self.assertEqual([entry[0][0] for entry in writes], ['patch', 'create'])
            self.assertFalse(any('tolerationSeconds' in item for item in state['pod']['spec']['tolerations']))
            writes.clear()
            repeated = retention.reconcile(config, path, apply=True, kube=kube)
            self.assertEqual(repeated['applied'], [])
            self.assertEqual([entry[0][0] for entry in writes], ['replace'])

    def test_ready_node_or_started_container_is_not_mutated(self):
        for condition in ['node', 'container', 'phase', 'owner']:
            config, state, writes, kube = self.fixture()
            if condition == 'node':
                state['ready'] = True
            elif condition == 'container':
                state['pod']['status']['containerStatuses'] = [{'name': 'front', 'state': {'running': {}}}]
            elif condition == 'phase':
                state['pod']['status']['phase'] = 'Running'
            else:
                state['pod']['metadata']['ownerReferences'][0]['uid'] = 'changed'
            with tempfile.TemporaryDirectory() as directory, self.assertRaises(ValueError):
                retention.reconcile(config, Path(directory) / 'evidence', apply=True, kube=kube)
            self.assertEqual(writes, [])

    def test_ambiguous_tolerations_and_substituted_intent_are_rejected(self):
        config, state, writes, kube = self.fixture()
        state['pod']['spec']['tolerations'].append(copy.deepcopy(state['pod']['spec']['tolerations'][0]))
        with self.assertRaises(ValueError):
            retention.pod_patch(state['pod'], 'node-a', config['pod_selector'])
        config, state, writes, kube = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'evidence'
            retention.reconcile(config, path, apply=True, kube=kube)
            writes.clear()
            config['nodes'][0]['reason'] = 'maintenance'
            with self.assertRaises(ValueError):
                retention.reconcile(config, path, apply=True, kube=kube)
            self.assertEqual(writes, [])
