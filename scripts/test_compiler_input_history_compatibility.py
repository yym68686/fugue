import copy
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from scripts import compiler_input_history_compatibility as h
from scripts import compiler_input_storage as storage


ROOT = Path(__file__).resolve().parent.parent


class HistoryCompatibilityTest(unittest.TestCase):
    def test_read_only_mode_cannot_be_replaced_by_an_operation(self):
        for extra in (['--operation', '/unread-operation.json'], ['--apply']):
            with self.subTest(extra=extra), patch('sys.argv', ['storage', '--mode', 'check-history-compatibility']+extra), \
                 patch.object(storage.Path, 'read_text') as read, patch.object(storage, 'readers') as readers, \
                 patch.object(h, 'check_history_compatibility') as check, \
                 self.assertRaisesRegex(ValueError, 'read-only'):
                try:
                    storage.main()
                finally:
                    read.assert_not_called(); readers.assert_not_called(); check.assert_not_called()

    def fixture(self):
        revision, rollback = 'a'*40, 'd'*40
        image = 'registry.example.test/control@sha256:'+'b'*64
        template = {'metadata': {'annotations': {'fugue.pro/source-commit': revision}},
                    'spec': {'containers': [{'name': 'control', 'image': image}]}}
        component = {'id': 'control', 'sourceRoots': ['internal/store'],
                     'artifact': {'buildPackage': './cmd/fugue-api'},
                     'workload': {'kind': 'Deployment', 'name': 'control', 'namespace': 'system', 'container': 'control'}}
        deployment = {'kind': 'Deployment', 'metadata': {'name': 'control', 'uid': 'deployment-id', 'namespace': 'system', 'generation': 2},
                      'spec': {'replicas': 1, 'template': template, 'selector': {'matchLabels': {'app': 'shared'}}},
                      'status': {'observedGeneration': 2, 'replicas': 1, 'updatedReplicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}}
        pod = {'metadata': {'name': 'control-pod', 'uid': 'pod-id', 'annotations': copy.deepcopy(template['metadata']['annotations']),
                            'ownerReferences': [{'kind': 'ReplicaSet', 'name': 'control-rs', 'uid': 'rs-id', 'controller': True}]},
               'spec': copy.deepcopy(template['spec']),
               'status': {'containerStatuses': [{'name': 'control', 'ready': True, 'imageID': 'containerd://sha256:'+'b'*64,
                                                 'containerID': 'containerd://original', 'restartCount': 0}],
                          'conditions': [{'type': 'Ready', 'status': 'True'}]}}
        rs = {'metadata': {'name': 'control-rs', 'uid': 'rs-id',
                           'ownerReferences': [{'kind': 'Deployment', 'uid': 'deployment-id', 'controller': True}]}}
        envelope = {'component': 'control', 'configSha': revision}
        data = {}
        for filename, key in (('forward.json', 'forwardManifestDigest'), ('lkg.json', 'lkgManifestDigest')):
            value = copy.deepcopy(deployment)
            if filename == 'lkg.json':
                value['spec']['template']['metadata']['annotations']['fugue.pro/source-commit'] = rollback
                value['spec']['template']['spec']['containers'][0]['image'] = 'registry.example.test/control@sha256:'+'c'*64
            data[filename] = json.dumps({'items': [value]}, separators=(',', ':'))
            envelope[key] = 'sha256:'+hashlib.sha256(data[filename].encode()).hexdigest()
        data['record.json'] = json.dumps(envelope)
        record = {'metadata': {'uid': 'record-id'}, 'immutable': True, 'data': data}
        pointer = {'metadata': {'uid': 'monitor-id'}, 'data': {'recordName': 'fugue-release-record-control-'+'a'*16,
                   'state.json': json.dumps({'consecutiveFailures': 0, 'rollbackStatus': ''})}}
        return {'registry': {'components': [component]}, 'deployment': deployment, 'pod': pod, 'rs': rs,
                'record': record, 'pointer': pointer, 'after': False, 'commands': []}

    def execute(self, f, mutation=None):
        def get(namespace, kind, name):
            if f['after'] and mutation:
                f['after'] = False
                mutation(f)
            value = {'deployment': f['deployment'], 'replicaset': f['rs']}.get(kind)
            if kind == 'configmap': value = f['pointer'] if name == 'fugue-release-monitor-control' else f['record']
            return copy.deepcopy(value)
        def kube(namespace, *args):
            self.assertEqual(args[:2], ('get', 'pods'))
            self.assertIn('-l', args)
            return json.dumps({'items': copy.deepcopy(f.get('pods', [f['pod']])),
                               'metadata': {'continue': f.get('continue', '')}})
        def command(args, **kw):
            f['commands'].append((args, kw))
            if args[:2] == ['git', 'show']:
                path = args[2].split(':', 1)[1]
                if f.get('missing_source') and args[2].startswith('d'*40+':') and path == h.CONTRACT_PATH:
                    raise ValueError('source-compatibility unavailable')
                return (ROOT/path).read_text()
            self.assertEqual(args[-2:], ['/proc/1/exe', h.ARGUMENT])
            self.assertEqual(kw, {'timeout': 5})
            report = h.source_contract('a'*40, 'store', './cmd/fugue-api', {})
            if f.get('runtime_change'): f['runtime_change'](report)
            f['after'] = True
            return json.dumps(report)
        with patch.object(h, 'get', side_effect=get), patch.object(h, 'kube', side_effect=kube), \
             patch.object(h, 'command', side_effect=command), \
             patch('scripts.compiler_input_storage.command', side_effect=command):
            return h.check_history_compatibility(f['registry'])

    def test_exact_live_binary_current_and_rollback_is_necessary_not_sufficient(self):
        f = self.fixture()
        result = self.execute(f)
        self.assertFalse(result['retirement_ready'])
        self.assertIn('controlled-enforcement-transition', result['remaining_proofs'])
        value = result['registered_releases'][0]
        self.assertEqual(value['rollback_revision'], 'd'*40)
        self.assertEqual(value['pods'][0]['uid'], 'pod-id')
        self.assertNotEqual(value['image'], value['rollback_image'])
        for command, _ in f['commands']:
            self.assertNotIn('--apply', command)
            self.assertNotIn('env', command)
            self.assertNotIn('secrets', command)
        runtime = [x for x in f['commands'] if x[0][0] == 'kubectl']
        self.assertEqual(len(runtime), 1)

    def test_old_or_unreported_running_binary_refuses(self):
        for mutate in (lambda r: r.pop('contract_sha256'), lambda r: r.update(role='schema'),
                       lambda r: r['capabilities'].pop(), lambda r: r.update(contract_sha256='0'*64),
                       lambda r: r.update(operatorReady=True)):
            f = self.fixture(); f['runtime_change'] = mutate
            with self.subTest(mutate=mutate), self.assertRaisesRegex(ValueError, 'live executable'):
                self.execute(f)

    def test_release_documents_have_a_separate_bound_from_binary_capabilities(self):
        f = self.fixture()
        envelope = json.loads(f['record']['data']['record.json'])
        for filename, key in (('forward.json', 'forwardManifestDigest'), ('lkg.json', 'lkgManifestDigest')):
            manifest = json.loads(f['record']['data'][filename])
            manifest['items'][0]['spec']['template']['metadata']['annotations']['neutral-metadata'] = 'x' * 32768
            raw = json.dumps(manifest)
            f['record']['data'][filename] = raw
            envelope[key] = 'sha256:' + hashlib.sha256(raw.encode()).hexdigest()
        f['record']['data']['record.json'] = json.dumps(envelope)
        self.assertFalse(self.execute(f)['retirement_ready'])
        # The larger release-document allowance cannot enlarge the executable
        # protocol, hide duplicates or bypass the release-document ceiling.
        raw = json.dumps({'neutral': 'x' * 32768})
        with self.assertRaisesRegex(ValueError, 'capability evidence exceeds bound'):
            h.strict_json(raw)
        self.assertEqual(h.release_json(raw)['neutral'], 'x' * 32768)
        for bad in ('{"id":1,"id":2}', '{broken', '"' + 'x' * (2 << 20) + '"'):
            with self.subTest(prefix=bad[:24]), self.assertRaises(ValueError):
                h.release_json(bad)

    def test_old_promised_rollback_is_not_certified_by_new_live_binary(self):
        f = self.fixture(); f['missing_source'] = True
        with self.assertRaisesRegex(ValueError, 'source-compatibility'):
            self.execute(f)
        self.assertFalse(any(x[0][0] == 'kubectl' for x in f['commands']))

    def test_unknown_monitor_health_and_incomplete_pod_list_refuse(self):
        for state in ({}, {'consecutiveFailures': False, 'rollbackStatus': ''},
                      {'consecutiveFailures': 0, 'rollbackStatus': None}, {'consecutiveFailures': 1, 'rollbackStatus': ''}):
            f = self.fixture(); f['pointer']['data']['state.json'] = json.dumps(state)
            with self.subTest(state=state), self.assertRaisesRegex(ValueError, 'failure or rollback'):
                self.execute(f)
        # The existing monitor omits an empty rollbackStatus in valid JSON.
        f = self.fixture(); f['pointer']['data']['state.json'] = '{"consecutiveFailures":0}'
        self.assertFalse(self.execute(f)['retirement_ready'])
        f = self.fixture(); f['continue'] = 'unobserved-next-page'
        with self.assertRaisesRegex(ValueError, 'population exceeds bound'): self.execute(f)

    def test_container_restart_pod_aba_and_release_rollback_changes_refuse(self):
        mutations = [
            lambda f: f['pod']['metadata'].update(uid='replacement'),
            lambda f: f['deployment']['metadata'].update(uid='new-owner'),
            lambda f: f['deployment']['metadata'].update(generation=3),
            lambda f: f['pod']['status']['containerStatuses'][0].update(restartCount=1),
            lambda f: f['pod']['status']['containerStatuses'][0].update(containerID='containerd://new'),
            lambda f: f['record']['metadata'].update(uid='new-record'),
            lambda f: f['pointer']['metadata'].update(uid='new-pointer'),
            lambda f: f['pointer']['data'].update(**{'state.json': '{"rollbackStatus":"running"}'}),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate), self.assertRaises((ValueError, StopIteration)):
                self.execute(self.fixture(), mutate)

    def test_unready_terminating_forged_image_and_unknown_status_refuse(self):
        for key in ('unready', 'terminating', 'image', 'container', 'restart', 'replicas'):
            f = self.fixture()
            if key == 'unready': f['pod']['status']['containerStatuses'][0]['ready'] = False
            if key == 'terminating': f['pod']['metadata']['deletionTimestamp'] = '2026-01-01T00:00:00Z'
            if key == 'image': f['pod']['status']['containerStatuses'][0]['imageID'] = 'sha256:'+'f'*64
            if key == 'container': del f['pod']['status']['containerStatuses'][0]['containerID']
            if key == 'restart': f['pod']['status']['containerStatuses'][0]['restartCount'] = True
            if key == 'replicas': f['deployment']['spec']['replicas'] = True
            with self.subTest(key=key), self.assertRaises(ValueError): self.execute(f)

    def test_manifest_image_tamper_and_mutable_record_refuse(self):
        for key in ('forward', 'rollback', 'mutable'):
            f = self.fixture()
            if key == 'mutable': f['record']['immutable'] = False
            else:
                filename = 'forward.json' if key == 'forward' else 'lkg.json'
                manifest = json.loads(f['record']['data'][filename])
                manifest['items'][0]['spec']['template']['spec']['containers'][0]['image'] = ('registry.example.test/control@sha256:'+'e'*64) if key == 'forward' else 'registry.example.test/control:mutable'
                raw = json.dumps(manifest)
                f['record']['data'][filename] = raw
                envelope = json.loads(f['record']['data']['record.json'])
                envelope['forwardManifestDigest' if key == 'forward' else 'lkgManifestDigest'] = 'sha256:'+hashlib.sha256(raw.encode()).hexdigest()
                f['record']['data']['record.json'] = json.dumps(envelope)
            with self.subTest(key=key), self.assertRaises(ValueError): self.execute(f)

    def test_shared_label_janitor_is_not_a_store_owner_or_runtime_target(self):
        f = self.fixture(); janitor = copy.deepcopy(f['pod'])
        janitor['metadata'].update(name='separate-janitor', uid='janitor-id', ownerReferences=[{'kind': 'Job', 'name': 'janitor', 'uid': 'job-id', 'controller': True}])
        f['pods'] = [janitor, f['pod']]
        self.assertEqual(len(self.execute(f)['registered_releases'][0]['pods']), 1)
        self.assertFalse(any('separate-janitor' in x[0] for x in f['commands']))

    def test_rs_replacement_and_population_bounds_refuse(self):
        f = self.fixture(); f['rs']['metadata']['uid'] = 'new-rs'
        with self.assertRaisesRegex(ValueError, 'ReplicaSet was replaced'): self.execute(f)
        f = self.fixture(); f['pods'] = [f['pod']]*257
        with self.assertRaisesRegex(ValueError, 'population exceeds'): self.execute(f)

    def test_contract_is_strict_and_bound_to_executable_role(self):
        raw = (ROOT/h.CONTRACT_PATH).read_text()
        for change in (lambda v: v.update(version=True), lambda v: v.update(operatorReady=True),
                       lambda v: v['roles']['store'].pop(), lambda v: v['roles']['store'].append(v['roles']['store'][0])):
            value = json.loads(raw); change(value)
            with patch.object(h, 'command', return_value=json.dumps(value)), self.assertRaises(ValueError):
                h.source_contract('a'*40, 'store', './cmd/fugue-api', {})
        with patch.object(h, 'command', side_effect=[raw, 'package main']), self.assertRaisesRegex(ValueError, 'compiled role'):
            h.source_contract('a'*40, 'store', './cmd/fugue-api', {})
        for raw in ('{"schema":1,"schema":2}', 'x'*16385, '{broken'):
            with self.subTest(raw=raw[:24]), self.assertRaises(ValueError): h.strict_json(raw)
        for revision, package in [('main', './cmd/fugue-api'), ('a'*40, './cmd/../../elsewhere')]:
            with self.assertRaises(ValueError): h.source_contract(revision, 'store', package, {})

    def test_unknown_extra_targets_and_duplicate_workloads_do_not_disappear(self):
        for change in ('empty', 'kind', 'target', 'duplicate'):
            f = self.fixture(); c = f['registry']['components'][0]
            if change == 'empty': f['registry']['components'] = []
            if change == 'kind': c['workload']['kind'] = 'DaemonSet'
            if change == 'target': c['artifactTargets'] = [{'kind': 'Deployment'}]
            if change == 'duplicate':
                extra = copy.deepcopy(c); extra['id'] = 'another'; f['registry']['components'].append(extra)
            with self.subTest(change=change), self.assertRaises(ValueError): self.execute(f)


if __name__ == '__main__': unittest.main()
