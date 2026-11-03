import copy
import hashlib
import json
import unittest
from unittest.mock import patch

from scripts.compiler_input_storage import compatible_revision, validate_reader, free_bytes, owned_pods, command


class CompilerInputStorageTest(unittest.TestCase):
    @patch('scripts.compiler_input_storage.subprocess.run')
    def test_failed_command_reports_only_static_stage_and_executor_category(self, run):
        import subprocess
        run.return_value = subprocess.CompletedProcess([], 1, 'private body', 'postgres://private-secret\ncompiler input maintenance completion not confirmed; inspect state before retrying; category=23503\n')
        with self.assertRaisesRegex(ValueError, '^storage command failed stage=maintenance-executor category=23503$'):
            command(['kubectl', 'exec', 'pod', '--', '/usr/local/bin/fugue-input-storage'])
        run.return_value.stderr = 'unrecognized sensitive failure'
        with self.assertRaisesRegex(ValueError, '^storage command failed stage=source-compatibility$'):
            command(['git', 'show', 'revision:path'])

    @patch('scripts.compiler_input_storage.kube')
    def test_reader_population_uses_controller_uid_despite_shared_labels(self, kube):
        deployment = {'metadata': {'uid': 'deployment-id'}, 'spec': {'selector': {'matchLabels': {'app': 'shared'}}}}
        def owned(name, kind, uid, **extra):
            return {'metadata': {'name': name, 'uid': name+'-id', 'ownerReferences': [{'kind': kind, 'uid': uid, 'controller': True}], **extra}}
        sets = [owned('current', 'Deployment', 'deployment-id'), owned('old', 'Deployment', 'deployment-id'), owned('unrelated', 'Deployment', 'another-id')]
        current = owned('current-pod', 'ReplicaSet', 'current-id')
        terminating = owned('old-pod', 'ReplicaSet', 'old-id', deletionTimestamp='2026-01-01T00:00:00Z')
        unrelated = owned('unrelated-pod', 'ReplicaSet', 'unrelated-id')
        forged = owned('same-name-new-uid', 'ReplicaSet', 'current-other-id')
        kube.side_effect = [json.dumps({'items': sets}), json.dumps({'items': [unrelated, current, terminating, forged]})]
        self.assertEqual(owned_pods('system', deployment)['items'], [current, terminating])
        self.assertIn('replicasets', kube.call_args_list[0].args)
        with self.assertRaises(ValueError): owned_pods('system', {'metadata': {}, 'spec': deployment['spec']})

    def fixture(self):
        revision = 'a'*40
        image = 'registry.example.test/control@sha256:'+'b'*64
        template = {'metadata': {'annotations': {'fugue.pro/source-commit': revision}}, 'spec': {'containers': [{'name': 'control', 'image': image}]}}
        w = {'kind': 'Deployment', 'name': 'control', 'namespace': 'system', 'container': 'control'}
        component = {'id': 'control', 'workload': w}
        deployment = {'kind': 'Deployment', 'metadata': {'name': 'control', 'namespace': 'system', 'generation': 2}, 'spec': {'replicas': 1, 'template': template}, 'status': {'observedGeneration': 2, 'replicas': 1, 'updatedReplicas': 1, 'readyReplicas': 1, 'availableReplicas': 1}}
        pod = {'metadata': {'name': 'control-pod', 'annotations': copy.deepcopy(template['metadata']['annotations'])}, 'spec': copy.deepcopy(template['spec']), 'status': {'containerStatuses': [{'name': 'control', 'ready': True, 'image': 'sha256:'+'c'*64}], 'conditions': [{'type': 'Ready', 'status': 'True'}]}}
        envelope = {'component': 'control', 'configSha': revision}
        data = {}
        for filename, key in (('forward.json', 'forwardManifestDigest'), ('lkg.json', 'lkgManifestDigest')):
            value = copy.deepcopy(deployment)
            if filename == 'lkg.json':
                value['spec']['template']['metadata']['annotations']['fugue.pro/source-commit'] = 'd'*40
            data[filename] = json.dumps({'items': [value]}, separators=(',', ':'))
            envelope[key] = 'sha256:'+hashlib.sha256(data[filename].encode()).hexdigest()
        data['record.json'] = json.dumps(envelope)
        return component, deployment, {'items': [pod]}, {'immutable': True, 'data': data}

    @patch('scripts.compiler_input_storage.compatible_revision')
    def test_checks_current_and_rollback_with_containerd_image_names(self, compatible):
        value = validate_reader(*self.fixture())
        self.assertEqual(value['pod'], 'control-pod')
        self.assertEqual([x.args[0] for x in compatible.call_args_list], ['a'*40, 'a'*40, 'd'*40])

    @patch('scripts.compiler_input_storage.compatible_revision')
    def test_rejects_rollout_and_binding_changes(self, _compatible):
        for mutation in ('unready', 'old-pod', 'mutable-record', 'layout-hash', 'extra-pod', 'terminating'):
            with self.subTest(mutation=mutation):
                c, d, pods, record = self.fixture()
                if mutation == 'unready': d['status']['readyReplicas'] = 0
                if mutation == 'old-pod': pods['items'][0]['metadata']['annotations']['fugue.pro/source-commit'] = 'e'*40
                if mutation == 'mutable-record': record['immutable'] = False
                if mutation == 'layout-hash': record['data']['lkg.json'] += ' '
                if mutation == 'extra-pod': pods['items'].append(copy.deepcopy(pods['items'][0]))
                if mutation == 'terminating': pods['items'][0]['metadata']['deletionTimestamp'] = '2026-01-01T00:00:00Z'
                if mutation == 'layout-hash': record['data']['lkg.json'] = record['data']['lkg.json'].replace('d'*40, 'e'*40)
                with self.assertRaises(ValueError): validate_reader(c, d, pods, record)

    @patch('scripts.compiler_input_storage.command')
    def test_incompatible_reader_rejected(self, command):
        command.return_value = 'old format'
        with self.assertRaises(ValueError): compatible_revision('a'*40)
        command.side_effect = [FORMAT for FORMAT in ('runtime-snapshot-separated-v4', 'sql.LevelRepeatableRead')]
        compatible_revision('a'*40)
        with self.assertRaises(ValueError): compatible_revision('tag')

    @patch('scripts.compiler_input_storage.kube')
    @patch('scripts.compiler_input_storage.get')
    def test_primary_health_and_disk_bytes(self, get, kube):
        database = {'namespace': 'system', 'cluster': 'postgres', 'container': 'postgres', 'dataPath': '/data'}
        get.side_effect = [{'status': {'phase': 'Cluster in healthy state', 'currentPrimary': 'postgres-1'}}, {'status': {'conditions': [{'type': 'Ready', 'status': 'True'}]}}]
        kube.return_value = 'Filesystem 1024-blocks Used Available Capacity Mounted on\n/dev/volume 9000 1000 8000 12% /data\n'
        self.assertEqual(free_bytes(database), 8000*1024)
        get.side_effect = [{'status': {'phase': 'Not enough disk space'}}]
        with self.assertRaises(ValueError): free_bytes(database)


if __name__ == '__main__': unittest.main()
