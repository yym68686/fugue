import base64
import copy
import json
import os
import subprocess
import unittest
from unittest.mock import patch

from scripts import recover_dns_transport as recovery
from scripts import reconcile_dns_transport as transport
from scripts import test_reconcile_dns_transport as fixtures


class RecoveryTests(unittest.TestCase):
    def config(self):
        config = fixtures.TransportTests().config()
        config['apiVersion'] = 'transport.fugue.dev/v2'
        config['listeners'][0]['port'] = 53
        config['recovery'] = {'listener': 'listener', 'node_uid': '11111111-1111-1111-1111-111111111111',
                              'service_uid': '22222222-2222-2222-2222-222222222222',
                              'old_pod_uid': '33333333-3333-3333-3333-333333333333',
                              'candidate_pod_uid': '44444444-4444-4444-4444-444444444444',
                              'candidate_image': 'registry.example/dns@sha256:' + 'a' * 64,
                              'origin': 'https://api.example.test', 'api_address': '8.8.8.8'}
        return config

    def candidate(self):
        config = self.config()
        pod = fixtures.TransportTests().backend()
        pod['metadata']['uid'] = config['recovery']['candidate_pod_uid']
        container = pod['spec']['containers'][0]
        container['image'] = config['recovery']['candidate_image']
        container['env'] = [{'name': 'FUGUE_DNS_CACHE_PATH', 'value': '/state/cache'},
                            {'name': 'FUGUE_EDGE_GROUP_ID', 'value': 'group'},
                            {'name': 'FUGUE_BUNDLE_SIGNING_KEY_ID', 'value': 'key'},
                            {'name': 'FUGUE_BUNDLE_SIGNING_KEY', 'valueFrom': {'secretKeyRef': {'name': 'signing', 'key': 'key'}}}]
        return {'pod': pod, 'snapshot': fixtures.TransportTests().snapshot(), 'snapshot_path': '/api/v1/namespaces/system/pods/candidate:8080/proxy/runtime-facts'}

    def publication(self, snapshot):
        assignment = snapshot['assignment']
        return {'artifact': {'artifact_kind': 'release_set', 'scope_key': 'global', 'status': 'validated',
                             'id': assignment['release_set_id'], 'content_hash': snapshot['parent_digest'], 'generation': 'generation'},
                'release': {'artifact_kind': 'release_set', 'scope_key': 'global', 'status': 'active', 'release_channel': 'full',
                            'artifact_id': assignment['release_set_id'], 'id': assignment['artifact_release_id'],
                            'fencing_token': assignment['fencing_token'], 'verification_state': 'verified', 'verified_lkg_generation': 'generation'}}

    def test_recovery_requires_explicit_distinct_instances_and_https(self):
        config = self.config()
        fixtures.TransportTests().load(config)
        for mutate in [lambda value: value['recovery'].update(candidate_pod_uid=value['recovery']['old_pod_uid']),
                       lambda value: value['recovery'].update(candidate_image='registry.example/dns:latest'),
                       lambda value: value['recovery'].update(origin='http://api.example.test'),
                       lambda value: value['listeners'][0].update(port=15353),
                       lambda value: value['recovery'].update(skip_verification=True)]:
            altered = copy.deepcopy(config)
            mutate(altered)
            with self.assertRaises(ValueError):
                fixtures.TransportTests().load(altered)

    def test_recovery_rejects_noncurrent_unverified_or_foreign_authority(self):
        snapshot = self.candidate()['snapshot']
        publication = self.publication(snapshot)
        recovery.validate_publication(snapshot, publication)
        for mutate in [lambda value: value['release'].update(fencing_token=999),
                       lambda value: value['artifact'].update(content_hash='sha256:' + 'f' * 64),
                       lambda value: value['release'].update(verification_state='pending'),
                       lambda value: value['release'].update(verified_lkg_generation='older'),
                       lambda value: value['release'].update(status='revoked'),
                       lambda value: value['release'].update(scope_key='foreign')]:
            altered = copy.deepcopy(publication)
            mutate(altered)
            with self.assertRaises(ValueError):
                recovery.validate_publication(snapshot, altered)

    def test_verification_uses_secret_stdin_and_refuses_verifier_failure(self):
        candidate, config = self.candidate(), self.config()
        def read(*arguments):
            if arguments[:2] == ('get', 'secret'):
                return {'data': {'key': base64.b64encode(b'synthetic-verification-material').decode()}}
            if arguments[:2] == ('get', '--raw'):
                self.assertEqual(arguments[2], candidate['snapshot_path'])
                return candidate['snapshot']
            self.assertEqual(arguments[-2:], ('cat', '/state/cache.platform-serving.json'))
            return {'schema': 'synthetic-checkpoint'}
        def verify(arguments, **options):
            self.assertEqual(arguments, ['/approved/verify'])
            request = json.loads(options['input'])
            self.assertEqual(request['signing_key'], 'synthetic-verification-material')
            self.assertEqual(request['snapshot'], candidate['snapshot'])
            self.assertTrue(options['capture_output'])
            return subprocess.CompletedProcess(arguments, 0, '{"verified":true}\n', '')
        with patch.object(transport, 'read_json', side_effect=read), patch.dict(os.environ, FUGUE_DNS_OBSERVATION_VERIFIER='/approved/verify'):
            with patch.object(recovery.subprocess, 'run', side_effect=verify):
                recovery.verify_candidate(candidate, config)
            with patch.object(recovery.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', 'synthetic-verification-material')):
                with self.assertRaisesRegex(ValueError, 'failed cryptographic') as failure:
                    recovery.verify_candidate(candidate, config)
                self.assertNotIn('synthetic-verification-material', str(failure.exception))

    def test_recovery_rechecks_pod_identity_and_never_bypasses_healthy_handoff(self):
        for scenario in ['valid', 'healthy', 'replacement']:
            candidate, config = self.candidate(), self.config()
            old = copy.deepcopy(candidate['pod'])
            old['metadata'].update(name='old', uid=config['recovery']['old_pod_uid'])
            old['status']['conditions'][0]['status'] = 'True' if scenario == 'healthy' else 'False'
            node = {'metadata': {'name': 'node', 'uid': config['recovery']['node_uid']}}
            current = {'metadata': {'uid': config['recovery']['service_uid']}, 'spec': {'selector': {'app': 'old'}}}
            def read(*arguments):
                if arguments[:2] == ('get', 'pods'):
                    return {'items': [old]}
                result = copy.deepcopy(old if arguments[2] == 'old' else candidate['pod'])
                if scenario == 'replacement':
                    result['metadata']['resourceVersion'] = 'changed'
                return result
            with patch.object(transport, 'read_json', side_effect=read), patch.object(transport, 'handoff_backend', return_value=candidate), \
                    patch.object(recovery, 'verify_candidate') as verify, patch.object(recovery, 'current_publication', return_value=self.publication(candidate['snapshot'])):
                if scenario == 'valid':
                    observed = recovery.witness(config, config['listeners'][0], current, node)
                    self.assertTrue(observed['recovery'])
                    verify.assert_called_once()
                else:
                    with self.assertRaises(ValueError):
                        recovery.witness(config, config['listeners'][0], current, node)
                    if scenario == 'healthy':
                        verify.assert_not_called()

    def test_unchanged_unready_listener_does_not_block_another_configuration(self):
        config = fixtures.TransportTests().config()
        old = transport.service(config, config['listeners'][0])
        old['metadata'].update(uid='existing', resourceVersion='3')
        config['generation'] = 2
        calls = []
        def kube(*arguments, body=None):
            calls.append(arguments)
            if arguments[:2] == ('get', 'services'):
                return json.dumps({'items': [old]})
            if arguments[:2] == ('get', 'service'):
                return json.dumps(old)
            self.fail('unchanged listener triggered health checks or writes')
        with patch.object(transport, 'kubectl', side_effect=kube):
            self.assertEqual(transport.reconcile(config, apply=True), 0)
        self.assertEqual(len(calls), 2)
