import copy
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

from scripts import capture_dns_observation_policy as observation
from scripts import reconcile_dns_transport as transport
from scripts.test_recover_dns_transport import RecoveryTests


class ObservationPolicyTests(unittest.TestCase):
    def fixture(self):
        fixture = RecoveryTests()
        config = fixture.config()
        config.pop('recovery')
        config['apiVersion'] = 'transport.fugue.dev/v1'
        candidate = fixture.candidate()
        current = transport.service(config, config['listeners'][0])
        current['metadata'].update(uid='service', resourceVersion='1')
        declaration = {'schema': 'fugue.dns-public-observation/v2', 'servers': ['8.8.8.8'],
                       'queries': [{'hostname': 'app.example.test', 'type': 'A', 'answer_source': 'verified_serving_policy'}]}
        return config, candidate, current, declaration

    def test_capture_is_read_only_identity_bound_and_never_exports_keys(self):
        for scenario in ['valid', 'service-changed', 'pod-changed', 'verification-failed', 'mutable-image', 'wrong-node', 'missing-listener']:
            with self.subTest(scenario=scenario):
                config, candidate, current, declaration = self.fixture()
                if scenario == 'mutable-image':
                    candidate['pod']['spec']['containers'][0]['image'] = 'example/dns:latest'
                if scenario == 'missing-listener':
                    config['listeners'][0]['port'] = 15353
                def read(*args):
                    self.assertEqual(args[0], 'get', 'observation attempted a write')
                    if args[1] == 'nodes':
                        return {'items': [{'metadata': {'name': candidate['pod']['spec']['nodeName']},
                                           'status': {'addresses': [{'address': '1.1.1.1' if scenario == 'wrong-node' else '8.8.8.8'}]}}]}
                    if args[1] == 'service':
                        value = copy.deepcopy(current)
                        if read.services and scenario == 'service-changed':
                            value['metadata']['resourceVersion'] = '2'
                        read.services += 1
                        return value
                    if args[1] == 'pod':
                        value = copy.deepcopy(candidate['pod'])
                        if scenario == 'pod-changed': value['metadata']['uid'] = 'replacement'
                        return value
                    if args[1] == 'endpointslices': return {'items': []}
                    self.fail('unexpected observation command')
                read.services = 0
                def verify(args, **kwargs):
                    self.assertEqual(args, ['/approved/verifier', '--export-policy'])
                    request = json.loads(kwargs['input'])
                    self.assertEqual(request['observation_queries'], [{'hostname': 'app.example.test', 'type': 'A'}])
                    self.assertEqual(request['signing_key'], 'synthetic-private-material')
                    self.assertNotIn('synthetic-private-material', ' '.join(args))
                    return subprocess.CompletedProcess(args, int(scenario == 'verification-failed'),
                                                       json.dumps({'schema': 'fugue.dns-observation-policy/v1'}), 'synthetic-private-material')
                with patch.object(transport, 'read_json', side_effect=read), \
                        patch.object(transport, 'handoff_backend', return_value=candidate), \
                        patch.object(transport, 'validate_handoff_endpoints') as endpoints, \
                        patch.object(observation, 'candidate_request', return_value={'signing_key': 'synthetic-private-material'}), \
                        patch.object(observation.subprocess, 'run', side_effect=verify):
                    if scenario == 'valid':
                        result = observation.capture(declaration, json.dumps(declaration).encode(), config, '/approved/verifier')
                        self.assertEqual(result['servers'][0]['pod_uid'], candidate['pod']['metadata']['uid'])
                        self.assertNotIn('synthetic-private-material', json.dumps(result))
                        endpoints.assert_called_once()
                    else:
                        with self.assertRaises(ValueError) as raised:
                            observation.capture(declaration, b'declaration', config, '/approved/verifier')
                        self.assertNotIn('synthetic-private-material', str(raised.exception))

    def test_ci_captures_before_independent_public_observation(self):
        workflow = Path('.github/workflows/ci.yml').read_text()
        capture = workflow.split('\n  dns_observation_policy:', 1)[1].split('\n  dns_public_observation:', 1)[0]
        public = workflow.split('\n  dns_public_observation:', 1)[1].split('\n  dns_slot_retirement:', 1)[0]
        self.assertIn('scripts.capture_dns_observation_policy', capture)
        self.assertIn('needs: [dns_observation_policy]', public)
        self.assertIn('actions/download-artifact@', public)
        self.assertIn('dns-observation-input/dns-observation-expectations.json', public)
        self.assertNotIn('--apply', capture)
        declaration = json.loads(Path('deploy/environments/production/dns-observation/public.json').read_text())
        self.assertTrue(any(q.get('answer_source') == 'verified_serving_policy' for q in declaration['queries']))
        self.assertTrue(all(not q.get('allowed_answers') for q in declaration['queries'] if q.get('answer_source')))

    def test_verifier_failures_keep_only_allowlisted_categories(self):
        cases = [(message, category) for message, category in observation.POLICY_VERIFIER_REJECTIONS.items()]
        cases += [('synthetic-private-material', 'verifier_failed'),
                  ('DNS observation policy authority or freshness mismatch\nsynthetic-private-material', 'verifier_failed'),
                  ('DNS observation policy rejected: snapshot_plan\nsynthetic-private-material', 'verifier_failed'),
                  ('DNS observation policy rejected: synthetic-private-material', 'verifier_failed')]
        for message, category in cases:
            with self.subTest(category=category, message=message), patch.object(
                    observation.subprocess, 'run', return_value=subprocess.CompletedProcess(
                        ['/approved/verifier', '--export-policy'], 1, 'synthetic-private-material', message)):
                with self.assertRaisesRegex(ValueError, ': ' + category + '$') as raised:
                    observation.verified_policy({'signing_key': 'synthetic-private-material'}, '/approved/verifier')
                self.assertNotIn('synthetic-private-material', str(raised.exception))

    def test_verifier_output_and_timeout_fail_closed_without_private_details(self):
        cases = [('not-json synthetic-private-material', 'output_encoding'),
                 (json.dumps(['synthetic-private-material']), 'output_schema'),
                 (json.dumps({'schema': 'synthetic-private-material'}), 'output_schema'),
                 ('synthetic-private-material' * (256 << 10), 'output_size')]
        for output, category in cases:
            with self.subTest(category=category), patch.object(observation.subprocess, 'run',
                    return_value=subprocess.CompletedProcess(['/approved/verifier'], 0, output, 'synthetic-private-material')):
                with self.assertRaisesRegex(ValueError, ': ' + category + '$') as raised:
                    observation.verified_policy({}, '/approved/verifier')
                self.assertNotIn('synthetic-private-material', str(raised.exception))
        with patch.object(observation.subprocess, 'run', side_effect=subprocess.TimeoutExpired(
                ['/approved/verifier'], 30, output='synthetic-private-material', stderr='synthetic-private-material')):
            with self.assertRaisesRegex(ValueError, ': verifier_timeout$') as raised:
                observation.verified_policy({}, '/approved/verifier')
            self.assertNotIn('synthetic-private-material', str(raised.exception))


if __name__ == '__main__':
    unittest.main()
