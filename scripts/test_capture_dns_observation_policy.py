import copy
import datetime
import io
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

    def coherent_fixture(self):
        config, candidate, current, declaration = self.fixture()
        queries = declaration['queries']
        request = {'signing_key': 'synthetic-private-material',
                   'checkpoint': {'applied_at': '2026-01-01T00:00:00Z', 'private': 'synthetic-private-material'},
                   'snapshot': {'checkpoint_valid_until': '2026-01-02T00:01:00Z',
                                'evaluated_at': '2026-01-01T00:01:01Z', 'ready': True}}
        assignment = {'artifact_id': 'dns-bundle', 'artifact_release_id': 'release', 'fencing_token': 7}
        request['checkpoint']['candidate'] = {'assignment': assignment}
        request['snapshot']['assignment'] = copy.deepcopy(assignment)
        return config, candidate, current, queries, request

    def test_same_assignment_refresh_discards_mixed_pairs_and_fully_reverifies(self):
        config, candidate, current, queries, first = self.coherent_fixture()
        last = copy.deepcopy(first)
        last['checkpoint']['applied_at'] = '2026-01-01T00:01:00Z'
        last['signing_key'] = 'synthetic-current-key'
        requests = [copy.deepcopy(first), copy.deepcopy(first), last]
        seen = []
        def verify(args, **kwargs):
            self.assertEqual(args, ['/approved/verifier', '--export-policy'])
            request = json.loads(kwargs['input'])
            seen.append(request)
            self.assertEqual(request['observation_queries'], queries)
            self.assertEqual(request['snapshot']['assignment'], request['checkpoint']['candidate']['assignment'])
            # Model the unchanged strict gate while disk lags the same
            # assignment's Ready in-memory snapshot for two observations.
            start = datetime.datetime.fromisoformat(request['checkpoint']['applied_at'].replace('Z', '+00:00'))
            until = datetime.datetime.fromisoformat(request['snapshot']['checkpoint_valid_until'].replace('Z', '+00:00'))
            mixed = start + datetime.timedelta(days=1) != until
            return subprocess.CompletedProcess(args, int(mixed),
                                               json.dumps({'schema': 'fugue.dns-observation-policy/v1'}),
                                               'DNS observation policy rejected: checkpoint_deadline' if mixed else '')
        output = io.StringIO()
        with patch.object(observation, 'candidate_request', side_effect=requests) as read, \
                patch.object(observation.subprocess, 'run', side_effect=verify), \
                patch.object(observation, 'unchanged_backend') as unchanged, \
                patch.object(observation.time, 'sleep') as sleep, \
                patch.object(observation.sys, 'stderr', output):
            policy = observation.coherent_policy(candidate, config, '/approved/verifier', queries, current)
        self.assertEqual(policy['schema'], 'fugue.dns-observation-policy/v1')
        self.assertEqual(read.call_count, 3)
        self.assertEqual(unchanged.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        self.assertEqual([value['checkpoint']['applied_at'] for value in seen],
                         [first['checkpoint']['applied_at']] * 2 + [last['checkpoint']['applied_at']])
        self.assertEqual(seen[-1]['signing_key'], 'synthetic-current-key')
        self.assertNotIn('synthetic-private-material', output.getvalue())
        self.assertNotIn('synthetic-current-key', output.getvalue())
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([event['outcome'] for event in events], ['retry', 'retry', 'verified'])
        self.assertNotEqual(events[0]['checkpoint_digest'], events[-1]['checkpoint_digest'])

    def test_persistent_deadline_mismatch_is_bounded_and_still_fails_closed(self):
        config, candidate, current, queries, request = self.coherent_fixture()
        output = io.StringIO()
        with patch.object(observation, 'candidate_request', side_effect=lambda *args: copy.deepcopy(request)) as read, \
                patch.object(observation.subprocess, 'run', return_value=subprocess.CompletedProcess(
                    [], 1, '', 'DNS observation policy rejected: checkpoint_deadline')) as verify, \
                patch.object(observation, 'unchanged_backend') as unchanged, \
                patch.object(observation.time, 'monotonic', return_value=0), \
                patch.object(observation.time, 'sleep') as sleep, \
                patch.object(observation.sys, 'stderr', output):
            with self.assertRaisesRegex(ValueError, ': checkpoint_deadline$'):
                observation.coherent_policy(candidate, config, '/approved/verifier', queries, current)
        self.assertEqual(read.call_count, observation.CAPTURE_MAX_ATTEMPTS)
        self.assertEqual(verify.call_count, read.call_count)
        self.assertEqual(sleep.call_count, read.call_count - 1)
        self.assertEqual(unchanged.call_count, read.call_count - 1)
        self.assertEqual(json.loads(output.getvalue().splitlines()[-1])['outcome'], 'rejected')
        self.assertNotIn('synthetic-private-material', output.getvalue())

    def test_only_the_authenticated_deadline_gate_can_trigger_a_retry(self):
        config, candidate, current, queries, request = self.coherent_fixture()
        messages = [message for message, category in observation.POLICY_VERIFIER_REJECTIONS.items()
                    if category != 'checkpoint_deadline']
        messages += ['synthetic-private-material',
                     'DNS observation policy rejected: checkpoint_deadline\nsynthetic-private-material']
        for message in messages:
            with self.subTest(message=message), \
                    patch.object(observation, 'candidate_request', return_value=copy.deepcopy(request)) as read, \
                    patch.object(observation.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, '', message)), \
                    patch.object(observation, 'unchanged_backend') as unchanged, \
                    patch.object(observation.time, 'sleep') as sleep, \
                    patch.object(observation.sys, 'stderr', io.StringIO()) as output:
                with self.assertRaises(ValueError):
                    observation.coherent_policy(candidate, config, '/approved/verifier', queries, current)
                self.assertEqual(read.call_count, 1)
                unchanged.assert_not_called()
                sleep.assert_not_called()
                self.assertNotIn('synthetic-private-material', output.getvalue())

    def test_retry_budget_exhaustion_stops_new_observations(self):
        config, candidate, current, queries, request = self.coherent_fixture()
        for phase in ['verification', 'identity-check', 'sleep']:
            with self.subTest(phase=phase):
                clock = [0]
                def exhaust(*args, **kwargs):
                    clock[0] = observation.CAPTURE_RETRY_BUDGET_SECONDS + 1
                def verify(*args, **kwargs):
                    if phase == 'verification': exhaust()
                    return subprocess.CompletedProcess([], 1, '', 'DNS observation policy rejected: checkpoint_deadline')
                with patch.object(observation, 'candidate_request', return_value=copy.deepcopy(request)) as read, \
                        patch.object(observation.subprocess, 'run', side_effect=verify), \
                        patch.object(observation, 'unchanged_backend', side_effect=exhaust if phase == 'identity-check' else None), \
                        patch.object(observation.time, 'monotonic', side_effect=lambda: clock[0]), \
                        patch.object(observation.time, 'sleep', side_effect=exhaust if phase == 'sleep' else None), \
                        patch.object(observation.sys, 'stderr', io.StringIO()):
                    with self.assertRaisesRegex(ValueError, ': checkpoint_deadline$'):
                        observation.coherent_policy(candidate, config, '/approved/verifier', queries, current)
                    self.assertEqual(read.call_count, 1)

    def test_backend_replacement_aborts_instead_of_retrying_stale_identity(self):
        config, candidate, current, queries, request = self.coherent_fixture()
        for rejected in [False, True]:
            with self.subTest(rejected=rejected), \
                    patch.object(observation, 'candidate_request', return_value=copy.deepcopy(request)) as read, \
                    patch.object(observation.subprocess, 'run', return_value=subprocess.CompletedProcess(
                        [], int(rejected), json.dumps({'schema': 'fugue.dns-observation-policy/v1'}),
                        'DNS observation policy rejected: checkpoint_deadline' if rejected else '')), \
                    patch.object(observation, 'unchanged_backend', side_effect=ValueError('backend changed')), \
                    patch.object(observation.time, 'sleep') as sleep, \
                    patch.object(observation.sys, 'stderr', io.StringIO()):
                with self.assertRaisesRegex(ValueError, 'backend changed'):
                    observation.coherent_policy(candidate, config, '/approved/verifier', queries, current)
                self.assertEqual(read.call_count, 1)
                sleep.assert_not_called()

    def test_diagnostics_never_echo_untrusted_timestamp_or_identity_strings(self):
        _, _, _, _, request = self.coherent_fixture()
        request['checkpoint']['applied_at'] = 'synthetic-private-material'
        request['snapshot']['evaluated_at'] = '2026-01-01T00:00:00Z\nsynthetic-private-material'
        output = io.StringIO()
        with patch.object(observation.sys, 'stderr', output):
            observation.capture_diagnostic(request, 1, 'rejected', 'checkpoint_deadline', 'synthetic-private-material')
        self.assertNotIn('synthetic-private-material', output.getvalue())
        event = json.loads(output.getvalue())
        for field in ['checkpoint_applied_at', 'snapshot_evaluated_at', 'pod_uid']:
            self.assertIsNone(event[field])
        self.assertEqual(event['snapshot_checkpoint_valid_until'], '2026-01-02T00:01:00Z')

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
                        patch.object(observation.subprocess, 'run', side_effect=verify), \
                        patch.object(observation.sys, 'stderr', io.StringIO()) as diagnostic:
                    if scenario == 'valid':
                        result = observation.capture(declaration, json.dumps(declaration).encode(), config, '/approved/verifier')
                        self.assertEqual(result['servers'][0]['pod_uid'], candidate['pod']['metadata']['uid'])
                        self.assertNotIn('synthetic-private-material', json.dumps(result))
                        endpoints.assert_called_once()
                    else:
                        with self.assertRaises(ValueError) as raised:
                            observation.capture(declaration, b'declaration', config, '/approved/verifier')
                        self.assertNotIn('synthetic-private-material', str(raised.exception))
                    self.assertNotIn('synthetic-private-material', diagnostic.getvalue())

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
