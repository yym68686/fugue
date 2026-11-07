import base64
import copy
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts import compiler_input_consumer_prepare as consumer
from scripts import compiler_input_publication_prepare as publication
from scripts import compiler_input_task_prepare as task
from scripts.test_compiler_input_publication_prepare import declaration as publication_declaration
from scripts.test_compiler_input_task_prepare import declaration as task_declaration, plan as task_plan, result as task_result, simulated


def declaration(mode='trusted'):
    c = task_declaration()
    del c['task']
    c['consumer'] = dict(mode=mode, expected_set_id='neutral-set', operation_key='neutral-consumer', **consumer.BOUNDS)
    if mode == 'register':
        # Complete canonical server-produced registration. Native typed decode
        # and normalization are authoritative, before any intent/session write.
        c['consumer']['registration'] = dict(id='neutral-set', release_set_id='neutral-parent',
            artifact_release_id='neutral-release', artifact_kind='release_set', scope={'scope_type': 'global'},
            scope_key='global', expected_generation='neutral-generation', topology_revision='neutral-topology',
            revision=1, requires_consumers=False, required_cardinality=0, optional_cardinality=0, consumers=[],
            heartbeat_deadline='2026-01-01T00:01:30Z', convergence_deadline='2026-01-01T00:05:00Z',
            created_at='2026-01-01T00:00:00Z', updated_at='2026-01-01T00:00:00Z')
    else:
        c['consumer']['heartbeat'] = dict(consumer_id='neutral-consumer', expected_consumer_set_id='neutral-set',
            desired_generation='neutral-desired', actual_generation='neutral-actual', candidate_generation='neutral-candidate',
            lkg_generation='neutral-lkg', issued_at='2026-01-01T00:00:00.123400000Z', sequence=2)
    return c


def plan(c):
    out = task_plan(task_declaration())
    out['ticket']['contract'] = {**consumer.contract(c), 'expected_plan_sha256': c['expectedPlanSHA256']}
    p = c['consumer']
    table, key = ('fugue_platform_artifacts', p['registration']['release_set_id']) if p['mode'] == 'register' else ('fugue_platform_expected_consumer_sets', p['expected_set_id'])
    out['ticket']['tuples'][0].update(table=table, key=key)
    return out


def result(c, p, confirmed=False):
    original = task_declaration()
    original['task']['operation_key'] = c['consumer']['operation_key']
    out = task_result(original, p, confirmed)
    del out['task_id'], out['task_activated']
    out.update(action='consumer-input-confirm' if confirmed else 'consumer-input-restore',
               **consumer.receipt_identity(c), consumer_activated=False)
    return out


def run_simulated(c, failure=''):
    return simulated(c, failure, adapter=consumer, plan_factory=plan, result_factory=result)


class ConsumerPreparationRunnerTest(unittest.TestCase):
    def test_three_purposes_bind_every_generation_and_expected_namespace(self):
        for mode in ('register', 'trusted', 'shadow'):
            c = declaration(mode)
            self.assertEqual(consumer.validate(c), c)
            consumer.plan_contract(plan(c), c)
            self.assertNotIn('task_id', consumer.contract(c))
            bad = plan(c)
            subject = bad['ticket']['contract']['registration' if mode == 'register' else 'heartbeat']
            for field in (('expected_generation', 'revision') if mode == 'register' else
                          ('desired_generation', 'actual_generation', 'candidate_generation', 'lkg_generation', 'sequence')):
                changed = copy.deepcopy(bad)
                intent = changed['ticket']['contract']['registration' if mode == 'register' else 'heartbeat']
                intent[field] = 'other' if isinstance(subject[field], str) else 3
                with self.subTest(mode=mode, field=field), self.assertRaises(ValueError):
                    consumer.plan_contract(changed, c)
        for change in ({'mode': 'retire'}, {'expected_set_id': ' other '}, {'expected_set_id': 'other'},
                       {'guard': {}}, {'registration': {}}, {'max_inputs': True}):
            c = declaration()
            c['consumer'].update(change)
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                consumer.validate(c)
        for adapter, other in ((task, task_declaration()), (publication, publication_declaration())):
            with self.assertRaises(ValueError):
                consumer.validate(other)
            with self.assertRaises(ValueError):
                adapter.validate(declaration())

    def test_heartbeat_native_defaults_types_bound_and_optional_namespace(self):
        h = consumer.contract(consumer.validate(declaration()))['heartbeat']
        self.assertEqual(h['issued_at'], '2026-01-01T00:00:00.1234Z')
        self.assertEqual(h['fencing_token'], 0)
        self.assertNotIn('serving_lkg', h)
        self.assertNotIn('compatibility_capabilities', h)
        for change in ({'sequence': True}, {'fencing_token': 1 << 63}, {'serving_lkg': 1},
                       {'issued_at': '2026-01-01'}, {'issued_at': '2026-01-01T00:00:00+00:00'},
                       {'compatibility_capabilities': ['x', 2]}, {'last_error': 'x'*65536}, {'guard': {}}):
            c = declaration()
            c['consumer']['heartbeat'].update(change)
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                consumer.validate(c)
        c = declaration()
        c['consumer']['heartbeat']['expected_consumer_set_id'] = ''
        consumer.validate(c)
        c['consumer']['mode'] = 'shadow'
        with self.assertRaises(ValueError):
            consumer.validate(c)

    def test_original_resource_bounds_and_complete_typed_ticket_are_required(self):
        for field, high in consumer.BOUNDS.items():
            for value in (True, 0, high+1):
                c = declaration()
                c['consumer'][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    consumer.validate(c)
        for field, value in [('minimumFreeBytes', (3 << 30)-1), ('maxWalGrowthBytes', (256 << 20)+1),
                             ('maxAvailableDropBytes', (128 << 20)+1), ('maxNewDeadlineFailures', 5),
                             ('executionAuthorized', False), ('expectedPlanSHA256', '')]:
            c = declaration()
            c[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                consumer.validate(c)
        c = declaration()
        for change in ({'epoch': True}, {'tuples': []}, {'backend_hash': 'sha256:'+'b'*64}, {'private_key': 'invented'}):
            bad = plan(c)
            bad['ticket'].update(change)
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                consumer.plan_contract(bad, c)
        bad = plan(c)
        bad['ticket']['contract']['heartbeat']['sequence'] = True
        c['consumer']['heartbeat']['sequence'] = 1
        with self.assertRaises(ValueError):
            consumer.plan_contract(bad, c)
        c = declaration('register')
        bad = plan(c)
        bad['ticket']['contract']['registration'].pop('updated_at')
        with self.assertRaises(ValueError):
            consumer.plan_contract(bad, c)

    def test_owner_and_lkg_absence_witnesses_are_exact(self):
        c = declaration()
        good = plan(c)
        good['ticket']['tuples'].append(dict(table='fugue_platform_lkg_snapshots', key='["release_set","global"]',
            xmin='0', ctid='(0,0)', relation=3, filenode=4))
        consumer.plan_contract(good, c)
        for key in ('["release_set", "global"]', '["", "global"]', 'not-json'):
            bad = copy.deepcopy(good)
            bad['ticket']['tuples'][-1]['key'] = key
            with self.subTest(key=key), self.assertRaises(ValueError):
                consumer.plan_contract(bad, c)
        bad = plan(c)
        bad['ticket']['tuples'][0]['key'] = 'other'
        with self.assertRaises(ValueError):
            consumer.plan_contract(bad, c)

    def test_worker_get_only_key_and_trusted_stdin_isolation(self):
        c = declaration()
        job = consumer.job_manifest(c, 'neutral', 'session')
        pod = job['spec']['template']['spec']
        container = pod['containers'][0]
        self.assertEqual(container['args'], ['-mode=consumer-input-restore', '-apply', '-consumer-input-plan=/preparation/plan.json'])
        self.assertTrue(job['spec']['suspend'])
        self.assertEqual((job['spec']['backoffLimit'], job['spec']['activeDeadlineSeconds']), (0, 600))
        self.assertEqual(container['resources']['requests'], {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': '256Mi'})
        self.assertFalse(pod['automountServiceAccountToken'])
        self.assertEqual(pod['preemptionPolicy'], 'Never')
        self.assertIn(consumer.VERIFICATION_ENV, {x['name'] for x in container['env']})
        for forbidden in ('SIGNING', 'ENCRYPTION_KEY', 'WRITER_', task.VERIFICATION_ENV, publication.VERIFICATION_ENV, 'envFrom', 'hostPath', '/bin/sh'):
            self.assertNotIn(forbidden, json.dumps(job))
        with patch.object(consumer.subprocess, 'run', return_value=types.SimpleNamespace(returncode=0, stdout='{}')) as call:
            consumer.native(c, {'pod': 'neutral-api', 'container': 'api'}, 'plan', consumer.contract(c))
        self.assertIn('-consumer-input-contract=-', call.call_args.args[0])
        self.assertEqual(json.loads(call.call_args.kwargs['input']), consumer.contract(c))

    def test_three_modes_preview_apply_receipts_without_business_acceptance(self):
        for mode in ('register', 'trusted', 'shadow'):
            c = declaration(mode)
            for apply in (False, True):
                with self.subTest(mode=mode, apply=apply), tempfile.TemporaryDirectory() as tmp, run_simulated(c) as state:
                    state['evidence'] = Path(tmp)/'evidence.json'
                    report = consumer.execute(c, state['evidence'], apply)
                    self.assertFalse(report['consumer_activated'])
                    self.assertEqual(report['status'], consumer.SUCCESS if apply else 'preview-passed')
                    if not apply:
                        self.assertEqual(state['created'], [])
                        self.assertNotIn('session', state['events'])
                        continue
                    self.assertTrue(report['cleanup_confirmed'] and report['terminal_receipt']['confirmed'])
                    self.assertLess(state['events'].index('create-ConfigMap'), state['events'].index('session'))
                    self.assertLess(state['events'].index('logs'), state['events'].index('native-confirm'))
                    self.assertLess(state['events'].index('cleanup'), state['events'].index('terminal'))
                    self.assertEqual(state['created'][0]['data']['preparation-purpose'], consumer.PURPOSE)
                    self.assertEqual(json.loads(state['created'][0]['data']['subject.json']), consumer.receipt_identity(c))
                    secret = next(v for v in state['created'] if v['kind'] == 'Secret')
                    self.assertEqual(base64.b64decode(secret['data']['verification-key']).decode(), report['verification_key'])
                    for purpose in consumer.shared.MAINTENANCE_PURPOSES:
                        self.assertGreaterEqual(state['maintenance_checks'].count('fugue.pro/purpose='+purpose), 5)

    def test_uncertain_attempts_do_not_replay_or_accept_business(self):
        for failure in ('backing', 'plan-differs', 'intent-readback', 'secret-create', 'unsuspend', 'job-uid',
                        'worker-failed', 'wrong-image', 'logs', 'final-wal', 'final-other', 'confirm', 'pin-token', 'concurrent-publication', 'concurrent-consumer'):
            c = declaration()
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, run_simulated(c, failure) as state:
                state['evidence'] = Path(tmp)/'evidence.json'
                with self.assertRaises(ValueError):
                    consumer.execute(c, state['evidence'], True)
                report = json.loads(state['evidence'].read_text())
                self.assertEqual(report['status'], 'completion-not-confirmed')
                self.assertFalse(report['consumer_activated'])
                self.assertLessEqual(sum(v['kind'] == 'Job' for v in state['created']), 1)
                if failure not in ('confirm', 'pin-token'):
                    self.assertFalse(state['confirmed'])

    def test_result_full_pins_and_cleanup_stay_in_cumulative_window(self):
        c, p = declaration(), plan(declaration())
        consumer.result_contract(result(c, p), c, p)
        consumer.result_contract(result(c, p, True), c, p, True)
        for change in ({'consumer_activated': True}, {'retirement_authorized': 0}, {'pins': []},
                       {'action': 'task-input-restore'}, {'mode': 'register'}, {'expected_set_id': 'other'}):
            bad = result(c, p)
            bad.update(change)
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                consumer.result_contract(bad, c, p)
        for failure in ('cleanup-wal', 'cleanup', 'terminal'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, run_simulated(c, failure) as state:
                state['evidence'] = Path(tmp)/'evidence.json'
                report = consumer.execute(c, state['evidence'], True)
                self.assertFalse(report['status'] == consumer.SUCCESS and report['cleanup_confirmed'] and report['terminal_receipt']['confirmed'])

    def test_serialized_ci_and_prepush_select_the_complete_dependency_scope(self):
        workflow = Path('.github/workflows/ci.yml').read_text()
        lane = workflow[workflow.index('      - id: consumer_prepare\n'):workflow.index('      - id: publication_prepare\n')]
        self.assertIn('if [ ! -f "$config" ]', lane)
        for path in ('scripts/compiler_input_consumer_prepare.py', 'scripts/compiler_input_prepare.py', 'scripts/compiler_input_copy.py'):
            self.assertEqual(lane.count(path), 2)
        self.assertEqual(lane.count('-m scripts.compiler_input_consumer_prepare '), 2)
        self.assertIn('attempt will not be replayed', lane)
        self.assertIn('retention-days: 90', lane)
        self.assertFalse(Path('deploy/environments/production/compiler-input-consumer-prepare/operation.json').exists())
        self.assertIn('scripts.test_compiler_input_consumer_prepare', Path('Makefile').read_text())
        self.assertIn('scripts.test_compiler_input_consumer_prepare', Path('scripts/prepush.py').read_text())


if __name__ == '__main__':
    unittest.main()
