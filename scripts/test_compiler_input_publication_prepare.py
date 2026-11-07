import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts import compiler_input_publication_prepare as publication
from scripts import compiler_input_task_prepare as task
from scripts.test_compiler_input_task_prepare import declaration as task_declaration, plan as task_plan, simulated


def declaration(mode='release'):
    c = task_declaration()
    del c['task']
    c['publication'] = dict(mode=mode, artifact_id='' if mode == 'lkg' else 'neutral-artifact',
        rollback_generation='target-generation' if mode == 'rollback' else '',
        release_id='neutral-release' if mode == 'lkg' else '',
        request={'release_channel': 'stable'} if mode == 'release' else {},
        operation_key='neutral-publication', **publication.BOUNDS)
    return c


def producer_declaration(action):
    c = declaration('lkg' if action in ('verify', 'verify-recovery') else
                    'rollback' if action == 'rollback' else 'release')
    p = c['publication']
    args = dict(action=action, policy_release_id='neutral-policy')
    if action in ('shadow', 'gray', 'full'):
        args['previous_release_id'] = 'neutral-previous'
        serving = action != 'shadow'
        p['request'] = dict(release_channel=action,
            reason='versioned platform configuration '+('serving ' if serving else '')+'producer',
            idempotency_key='producer/neutral-policy/'+(action+'/' if serving else '')+p['artifact_id'])
        if serving:
            args.update(previous_full_release_id='neutral-full', baseline_artifact_id='neutral-baseline')
        if action == 'gray':
            args['canary_rule_ref'] = p['request']['canary_rule_ref'] = 'cohort=neutral'
    else:
        args['baseline_artifact_id'] = 'neutral-baseline'
        if action == 'rollback':
            args.update(previous_full_release_id='neutral-full', failed_release_id='neutral-failed',
                        rollback=dict(release_channel='full', to_generation=p['rollback_generation'], reason='neutral recovery'))
        else:
            args['verification'] = dict(fencing_token=7, reason='neutral verification',
                evidence=dict(consumer_convergence=True, local_probe=True, platform_evidence=True,
                    watch_window=True, baseline_monotonic=True, database_rollback_compatible=True,
                    expected_consumer_set_id='neutral-set', evidence_refs=['neutral-evidence']))
    p['producer'] = args
    return c


def plan(c):
    p = task_plan(task_declaration())
    p['ticket']['contract'] = {**publication.contract(c), 'expected_plan_sha256': c['expectedPlanSHA256']}
    owner = p['ticket']['tuples'][0]
    if c['publication']['mode'] == 'lkg':
        owner.update(table='fugue_platform_artifact_releases', key=c['publication']['release_id'])
    else:
        owner.update(table='fugue_platform_artifacts', key=c['publication']['artifact_id'])
    return p


def result(c, p, confirmed=False):
    inputs = p['ticket']['inputs']
    pins = [dict(action='' if confirmed else 'archive-local-pin',
        key=c['publication']['operation_key']+'/'+hashlib.sha256(m['digest'].encode()).hexdigest(),
        digest=m['digest'], token=1, state='verified', applied=not confirmed, verified=True,
        raw_bytes=m['raw_bytes'], retirement_authorized=False) for m in inputs]
    raw = sum(m['raw_bytes'] for m in inputs)
    return dict(action='publication-input-confirm' if confirmed else 'publication-input-restore',
        **publication.receipt_identity(c), plan_sha256=c['expectedPlanSHA256'],
        input_count=len(inputs), raw_bytes=raw, stored_bytes=0 if confirmed else raw, pins=pins,
        prepared=confirmed, publication_activated=False, retirement_authorized=False,
        usage=dict(put_payload_bytes=0, get_payload_bytes=0 if confirmed else raw,
                   put_requests=0, get_requests=0 if confirmed else len(inputs), source_raw_bytes=0))


def run_simulated(c, failure=''):
    return simulated(c, failure, adapter=publication, plan_factory=plan, result_factory=result)


class PublicationPreparationRunnerTest(unittest.TestCase):
    def test_six_original_producer_workflows_use_same_preparation_without_activation(self):
        for action in ('shadow', 'gray', 'full', 'rollback', 'verify', 'verify-recovery'):
            c = producer_declaration(action)
            with self.subTest(action=action), tempfile.TemporaryDirectory() as tmp, run_simulated(c) as state:
                self.assertEqual(publication.validate(c), c)
                self.assertEqual(publication.contract(c)['producer'], c['publication']['producer'])
                state['evidence'] = Path(tmp)/'evidence.json'
                report = publication.execute(c, state['evidence'], True)
                self.assertEqual(report['status'], publication.SUCCESS)
                self.assertFalse(report['publication_activated'])
                self.assertTrue(report['terminal_receipt']['confirmed'])
                self.assertEqual(sum(v['kind'] == 'Job' for v in state['created']), 1)

    def test_producer_original_request_evidence_and_purpose_are_bound(self):
        for action, field, value in (
            ('shadow', 'policy_release_id', 'other'), ('shadow', 'baseline_artifact_id', 'other'),
            ('gray', 'previous_full_release_id', ''), ('full', 'baseline_artifact_id', ''),
            ('rollback', 'failed_release_id', ''), ('verify', 'previous_release_id', 'other'),
            ('verify-recovery', 'canary_rule_ref', 'other'), ('shadow', 'guard', {}),
            ('shadow', 'action', 'unknown'), ('shadow', 'policy_release_id', '中'*171)):
            c = producer_declaration(action)
            c['publication']['producer'][field] = value
            with self.subTest(action=action, field=field), self.assertRaises(ValueError):
                publication.validate(c)
        for request in ({'idempotency_key': 'other'}, {'reason': 'other'}, {'soft_override': True}):
            c = producer_declaration('gray')
            c['publication']['request'].update(request)
            with self.subTest(request=request), self.assertRaises(ValueError):
                publication.validate(c)
        for action, section, change in (
            ('rollback', 'rollback', {'force_publish': True}),
            ('rollback', 'rollback', {'soft_override': 0}),
            ('rollback', 'rollback', {'to_generation': 'other'}),
            ('verify', 'verification', {'fencing_token': True}),
            ('verify', 'verification', {'allow_initial_lkg': True}),
            ('verify', 'verification', {'evidence': {'consumer_convergence': 1}}),
            ('verify', 'verification', {'evidence': {'evidence_refs': ['<']*20000}})):
            c = producer_declaration(action)
            c['publication']['producer'][section].update(change)
            with self.subTest(action=action, change=list(change)), self.assertRaises(ValueError):
                publication.validate(c)
        c = producer_declaration('verify')
        c['publication']['producer']['verification']['evidence'] = {}
        normalized = publication.contract(publication.validate(c))['producer']['verification']['evidence']
        self.assertEqual(len(normalized), 6)
        self.assertTrue(all(v is False for v in normalized.values()))

    def test_producer_lane_absence_is_witnessed_and_never_crosses_other_purposes(self):
        c = producer_declaration('shadow')
        good = plan(c)
        lane = dict(table='fugue_platform_release_lanes', key='neutral-kind/global/shadow',
                    xmin='0', ctid='(0,0)', relation=3, filenode=4)
        good['ticket']['tuples'].append(lane)
        good['ticket']['tuples'].sort(key=lambda v: v['table']+'/'+v['key'])
        publication.plan_contract(good, c)
        for change in ({'ctid': '(1,1)'}, {'xmin': '11'}, {'key': 'x'*2049}, {'filenode': True}):
            bad = copy.deepcopy(good)
            next(v for v in bad['ticket']['tuples'] if v['table'] == lane['table']).update(change)
            with self.subTest(change=list(change)), self.assertRaises(ValueError):
                publication.plan_contract(bad, c)
        for adapter, other, factory in ((publication, declaration(), plan), (task, task_declaration(), task_plan)):
            bad = factory(other)
            bad['ticket']['tuples'].append(lane)
            bad['ticket']['tuples'].sort(key=lambda v: v['table']+'/'+v['key'])
            with self.subTest(purpose=adapter.PURPOSE), self.assertRaises(ValueError):
                adapter.plan_contract(bad, other)
        bad = copy.deepcopy(good)
        bad['ticket']['contract']['producer']['policy_release_id'] = 'other'
        with self.assertRaises(ValueError):
            publication.plan_contract(bad, c)

    def test_modes_have_distinct_business_identity_and_no_task_impersonation(self):
        for mode in ('release', 'rollback', 'lkg'):
            c = declaration(mode)
            self.assertEqual(publication.validate(c), c)
            publication.plan_contract(plan(c), c)
            self.assertNotIn('task_id', publication.contract(c))
        for change in ({'mode': 'task'}, {'guard': {}}, {'release_id': 'other'},
                       {'artifact_id': ''}, {'rollback_generation': 'other'}):
            c = declaration()
            c['publication'].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                publication.validate(c)
        for mode in ('rollback', 'lkg'):
            c = declaration(mode)
            c['publication']['request'] = {'reason': 'different request'}
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                publication.validate(c)
        with self.assertRaises(ValueError):
            publication.validate(task_declaration())
        with self.assertRaises(ValueError):
            task.validate(declaration())

    def test_original_resource_and_restore_bounds_cannot_be_widened(self):
        for field, high in publication.BOUNDS.items():
            for value in (True, 0, high+1):
                c = declaration()
                c['publication'][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    publication.validate(c)
        for field, value in [('minimumFreeBytes', (3 << 30)-1), ('maxWalGrowthBytes', (256 << 20)+1),
                             ('maxAvailableDropBytes', (128 << 20)+1), ('maxNewDeadlineFailures', 5),
                             ('executionAuthorized', False), ('expectedPlanSHA256', '')]:
            c = declaration()
            c[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                publication.validate(c)

    def test_complete_request_defaults_preconditions_and_strict_types(self):
        c = declaration()
        c['publication']['request'].update(force_publish=True, reason='审计',
            producer_reconfiguration={'serving_full': {'fencing_token': 9}},
            dns_publication_precondition={'lanes': {'gray': None, 'full': {'release_id': 'current'}}},
            kernel_break_glass={'expires_at': '2026-01-01T00:00:00.123400000+00:00'})
        request = publication.contract(publication.validate(c))['request']
        self.assertEqual(request['producer_reconfiguration']['serving_full']['fencing_token'], 9)
        self.assertEqual(request['producer_reconfiguration']['previous_policy']['artifact_id'], '')
        self.assertIsNone(request['dns_publication_precondition']['lanes']['gray'])
        self.assertEqual(request['kernel_break_glass']['expires_at'], '2026-01-01T00:00:00.1234Z')
        self.assertEqual(publication.release_request({}), {'release_channel': ''})
        for request in ({'force_publish': 1}, {'soft_override': None}, {'release_channel': None},
                        {'guard': {}}, {'producer_reconfiguration': {'serving_full': {'fencing_token': True}}},
                        {'dns_publication_precondition': {'lanes': []}},
                        {'kernel_break_glass': {'expires_at': '2026-01-01'}},
                        {'reason': '<'*11000}):
            with self.subTest(request_type=list(request)), self.assertRaises(ValueError):
                publication.release_request(request)

    def test_ticket_rejects_cross_purpose_changed_request_and_bool_number_aliases(self):
        c = declaration()
        good = plan(c)
        publication.plan_contract(good, c)
        with self.assertRaises(ValueError):
            publication.plan_contract(task_plan(task_declaration()), c)
        with self.assertRaises(ValueError):
            task.plan_contract(good, task_declaration())
        for field, value in [('mode', 'rollback'), ('artifact_id', 'different'),
                             ('request', {'release_channel': 'gray'}), ('max_inputs', True), ('version', True)]:
            bad = copy.deepcopy(good)
            bad['ticket']['contract'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                publication.plan_contract(bad, c)
        # Python equality considers True == 1: an exact typed contract must not.
        c['publication']['max_inputs'] = 1
        bad = plan(c)
        bad['ticket']['contract']['max_inputs'] = True
        with self.assertRaises(ValueError):
            publication.plan_contract(bad, c)
        c = declaration()
        c['publication']['request']['force_publish'] = True
        bad = plan(c)
        bad['ticket']['contract']['request']['force_publish'] = 1
        with self.assertRaises(ValueError):
            publication.plan_contract(bad, c)

    def test_scoped_lkg_absence_and_owner_witness_are_exact(self):
        c = declaration('lkg')
        good = plan(c)
        selector = dict(table='fugue_platform_lkg_snapshots', key=publication.go_json(['dns_answer_bundle', 'neutral<&scope']),
                        xmin='0', ctid='(0,0)', relation=3, filenode=4)
        good['ticket']['tuples'].append(selector)
        good['ticket']['tuples'].sort(key=lambda v: v['table']+'/'+v['key'])
        publication.plan_contract(good, c)
        for change in ({'ctid': '(1,1)'}, {'xmin': '11'}, {'key': '["dns_answer_bundle", "neutral"]'},
                       {'key': '["dns_answer_bundle","neutral<&scope"]'}, {'key': '["", "scope"]'}):
            bad = copy.deepcopy(good)
            bad['ticket']['tuples'][-1].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                publication.plan_contract(bad, c)
        bad = plan(c)
        bad['ticket']['tuples'][0]['key'] = 'different-release'
        with self.assertRaises(ValueError):
            publication.plan_contract(bad, c)

    def test_get_only_result_full_pins_and_purpose_are_not_activation(self):
        for mode in ('release', 'rollback', 'lkg'):
            c = declaration(mode)
            p = plan(c)
            publication.result_contract(result(c, p), c, p)
            publication.result_contract(result(c, p, True), c, p, True)
        c, p = declaration(), plan(declaration())
        for change in ({'mode': 'lkg'}, {'artifact_id': 'other'}, {'publication_activated': True},
                       {'task_id': 'invented'}, {'task_activated': False}, {'retirement_authorized': 0},
                       {'pins': []}, {'input_count': True}, {'action': 'task-input-restore'}):
            bad = result(c, p)
            bad.update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                publication.result_contract(bad, c, p)
        for field, value in [('put_requests', 1), ('source_raw_bytes', 1), ('get_payload_bytes', (448 << 20)+1)]:
            bad = result(c, p)
            bad['usage'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                publication.result_contract(bad, c, p)

    def test_worker_and_trusted_stdin_keep_purpose_key_and_secret_boundaries(self):
        c = declaration()
        job = publication.job_manifest(c, 'neutral', 'session')
        pod = job['spec']['template']['spec']
        container = pod['containers'][0]
        self.assertEqual(container['args'], ['-mode=publication-input-restore', '-apply', '-publication-input-plan=/preparation/plan.json'])
        self.assertTrue(job['spec']['suspend'])
        self.assertEqual((job['spec']['backoffLimit'], job['spec']['activeDeadlineSeconds']), (0, 600))
        self.assertEqual(container['resources']['requests'], {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': '256Mi'})
        self.assertFalse(pod['automountServiceAccountToken'])
        self.assertEqual(pod['preemptionPolicy'], 'Never')
        env = {v['name']: v for v in container['env']}
        self.assertIn(publication.VERIFICATION_ENV, env)
        for forbidden in ('SIGNING', 'ENCRYPTION_KEY', 'WRITER_', task.VERIFICATION_ENV, 'envFrom', 'hostPath', '/bin/sh'):
            self.assertNotIn(forbidden, json.dumps(job))
        with patch.object(publication.subprocess, 'run', return_value=types.SimpleNamespace(returncode=0, stdout='{}')) as call:
            publication.native(c, {'pod': 'neutral-api', 'container': 'api'}, 'plan', publication.contract(c))
        self.assertIn('-publication-input-contract=-', call.call_args.args[0])
        self.assertEqual(json.loads(call.call_args.kwargs['input']), publication.contract(c))

    def test_preview_and_each_mode_success_share_receipts_without_business_activation(self):
        for mode in ('release', 'rollback', 'lkg'):
            c = declaration(mode)
            for apply in (False, True):
                with self.subTest(mode=mode, apply=apply), tempfile.TemporaryDirectory() as tmp, run_simulated(c) as state:
                    state['evidence'] = Path(tmp)/'evidence.json'
                    report = publication.execute(c, state['evidence'], apply)
                    self.assertFalse(report['publication_activated'])
                    self.assertEqual(report['status'], publication.SUCCESS if apply else 'preview-passed')
                    if not apply:
                        self.assertEqual(state['created'], [])
                        self.assertNotIn('session', state['events'])
                        continue
                    self.assertTrue(report['cleanup_confirmed'] and report['terminal_receipt']['confirmed'])
                    self.assertLess(state['events'].index('create-ConfigMap'), state['events'].index('session'))
                    self.assertLess(state['events'].index('logs'), state['events'].index('native-confirm'))
                    self.assertLess(state['events'].index('cleanup'), state['events'].index('terminal'))
                    intent = state['created'][0]['data']
                    self.assertEqual(intent['preparation-purpose'], publication.PURPOSE)
                    self.assertEqual(json.loads(intent['subject.json']), publication.receipt_identity(c))
                    self.assertEqual(intent['declaration-sha256'], publication.r.identity(c))
                    secret = next(v for v in state['created'] if v['kind'] == 'Secret')
                    self.assertEqual(base64.b64decode(secret['data']['verification-key']).decode(), report['verification_key'])
                    self.assertGreaterEqual(state['maintenance_checks'].count('fugue.pro/purpose='+publication.PURPOSE), 5)

    def test_failed_or_uncertain_attempt_never_replays_or_activates(self):
        for failure in ('backing', 'plan-differs', 'intent-readback', 'secret-create', 'unsuspend', 'job-uid',
                        'worker-failed', 'wrong-image', 'logs', 'final-wal', 'final-other', 'confirm', 'pin-token',
                        'concurrent-publication'):
            c = declaration()
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, run_simulated(c, failure) as state:
                state['evidence'] = Path(tmp)/'evidence.json'
                with self.assertRaises(ValueError):
                    publication.execute(c, state['evidence'], True)
                report = json.loads(state['evidence'].read_text())
                self.assertEqual(report['status'], 'completion-not-confirmed')
                self.assertFalse(report['publication_activated'])
                self.assertLessEqual(sum(v['kind'] == 'Job' for v in state['created']), 1)
                if failure not in ('confirm', 'pin-token'):
                    self.assertFalse(state['confirmed'])
                if failure == 'intent-readback':
                    self.assertNotIn('session', state['events'])
                    self.assertIsNotNone(state['terminal'])

    def test_cleanup_is_within_same_cumulative_window_and_failure_is_not_success(self):
        for adapter, c, factory, receipt in ((publication, declaration(), plan, result),
                                             (task, task_declaration(), task_plan, None)):
            kwargs = dict(adapter=adapter, plan_factory=factory)
            if receipt:
                kwargs['result_factory'] = receipt
            for failure in ('cleanup-wal', 'cleanup', 'terminal'):
                with self.subTest(adapter=adapter.PURPOSE, failure=failure), tempfile.TemporaryDirectory() as tmp, simulated(c, failure, **kwargs) as state:
                    state['evidence'] = Path(tmp)/'evidence.json'
                    report = adapter.execute(c, state['evidence'], True)
                    self.assertFalse(report['status'] == adapter.SUCCESS and report['cleanup_confirmed'] and report['terminal_receipt']['confirmed'])
                    if failure != 'terminal':
                        self.assertEqual(state['terminal']['status'], 'completion-not-confirmed')

    def test_local_evidence_failure_still_writes_independent_uncertain_terminal(self):
        c = declaration()
        original_write = Path.write_text
        with tempfile.TemporaryDirectory() as tmp, run_simulated(c) as state:
            state['evidence'] = Path(tmp)/'evidence.json'
            def write(path, text, *args, **kwargs):
                if state['cleaned']:
                    raise OSError('synthetic disk unavailable')
                return original_write(path, text, *args, **kwargs)
            with patch.object(Path, 'write_text', write), self.assertRaises(OSError):
                publication.execute(c, state['evidence'], True)
            self.assertEqual(state['terminal']['status'], 'completion-not-confirmed')
            self.assertEqual(state['terminal']['evidence_error_type'], 'OSError')
            self.assertFalse(state['terminal']['publication_activated'])

    def test_existing_serialized_ci_lane_requires_declaration_and_exact_dependencies(self):
        workflow = Path('.github/workflows/ci.yml').read_text()
        start = workflow.index('      - id: publication_prepare\n')
        end = workflow.index('      - id: task_prepare\n', start)
        lane = workflow[start:end]
        self.assertIn('if [ ! -f "$config" ]', lane)
        self.assertEqual(lane.count('scripts/compiler_input_prepare.py'), 2)
        self.assertEqual(lane.count('-m scripts.compiler_input_publication_prepare '), 2)
        self.assertIn('attempt will not be replayed', lane)
        self.assertIn('retention-days: 90', lane)
        self.assertIn("if: always() && steps.publication_prepare.outputs.selected == 'true'", lane)
        self.assertFalse(Path('deploy/environments/production/compiler-input-publication-prepare/operation.json').exists())


if __name__ == '__main__':
    unittest.main()
