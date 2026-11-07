import base64
import copy
from contextlib import ExitStack, contextmanager
import datetime
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts import compiler_input_task_prepare as task
from scripts.test_compiler_input_hot_index import declaration as index_declaration, metadata


UID = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'


def declaration():
    c = index_declaration()
    del c['page']
    c.update(task={'task_id': 'neutral-task', 'updater_id': 'neutral-updater', 'operation_key': 'neutral-operation',
                   **task.BOUNDS}, backendHash='sha256:'+'e'*64, expectedPlanSHA256='f'*64)
    return c


def plan(c):
    now = datetime.datetime.now(datetime.timezone.utc)
    return {'verification_key': base64.b64encode(b'v'*32).decode(), 'ticket': {
        'version': 1, 'contract': {**task.contract(c), 'expected_plan_sha256': c['expectedPlanSHA256']},
        'backend_hash': c['backendHash'], 'closure_sha256': 'd'*64, 'epoch': 0, 'horizon': 10,
        'issued_at': now.isoformat(), 'expires_at': (now+datetime.timedelta(minutes=10)).isoformat(),
        'inputs': [{'digest': 'sha256:'+'a'*64, 'format': 'runtime-snapshot-json-chunk-codec-v2',
                    'location': 'legacy', 'raw_bytes': 1024, 'chunk_size': 65536, 'chunks': 1, 'created_at': now.isoformat()}],
        'tuples': [{'table': 'fugue_node_update_tasks', 'key': c['task']['task_id'], 'xmin': '10',
                    'ctid': '(1,1)', 'relation': 1, 'filenode': 2}], 'signature': base64.b64encode(b's'*64).decode()}}


def result(c, p, confirmed=False):
    digest = p['ticket']['inputs'][0]['digest']
    pin = {'action': '' if confirmed else 'archive-local-pin',
           'key': c['task']['operation_key']+'/'+hashlib.sha256(digest.encode()).hexdigest(),
           'digest': digest, 'token': 1, 'state': 'verified', 'applied': not confirmed, 'verified': True,
           'raw_bytes': 1024, 'retirement_authorized': False}
    return {'action': 'task-input-confirm' if confirmed else 'task-input-restore', 'task_id': c['task']['task_id'],
            'plan_sha256': c['expectedPlanSHA256'], 'input_count': 1, 'raw_bytes': 1024,
            'stored_bytes': 0 if confirmed else 1024, 'pins': [pin], 'prepared': confirmed,
            'task_activated': False, 'retirement_authorized': False,
            'usage': {'put_payload_bytes': 0, 'get_payload_bytes': 0 if confirmed else 1024,
                      'put_requests': 0, 'get_requests': 0 if confirmed else 1, 'source_raw_bytes': 0}}


@contextmanager
def simulated(c, failure='', adapter=task, plan_factory=plan, result_factory=result):
    task = adapter
    p = plan_factory(c)
    state = {'created': [], 'resources': {}, 'unsuspended': False, 'completed': False, 'confirmed': False,
             'cleaned': False, 'terminal': None, 'events': [], 'maintenance_checks': []}
    def create(item, dry=False):
        if not dry:
            state['events'].append('create-'+item['kind'])
            state['created'].append(copy.deepcopy(item))
            state['resources'][item['metadata']['name']] = {**copy.deepcopy(item), 'metadata': {**item['metadata'], 'uid': UID}}
            if item['kind'] == 'Secret' and failure == 'secret-create':
                raise task.r.Refusal('secret creation uncertain')
        return {'metadata': {'uid': UID}}
    def get(namespace, kind, name):
        if kind == 'job':
            return {'metadata': {'uid': UID if failure != 'job-uid' else 'changed'}}
        found = copy.deepcopy(state['resources'][name])
        if failure == 'intent-readback' and name.endswith('-receipt'):
            found['data']['verification-key'] = 'different'
        return found
    def native(c, executor, mode, payload):
        state['events'].append('native-'+mode)
        if mode == 'plan':
            out = copy.deepcopy(p)
            if failure == 'plan-differs':
                out['ticket']['contract']['expected_plan_sha256'] = 'b'*64
            return out
        state['confirmed'] = True
        if failure == 'confirm':
            raise task.r.Refusal('fresh confirmation refused')
        out = result_factory(c, p, True)
        if failure == 'pin-token':
            out['pins'][0]['token'] = 2
        return out
    def meta(c, reader=False):
        m = metadata()
        if (failure == 'final-wal' and state['completed']) or (failure == 'cleanup-wal' and state['cleaned']):
            m['wal_lsn'] = 'A/10000101'
        return m
    def activity(*args):
        outer = dict(schema='fugue.compiler-input-writer.observation.v1', attempts=0, in_flight=0,
                     successful_returns=0, deadline_failures=0, canceled_failures=0, other_failures=0)
        if failure == 'final-other' and state['completed']:
            outer.update(attempts=1, other_failures=1)
        return {UID: {'write_failures': 0, 'budget_fallbacks': 0, 'category': '', 'outer_writer': outer}}
    def kube(namespace, verb, *args):
        if verb == 'logs':
            # Durable termination evidence must precede potentially failing logs.
            body = json.loads(state['evidence'].read_text())
            assert 'terminal' in body['execution']
            state['events'].append('logs')
            if failure == 'logs':
                raise task.r.Refusal('logs unavailable')
            return json.dumps(result_factory(c, p))
        if verb == 'get' and args[0] == 'jobs':
            state['maintenance_checks'].append(args[2])
            if state['unsuspended'] and ((failure == 'concurrent-publication' and args[2].endswith('publication-prepare'))
                                       or (failure == 'concurrent-consumer' and args[2].endswith('consumer-prepare'))):
                return '{"items":[{"metadata":{"uid":"foreign"}}]}'
            return '{"items":[]}'
        if verb == 'get' and args[0] == 'pods':
            state['completed'] = True
            terminal = {'reason': 'Completed', 'exitCode': 0, 'signal': 0,
                        'startedAt': '2026-01-01T00:00:00Z', 'finishedAt': '2026-01-01T00:00:01Z'}
            if failure == 'worker-failed':
                terminal['exitCode'] = 1
            pod = {'metadata': {'name': 'neutral-worker', 'uid': UID, 'ownerReferences': [{'controller': True, 'uid': UID}],
                                'annotations': {'fugue.pro/source-commit': c['sourceCommit']}},
                   'spec': {'nodeName': c['node'], 'priority': -1000000, 'preemptionPolicy': 'Never'},
                   'status': {'phase': 'Failed' if failure == 'worker-failed' else 'Succeeded', 'containerStatuses': [
                       {'name': 'restore', 'restartCount': 0, 'imageID': c['image'], 'state': {'terminated': terminal}}]}}
            if failure == 'wrong-image':
                pod['status']['containerStatuses'][0]['imageID'] = 'wrong'
            return json.dumps({'items': [pod]})
        raise AssertionError('unexpected kube call')
    def session(*args):
        state['events'].append('session')
        return {'database-url': 'synthetic-private-db', 'reader-access-key-id': 'synthetic-reader',
                'reader-secret-access-key': 'synthetic-private-reader'}
    def release(*args):
        state['events'].append('unsuspend')
        if failure == 'unsuspend':
            raise task.r.Refusal('release completion uncertain')
        state['unsuspended'] = True
    def cleanup(*args):
        state['events'].append('cleanup')
        state['cleaned'] = failure != 'cleanup'
        if not state['cleaned']:
            raise task.r.Refusal('cleanup incomplete')
    def receipt(c, report):
        state['terminal'] = copy.deepcopy(report)
        state['events'].append('terminal')
        return {'uid': UID, 'confirmed': failure != 'terminal'}
    primary = {'status': {'containerStatuses': [{'name': 'postgres', 'restartCount': 91}]}}
    executor = {'pod': 'neutral-api', 'container': 'api'}
    serving = {'api_pods': {'neutral-api': UID}}
    with ExitStack() as stack:
        stack.enter_context(patch.dict(task.os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main', GITHUB_SHA='a'*40))
        for obj, name, effect in [(task, 'command', lambda *args: 'a'*40), (task.impact, 'node_gate', lambda *args: {}),
            (task.r, 'primary_gate', lambda *args: ({}, primary, 4 << 30)),
            (task.impact, 'backing_gate', lambda *args, **kwargs: {'capacity_gate_passed': failure != 'backing'}),
            (task.r, 'serving_gate', lambda *args: (executor, {}, serving)), (task, 'metadata', meta),
            (task.impact, 'api_activity', activity), (task.impact, 'api_log_signals', lambda *args: {}),
            (task, 'native', native), (task, 'session', session), (task, 'get', get), (task, 'kube', kube),
            (task.r, 'create', create), (task.r, 'api_write', release), (task.index, 'cleanup', cleanup),
            (task, 'terminal_receipt', receipt), (task.time, 'sleep', lambda *args: None),
            (task.r, 'cluster_get', lambda *args: {'metadata': {'uid': UID}, 'value': -1000000, 'preemptionPolicy': 'Never'})]:
            stack.enter_context(patch.object(obj, name, side_effect=effect))
        yield state


class TaskPreparationRunnerTest(unittest.TestCase):
    def test_declaration_preserves_original_bounds_and_exact_plan(self):
        c = declaration()
        self.assertEqual(task.validate(c), c)
        for key, value in [('version', True), ('executionAuthorized', False), ('expectedPlanSHA256', ''),
                           ('minimumFreeBytes', (3 << 30)-1), ('maxWalGrowthBytes', (256 << 20)+1),
                           ('maxAvailableDropBytes', (128 << 20)+1), ('maxNewDeadlineFailures', 5)]:
            changed = copy.deepcopy(c)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                task.validate(changed)
        for key, high in task.BOUNDS.items():
            for value in (True, 0, high+1):
                changed = copy.deepcopy(c)
                changed['task'][key] = value
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    task.validate(changed)

    def test_worker_explicit_reader_only_public_key_no_platform_secrets(self):
        c = declaration()
        job = task.job_manifest(c, 'neutral', 'session')
        pod = job['spec']['template']['spec']
        container = pod['containers'][0]
        self.assertEqual((job['spec']['backoffLimit'], job['spec']['activeDeadlineSeconds']), (0, 600))
        self.assertTrue(job['spec']['suspend'])
        self.assertFalse(pod['automountServiceAccountToken'])
        self.assertEqual(pod['preemptionPolicy'], 'Never')
        self.assertEqual(container['args'], ['-mode=task-input-restore', '-apply', '-task-input-plan=/preparation/plan.json'])
        self.assertEqual(container['resources']['requests'], {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': '256Mi'})
        env = {x['name']: x for x in container['env']}
        self.assertEqual(env['FUGUE_TASK_INPUT_PREPARATION_VERIFICATION_KEY']['valueFrom']['secretKeyRef']['key'], 'verification-key')
        self.assertIn('FUGUE_COMPILER_INPUT_ARCHIVE_READER_SECRET_ACCESS_KEY', env)
        self.assertEqual(env['FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED']['value'], 'true')
        for forbidden in ('SIGNING', 'ENCRYPTION_KEY', 'WRITER_', 'envFrom', 'hostPath', '/bin/sh', '-mode=task-input-prepare'):
            self.assertNotIn(forbidden, json.dumps(job))
        self.assertTrue(container['volumeMounts'][0]['readOnly'])

    def test_ticket_contract_rejects_changed_identity_geometry_expiry_and_unknowns(self):
        c = declaration()
        good = plan(c)
        self.assertEqual(task.plan_contract(good, c), good)
        for field, value in [('epoch', True), ('horizon', 0), ('backend_hash', 'sha256:'+'b'*64),
                             ('signature', 'not-base64'), ('tuples', []), ('private', 'not-allowed')]:
            changed = copy.deepcopy(good)
            changed['ticket'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                task.plan_contract(changed, c)
        changed = copy.deepcopy(good)
        changed['ticket']['inputs'][0]['chunks'] = 2
        with self.assertRaises(ValueError):
            task.plan_contract(changed, c)
        for fmt, size in [('runtime-snapshot-json-chunk-codec-v2', 262140),
                          ('runtime-snapshot-shared-v3', 65536), ('runtime-snapshot-separated-v4', 65536)]:
            changed = copy.deepcopy(good)
            changed['ticket']['inputs'][0].update(format=fmt, chunk_size=size)
            with self.subTest(format=fmt), self.assertRaises(ValueError):
                task.plan_contract(changed, c)
        changed = copy.deepcopy(good)
        changed['ticket']['issued_at'] = '2026-01-01T00:00:00Z'
        with self.assertRaises(ValueError):
            task.plan_contract(changed, c)

    def test_result_requires_exact_full_pins_and_get_only_usage(self):
        c, p = declaration(), plan(declaration())
        task.result_contract(result(c, p), c, p)
        task.result_contract(result(c, p, True), c, p, True)
        for field, value in [('prepared', True), ('task_activated', True), ('retirement_authorized', True),
                             ('input_count', True), ('raw_bytes', 1023), ('pins', [])]:
            changed = result(c, p)
            changed[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                task.result_contract(changed, c, p)

        for confirmed in (False, True):
            for field, value in [('action', 'unreviewed'), ('applied', confirmed), ('applied', int(not confirmed)),
                                 ('verified', 1), ('token', True), ('retirement_authorized', 0)]:
                changed = result(c, p, confirmed)
                changed['pins'][0][field] = value
                with self.subTest(confirmed=confirmed, field=field, value=value), self.assertRaises(ValueError):
                    task.result_contract(changed, c, p, confirmed)
        for field, value in [('put_requests', 1), ('put_payload_bytes', 1), ('get_payload_bytes', (448 << 20)+1)]:
            changed = result(c, p)
            changed['usage'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                task.result_contract(changed, c, p)

    def test_configuration_selector_witness_accepts_absence_not_other_scope(self):
        c = declaration()
        good = plan(c)
        selector = dict(table='fugue_platform_lkg_snapshots', key='["release_set","global"]',
                        xmin='0', ctid='(0,0)', relation=3, filenode=4)
        good['ticket']['tuples'].append(selector)
        good['ticket']['tuples'].sort(key=lambda v: v['table']+'/'+v['key'])
        task.plan_contract(good, c)
        for key in ('["release_set","other"]', 'untrusted', '["dns_answer_bundle","global"]'):
            bad = copy.deepcopy(good)
            next(v for v in bad['ticket']['tuples'] if v['table'] == selector['table'])['key'] = key
            with self.subTest(key=key), self.assertRaises(ValueError):
                task.plan_contract(bad, c)

    def test_trusted_session_strips_platform_keys_and_uses_only_explicit_reader(self):
        c = declaration()
        private = {'platform': {'compiler_input_archive': {'reader': {'ciphertext': 'synthetic-encrypted-reader'}}}}
        container = {'name': 'api', 'env': [{'name': 'FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'value': 'synthetic-trusted-only'}]}
        deployment = {'spec': {'template': {'spec': {'containers': [container]}}}}
        executor = {'pod': 'neutral-api', 'container': 'api'}
        reader = {'reader-access-key-id': 'synthetic-reader', 'reader-secret-access-key': 'synthetic-reader-secret'}
        database = {'database-url': 'postgres://synthetic?application_name='+task.index.PURPOSE}
        with patch.object(task, 'metadata', return_value=private) as meta, \
             patch.object(task.r, 'credential_key', return_value=b'synthetic-trusted-key') as key, \
             patch.object(task.r, 'reader_session', return_value=reader.copy()) as decode, \
             patch.object(task.index, 'session', return_value=database.copy()) as db:
            data = task.session(c, {}, {}, deployment, executor)
        meta.assert_called_once_with(c, reader=True)
        key.assert_called_once_with(c['namespace'], container, {})
        self.assertEqual(decode.call_args.args[1:], (b'synthetic-trusted-key', c['backendHash']))
        db.assert_called_once_with(c, {}, {})
        self.assertEqual(private, {})
        self.assertEqual(set(data), set(reader) | {'database-url'})
        self.assertIn('application_name='+task.PURPOSE, data['database-url'])
        self.assertNotIn('synthetic-trusted-key', repr(data))
        with patch.object(task, 'metadata', return_value={'platform': {}}), \
             patch.object(task.r, 'credential_key', return_value=b'synthetic-trusted-key'), \
             patch.object(task.r, 'reader_session', side_effect=ValueError('reader unavailable')), \
             patch.object(task.index, 'session') as db:
            with self.assertRaises(ValueError):
                task.session(c, {}, {}, deployment, executor)
            db.assert_not_called()

    def test_terminal_receipt_real_readback_rejects_drift_and_oversize(self):
        report = {'declaration_sha256': 'd'*64, 'intent_uid': UID, 'ticket_sha256': 'e'*64,
                  'verification_key': 'public-key', 'job': 'neutral', 'status': 'completion-not-confirmed',
                  'cleanup_confirmed': False, 'source_preserved': True, 'route_changes_authorized': False,
                  'retirement_authorized': False, 'task_activated': False, 'finished_at': '2026-01-01T00:00:00Z'}
        for drift in ('', 'uid', 'immutable', 'data'):
            captured = {}
            def create(item):
                captured.update(copy.deepcopy(item))
                return {'metadata': {'uid': UID}}
            def get(*args):
                out = copy.deepcopy(captured)
                out['metadata']['uid'] = UID
                if drift == 'uid':
                    out['metadata']['uid'] = 'changed'
                elif drift == 'immutable':
                    out['immutable'] = False
                elif drift == 'data':
                    out['data']['receipt.json'] = '{}'
                return out
            with self.subTest(drift=drift), patch.object(task.r, 'create', side_effect=create), patch.object(task, 'get', side_effect=get):
                if drift:
                    with self.assertRaises(ValueError):
                        task.terminal_receipt(declaration(), report)
                else:
                    self.assertTrue(task.terminal_receipt(declaration(), report)['confirmed'])
                    self.assertEqual(json.loads(captured['data']['receipt.json'])['status'], 'completion-not-confirmed')
                    self.assertNotIn('job_uid', json.loads(captured['data']['receipt.json']))
        with patch.object(task.r, 'create') as create, self.assertRaises(ValueError):
            task.terminal_receipt(declaration(), {**report, 'refusal_reason': 'x'*(129 << 10)})
        create.assert_not_called()

    def test_only_existing_serialized_storage_lane_runs_explicit_declaration(self):
        workflow = Path('.github/workflows/ci.yml').read_text()
        start = workflow.index('      - id: task_prepare\n')
        end = workflow.index('      - name: Check current maintenance', start)
        lane = workflow[start:end]
        self.assertIn('if [ ! -f "$config" ]', lane)
        self.assertIn('git diff --exit-code HEAD origin/main', lane)
        self.assertEqual(lane.count('scripts/compiler_input_copy_resume.py'), 2)
        self.assertIn('attempt will not be replayed', lane)
        self.assertIn('retention-days: 90', lane)
        self.assertEqual(lane.count('-m scripts.compiler_input_task_prepare '), 2)
        self.assertIn("if: always() && steps.task_prepare.outputs.selected == 'true'", lane)
    def test_trusted_stdin_does_not_write_file_or_pass_secrets_in_args(self):
        c = declaration()
        executor = {'pod': 'neutral-api', 'container': 'api'}
        with patch.object(task.subprocess, 'run', return_value=types.SimpleNamespace(returncode=0, stdout='{}')) as call:
            task.native(c, executor, 'plan', task.contract(c))
        argv = call.call_args.args[0]
        self.assertIn('-task-input-contract=-', argv)
        self.assertNotIn('/bin/sh', argv)
        self.assertEqual(json.loads(call.call_args.kwargs['input']), task.contract(c))

    def test_preview_creates_no_resources_and_consumes_no_intent(self):
        c = declaration()
        with tempfile.TemporaryDirectory() as tmp, simulated(c) as state:
            state['evidence'] = Path(tmp)/'evidence.json'
            report = task.execute(c, state['evidence'])
        self.assertEqual(report['status'], 'preview-passed')
        self.assertEqual(state['created'], [])
        self.assertNotIn('session', state['events'])
        self.assertFalse(state['unsuspended'])

    def test_success_orders_intent_then_session_and_termination_then_confirmation(self):
        c = declaration()
        with tempfile.TemporaryDirectory() as tmp, simulated(c) as state:
            state['evidence'] = Path(tmp)/'evidence.json'
            report = task.execute(c, state['evidence'], True)
        self.assertEqual(report['status'], 'task-inputs-prepared')
        self.assertTrue(report['cleanup_confirmed'])
        self.assertTrue(report['terminal_receipt']['confirmed'])
        self.assertFalse(report['task_activated'])
        self.assertLess(state['events'].index('create-ConfigMap'), state['events'].index('session'))
        self.assertLess(state['events'].index('logs'), state['events'].index('native-confirm'))
        self.assertLess(state['events'].index('cleanup'), state['events'].index('terminal'))
        secret = next(v for v in state['created'] if v['kind'] == 'Secret')
        key = base64.b64decode(secret['data']['verification-key']).decode()
        self.assertEqual(key, report['verification_key'])

    def test_failures_do_not_confirm_or_report_prepared_or_replay(self):
        for failure in ('backing', 'plan-differs', 'intent-readback', 'secret-create', 'unsuspend', 'job-uid',
                        'worker-failed', 'wrong-image', 'logs', 'final-wal', 'final-other', 'confirm', 'pin-token'):
            c = declaration()
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, simulated(c, failure) as state:
                state['evidence'] = Path(tmp)/'evidence.json'
                with self.assertRaises(ValueError):
                    task.execute(c, state['evidence'], True)
                report = json.loads(state['evidence'].read_text())
                self.assertEqual(report['status'], 'completion-not-confirmed')
                self.assertFalse(report['task_activated'])
                self.assertLessEqual(sum(x['kind'] == 'Job' for x in state['created']), 1)
                if failure not in ('confirm', 'pin-token'):
                    self.assertFalse(state['confirmed'])
                if failure == 'intent-readback':
                    self.assertNotIn('session', state['events'])
                    self.assertIsNotNone(state['terminal'])

    def test_cleanup_or_terminal_failure_cannot_be_successful_acceptance(self):
        for failure in ('cleanup', 'terminal'):
            c = declaration()
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp, simulated(c, failure) as state:
                state['evidence'] = Path(tmp)/'evidence.json'
                report = task.execute(c, state['evidence'], True)
                self.assertFalse(report['cleanup_confirmed'] and report['terminal_receipt']['confirmed'])

    def test_metadata_only_reader_material_never_enters_public_report(self):
        c = declaration()
        with patch.object(task.subprocess, 'run', return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(metadata()))) as call:
            task.metadata(c)
        sql = call.call_args.kwargs['input']
        self.assertIn('READ ONLY', sql)
        for forbidden in ('ciphertext', 'content_json', 'fugue_object_storage_config', 'INSERT', 'UPDATE', 'DELETE'):
            self.assertNotIn(forbidden, sql)


if __name__ == '__main__':
    unittest.main()
