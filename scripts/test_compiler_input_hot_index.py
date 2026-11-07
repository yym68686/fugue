import base64
import copy
from contextlib import ExitStack, contextmanager
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts import compiler_input_hot_index as h
from scripts.test_compiler_input_copy import declaration as copy_declaration


def declaration():
    c = copy_declaration()
    del c['copy']
    c.update(page={'afterID': '', 'limit': 8}, maxWalGrowthBytes=256 << 20, maxAvailableDropBytes=128 << 20)
    return c


def result():
    return {'action': 'archive-hot-index', 'observed_at': '2026-01-01T00:00:00Z', 'selected': 1,
            'next_after_id': 'neutral-artifact', 'source_bytes': 1024, 'index_bytes': 64,
            'wal_bytes': 8192, 'applied': True, 'retirement_authorized': False}


def metadata():
    return {'epoch': 0, 'fenced': False, 'leases': 0, 'wal_lsn': 'A/100', 'wal_reset': '2026-01-01 00:00:00+00',
            'active_clients': 2, 'worker_connections': 0, 'index_present': True}


@contextmanager
def simulated_executor(c, failure=''):
    uid = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
    state = {'created': [], 'released': False, 'deleted': [], 'completed': False, 'terminal': None}
    primary = {'status': {'containerStatuses': [{'name': 'postgres', 'restartCount': 0}]}}
    serving = {'api_pods': {'neutral-api': uid}}
    def meta(*args):
        value = metadata()
        if failure == 'prestart-epoch' and 'Job' in state['created']:
            value['epoch'] = 1
        if failure == 'final-wal' and state['completed']:
            value['wal_lsn'] = 'A/10000101'
        return value
    def activity(*args):
        outer = dict(schema='fugue.compiler-input-writer.observation.v1', attempts=0, in_flight=0,
                     successful_returns=0, deadline_failures=0, canceled_failures=0, other_failures=0)
        if failure == 'prestart-canceled' and 'Job' in state['created']:
            outer.update(attempts=1, canceled_failures=1)
        if failure == 'final-other' and state['completed']:
            outer.update(attempts=1, other_failures=1)
        return {uid: dict(write_failures=0, budget_fallbacks=0, category='', outer_writer=outer)}
    def create(value, dry=False):
        if not dry:
            state['created'].append(value['kind'])
            if value['kind'] == 'Secret' and failure == 'secret-create':
                raise h.r.Refusal('session create unconfirmed')
        return {'metadata': {'uid': uid}}
    def release(*args):
        if failure == 'unknown-release':
            raise h.r.Refusal('release completion unknown')
        state['released'] = True
    def terminal(c, report):
        assert report['cleanup_confirmed'] and state['deleted']
        state['terminal'] = copy.deepcopy(report)
        return {'uid': uid, 'confirmed': failure != 'receipt-readback'}
    def kube(namespace, verb, *args):
        if verb == 'logs':
            if state.get('before_logs'):
                state['before_logs']()
            if failure == 'logs-unavailable':
                raise h.r.Refusal('bounded logs unavailable')
            return json.dumps(result() if failure != 'invalid-result' else {**result(), 'retirement_authorized': True})
        if any(str(x).startswith('--field-selector=') for x in args):
            return '{"items":[]}'
        if verb == 'get' and args[0] == 'jobs':
            return '{"items":[]}'
        if verb == 'get' and args[0] == 'pods':
            if state['deleted']:
                return '{"items":[]}'
            state['completed'] = True
            status = dict(name='index', restartCount=0, imageID=c['image'], state={'terminated':
                dict(reason='Completed', exitCode=0, signal=0, startedAt='2026-01-01T00:00:00Z', finishedAt='2026-01-01T00:00:01Z')})
            pod = dict(metadata=dict(name='neutral-index-pod', uid=uid, ownerReferences=[{'controller': True, 'uid': uid}],
                                     annotations={'fugue.pro/source-commit': c['sourceCommit']}),
                       spec=dict(nodeName=c['node'], priority=-1000000, preemptionPolicy='Never'),
                       status=dict(phase='Succeeded', containerStatuses=[status]))
            if failure == 'worker-failed':
                pod['status']['phase'] = 'Failed'
                status['state']['terminated'].update(exitCode=2, reason='Error')
            if failure == 'missing-termination':
                status.pop('state')
            if failure == 'nonzero-success':
                status['state']['terminated']['exitCode'] = 2
            if failure == 'wrong-image':
                status['imageID'] = 'different'
            return json.dumps({'items': [pod]})
        raise AssertionError('unexpected kubectl call')
    def delete(*args):
        if state.get('before_cleanup'):
            state['before_cleanup']()
        if failure == 'cleanup-unavailable':
            raise h.r.Refusal('cleanup unconfirmed')
        state['deleted'].append(args)
    with ExitStack() as stack:
        stack.enter_context(patch.dict(h.os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main', GITHUB_SHA='a'*40))
        replacements = [(h, 'command', lambda argv: 'a'*40 if argv[1] == 'rev-parse' else 'store.RunCompilerInputHotIndex'),
                        (h.impact, 'node_gate', lambda *args: {}), (h.r, 'primary_gate', lambda *args: ({}, primary, 4 << 30)),
                        (h.impact, 'backing_gate', lambda *args, **kwargs: {'capacity_gate_passed': failure != 'backing-reserve'}),
                        (h.r, 'serving_gate', lambda *args: ({}, {}, serving)), (h, 'metadata', meta),
                        (h.impact, 'api_activity', activity), (h.impact, 'api_log_signals', lambda *args: {}),
                        (h, 'session', lambda *args: {'database-url': 'synthetic-private'}), (h, 'kube', kube),
                        (h, 'get', lambda *args: {'metadata': {'uid': uid}}),
                        (h.r, 'cluster_get', lambda *args: dict(metadata={'uid': uid}, value=-1000000, preemptionPolicy='Never')),
                        (h.r, 'cluster_kube', lambda *args: '{"items":[]}'), (h.r, 'create', create),
                        (h.r, 'api_write', release), (h.r, 'delete_exact', delete), (h, 'terminal_receipt', terminal),
                        (h.time, 'sleep', lambda *args: None)]
        for obj, name, effect in replacements:
            stack.enter_context(patch.object(obj, name, side_effect=effect))
        yield state


class HotIndexTest(unittest.TestCase):
    def test_scope_and_original_resource_stops_cannot_expand(self):
        c = declaration()
        self.assertEqual(h.validate(c), c)
        for key, value in [('version', True), ('executionAuthorized', False), ('minimumFreeBytes', (3 << 30)-1),
                           ('maxWalGrowthBytes', (256 << 20)+1), ('maxAvailableDropBytes', (128 << 20)+1),
                           ('maxNewDeadlineFailures', 5), ('maxNewDeadlineFailures', True), ('copy', {}), ('readCount', 0)]:
            changed = copy.deepcopy(c)
            changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                h.validate(changed)
        for page in ({'afterID': '', 'limit': 33}, {'afterID': 'a;exit', 'limit': 1},
                     {'afterID': 'a\nb', 'limit': 1}, {'afterID': '', 'limit': True},
                     {'afterID': '', 'limit': 1, 'pages': 2}):
            changed = copy.deepcopy(c)
            changed['page'] = page
            with self.subTest(page=page), self.assertRaises(ValueError):
                h.validate(changed)

    def test_worker_has_only_one_explicit_native_page_and_no_archive_keys(self):
        c = declaration()
        job = h.job_manifest(c, 'neutral-worker', 'session')
        spec = job['spec']
        pod = spec['template']['spec']
        container = pod['containers'][0]
        self.assertEqual((spec['backoffLimit'], spec['activeDeadlineSeconds'], spec['parallelism'], spec['completions']), (0, 600, 1, 1))
        self.assertTrue(spec['suspend'])
        self.assertFalse(pod['automountServiceAccountToken'])
        self.assertEqual(pod['preemptionPolicy'], 'Never')
        self.assertEqual(container['command'], ['nice', '-n', '19', '/usr/local/bin/fugue-input-storage'])
        self.assertEqual(container['args'], ['-mode=archive-hot-index', '-apply', '-hot-index-after=', '-hot-index-limit=8'])
        self.assertEqual(container['resources']['requests'], {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': '256Mi'})
        self.assertEqual(container['resources']['requests'], container['resources']['limits'])
        env = {x['name']: x for x in container['env']}
        self.assertEqual(set(env), {'FUGUE_DATABASE_URL', 'PGOPTIONS', 'GOMAXPROCS', 'GOMEMLIMIT'})
        body = json.dumps(job)
        for forbidden in ('/bin/sh', 'R2', 'SIGNING', 'ENCRYPTION_KEY', 'hostPath', 'hostNetwork', 'envFrom', '-rehearse', '-mode=archive-hot-inventory'):
            self.assertNotIn(forbidden, body)

    def test_session_reuses_db_role_without_fetching_archive_credentials(self):
        c = declaration()
        cluster = {'spec': {'bootstrap': {'initdb': {'secret': {'name': 'app-role'}, 'database': c['database'], 'owner': 'app'}}}}
        secret = {'data': {k: base64.b64encode(v.encode()).decode() for k, v in [('username', 'app'), ('password', 'synthetic-private-value')]}}
        with patch.object(h.r, 'get', return_value=secret), patch.object(h.r, 'credential_key') as key, patch.object(h.r, 'reader_session') as reader:
            data = h.session(c, cluster, {'status': {'podIP': '127.0.0.1'}})
        key.assert_not_called()
        reader.assert_not_called()
        self.assertEqual(set(data), {'database-url'})
        parsed = h.urllib.parse.urlsplit(data['database-url'])
        query = h.urllib.parse.parse_qs(parsed.query)
        self.assertEqual(query['sslmode'], ['require'])
        self.assertEqual(query['application_name'], [h.PURPOSE])
        self.assertNotIn('default_transaction_read_only', query['options'][0])

    def test_metadata_reads_no_body_and_wal_reset_and_pressure_refuse(self):
        c = declaration()
        before = metadata()
        with patch.object(h.subprocess, 'run', return_value=types.SimpleNamespace(returncode=0, stdout=json.dumps(before))) as run:
            self.assertEqual(h.metadata(c), before)
        query = run.call_args.kwargs['input']
        for forbidden in ('content_json', 'ciphertext', 'fugue_object_storage_config', 'INSERT', 'UPDATE', 'DELETE'):
            self.assertNotIn(forbidden, query)
        self.assertIn('READ ONLY', query)
        h.check_metadata(c, before, {**before, 'wal_lsn': 'A/200', 'worker_connections': 1})
        for key, value in [('epoch', 1), ('wal_reset', 'changed'), ('wal_lsn', 'A/FF'),
                           ('wal_lsn', 'A/10000101'), ('leases', 1), ('fenced', True), ('index_present', False),
                           ('active_clients', 7), ('worker_connections', 2)]:
            with self.subTest(key=key), self.assertRaises(ValueError):
                h.check_metadata(c, before, {**before, key: value})

    def test_native_receipt_refuses_unknown_bytes_invalid_cursor_or_mutations(self):
        c = declaration()
        good = result()
        self.assertEqual(h.result_contract(json.dumps(good), c), good)
        for key, value in [('selected', True), ('selected', 9), ('source_bytes', 32 << 20),
                           ('wal_bytes', (256 << 20)+1), ('next_after_id', ''), ('observed_at', 'not-a-time'),
                           ('applied', False), ('retirement_authorized', True), ('private', 'hidden')]:
            changed = {**good, key: value}
            with self.subTest(key=key), self.assertRaises(ValueError):
                h.result_contract(json.dumps(changed), c)
        with self.assertRaises(ValueError):
            h.result_contract(json.dumps(good)[:-1]+',"applied":true}', c)
        with self.assertRaises(ValueError):
            h.result_contract(json.dumps(good)+'\n'+json.dumps(good), c)
        empty = {**good, 'selected': 0, 'next_after_id': '', 'source_bytes': 0, 'index_bytes': 0}
        self.assertEqual(h.result_contract(json.dumps(empty), c), empty)

    def test_untrusted_context_cannot_create_any_resource(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'result.json'
            with patch.dict(h.os.environ, {'GITHUB_ACTIONS': 'false'}, clear=True), patch.object(h.r, 'create') as create:
                with self.assertRaises(ValueError):
                    h.execute(declaration(), path, apply=True)
                create.assert_not_called()
            evidence = json.loads(path.read_text())
            self.assertEqual(evidence['status'], 'completion-not-confirmed')
            self.assertFalse(evidence['retirement_authorized'])

    def test_replayed_declaration_refuses_before_job_or_secret(self):
        c = declaration()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'result.json'
            primary = {'status': {'containerStatuses': [{'name': 'postgres', 'restartCount': 0}]}}
            serving = {'api_pods': {}}
            created = []
            def create(value, dry=False):
                created.append((value, dry))
                if value['kind'] == 'ConfigMap':
                    raise h.r.Refusal('existing immutable receipt')
                return {'metadata': {'uid': 'neutral'}}
            with patch.dict(h.os.environ, {'GITHUB_ACTIONS': 'true', 'GITHUB_REF': 'refs/heads/main', 'GITHUB_SHA': 'a'*40}), \
                 patch.object(h, 'command', side_effect=lambda argv: 'a'*40 if argv[1] == 'rev-parse' else 'store.RunCompilerInputHotIndex'), \
                 patch.object(h.impact, 'node_gate', return_value={}), patch.object(h.r, 'primary_gate', return_value=({}, primary, 4 << 30)), \
                 patch.object(h.impact, 'backing_gate', return_value={'capacity_gate_passed': True}), \
                 patch.object(h.r, 'serving_gate', return_value=({}, {}, serving)), patch.object(h, 'metadata', return_value=metadata()), \
                 patch.object(h.impact, 'api_activity', return_value={}), patch.object(h.impact, 'api_log_signals', return_value={}), \
                 patch.object(h, 'session', return_value={'database-url': 'synthetic-private'}), \
                 patch.object(h, 'kube', return_value='{"items":[]}'), patch.object(h.r, 'create', side_effect=create):
                with self.assertRaises(ValueError):
                    h.execute(c, path, apply=True)
            self.assertEqual([x[0]['kind'] for x in created], ['PriorityClass', 'Job', 'ConfigMap'])
            self.assertTrue(all(dry for _, dry in created[:2]))
            self.assertNotIn('synthetic-private', path.read_text())

    def test_success_and_preview_have_distinct_receipted_mutation_scope(self):
        c = declaration()
        for apply in (False, True):
            with self.subTest(apply=apply), tempfile.TemporaryDirectory() as directory, simulated_executor(c) as state:
                target = Path(directory)/'result.json'
                report = h.execute(c, target, apply=apply)
                body = target.read_text()
            self.assertNotIn('synthetic-private', body)
            self.assertEqual(state['released'], apply)
            self.assertEqual(report['status'], 'index-page-committed' if apply else 'preview-passed')
            self.assertEqual(report['cleanup_confirmed'], apply)
            self.assertFalse(report['retirement_authorized'])
            if apply:
                self.assertEqual(state['created'], ['ConfigMap', 'PriorityClass', 'Job', 'Secret'])
                self.assertTrue(report['terminal_receipt']['confirmed'])
                self.assertEqual([x[1] for x in state['deleted']], ['Job', 'PriorityClass'])
                self.assertEqual(report['result'], result())
            else:
                self.assertFalse(state['created']); self.assertFalse(state['deleted']); self.assertIsNone(state['terminal'])

    def test_backing_reserve_refuses_before_credentials_or_any_resources(self):
        c = declaration()
        with tempfile.TemporaryDirectory() as directory, simulated_executor(c, 'backing-reserve') as state, \
                patch.object(h, 'session', side_effect=AssertionError('must not fetch credentials')):
            with self.assertRaisesRegex(ValueError, 'backing reserve unavailable'):
                h.execute(c, Path(directory)/'result.json', apply=True)
        self.assertFalse(state['created']); self.assertFalse(state['released'])

    def test_prestart_drift_or_ambiguous_release_consumes_intent_without_retry(self):
        for failure in ('prestart-epoch', 'prestart-canceled', 'unknown-release', 'secret-create'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory, simulated_executor(declaration(), failure) as state:
                target = Path(directory)/'result.json'
                with self.assertRaises(ValueError):
                    h.execute(declaration(), target, apply=True)
                report = json.loads(target.read_text())
            self.assertFalse(state['released']); self.assertTrue(report['cleanup_confirmed'])
            self.assertEqual(report['status'], 'completion-not-confirmed')
            self.assertEqual(state['created'].count('Job'), 1)
            self.assertIn('intent_uid', report)

    def test_termination_is_saved_before_logs_and_cleanup_on_failures(self):
        for failure in ('worker-failed', 'missing-termination', 'nonzero-success', 'logs-unavailable', 'invalid-result'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory, simulated_executor(declaration(), failure) as state:
                target = Path(directory)/'result.json'
                checks = []
                def saved():
                    value = json.loads(target.read_text())['execution']['terminal']
                    checks.append(value)
                    self.assertEqual(value['phase'], 'Failed' if failure == 'worker-failed' else 'Succeeded')
                    self.assertEqual(value['metadata_complete'], failure != 'missing-termination')
                state['before_logs'] = state['before_cleanup'] = saved
                with self.assertRaises(ValueError):
                    h.execute(declaration(), target, apply=True)
                report = json.loads(target.read_text())
            self.assertGreaterEqual(len(checks), 3)
            self.assertEqual(report['execution']['terminal'], checks[0])
            self.assertTrue(report['cleanup_confirmed'])
            self.assertEqual(report['status'], 'completion-not-confirmed')
            self.assertNotIn('result', report)

    def test_final_impact_and_worker_identity_never_become_completion(self):
        for failure in ('final-wal', 'final-other', 'wrong-image'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory, simulated_executor(declaration(), failure) as state:
                target = Path(directory)/'result.json'
                with self.assertRaises(ValueError):
                    h.execute(declaration(), target, apply=True)
                report = json.loads(target.read_text())
            self.assertTrue(state['released']); self.assertTrue(report['cleanup_confirmed'])
            self.assertEqual(report['status'], 'completion-not-confirmed')

    def test_cleanup_or_receipt_failure_is_not_successful_execution(self):
        for failure in ('cleanup-unavailable', 'receipt-readback'):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory, simulated_executor(declaration(), failure) as state:
                target = Path(directory)/'result.json'
                report = h.execute(declaration(), target, apply=True)
            self.assertFalse(report['cleanup_confirmed'] and report.get('terminal_receipt', {}).get('confirmed') is True)
            if failure == 'cleanup-unavailable':
                self.assertIsNone(state['terminal'])

    def test_terminal_receipt_is_immutable_and_read_back_exactly(self):
        c = declaration()
        report = dict(declaration_sha256='a'*64, job='neutral-index', job_uid='neutral-uid', status='index-page-committed',
                      cleanup_confirmed=True, source_preserved=True, route_changes_authorized=False, retirement_authorized=False,
                      finished_at='2026-01-01T00:00:01Z', result=result())
        state = {}
        def create(item):
            state['item'] = {**copy.deepcopy(item), 'metadata': {**item['metadata'], 'uid': 'neutral-receipt'}}
            return state['item']
        with patch.object(h.r, 'create', side_effect=create), patch.object(h, 'get', side_effect=lambda *args: state['item']):
            self.assertTrue(h.terminal_receipt(c, report)['confirmed'])
            self.assertTrue(state['item']['immutable'])
            self.assertEqual(set(state['item']['data']), {'declaration-sha256', 'receipt.json'})
        with patch.object(h.r, 'create', side_effect=create), patch.object(h, 'get', return_value={'metadata': {'uid': 'changed'}}):
            with self.assertRaises(ValueError):
                h.terminal_receipt(c, report)


if __name__ == '__main__':
    unittest.main()
