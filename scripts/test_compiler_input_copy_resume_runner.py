import copy
from contextlib import ExitStack, contextmanager
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from scripts import compiler_input_copy as runner
from scripts import compiler_input_copy_resume as proof
from scripts import compiler_input_copy_resume_runner as trusted
from scripts.test_compiler_input_copy_resume import fixture


def declaration():
    value = fixture()
    c = copy.deepcopy(value['proof']['prior_declaration'])
    c.update(version=2, copy=value['native'], resumeProof=value['proof'], terminalReceiptRequired=True)
    return c, value


def successful_native(c):
    return dict(action='archive-copy', digest=c['copy']['digest'], source_format=c['copy']['source_format'],
                raw_bytes=65537, verified_raw_bytes=65537, segments=2, state='verified', source_preserved=True,
                route_unchanged=True, elapsed_ms=100, usage=dict(put_payload_bytes=200, get_payload_bytes=2868,
                put_requests=1, get_requests=4, source_raw_bytes=65537))


@contextmanager
def simulated_executor(c, value, failure=''):
    """No cluster calls: exercise the real runner's orchestration and cleanup."""
    uid = c['copy']['resume']['prior_receipt_uid']
    prior_proof = proof.verify(**value)
    serving = {'api_pods': {'neutral-api': uid}}
    primary = dict(status={'containerStatuses': [{'name': 'postgres', 'restartCount': 0}]})
    state = {'released': False, 'completed': False, 'reserved': [], 'deleted': [], 'terminal': []}
    native = successful_native(c)
    observed = dict(manifest={'raw_bytes': 65537}, source_chunks=2, other_leases=0, all_leases=0, epoch=0,
                    wal_bytes='100', active_clients=1, worker_connections=0, resume=value['observed'])
    def metadata(contract, credentials=False):
        result = copy.deepcopy(observed)
        if credentials: result['platform'] = {}
        if state['completed']:
            result['archive'] = dict(state='verified', verified=True, digest=c['copy']['digest'], stored_bytes=1434, raw_bytes=65537, segments=2)
        else: result['archive'] = {'state': 'uploading'}
        if failure == 'prestart-drift' and state['reserved']:
            result['resume']['archive']['token'] += 1
        return result
    def get(namespace, kind, name):
        if kind == 'job': return {'metadata': {'uid': uid}}
        raise AssertionError('unexpected read '+kind)
    def kube(namespace, verb, *args):
        if verb == 'logs':
            if state.get('before_logs'): state['before_logs']()
            if failure == 'logs-unavailable': raise readonly_refusal('copy log observation unavailable')
            return json.dumps(native)
        selfield = any(str(x).startswith('--field-selector=') for x in args)
        if selfield: return '{"items":[]}'
        if verb == 'get' and args[0] == 'jobs': return '{"items":[]}'
        if verb == 'get' and args[0] == 'pods':
            if state['deleted']: return '{"items":[]}'
            state['completed'] = True
            pod = dict(metadata=dict(name='neutral-copy-pod', uid=uid, ownerReferences=[{'uid': uid, 'controller': True}]),
                       spec=dict(nodeName=c['node'], priority=-1000000, preemptionPolicy='Never'),
                       status=dict(phase='Succeeded', containerStatuses=[dict(name='copy', restartCount=0, imageID=c['image'],
                           state={'terminated': dict(reason='Completed', exitCode=0, signal=0,
                               startedAt='2026-01-02T03:04:05Z', finishedAt='2026-01-02T03:04:06Z')})]))
            if failure == 'worker-failed':
                pod['status']['phase'] = 'Failed'
                pod['status']['containerStatuses'][0]['state']['terminated'].update(reason='Error', exitCode=2)
            if failure == 'missing-termination':
                pod['status']['containerStatuses'][0].pop('state')
            if failure == 'nonzero-success':
                pod['status']['containerStatuses'][0]['state']['terminated']['exitCode'] = 2
            return json.dumps({'items': [pod]})
        raise AssertionError('unexpected kube read')
    def create(item, dry=False):
        if not dry: state['reserved'].append(item['kind'])
        return {'metadata': {'uid': uid}}
    def release(*args):
        if failure == 'unknown-release': raise readonly_refusal('ambiguous release')
        state['released'] = True
    def terminal(contract, report):
        assert state['deleted'] and report['cleanup_confirmed']
        state['terminal'].append(copy.deepcopy(report))
        return dict(confirmed=True, uid=uid)
    def delete(*args):
        if state.get('before_cleanup'): state['before_cleanup']()
        state['deleted'].append(args)
    readonly_refusal = runner.r.Refusal
    with ExitStack() as stack:
        stack.enter_context(patch.dict(os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main', GITHUB_SHA='f'*40))
        replacements = [(runner, 'command', lambda args: 'f'*40 if args[1] == 'rev-parse' else 'store.RunCompilerInputArchiveCopy CompilerInputArchiveCopyResumeContract expectedResume:'),
                        (runner, 'node_gate', lambda *args: {}), (runner.r, 'primary_gate', lambda *args: ({}, primary, 4 << 30)),
                        (runner, 'backing_gate', lambda *args, **kwargs: {'capacity_gate_passed': True}),
                        (runner.r, 'serving_gate', lambda *args: ({}, {}, serving)), (runner, 'metadata', metadata),
                        (trusted, 'fetch_proof', lambda *args: prior_proof), (runner, 'copy_session', lambda *args: {'copy.json': '{}'}),
                        (runner, 'api_activity', lambda *args: {uid: {'write_failures': 0, 'budget_fallbacks': 0}}),
                        (runner, 'api_log_signals', lambda *args: {}), (runner, 'get', get), (runner, 'kube', kube),
                        (runner.r, 'cluster_kube', lambda *args: '{"items":[]}'), (runner.r, 'create', create),
                        (runner.r, 'api_write', release), (runner.r, 'delete_exact', delete),
                        (trusted, 'terminal_receipt', terminal)]
        for obj, key, changed in replacements: stack.enter_context(patch.object(obj, key, side_effect=changed))
        yield state


class ResumeRunnerTest(unittest.TestCase):
    def test_new_outer_contract_requires_its_own_authorization_and_complete_proof(self):
        c, _ = declaration()
        self.assertEqual(runner.validate(c), c)
        self.assertEqual(c['version'], 2)
        for key, changed in [('executionAuthorized', False), ('terminalReceiptRequired', False), ('version', 1), ('resumeProof', {})]:
            invalid = copy.deepcopy(c); invalid[key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): runner.validate(invalid)
        for key, changed in [('clusterUID', 'b'*8+'-'+('b'*4+'-')*3+'b'*12), ('database', 'other'),
                             ('minimumFreeBytes', (3 << 30)-1), ('maxWalGrowthBytes', c['maxWalGrowthBytes']+1),
                             ('maxAvailableDropBytes', c['maxAvailableDropBytes']+1), ('maxNewDeadlineFailures', 5)]:
            invalid = copy.deepcopy(c); invalid[key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): runner.validate(invalid)
        for key, changed in [('payload_bytes_per_second', (2 << 20)+1), ('max_seconds', 541)]:
            invalid = copy.deepcopy(c); invalid['copy'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): runner.validate(invalid)
        invalid = copy.deepcopy(c); invalid['executionAuthorized'] = False
        with patch.object(runner, 'metadata', side_effect=AssertionError('no DB')), patch.object(runner.r, 'create', side_effect=AssertionError('no writes')):
            with self.assertRaises(ValueError): runner.execute(invalid, None, apply=True)

    def test_trusted_fetch_binds_repository_receipt_run_and_bounded_zip(self):
        c, value = declaration()
        def github(path, limit):
            self.assertTrue(path.startswith('repos/example/neutral/actions/'))
            if path.endswith('/attempts/1'): return json.dumps(value['run']).encode()
            if path.endswith('/artifacts/123'): return json.dumps(value['artifact']).encode()
            self.assertTrue(path.endswith('/artifacts/123/zip')); self.assertEqual(limit, proof.MAX_ZIP)
            return value['raw_zip']
        observed = {'resume': value['observed']}
        with patch.dict(os.environ, GITHUB_REPOSITORY='example/neutral', GH_TOKEN='local-test-only'), \
                patch.object(trusted, 'command', return_value=json.dumps(c['resumeProof']['prior_declaration'])) as command, \
                patch.object(trusted, 'get', return_value=value['receipt']) as get, patch.object(trusted, 'github_bytes', side_effect=github) as fetch:
            result = trusted.fetch_proof(c, observed)
            self.assertTrue(result['proof_verified']); self.assertFalse(result['execution_authorized'])
            self.assertEqual(fetch.call_count, 3)
            self.assertEqual(get.call_args.args[:2], (c['namespace'], 'configmap'))
            self.assertEqual(command.call_args.args[0], ['git', 'show', 'f'*40+':deploy/example/copy.json'])
            with patch.dict(os.environ, GITHUB_REPOSITORY='example/other'):
                with self.assertRaises(ValueError): trusted.fetch_proof(c, observed)
            self.assertEqual(fetch.call_count, 3)
            with patch.dict(os.environ, GH_TOKEN=''):
                with self.assertRaises(ValueError): trusted.fetch_proof(c, observed)
            self.assertEqual(fetch.call_count, 3)
            with patch.object(trusted, 'command', return_value='{}'):
                with self.assertRaises(ValueError): trusted.fetch_proof(c, observed)
            self.assertEqual(fetch.call_count, 3)

    def test_github_stream_stops_at_byte_bound_failure_or_timeout(self):
        real = subprocess.Popen
        processes = []
        def run(program):
            def spawn(args, **kwargs):
                self.assertEqual(args[:4], ['gh', 'api', '--hostname', 'github.com'])
                p = real([sys.executable, '-c', program], **kwargs)
                processes.append(p); return p
            return patch.object(trusted.subprocess, 'Popen', side_effect=spawn)
        with run('import sys; sys.stdout.buffer.write(b"bounded")'):
            self.assertEqual(trusted.github_bytes('repos/example/neutral/actions/runs/1', 8), b'bounded')
        with run('import sys; sys.stdout.buffer.write(b"x"*100000)'):
            with self.assertRaises(ValueError): trusted.github_bytes('unused', 16)
        with run('raise SystemExit(1)'):
            with self.assertRaises(ValueError): trusted.github_bytes('unused', 16)
        with run('import time; time.sleep(30)'), patch.object(trusted.select, 'select', return_value=([], [], [])):
            with self.assertRaises(ValueError): trusted.github_bytes('unused', 16)
        self.assertTrue(all(p.poll() is not None for p in processes))

    def test_resume_receipt_counts_full_gets_and_only_remaining_puts(self):
        c, _ = declaration(); native = successful_native(c)
        self.assertEqual(runner.result_contract(json.dumps(native), c), native)
        for key, changed in [('get_payload_bytes', 400), ('get_requests', 2), ('put_requests', 2)]:
            invalid = copy.deepcopy(native); invalid['usage'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): runner.result_contract(json.dumps(invalid), c)
        failed = copy.deepcopy(native)
        failed.update(state='', segments=0, verified_raw_bytes=0, source_preserved=False, route_unchanged=False,
                      failure=dict(stage='source-read', category='55P03', segment_ordinal=1, segment_bytes_read=0))
        failed['usage'].update(put_requests=0, put_payload_bytes=0, get_requests=1, get_payload_bytes=1234, source_raw_bytes=65536)
        self.assertEqual(runner.result_contract(json.dumps(failed), c, success=False), failed)
        with self.assertRaises(ValueError): runner.result_contract(json.dumps(failed), c)

    def test_cumulative_actual_usage_never_loses_the_first_attempt(self):
        c, value = declaration(); native = successful_native(c)
        result = proof.verify(**value)
        total = trusted.cumulative_usage(c, result['prior_usage'], native['usage'])
        self.assertEqual(total, dict(put_payload_bytes=1434, get_payload_bytes=4102,
                                    put_requests=2, get_requests=5, source_raw_bytes=131073))
        for key in proof.USAGE_TO_LIMIT:
            invalid = copy.deepcopy(native['usage']); invalid[key] = c['resumeProof']['cumulative_budget'][proof.USAGE_TO_LIMIT[key]]
            with self.subTest(key=key), self.assertRaises(ValueError): trusted.cumulative_usage(c, result['prior_usage'], invalid)

    def test_terminal_receipt_is_create_only_bounded_and_read_back(self):
        c, value = declaration()
        result = proof.verify(**value)
        report = dict(declaration_sha256=runner.r.identity(c), job='neutral-copy', job_uid=c['copy']['resume']['prior_receipt_uid'],
                      status='copy-verified-source-preserved', cleanup_confirmed=True, retirement_authorized=False,
                      route_changes_authorized=False, execution={'pod_uid': 'neutral'}, native_result=successful_native(c),
                      cumulative_usage=trusted.cumulative_usage(c, result['prior_usage'], successful_native(c)['usage']),
                      finished_at='2026-01-02T03:04:05Z')
        created = []
        def create(cm):
            self.assertTrue(cm['immutable']); self.assertEqual(cm['metadata']['name'], 'neutral-copy-result')
            self.assertEqual(set(cm['data']), {'declaration-sha256', 'result.json'})
            self.assertNotIn('credentials', cm['data']['result.json'])
            cm = copy.deepcopy(cm); cm['metadata']['uid'] = c['copy']['resume']['prior_receipt_uid']
            created.append(cm); return cm
        with patch.object(trusted.readonly, 'create', side_effect=create), patch.object(trusted, 'get', side_effect=lambda *args: created[-1]):
            self.assertTrue(trusted.terminal_receipt(c, report)['confirmed'])
        with patch.object(trusted.readonly, 'create', side_effect=ValueError('already exists')):
            with self.assertRaises(ValueError): trusted.terminal_receipt(c, report)
        report['cleanup_confirmed'] = False
        with patch.object(trusted.readonly, 'create', side_effect=AssertionError('no write')):
            with self.assertRaises(ValueError): trusted.terminal_receipt(c, report)

    def test_real_executor_uses_prefix_aware_completion_and_exact_cleanup(self):
        c, value = declaration()
        with tempfile.TemporaryDirectory() as directory, simulated_executor(c, value) as state:
            report = runner.execute(c, Path(directory)/'receipt.json', apply=True)
        self.assertTrue(state['released']); self.assertTrue(report['cleanup_confirmed'])
        self.assertEqual(report['status'], 'copy-verified-source-preserved')
        self.assertEqual(report['cumulative_usage']['get_requests'], 5)
        self.assertTrue(report['terminal_receipt']['confirmed'])
        self.assertEqual([item[1] for item in state['deleted']], ['Job', 'PriorityClass'])
        self.assertEqual(len(state['terminal']), 1)
        self.assertEqual(state['reserved'], ['ConfigMap', 'PriorityClass', 'Job', 'Secret'])

    def test_termination_is_durable_before_log_failure_or_cleanup(self):
        for failure in ('worker-failed', 'logs-unavailable', 'missing-termination', 'nonzero-success'):
            c, value = declaration()
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory, simulated_executor(c, value, failure=failure) as state:
                target = Path(directory)/'receipt.json'
                checks = []
                def saved():
                    observed = json.loads(target.read_text())['execution']['terminal']
                    self.assertEqual(observed['phase'], 'Failed' if failure == 'worker-failed' else 'Succeeded')
                    self.assertEqual(observed['metadata_complete'], failure != 'missing-termination')
                    checks.append(observed)
                state['before_logs'] = state['before_cleanup'] = saved
                with self.assertRaises(ValueError): runner.execute(c, target, apply=True)
                report = json.loads(target.read_text())
            self.assertGreaterEqual(len(checks), 3)
            self.assertEqual(report['execution']['terminal'], checks[0])
            self.assertEqual(report['status'], 'completion-not-confirmed')
            self.assertTrue(report['cleanup_confirmed'])
            self.assertNotIn('result', report)
            if failure == 'logs-unavailable': self.assertNotIn('native_result', report)
            else: self.assertEqual(len(state['terminal']), 1)

    def test_preview_never_creates_execution_or_terminal_receipts(self):
        c, value = declaration()
        with tempfile.TemporaryDirectory() as directory, simulated_executor(c, value) as state:
            report = runner.execute(c, Path(directory)/'receipt.json', apply=False)
        self.assertEqual(report['status'], 'preview-passed')
        self.assertFalse(state['released']); self.assertFalse(state['reserved']); self.assertFalse(state['terminal'])

    def test_prestart_drift_and_ambiguous_release_refuse_without_replay(self):
        for failure in ('prestart-drift', 'unknown-release'):
            c, value = declaration()
            with tempfile.TemporaryDirectory() as directory, simulated_executor(c, value, failure=failure) as state:
                target = Path(directory)/'receipt.json'
                with self.subTest(failure=failure), self.assertRaises(ValueError): runner.execute(c, target, apply=True)
                report = json.loads(target.read_text())
            self.assertFalse(state['released']); self.assertTrue(report['cleanup_confirmed'])
            self.assertEqual(report['status'], 'completion-not-confirmed')
            self.assertFalse(state['terminal'])
            self.assertEqual(state['reserved'].count('Job'), 1)


class ResumeMetadataSQLTest(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('FUGUE_INPUT_ARCHIVE_TEST_PG_DSN'), 'explicit local PostgreSQL fixture required')
    def test_actual_postgres_metadata_is_one_bounded_readonly_snapshot(self):
        from urllib.parse import urlsplit
        dsn = os.environ['FUGUE_INPUT_ARCHIVE_TEST_PG_DSN']
        parsed = urlsplit(dsn)
        self.assertIn(parsed.hostname, ('127.0.0.1', 'localhost'))
        self.assertTrue(parsed.path.startswith('/fugue_test_'))
        c, value = declaration(); m = value['observed']; source = m['source']; archive = m['archive']; segment = m['receipts'][0]
        def literal(v): return "'"+str(v).replace("'", "''")+"'"
        sql = '''CREATE TEMP TABLE fugue_platform_input_manifests(content_hash text, format text, storage_location text, size_bytes bigint, chunk_size int, chunk_count int, created_at timestamptz);
CREATE TEMP TABLE fugue_platform_input_archives(content_hash text, archive_format text, state text, verified_at timestamptz, verified_digest text, backend_hash text, object_prefix text, source_epoch bigint, fencing_token bigint, source_format text, raw_bytes bigint, segment_bytes int, segment_count int);
CREATE TEMP TABLE fugue_platform_input_archive_segments(content_hash text, ordinal int, object_key text, raw_bytes int, stored_bytes int, segment_hash text, stored_hash text, uploaded_at timestamptz, verified_at timestamptz);
CREATE TEMP TABLE fugue_platform_input_archive_control(singleton bool, epoch bigint, writes_fenced bool, retirement_started_at timestamptz);
CREATE TEMP TABLE fugue_platform_input_archive_leases(expires_at timestamptz);
'''
        sql += 'INSERT INTO fugue_platform_input_manifests VALUES('+','.join(literal(source[k]) for k in ('digest','format','location','raw_bytes','chunk_size','chunk_count','created_utc'))+');\n'
        sql += 'INSERT INTO fugue_platform_input_archives VALUES('+','.join([literal(source['digest']), literal(archive['archive_format']), literal('uploading'), 'NULL', literal('')]+[literal(archive[k]) for k in ('backend_hash','prefix','epoch','token','source_format','raw_bytes','segment_bytes','segment_count')])+');\n'
        sql += 'INSERT INTO fugue_platform_input_archive_segments VALUES('+','.join([literal(source['digest'])]+[literal(segment[k]) for k in ('ordinal','key','raw_bytes','stored_bytes','raw_hash','stored_hash')]+['now()', 'now()'])+');\n'
        sql += 'INSERT INTO fugue_platform_input_archive_control VALUES(true,0,false,NULL);\n'
        sql += "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB';\n"
        sql += 'SELECT '+trusted.metadata_sql(c)+';\nCOMMIT;\n'
        result = subprocess.run(['psql', dsn, '-X', '-qAt', '-v', 'ON_ERROR_STOP=1'], input=sql, text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        observed = json.loads(result.stdout)
        self.assertEqual(observed, m)
        proof.verify_metadata(c['copy'], observed)
        self.assertNotIn('data', trusted.metadata_sql(c))


if __name__ == '__main__': unittest.main()
