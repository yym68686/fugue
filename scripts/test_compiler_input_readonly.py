import base64
import copy
from contextlib import ExitStack, contextmanager
import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch

from scripts import compiler_input_readonly as r

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    CRYPTO_AVAILABLE = True
except ModuleNotFoundError:
    AESGCM = None
    CRYPTO_AVAILABLE = False


def declaration():
    return dict(version=2, executionAuthorized=True, namespace='platform-system', node='runtime-one', nodeUID='1'*8+'-'+'1'*4+'-'+'1'*4+'-'+'1'*4+'-'+'1'*12,
                sourceCommit='a'*40, image='registry.example/platform@sha256:'+'b'*64, cluster='platform-postgres', clusterUID='2'*8+'-'+'2'*4+'-'+'2'*4+'-'+'2'*4+'-'+'2'*12,
                primary='platform-postgres-1', primaryUID='3'*8+'-'+'3'*4+'-'+'3'*4+'-'+'3'*4+'-'+'3'*12,
                postgresContainer='postgres', dataPath='/var/lib/postgresql/data', database='platform', digest='sha256:'+'c'*64,
                rawBytes=173000000, storedBytes=16000000, segments=11, readCount=3, apiService='platform-api', readyURL='https://control.example/readyz', minimumFreeBytes=1 << 30)


def failure():
    return dict(schema='fugue.compiler-input-hot-inventory-failure/v1', inspection_complete=False, metadata_complete=False, local_copies_verified=False,
                retirement_authorized=False, plan_sha256='', stage='dependency-graph', reason='deadline', category='deadline', coherent_metadata=True,
                root_inventory_complete=True, root_counts={'active-release': 3},
                progress=dict(phase='resolve-identities', resolved_nodes=24, unresolved_nodes=3, edges=40, root_bindings=0, resolver_calls=2))


def read(c):
    return dict(action='archive-read-check', digest=c['digest'], read_only=True, verified=True, retirement_authorized=False, raw_bytes=c['rawBytes'],
                stored_bytes=c['storedBytes'], segments=c['segments'], metadata_elapsed_ms=12, readback_elapsed_ms=5200, total_elapsed_ms=5212)


def metadata_declaration():
    c = declaration()
    c.update(version=3, readCount=0)
    for key in ('digest', 'rawBytes', 'storedBytes', 'segments'):
        del c[key]
    return c


@contextmanager
def simulated_executor(c, fault=''):
    pod = {'metadata': {'name': 'readonly-pod', 'uid': 'pod-uid', 'annotations': {'fugue.pro/source-commit': c['sourceCommit']},
                       'ownerReferences': [{'kind': 'Job', 'controller': True, 'uid': 'job-uid'}]},
           'spec': {'nodeName': c['node'], 'priority': -1000000, 'preemptionPolicy': 'Never'},
           'status': {'phase': 'Failed', 'containerStatuses': [{'name': 'readonly', 'imageID': c['image'], 'restartCount': 0,
               'state': {'terminated': {'reason': 'Error', 'exitCode': 1, 'signal': 0,
                   'startedAt': '2026-01-01T00:00:00Z', 'finishedAt': '2026-01-01T00:00:01Z'}}}]}}
    if fault == 'missing-terminal':
        del pod['status']['containerStatuses'][0]['state']
    state = {'deleted': [], 'created': [], 'node_calls': [], 'log_commands': []}
    def kube(ns, *args):
        if args[:2] in (('get', 'jobs'), ('get', 'events'), ('get', 'secret')):
            return '{"items":[]}'
        if args[:2] == ('get', 'pods'):
            items = [pod] if not state['deleted'] else []
            if state['deleted'] and fault == 'unrelated-pod':
                other = copy.deepcopy(pod)
                other['metadata']['ownerReferences'][0]['uid'] = 'not-ours'
                items = [other]
            return json.dumps({'items': items})
        if args[0] == 'logs':
            state['log_commands'].append(args)
            if fault == 'logs-unavailable':
                raise RuntimeError('synthetic unavailable log')
            return 'x'*16385 if fault == 'logs-overflow' else json.dumps(failure())
        raise AssertionError('unexpected command')
    def get(ns, kind, name):
        if kind == 'job':
            return {'metadata': {'uid': 'job-uid'}}
        value = copy.deepcopy(pod)
        if fault == 'replaced-pod':
            value['metadata']['uid'] = 'replacement'
        return value
    def create(item, dry=False):
        state['created'].append(copy.deepcopy(item))
        return {'metadata': {'uid': {'Job': 'job-uid', 'Secret': 'secret-uid', 'PriorityClass': 'priority-uid'}.get(item['kind'], 'receipt-uid')}}
    def node(config, uid=''):
        state['node_calls'].append(uid)
        return {}
    with ExitStack() as stack:
        stack.enter_context(patch.dict(r.os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main', GITHUB_SHA='d'*40))
        for key, function in [('command', lambda *a: 'd'*40), ('node_gate', node),
                ('primary_gate', lambda *a: ({}, {'status': {'containerStatuses': []}}, 10 << 30)),
                ('serving_gate', lambda *a: ({}, {}, {})), ('sql_metadata', lambda *a: {}),
                ('session', lambda *a: {'private-session': 'never-record-this'}), ('kube', kube), ('get', get), ('create', create),
                ('cluster_get', lambda *a: {'metadata': {'uid': 'priority-uid'}, 'value': -1000000, 'preemptionPolicy': 'Never'}),
                ('cluster_kube', lambda *a: '{"items":[]}'), ('api_write', lambda *a: {}),
                ('delete_exact', lambda c, kind, name, uid: state['deleted'].append((kind, uid)))]:
            stack.enter_context(patch.object(r, key, side_effect=function))
        yield state


class ReadonlyTest(unittest.TestCase):
    def test_terminal_evidence_survives_log_failure_or_replaced_pod(self):
        for fault in ('logs-unavailable', 'logs-overflow', 'replaced-pod'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as tmp, simulated_executor(metadata_declaration(), fault) as state:
                path = Path(tmp)/'result.json'
                with self.assertRaises((ValueError, RuntimeError)):
                    r.execute(metadata_declaration(), path, True, output_limit=16384)
                report = json.loads(path.read_text())
                self.assertEqual(report['status'], 'completion-not-confirmed')
                self.assertTrue(report['execution']['terminal']['metadata_complete'])
                self.assertTrue(report['cleanup_confirmed'])
                self.assertEqual(state['deleted'], [('Job', 'job-uid'), ('PriorityClass', 'priority-uid')])
                self.assertIn('--limit-bytes=16385', state['log_commands'][0])
                self.assertNotIn('never-record-this', path.read_text())

    def test_cumulative_stop_before_unsuspend_preserves_exact_cleanup(self):
        calls = []
        def watch(uid=''):
            calls.append(uid)
            if uid:
                raise r.Refusal('cumulative stage stop')
        with tempfile.TemporaryDirectory() as tmp, simulated_executor(metadata_declaration()) as state, patch.object(r, 'api_write') as write:
            path = Path(tmp)/'result.json'
            with self.assertRaisesRegex(ValueError, 'cumulative stage stop'):
                r.execute(metadata_declaration(), path, True, cumulative_watch=watch)
            write.assert_not_called()
            self.assertEqual(calls, ['', 'job-uid'])
            self.assertEqual(state['node_calls'], ['', 'job-uid'])
            self.assertTrue(json.loads(path.read_text())['cleanup_confirmed'])

    def test_missing_terminal_is_recorded_not_fabricated(self):
        with tempfile.TemporaryDirectory() as tmp, simulated_executor(metadata_declaration(), 'missing-terminal'):
            report = r.execute(metadata_declaration(), Path(tmp)/'result.json', True)
            self.assertFalse(report['execution']['terminal']['metadata_complete'])
            self.assertTrue(report['cleanup_confirmed'])

    def test_unrelated_cleanup_pod_never_reported_as_cleared(self):
        with tempfile.TemporaryDirectory() as tmp, simulated_executor(metadata_declaration(), 'unrelated-pod') as state:
            report = r.execute(metadata_declaration(), Path(tmp)/'result.json', True)
            self.assertFalse(report['cleanup_confirmed'])
            self.assertEqual(state['deleted'], [('Job', 'job-uid')])

    def test_metadata_only_intent_cannot_expand_into_body_reads(self):
        c = metadata_declaration()
        self.assertEqual(r.validate(c), c)
        for key, value in [('readCount', 1), ('readCount', False), ('digest', declaration()['digest']), ('rawBytes', 1)]:
            changed = dict(c); changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): r.validate(changed)
        old = declaration(); old['readCount'] = 0
        with self.assertRaises(ValueError): r.validate(old)
        job = r.job_manifest(c, 'metadata-only', 'session')
        container = job['spec']['template']['spec']['containers'][0]
        self.assertEqual(container['args'][0].count('-mode=archive-hot-inventory'), 1)
        self.assertNotIn('-mode=archive-read-check', json.dumps(job))
        self.assertNotIn('FUGUE_COMPILER_INPUT_ARCHIVE_', json.dumps(job))
        self.assertNotEqual(r.identity(c), r.identity(declaration()))
        got = r.result_contract(json.dumps(failure()), c)
        self.assertEqual(got['reads'], [])
        self.assertFalse(got['native_archive_readback_performed'])
        self.assertFalse(got['graph_command_completed'])
        self.assertFalse(got['retirement_authorized'])
        with self.assertRaises(ValueError): r.result_contract(json.dumps(failure())+'\n'+json.dumps(read(declaration())), c)

    def test_metadata_only_session_does_not_fetch_or_decrypt_r2_credentials(self):
        c = metadata_declaration()
        cluster = {'spec': {'bootstrap': {'initdb': {'secret': {'name': 'application-role'}, 'database': c['database'], 'owner': 'app'}}}}
        secret = {'data': {k: base64.b64encode(v.encode()).decode() for k, v in [('username', 'app'), ('password', 'synthetic-private-database-value')]}}
        with patch.object(r, 'get', return_value=secret) as get, patch.object(r, 'credential_key') as key, patch.object(r, 'reader_session') as reader:
            data = r.session(c, cluster, {'status': {'podIP': '127.0.0.1'}}, {}, {}, {})
            key.assert_not_called(); reader.assert_not_called()
            get.assert_called_once_with(c['namespace'], 'secret', 'application-role')
        self.assertEqual(set(data), {'database-readonly-url'})
        self.assertIn('default_transaction_read_only%3Don', data['database-readonly-url'])
        response = types.SimpleNamespace(returncode=0, stdout='{"active_leases":0,"fenced":false}')
        with patch.object(r.subprocess, 'run', return_value=response) as run:
            self.assertEqual(r.sql_metadata(c), {'active_leases': 0, 'fenced': False})
        query = run.call_args.kwargs['input']
        for forbidden in ('fugue_object_storage_config', 'ciphertext', 'backend_hash', 'raw_bytes'):
            self.assertNotIn(forbidden, query)
        self.assertIn('REPEATABLE READ READ ONLY', query)

    def test_declaration_rejects_hidden_mutations_and_scope_expansion(self):
        self.assertEqual(r.validate(declaration()), declaration())
        for key, value in [('executionAuthorized', False), ('readCount', 4), ('readCount', True), ('rawBytes', 0), ('minimumFreeBytes', (1 << 30)-1),
                           ('image', 'registry.example/platform:latest'), ('digest', '*'), ('sourceCommit', 'main'), ('node', 'node;exit'), ('readyURL', 'https://key@control.example/readyz')]:
            c = declaration(); c[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                r.validate(c)
        c = declaration(); c['apply'] = True
        with self.assertRaises(ValueError): r.validate(c)

    def test_job_is_suspended_bounded_and_has_reader_session_only(self):
        c = declaration(); job = r.job_manifest(c, 'diagnostic', 'session')
        self.assertTrue(job['spec']['suspend'])
        self.assertEqual((job['spec']['activeDeadlineSeconds'], job['spec']['backoffLimit'], job['spec']['parallelism']), (600, 0, 1))
        spec = job['spec']['template']['spec']; container = spec['containers'][0]
        self.assertFalse(spec['automountServiceAccountToken'])
        self.assertEqual(spec['preemptionPolicy'], 'Never')
        self.assertEqual(spec['nodeSelector'], {'kubernetes.io/hostname': c['node']})
        self.assertEqual(container['image'], c['image'])
        self.assertEqual(container['resources']['requests'], container['resources']['limits'])
        self.assertEqual(container['resources']['limits'], {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': '256Mi'})
        self.assertFalse(container['securityContext']['allowPrivilegeEscalation'])
        self.assertTrue(container['securityContext']['readOnlyRootFilesystem'])
        body = json.dumps(job)
        for forbidden in ('BUNDLE_SIGNING', 'ENCRYPTION_KEY', 'WRITER', 'BOOTSTRAP', 'hostPath', 'hostNetwork', 'volumes', 'envFrom', '-apply', '-mode=archive-route'):
            self.assertNotIn(forbidden, body)
        self.assertEqual(container['args'][0].count('-mode=archive-hot-inventory'), 1)
        self.assertEqual(container['args'][0].count('-mode=archive-read-check'), 3)

    def test_native_failure_is_evidence_and_never_complete(self):
        c = declaration(); graph = failure()
        body = '\n'.join(json.dumps(x) for x in [graph, read(c), read(c), read(c)])+'\nhot inventory incomplete; stage=dependency-graph\n'
        value = r.result_contract(body, c)
        self.assertFalse(value['graph_command_completed'])
        self.assertTrue(value['all_reads_verified'])
        self.assertFalse(value['retirement_authorized'])
        for key, value in [('retirement_authorized', True), ('plan_sha256', 'a'*64), ('category', 'private https://credential')]:
            changed = copy.deepcopy(graph); changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): r.result_contract(json.dumps(changed), c)
        for key, value in [('verified', False), ('raw_bytes', c['rawBytes']+1), ('digest', 'sha256:'+'d'*64)]:
            changed = read(c); changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): r.result_contract(json.dumps(graph)+'\n'+json.dumps(changed), c)
        incomplete = r.result_contract(json.dumps(graph), c)
        self.assertFalse(incomplete['all_reads_verified'])

    def test_aggregate_costs_reject_identities_and_non_numeric_fields(self):
        graph = failure()
        graph['cost'] = dict(root_inventory_us=10, traversal_us=30, resolver_us=20,
                             **{k: dict(queries=2, rows=3, query_return_us=10, rows_next_us=8, decode_us=5, source_bytes=1024, transport_bytes=64)
                                for k in ('artifacts', 'inputs', 'ledger')})
        got = r.result_contract(json.dumps(graph), declaration())
        self.assertEqual(got['graph']['cost'], graph['cost'])
        self.assertFalse(got['retirement_authorized'])
        for field, value in [('source_bytes', -1), ('decode_us', True), ('query_return_us', 'private-value'), ('identity', 'private-value')]:
            changed = copy.deepcopy(graph)
            changed['cost']['artifacts'][field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                r.result_contract(json.dumps(changed), declaration())
        for value in (True, -1, 600_000_001, 'private-value'):
            changed = copy.deepcopy(graph)
            changed['cost']['traversal_us'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                r.result_contract(json.dumps(changed), declaration())

    def test_exact_delete_uses_uid_and_never_cascade_or_force(self):
        with patch.object(r, 'api_write', return_value={}) as write:
            r.delete_exact(declaration(), 'Job', 'synthetic', 'exact-uid')
        argv, payload = write.call_args.args
        self.assertEqual(payload['preconditions'], {'uid': 'exact-uid'})
        self.assertEqual(payload['propagationPolicy'], 'Foreground')
        self.assertNotIn('--force', argv)

    def test_write_errors_do_not_expose_submitted_credentials(self):
        secret = 'synthetic-private-reader-value'
        completed = types.SimpleNamespace(returncode=1, stdout=secret, stderr=secret)
        with patch.dict(r.os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main'), patch.object(r.subprocess, 'run', return_value=completed) as run:
            with self.assertRaises(ValueError) as error: r.api_write(['create', '-f', '-'], {'secret': secret})
            self.assertNotIn(secret, str(error.exception))
            self.assertNotIn(secret, json.dumps(run.call_args.args))

    def test_manual_execution_cannot_create_even_a_server_dry_run(self):
        with patch.dict(r.os.environ, {}, clear=True), patch.object(r.subprocess, 'run') as run:
            with self.assertRaisesRegex(ValueError, 'main CI lane'): r.create({'kind': 'Job'}, dry=True)
            run.assert_not_called()

    def test_pod_proof_rejects_wrong_node_image_restarts_and_owner(self):
        c = declaration()
        pod = {'metadata': {'name': 'job-pod', 'uid': 'pod-uid', 'annotations': {'fugue.pro/source-commit': c['sourceCommit']},
                            'ownerReferences': [{'kind': 'Job', 'controller': True, 'uid': 'job-uid'}]},
               'spec': {'nodeName': c['node'], 'priority': -1000000, 'preemptionPolicy': 'Never'}, 'status': {'phase': 'Succeeded', 'containerStatuses': [{'name': 'readonly', 'restartCount': 0, 'imageID': c['image']}]}}
        self.assertEqual(r.pod_check(c, 'job-uid', pod)['pod_uid'], 'pod-uid')
        for path, value in [(('spec', 'nodeName'), 'different'), (('metadata', 'uid'), 'irrelevant')]:
            if path[-1] == 'uid': continue
            changed = copy.deepcopy(pod); changed[path[0]][path[1]] = value
            with self.assertRaises(ValueError): r.pod_check(c, 'job-uid', changed)
        for key, value in [('restartCount', 1), ('imageID', 'different')]:
            changed = copy.deepcopy(pod); changed['status']['containerStatuses'][0][key] = value
            with self.assertRaises(ValueError): r.pod_check(c, 'job-uid', changed)
        with self.assertRaises(ValueError): r.pod_check(c, 'different', pod)

    def test_node_reservations_are_checked_separately_from_usage(self):
        c = declaration()
        node = {'metadata': {'uid': c['nodeUID']}, 'spec': {}, 'status': {'conditions': [{'type': k, 'status': v} for k, v in [('Ready', 'True'), ('MemoryPressure', 'False'), ('DiskPressure', 'False'), ('PIDPressure', 'False')]], 'allocatable': {'cpu': '8', 'memory': '16Gi', 'ephemeral-storage': '100Gi'}}}
        metric = {'timestamp': r.datetime.datetime.now(r.datetime.timezone.utc).isoformat(), 'usage': {'cpu': '1', 'memory': '1Gi'}}
        pods = {'items': [{'metadata': {}, 'spec': {'containers': [{'resources': {'requests': {'cpu': '8'}}}]}}]}
        with patch.object(r, 'cluster_get', side_effect=[node, metric]), patch.object(r, 'cluster_kube', return_value=json.dumps(pods)):
            with self.assertRaisesRegex(ValueError, 'reservation'): r.node_gate(c)

    @unittest.skipUnless(CRYPTO_AVAILABLE, 'trusted CI reader-decryption dependency is not installed')
    def test_go_aes_gcm_compatible_decryption_only_returns_reader(self):
        key = hashlib.sha256(b'synthetic-key').digest(); nonce = bytes(range(12))
        credentials = {'access_key_id': 'reader-id', 'secret_access_key': 'reader-secret'}
        cipher = base64.b64encode(nonce+AESGCM(key).encrypt(nonce, json.dumps(credentials).encode(), None)).decode()
        platform = {'account_id': 'a'*32, 'compiler_input_archive': {'state': 'active', 'account_id': 'a'*32, 'bucket': 'compiler-archive', 'prefix': 'inputs/v1',
                    'reader_configured': True, 'writer_configured': True, 'retention_policy_sha256': 'b'*64, 'protection_verified_at': '2026-01-01T00:00:00Z', 'reader': {'ciphertext': cipher}}}
        canonical = dict(Endpoint='https://'+'a'*32+'.r2.cloudflarestorage.com', AccountID='a'*32, Region='auto', Bucket='compiler-archive', Prefix='inputs/v1')
        digest = 'sha256:'+hashlib.sha256(json.dumps(canonical, separators=(',', ':')).encode()).hexdigest()
        data = r.reader_session(platform, key, digest)
        self.assertEqual(data['reader-secret-access-key'], credentials['secret_access_key'])
        self.assertNotIn('writer', json.dumps(data))
        for bad_key, bad_digest in [(bytes(32), digest), (key, 'sha256:'+'0'*64)]:
            with self.assertRaises(ValueError): r.reader_session(platform, bad_key, bad_digest)

    def test_preview_and_failed_preflight_never_create_execution_resources(self):
        c = declaration()
        with tempfile.TemporaryDirectory() as tmp, patch.object(r, 'node_gate', side_effect=ValueError('unhealthy')), patch.object(r, 'create') as create:
            with self.assertRaises(ValueError): r.execute(c, Path(tmp)/'evidence.json', apply=True)
            create.assert_not_called()
            self.assertEqual(json.loads((Path(tmp)/'evidence.json').read_text())['status'], 'completion-not-confirmed')
            with self.assertRaises(FileExistsError): r.execute(c, Path(tmp)/'evidence.json', apply=True)

    def test_one_shot_cleanup_preserves_failed_graph_and_cleans_only_own_resources(self):
        c = declaration()
        deployment = {'spec': {'template': {'spec': {'containers': [{'name': 'api'}]}}}}
        pod = {'metadata': {'name': 'readonly-pod', 'uid': 'pod-uid', 'annotations': {'fugue.pro/source-commit': c['sourceCommit']},
                           'ownerReferences': [{'kind': 'Job', 'controller': True, 'uid': 'job-uid'}]},
               'spec': {'nodeName': c['node'], 'priority': -1000000, 'preemptionPolicy': 'Never'}, 'status': {'phase': 'Failed', 'containerStatuses': [{'name': 'readonly', 'imageID': c['image'], 'restartCount': 0}]}}
        logs = '\n'.join(json.dumps(x) for x in [failure(), read(c), read(c), read(c)])
        def kube(ns, *args):
            if args[:2] == ('get', 'jobs'): return '{"items":[]}'
            if args[:2] == ('get', 'events'): return '{"items":[]}'
            if args[:2] == ('get', 'pods'):
                return json.dumps({'items': [] if deleted else [pod]})
            if args[:2] == ('get', 'secret'): return '{"items":[]}'
            if args[0] == 'logs': return logs
            self.fail('unexpected command '+str(args))
        deleted = []
        def remove(c, kind, name, uid): deleted.append((kind, uid))
        with tempfile.TemporaryDirectory() as tmp, patch.dict(r.os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main', GITHUB_SHA='d'*40), \
                patch.object(r, 'command', return_value='d'*40), patch.object(r, 'node_gate', return_value={}), \
                patch.object(r, 'primary_gate', return_value=({}, {'status': {'containerStatuses': []}}, 10 << 30)), \
                patch.object(r, 'serving_gate', return_value=({'container': 'api'}, deployment, {})), \
                patch.object(r, 'sql_metadata', return_value={}), patch.object(r, 'session', return_value={'reader': 'private-value'}), \
                patch.object(r, 'kube', side_effect=kube), patch.object(r, 'get', side_effect=lambda ns, kind, name: {'metadata': {'uid': 'job-uid'}} if kind == 'job' else pod), \
                patch.object(r, 'create', side_effect=[{}, {}, {}, {'metadata': {'uid': 'priority-uid'}}, {}, {'metadata': {'uid': 'job-uid'}}, {'metadata': {'uid': 'secret-uid'}}]) as create, \
                patch.object(r, 'cluster_get', return_value={'metadata': {'uid': 'priority-uid'}, 'value': -1000000, 'preemptionPolicy': 'Never'}), \
                patch.object(r, 'cluster_kube', return_value='{"items":[]}'), \
                patch.object(r, 'api_write', return_value={}), patch.object(r, 'delete_exact', side_effect=remove):
            report = r.execute(c, Path(tmp)/'evidence.json', apply=True)
            self.assertEqual(report['status'], 'diagnostic-finished')
            self.assertTrue(report['cleanup_confirmed'])
            self.assertFalse(report['result']['graph_command_completed'])
            self.assertTrue(report['result']['all_reads_verified'])
            self.assertNotIn('private-value', (Path(tmp)/'evidence.json').read_text())
            self.assertEqual(deleted, [('Job', 'job-uid'), ('PriorityClass', 'priority-uid')])
            receipt = create.call_args_list[2].args[0]
            self.assertEqual(receipt['kind'], 'ConfigMap')
            self.assertTrue(receipt['immutable'])
            self.assertIn('no-retry', receipt['data']['intent'])

    def test_duplicate_receipt_never_starts_a_job(self):
        c = declaration()
        with tempfile.TemporaryDirectory() as tmp, patch.dict(r.os.environ, GITHUB_ACTIONS='true', GITHUB_REF='refs/heads/main', GITHUB_SHA='d'*40), \
                patch.object(r, 'command', return_value='d'*40), patch.object(r, 'node_gate', return_value={}), \
                patch.object(r, 'primary_gate', return_value=({}, {'status': {'containerStatuses': []}}, 10 << 30)), patch.object(r, 'serving_gate', return_value=({}, {}, {})), \
                patch.object(r, 'sql_metadata', return_value={}), patch.object(r, 'session', return_value={}), \
                patch.object(r, 'kube', return_value='{"items":[]}'), \
                patch.object(r, 'create', side_effect=[{}, {}, r.Refusal('duplicate receipt')]) as create, patch.object(r, 'api_write') as write:
            with self.assertRaisesRegex(ValueError, 'duplicate receipt'): r.execute(c, Path(tmp)/'evidence.json', apply=True)
            self.assertEqual(create.call_count, 3)
            write.assert_not_called()


if __name__ == '__main__':
    unittest.main()
