import base64
import copy
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
    return dict(version=1, executionAuthorized=True, namespace='platform-system', node='runtime-one', nodeUID='1'*8+'-'+'1'*4+'-'+'1'*4+'-'+'1'*4+'-'+'1'*12,
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


class ReadonlyTest(unittest.TestCase):
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
               'spec': {'nodeName': c['node']}, 'status': {'phase': 'Succeeded', 'containerStatuses': [{'name': 'readonly', 'restartCount': 0, 'imageID': c['image']}]}}
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
               'spec': {'nodeName': c['node']}, 'status': {'phase': 'Failed', 'containerStatuses': [{'name': 'readonly', 'imageID': c['image'], 'restartCount': 0}]}}
        logs = '\n'.join(json.dumps(x) for x in [failure(), read(c), read(c), read(c)])
        def kube(ns, *args):
            if args[:2] == ('get', 'jobs'): return '{"items":[]}'
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
                patch.object(r, 'create', side_effect=[{}, {}, {'metadata': {'uid': 'job-uid'}}, {'metadata': {'uid': 'secret-uid'}}]) as create, \
                patch.object(r, 'api_write', return_value={}), patch.object(r, 'delete_exact', side_effect=remove):
            report = r.execute(c, Path(tmp)/'evidence.json', apply=True)
            self.assertEqual(report['status'], 'diagnostic-finished')
            self.assertTrue(report['cleanup_confirmed'])
            self.assertFalse(report['result']['graph_command_completed'])
            self.assertTrue(report['result']['all_reads_verified'])
            self.assertNotIn('private-value', (Path(tmp)/'evidence.json').read_text())
            self.assertEqual(deleted, [('Job', 'job-uid')])
            receipt = create.call_args_list[1].args[0]
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
                patch.object(r, 'create', side_effect=[{}, r.Refusal('duplicate receipt')]) as create, patch.object(r, 'api_write') as write:
            with self.assertRaisesRegex(ValueError, 'duplicate receipt'): r.execute(c, Path(tmp)/'evidence.json', apply=True)
            self.assertEqual(create.call_count, 2)
            write.assert_not_called()


if __name__ == '__main__':
    unittest.main()
