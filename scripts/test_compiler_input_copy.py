import copy
import json
import unittest

from scripts import compiler_input_copy as c
from scripts.test_compiler_input_readonly import metadata_declaration


def declaration():
    v = metadata_declaration()
    del v['readCount']
    v.update(version=1, minimumFreeBytes=3 << 30, maxWalGrowthBytes=512 << 20, maxAvailableDropBytes=768 << 20, maxNewDeadlineFailures=4,
             copy={'version': 1, 'digest': 'sha256:'+'d'*64, 'source_format': 'runtime-snapshot-json-chunk-codec-v2', 'raw_bytes': 65537,
                   'backend_hash': 'sha256:'+'e'*64, 'segment_bytes': 65536, 'max_put_bytes': 131074, 'max_get_bytes': 262148,
                   'max_put_requests': 2, 'max_get_requests': 4, 'payload_bytes_per_second': 2 << 20, 'max_seconds': 540})
    return v


def receipt(v):
    return dict(action='archive-copy', digest=v['copy']['digest'], source_format=v['copy']['source_format'], raw_bytes=65537, verified_raw_bytes=65537,
                segments=2, state='verified', source_preserved=True, route_unchanged=True, elapsed_ms=100,
                usage=dict(put_payload_bytes=1234, get_payload_bytes=2468, put_requests=2, get_requests=4, source_raw_bytes=65537))


class CopyTest(unittest.TestCase):
    def test_authority_and_budgets_cannot_expand(self):
        v = declaration()
        self.assertEqual(c.validate(v), v)
        for key, value in [('executionAuthorized', False), ('minimumFreeBytes', (3 << 30)-1), ('readCount', 1), ('mode', 'archive-route'),
                           ('maxWalGrowthBytes', 0), ('maxNewDeadlineFailures', True), ('maxNewDeadlineFailures', 9)]:
            invalid = copy.deepcopy(v); invalid[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): c.validate(invalid)
        for key, value in [('raw_bytes', True), ('digest', '*'), ('source_format', 'runtime-snapshot-separated-v4'), ('max_get_requests', 5),
                           ('max_seconds', 601), ('payload_bytes_per_second', 0), ('segment_bytes', 16 << 20), ('route', True)]:
            invalid = copy.deepcopy(v); invalid['copy'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): c.validate(invalid)

    def test_job_has_no_graph_route_shell_or_platform_signing_key(self):
        job = c.job_manifest(declaration(), 'isolated-copy', 'session')
        spec = job['spec']; pod = spec['template']['spec']; container = pod['containers'][0]
        self.assertEqual((spec['backoffLimit'], spec['parallelism'], spec['completions'], spec['activeDeadlineSeconds']), (0, 1, 1, 600))
        self.assertTrue(spec['suspend']); self.assertFalse(pod['automountServiceAccountToken'])
        self.assertEqual(container['command'], ['nice', '-n', '19', '/usr/local/bin/fugue-input-storage'])
        self.assertEqual(container['args'], ['-mode=archive-copy', '-apply', '-copy-contract=/session/copy.json'])
        env = {x['name']: x for x in container['env']}
        self.assertEqual(env['FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED']['value'], 'false')
        for forbidden in ('FUGUE_BUNDLE_SIGNING_KEY', 'FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY'):
            self.assertNotIn(forbidden, env)
        self.assertEqual(container['resources']['limits'], {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': '256Mi'})
        self.assertEqual(pod['volumes'][0]['secret']['items'], [{'key': 'copy.json', 'path': 'copy.json'}])
        self.assertNotIn('archive-hot-inventory', json.dumps(job))

    def test_receipts_require_two_full_passes_and_preserved_source(self):
        v = declaration(); good = receipt(v)
        self.assertEqual(c.result_contract(json.dumps(good), v), good)
        for key, value in [('verified_raw_bytes', 0), ('route_unchanged', False), ('state', 'routed'), ('segments', 3), ('credentials', 'synthetic-secret')]:
            bad = copy.deepcopy(good); bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): c.result_contract(json.dumps(bad), v)
        for key, value in [('get_payload_bytes', 2467), ('get_requests', 3), ('put_requests', 3), ('source_raw_bytes', 65538)]:
            bad = copy.deepcopy(good); bad['usage'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): c.result_contract(json.dumps(bad), v)
        partial = copy.deepcopy(good)
        partial.update(state='', verified_raw_bytes=0, segments=0, source_preserved=False, route_unchanged=False)
        partial['usage'].update(put_requests=1, get_requests=1, get_payload_bytes=1234, source_raw_bytes=2000)
        self.assertEqual(c.result_contract(json.dumps(partial), v, success=False), partial)
        with self.assertRaises(ValueError): c.result_contract(json.dumps(partial), v)

    def test_watchdog_stops_on_wal_source_or_connection_drift(self):
        v = declaration()
        before = dict(epoch=0, manifest={'raw_bytes': 65537}, source_chunks=2, wal_bytes='1000', active_clients=3, worker_connections=0)
        current = copy.deepcopy(before); current.update(wal_bytes='2000', worker_connections=1)
        c.check_impact(v, before, current)
        for key, value in [('epoch', 1), ('source_chunks', 1), ('manifest', {}), ('wal_bytes', '999'),
                           ('wal_bytes', str(1001+v['maxWalGrowthBytes'])), ('worker_connections', 2), ('active_clients', 8)]:
            changed = copy.deepcopy(current); changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): c.check_impact(v, before, changed)


if __name__ == '__main__': unittest.main()
