import copy
import json
import unittest
from unittest.mock import patch

from scripts import compiler_input_copy as c
from scripts.test_compiler_input_readonly import metadata_declaration


def declaration():
    v = metadata_declaration()
    del v['readCount']
    v.update(version=1, minimumFreeBytes=3 << 30, maxWalGrowthBytes=512 << 20, maxAvailableDropBytes=768 << 20, maxNewDeadlineFailures=4,
             storage={**{k: 'neutral' for k in ('longhorn_namespace', 'volume', 'replica', 'instance_manager', 'node')},
                      **{k: 'a'*8+'-'+('a'*4+'-')*3+'a'*12 for k in ('volume_uid', 'pvc_uid', 'pv_uid', 'replica_uid', 'instance_manager_uid', 'disk_id')}, 'disk_path': '/var/lib/storage'},
             copy={'version': 1, 'digest': 'sha256:'+'d'*64, 'source_format': 'runtime-snapshot-json-chunk-codec-v2', 'raw_bytes': 65537,
                   'backend_hash': 'sha256:'+'e'*64, 'segment_bytes': 65536, 'max_put_bytes': 131074, 'max_get_bytes': 262148,
                   'max_put_requests': 2, 'max_get_requests': 4, 'payload_bytes_per_second': 2 << 20, 'max_seconds': 540})
    return v


def receipt(v):
    return dict(action='archive-copy', digest=v['copy']['digest'], source_format=v['copy']['source_format'], raw_bytes=65537, verified_raw_bytes=65537,
                segments=2, state='verified', source_preserved=True, route_unchanged=True, elapsed_ms=100,
                usage=dict(put_payload_bytes=1234, get_payload_bytes=2468, put_requests=2, get_requests=4, source_raw_bytes=65537))


class CopyTest(unittest.TestCase):
    def test_outer_writer_impact_covers_queue_failures_without_changing_old_counters(self):
        v = declaration()
        writer = dict(schema='fugue.compiler-input-writer.observation.v1', attempts=4, in_flight=1,
                      successful_returns=1, deadline_failures=1, canceled_failures=1, other_failures=0)
        baseline = dict(outer_writer=writer, write_failures=0, budget_fallbacks=0)
        current = copy.deepcopy(baseline)
        current['outer_writer'].update(attempts=9, in_flight=0, successful_returns=3, deadline_failures=5)
        c.check_writer_impact(v, baseline, current)
        for category in ('deadline_failures', 'canceled_failures', 'other_failures'):
            bad = copy.deepcopy(current)
            bad['outer_writer'][category] += 1
            bad['outer_writer']['attempts'] += 1
            with self.subTest(category=category), self.assertRaises(ValueError): c.check_writer_impact(v, baseline, bad)
        reset = copy.deepcopy(baseline)
        reset['outer_writer'].update(attempts=3, canceled_failures=0)
        with self.assertRaises(ValueError): c.check_writer_impact(v, baseline, reset)

    def test_outer_writer_observation_fails_closed_on_missing_inconsistent_or_private_fields(self):
        good = dict(schema='fugue.compiler-input-writer.observation.v1', attempts=1, in_flight=1,
                    successful_returns=0, deadline_failures=0, canceled_failures=0, other_failures=0)
        self.assertEqual(c.writer_activity({'compiler_input_writer_activity': good}), good)
        for key, changed in [('schema', 'other'), ('attempts', True), ('attempts', 2), ('in_flight', -1),
                             ('other_failures', 1 << 64), ('private_message', 'synthetic')]:
            bad = dict(good); bad[key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): c.writer_activity({'compiler_input_writer_activity': bad})
        for bad in ({}, {'compiler_input_writer_activity': None}, {'compiler_input_writer_activity': {}},
                    {'compiler_input_writer_activity': {k: x for k, x in good.items() if k != 'canceled_failures'}}):
            with self.subTest(bad=bad), self.assertRaises(ValueError): c.writer_activity(bad)
        with self.assertRaises(ValueError): c.check_writer_impact(declaration(), {}, {})

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
        partial['failure'] = dict(stage='source-read', category='55P03', segment_ordinal=1, segment_bytes_read=0)
        self.assertEqual(c.result_contract(json.dumps(partial), v, success=False), partial)
        with self.assertRaises(ValueError): c.result_contract(json.dumps(partial), v)
        for key, value in [('stage', 'private message'), ('category', 'private SQL URL'), ('segment_ordinal', True), ('segment_bytes_read', 1), ('message', 'synthetic-private')]:
            bad = copy.deepcopy(partial); bad['failure'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): c.result_contract(json.dumps(bad), v, success=False)

    def test_only_bounded_native_category_is_retained_before_cleanup(self):
        prefix = 'compiler input maintenance completion not confirmed; inspect state before retrying; category='
        self.assertEqual(c.error_category('{"action":"archive-copy"}\n'+prefix+'55P03\n'), '55P03')
        self.assertEqual(c.error_category(prefix+'unclassified\n'), 'unclassified')
        self.assertIsNone(c.error_category('arbitrary private diagnostic\n'))
        with self.assertRaises(ValueError): c.error_category(prefix+'deadline\n'+prefix+'55P03\n')
        with self.assertRaises(ValueError): c.error_category(prefix+'synthetic-private-value\n')

    def test_worker_termination_keeps_only_fixed_metadata(self):
        terminated = dict(reason='OOMKilled', exitCode=137, signal=9,
                          startedAt='2026-01-02T03:04:05Z', finishedAt='2026-01-02T03:04:06Z',
                          message='synthetic-private-message', containerID='synthetic-private-runtime-id')
        pod = {'status': {'phase': 'Failed', 'message': 'synthetic-private-pod-message',
                          'containerStatuses': [{'name': 'copy', 'state': {'terminated': terminated},
                                                 'lastState': {'terminated': {'message': 'synthetic-private-history'}}}]}}
        observed = c.worker_terminal(pod)
        self.assertEqual(observed, dict(phase='Failed', metadata_complete=True, reason='OOMKilled', exit_code=137,
                                       signal=9, started_at=terminated['startedAt'], finished_at=terminated['finishedAt']))
        self.assertNotIn('synthetic-private', json.dumps(observed))
        terminated['reason'] = 'synthetic-private-reason'
        self.assertEqual(c.worker_terminal(pod)['reason'], 'other')
        del terminated['signal']
        self.assertEqual(c.worker_terminal(pod)['signal'], 0)

    def test_worker_termination_missing_or_malformed_is_not_completion_proof(self):
        state = dict(reason='Completed', exitCode=0, signal=0,
                     startedAt='2026-01-02T03:04:05Z', finishedAt='2026-01-02T03:04:06Z')
        pod = {'status': {'phase': 'Succeeded', 'containerStatuses': [{'name': 'copy', 'state': {'terminated': state}}]}}
        self.assertTrue(c.worker_terminal(pod)['metadata_complete'])
        for key, changed in [('reason', None), ('reason', ''), ('reason', '  '), ('reason', 0),
                             ('exitCode', True), ('exitCode', -1), ('signal', '9'), ('signal', 1 << 31),
                             ('startedAt', 'synthetic-private-time'), ('finishedAt', '2026-02-30T00:00:00Z'),
                             ('finishedAt', '2026-01-02T03:04:04Z'), ('finishedAt', '2026-01-02T03:04:06Z synthetic-private')]:
            invalid = copy.deepcopy(pod); invalid['status']['containerStatuses'][0]['state']['terminated'][key] = changed
            with self.subTest(key=key, changed=changed):
                result = c.worker_terminal(invalid)
                self.assertFalse(result['metadata_complete'])
                self.assertNotIn('synthetic-private', json.dumps(result))
        missing_reason = copy.deepcopy(pod)
        del missing_reason['status']['containerStatuses'][0]['state']['terminated']['reason']
        self.assertFalse(c.worker_terminal(missing_reason)['metadata_complete'])
        for statuses in (None, [], [{'name': 'other'}], [pod['status']['containerStatuses'][0]]*2,
                         [{'name': 'copy', 'state': {}}], [{'name': 'copy', 'state': {'running': {}}}],
                         [{'name': 'copy', 'state': {'terminated': state, 'running': {}}}]):
            invalid = copy.deepcopy(pod); invalid['status']['containerStatuses'] = statuses
            with self.subTest(statuses=statuses): self.assertFalse(c.worker_terminal(invalid)['metadata_complete'])
        with self.assertRaises(ValueError): c.worker_terminal({'status': {'phase': 'Running'}})

    def test_watchdog_stops_on_wal_source_or_connection_drift(self):
        v = declaration()
        before = dict(epoch=0, manifest={'raw_bytes': 65537}, source_chunks=2, wal_bytes='1000', active_clients=3, worker_connections=0, other_leases=0, all_leases=0)
        current = copy.deepcopy(before); current.update(wal_bytes='2000', worker_connections=1, all_leases=1)
        c.check_impact(v, before, current)
        for key, value in [('epoch', 1), ('source_chunks', 1), ('manifest', {}), ('wal_bytes', '999'),
                           ('wal_bytes', str(1001+v['maxWalGrowthBytes'])), ('worker_connections', 2), ('active_clients', 8), ('other_leases', 1), ('all_leases', 2)]:
            changed = copy.deepcopy(current); changed[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError): c.check_impact(v, before, changed)

    def test_backing_gate_binds_replica_and_reserves_full_worker_and_drop(self):
        v = declaration(); s = v['storage']
        pvc = {'metadata': {'uid': s['pvc_uid']}, 'spec': {'volumeName': 'pv'}}
        pv = {'metadata': {'uid': s['pv_uid']}, 'spec': {'claimRef': {'uid': s['pvc_uid']}, 'csi': {'driver': 'driver.longhorn.io', 'volumeHandle': s['volume']}}}
        volume = {'metadata': {'uid': s['volume_uid']}, 'spec': {'dataEngine': 'v1', 'numberOfReplicas': 1}, 'status': {'state': 'attached', 'robustness': 'healthy', 'currentNodeID': s['node']}}
        replica = {'metadata': {'uid': s['replica_uid'], 'name': s['replica']}, 'spec': {'volumeName': s['volume'], 'nodeID': s['node'], 'diskID': s['disk_id'], 'diskPath': s['disk_path'], 'active': True}, 'status': {'currentState': 'running', 'instanceManagerName': s['instance_manager']}}
        manager = {'metadata': {'uid': s['instance_manager_uid']}, 'spec': {'nodeName': s['node']}, 'status': {'containerStatuses': [{'name': 'instance-manager', 'ready': True}]}}
        total = 100 << 30
        floor = total//4+(16<<30)+(192<<20)
        fs = {'total_bytes': total, 'free_bytes': floor+(256<<20)+v['maxAvailableDropBytes'], 'available_bytes': 1}
        def get(ns, kind, name):
            return {'pvc': pvc, 'volumes.longhorn.io': volume, 'pod': manager}[kind]
        def kube(ns, verb, *args):
            if verb == 'get': return json.dumps({'items': [replica]})
            self.assertEqual(verb, 'exec'); self.assertIn('/host'+s['disk_path'], args)
            return json.dumps(fs)
        primary = {'spec': {'nodeName': s['node']}}
        with patch.object(c, 'get', side_effect=get), patch.object(c.r, 'cluster_get', return_value=pv), patch.object(c, 'kube', side_effect=kube):
            self.assertTrue(c.backing_gate(v, primary, starting=True)['capacity_gate_passed'])
            fs['free_bytes'] -= 1
            self.assertFalse(c.backing_gate(v, primary, starting=True)['capacity_gate_passed'])
            self.assertTrue(c.backing_gate(v, primary)['capacity_gate_passed'])
            replica['spec']['diskID'] = 'changed'
            with self.assertRaises(ValueError): c.backing_gate(v, primary)


if __name__ == '__main__': unittest.main()
