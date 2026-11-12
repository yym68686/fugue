import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import reconcile_longhorn_snapshot_cleanup as gc
from scripts.test_recover_longhorn_snapshot import config, review


def policy():
    return dict(schema='fugue.local-snapshot-cleanup/v1', enabled=True, namespace='platform', cluster='db',
                longhornNamespace='storage', coordinationLease='db-backup', minimumExtraFreeBytes=16 << 30,
                workingReserveBytes=192 << 20, maxFoldWriteBytes=6 << 30, maxWaitSeconds=60,
                readyURL='https://health.example.test/readyz')


def row():
    return dict(id='artifact_example', run='run_example', snapshot='old-point', alternate_snapshot=None,
                status='expired', protected=False, physical_deleted=True, run_terminal=True,
                run_lease_active=False, restore_references=0, target_type='control-plane-db')


class FakeCluster:
    def __init__(self):
        self.ledger = [row()]
        self.records = []
        self.holder = ''

    def get(self, kind, name, ns=None):
        if kind == 'leases.coordination.k8s.io':
            return dict(spec=dict(holderIdentity=self.holder))
        raise AssertionError('unexpected read')

    def items(self, path, **selectors):
        if path == '/api/v1/configmaps':
            return self.records
        if path.endswith('/snapshots'):
            return [dict(metadata=dict(name='old-point', uid='snapshot-uid', labels={'fugue.pro/backup-run': 'run_example'}),
                         spec=dict(volume='pvc-data'))]
        if path.endswith('/volumes'):
            return [dict(metadata=dict(name='pvc-data'), status={})]
        if path == '/api/v1/pods':
            return []
        raise AssertionError('unexpected list')

    def volume_ledger(self, _):
        return self.ledger

    def failed_snapshot_run(self, _):
        return []


class CleanupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'evidence.json'
        self.kube = FakeCluster()
        self.binding = {k: config()[k] for k in ('namespace', 'cluster', 'clusterUID', 'primary', 'primaryUID', 'pvcUID', 'volume', 'volumeUID')}
        self.binding_patch = patch.object(gc, 'database_binding', return_value=self.binding)
        self.binding_patch.start()
        self.addCleanup(self.binding_patch.stop)

    def test_all_aliases_must_be_retired_before_any_native_request(self):
        for field, value in [('protected', True), ('status', 'active'), ('physical_deleted', False),
                             ('run_terminal', None), ('run_lease_active', True), ('restore_references', 1), ('run', 'other')]:
            with self.subTest(field=field), patch.object(gc, 'observe') as observe:
                self.kube.ledger = [row(), {**row(), 'id': 'alias', field: value}]
                result = gc.reconcile(self.kube, policy(), self.path, execute=True)
                self.assertEqual(result['phase'], 'idle' if field in ('protected', 'status') else 'blocked')
                observe.assert_not_called()

    def test_capacity_failure_is_visible_and_retried_without_deletion(self):
        with patch.object(gc, 'observe', return_value={**review(), 'ready_for_review': False}), patch.object(gc, 'Recovery') as executor:
            # Policy validation constructs a dry executor, never .run().
            for _ in range(2):
                result = gc.reconcile(self.kube, policy(), self.path, execute=True)
                self.assertEqual(result['phase'], 'blocked')
                self.assertIn('working-space', result['blocked'][0]['reason'])
            executor.return_value.run.assert_not_called()

    def test_preview_never_executes_and_apply_runs_one_native_executor(self):
        with patch.object(gc, 'observe', return_value=review()), patch.object(gc, 'Recovery') as executor:
            executor.return_value.state = dict(phase='completed')
            executor.return_value.operation = 'operation'
            result = gc.reconcile(self.kube, policy(), self.path)
            self.assertEqual(result['phase'], 'ready')
            executor.return_value.run.assert_not_called()
            result = gc.reconcile(self.kube, policy(), self.path, execute=True)
            self.assertEqual(result['completed'], 1)
            executor.return_value.run.assert_called_once()
            self.assertIn('cleanup_policy_sha256', executor.call_args.kwargs)

    def test_restart_resumes_intent_even_after_snapshot_is_gone(self):
        p, c = policy(), config()
        c.update(ledgerNamespace='platform', ledgerPrimary='db-1', ledgerPrimaryUID='pod-uid',
                 coordinationNamespace='platform', targetType='control-plane-db')
        digest = hashlib.sha256(gc.canonical(p).encode()).hexdigest()
        r = gc.Recovery(None, c, gc.approval(p, c), self.path)
        self.kube.records = [dict(metadata=dict(name=r.operation, namespace='platform'), data=dict(
            cleanup_policy_sha256=digest, **{'receipt.json': json.dumps(dict(phase='delete-intent')),
             'declaration.json': json.dumps(c), 'approval.json': json.dumps(gc.approval(p, c))}))]
        with patch.object(gc, 'observe') as observe, patch.object(gc.Recovery, 'run') as run:
            result = gc.reconcile(self.kube, p, self.path, execute=True)
            run.assert_called_once()
            observe.assert_not_called()
            self.assertEqual(result['operation'], r.operation)
        self.kube.records[0]['data']['cleanup_policy_sha256'] = 'f' * 64
        with self.assertRaisesRegex(ValueError, 'different policy'):
            gc.reconcile(self.kube, p, self.path, execute=True)

    def test_busy_lease_or_disabled_policy_never_starts_cleanup(self):
        with patch.object(gc, 'observe') as observe:
            self.kube.holder = 'backup/run'
            self.assertEqual(gc.reconcile(self.kube, policy(), self.path, execute=True)['phase'], 'blocked')
            self.assertEqual(gc.reconcile(self.kube, {**policy(), 'enabled': False}, self.path, execute=True)['phase'], 'disabled')
            observe.assert_not_called()

    def test_retained_points_are_not_reported_as_gc_failures(self):
        self.kube.ledger = [{**row(), 'status': 'active'}]
        result = gc.reconcile(self.kube, policy(), self.path, execute=True)
        self.assertEqual(result['phase'], 'idle')
        self.assertEqual(result['candidates'], 0)
        self.assertEqual(len(result['retained']), 1)
        self.assertEqual(result['blocked'], [])

    def test_policy_cannot_reduce_reserve_or_add_a_delete_shortcut(self):
        for field, value in [('minimumExtraFreeBytes', 1), ('workingReserveBytes', 1), ('force', True)]:
            with self.assertRaises(ValueError):
                gc.validate_policy({**policy(), field: value})

    def test_managed_database_uses_platform_ledger_and_lease(self):
        binding = {**self.binding, 'namespace': 'tenant', 'cluster': 'managed-db', 'primary': 'managed-db-1', 'volume': 'tenant-volume'}
        snapshot = dict(metadata=dict(name='old-point', uid='snapshot-uid', labels={'fugue.pro/backup-run': 'run_example'}), spec=dict(volume='tenant-volume'))
        rows = [{**row(), 'target_type': 'app-database', 'service_name': 'managed-db'}]
        pod = dict(metadata=dict(name='managed-db-1', namespace='tenant', labels={'cnpg.io/cluster': 'managed-db'}))
        with patch.object(gc, 'database_binding', return_value=binding):
            c = gc.declaration(self.kube, policy(), self.binding, snapshot, rows, pod)
        self.assertEqual(c['namespace'], 'tenant')
        self.assertEqual(c['ledgerNamespace'], 'platform')
        self.assertEqual(c['coordinationNamespace'], 'platform')

    def test_failed_source_without_artifacts_uses_distinct_terminal_run_proof(self):
        proof = dict(id='run_example', run='run_example', snapshot='old-point', kind='failed-run',
                     status='failed', no_artifacts=True, terminal_source=True, run_terminal=True,
                     run_lease_active=False, target_type='control-plane-db', restore_references=0)
        self.assertEqual(gc.candidate_ledger([proof], 'old-point', 'run_example'), [proof])
        for field, value in [('no_artifacts', False), ('terminal_source', False), ('run_lease_active', True)]:
            with self.assertRaises(ValueError):
                gc.candidate_ledger([{**proof, field: value}], 'old-point', 'run_example')


if __name__ == '__main__':
    unittest.main()
