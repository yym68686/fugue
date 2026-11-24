import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from scripts import install_longhorn_snapshot_cleanup as release
from scripts import run_longhorn_snapshot_cleanup as worker
from scripts.test_reconcile_longhorn_snapshot_cleanup import FakeCluster, policy, row

KEY = ('pvc-data', 'old-point', 'run_example')
SECOND = ('pvc-other', 'second-point', 'run_other')


def event(kind='DELETED', version='10', deleting=True):
    return dict(type=kind, object=dict(kind='Backup', metadata=dict(
        resourceVersion=version, deletionTimestamp='2026-01-01T02:00:00Z' if deleting else None,
        labels={worker.WATCH_LABEL: KEY[2]}), spec=dict(snapshotName=KEY[1]), status=dict(volumeName=KEY[0])))


class QueueTests(unittest.TestCase):
    def setUp(self):
        state = tempfile.TemporaryDirectory()
        self.addCleanup(state.cleanup)
        self.directory = Path(state.name)
        self.path = self.directory / 'queue.sqlite3'
        self.queue = worker.Queue(self.path)
        self.watch = worker.RemoteWatch(Mock(), 'storage', self.queue, threading.Event(), threading.Event())

    def test_only_owned_delete_tombstones_enqueue_and_cursor_survives_restart(self):
        with patch.object(worker.time, 'time', return_value=100), patch.object(worker, 'emit') as emit:
            for i, e in enumerate([event('ADDED'), event(deleting=False), event('MODIFIED'), event(), event()], 20):
                e['object']['metadata']['resourceVersion'] = str(i)
                self.assertTrue(self.watch.accept(json.dumps(e)))
        emit.assert_called_once()
        restored = worker.Queue(self.path)
        self.assertEqual(restored.count(), {'pending': 1})
        self.assertEqual(restored.get('remote-backup-resource-version'), '24')
        self.assertEqual(restored.due(100), (*KEY, 0))
        self.assertFalse(self.watch.accept('{"type":"ERROR","object":{"code":410}}'))
        self.assertEqual(restored.get('remote-backup-resource-version'), '24')
        self.assertEqual(restored.refill([KEY], 200), 0)

    def test_invalid_key_and_cursor_update_are_atomic(self):
        self.queue.checkpoint('10')
        with self.assertRaises(ValueError):
            self.queue.checkpoint('11', ('bad/key', KEY[1], KEY[2]))
        self.assertEqual(self.queue.get('remote-backup-resource-version'), '10')
        self.assertEqual(self.queue.count(), {})

    def test_capacity_deferral_does_not_delay_another_volume_and_keeps_reason(self):
        self.queue.refill([KEY, SECOND], 100)
        worker.reconcile_job(self.queue, self.queue.due(100), dict(
            phase='blocked', observed_targets=[KEY],
            blocked=[dict(snapshot=KEY[1], reason='database filesystem below stop line')]), 100, True)
        self.assertEqual(self.queue.due(100), (*SECOND, 0))
        with self.queue.transaction() as db:
            self.assertEqual(db.execute('SELECT state,reason,due FROM jobs WHERE volume=?', (KEY[0],)).fetchone(),
                             ('retry', 'database filesystem below stop line', 130))

    def test_absence_is_not_a_native_completion_and_can_be_reconsidered(self):
        self.queue.refill([KEY, SECOND], 100)
        worker.reconcile_job(self.queue, self.queue.due(100), dict(phase='idle', observed_targets=[]), 100, True)
        self.assertEqual(self.queue.count(), {'absent': 1, 'pending': 1})
        worker.reconcile_job(self.queue, self.queue.due(100),
                             dict(phase='completed', selected_target=SECOND), 100, True)
        self.assertEqual(self.queue.count(), {'absent': 1, 'completed': 1})
        self.queue.recheck_gone([KEY, SECOND], 200)
        self.assertEqual(self.queue.due(200), (*KEY, 1))
        self.assertEqual(self.queue.count(), {'retry': 1, 'completed': 1})

    def test_activation_only_accelerates_preview_ready_and_retains_capacity_backoff(self):
        self.queue.refill([KEY, SECOND], 100)
        worker.reconcile_job(self.queue, (*KEY, 0), dict(phase='ready', selected_target=KEY), 100, False)
        self.queue.record(SECOND, 'retry', 'space', 500)
        self.assertIsNone(self.queue.due(101))
        self.queue.activate(101)
        self.assertEqual(self.queue.due(101), (*KEY, 0))
        self.assertEqual(self.queue.count(), {'retry': 2})

    def test_inventory_only_backfills_remotely_retired_ledger_targets(self):
        kube = FakeCluster()
        with patch.object(worker, 'database_binding', return_value={}):
            self.assertEqual(worker.inventory(kube, policy()), [KEY])
            kube.ledger = [{**row(), 'physical_deleted': False}]
            self.assertEqual(worker.inventory(kube, policy()), [])

    def test_first_observe_pass_calls_reconciler_read_only(self):
        kube = Mock()
        with patch.object(worker, 'inventory', return_value=[KEY]), patch.object(worker, 'emit'), \
             patch.object(worker, 'reconcile', return_value=dict(phase='ready', selected_target=KEY)) as reconcile:
            worker.run(kube, policy(), self.directory, '1' * 40, 'observe', once=True)
        self.assertEqual(reconcile.call_args.kwargs, dict(execute=False, targets={KEY}))
        state = json.loads((self.directory / 'status.json').read_text())
        self.assertTrue(state['inventory_verified'])
        self.assertFalse(state['watch_bootstrapped'])
        self.assertEqual(state['queue'], {'retry': 1})

    def test_host_release_exact_bundle_and_install_program(self):
        compile(release.HOST_INSTALL, '<host installer>', 'exec')
        _, packet = release.packet('1' * 40)
        self.assertIn(packet['mode'], ('observe', 'active'))
        self.assertEqual(set(packet['files']), set(release.FILES))
        self.assertIn('DynamicUser=yes', packet['files']['executor.service'])
        self.assertEqual(len(packet['bundle_sha256']), 64)

    def test_install_cannot_move_runtime_to_a_second_node(self):
        target, packet = release.packet('1' * 40)
        node = dict(metadata=dict(uid='declared-node'))
        existing = dict(immutable=True, data=dict(node='previous-host', nodeUID='previous-node'))
        kube = Mock(side_effect=[node, existing])
        with self.assertRaisesRegex(ValueError, 'different host identity'):
            release.host_binding(target, packet, kube)
        self.assertEqual(kube.call_count, 2)


if __name__ == '__main__':
    unittest.main()
