import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.recover_longhorn_snapshot import Cluster, Recovery, TOKEN, admission_probes, barrier_objects, canonical


def config():
    return dict(schema='fugue.local-snapshot-review/v1', namespace='platform', cluster='db', clusterUID='cluster-uid',
                primary='db-1', primaryUID='pod-uid', pvcUID='pvc-uid', volume='pvc-data', volumeUID='volume-uid',
                longhornNamespace='storage', snapshot='old-point', snapshotUID='snapshot-uid', artifact='artifact_example',
                run='run_example', coordinationLease='db-backup', minimumExtraFreeBytes=16 << 30,
                workingReserveBytes=192 << 20, maxFoldWriteBytes=6 << 30)


def approval(c):
    return dict(schema='fugue.local-snapshot-recovery/v1', declarationSHA256=hashlib.sha256(canonical(c).encode()).hexdigest(),
                snapshotUID=c['snapshotUID'], authorized=True, maxWaitSeconds=60, readyURL='https://health.example.test/readyz')


def review():
    return dict(ready_for_review=True, snapshot_resource_version='12',
                snapshot_chain={'volume-head': dict(parent='kept', children={}, removed=False, usercreated=False),
                                'old-point': dict(parent='', children={'kept': True}, removed=False, usercreated=True),
                                'kept': dict(parent='old-point', children={'volume-head': True}, removed=False, usercreated=True)},
                collateral_head_parent_prunes=[], budget=dict(physical_free_bytes=50 << 30, minimum_physical_free_bytes=25 << 30))


class FakeCluster:
    def __init__(self):
        self.objects = {}
        self.deletes = 0
        self.lease = dict(apiVersion='coordination.k8s.io/v1', kind='Lease',
                          metadata=dict(name='db-backup', uid='lease-uid', resourceVersion='1'), spec=dict(holderIdentity=''))
        self.snapshot = dict(metadata=dict(name='old-point', uid='snapshot-uid', resourceVersion='12'))
        self.fail_delete = False

    def maybe(self, kind, name, ns=None):
        if kind == 'configmaps':
            return copy.deepcopy(self.objects.get(name))
        if kind == 'snapshots.longhorn.io':
            return copy.deepcopy(self.snapshot)
        return None

    def get(self, kind, name, ns=None):
        if kind == 'leases.coordination.k8s.io':
            return copy.deepcopy(self.lease)
        if kind == 'snapshots.longhorn.io':
            return copy.deepcopy(self.snapshot)
        raise AssertionError('unexpected read')

    def create(self, obj):
        obj = copy.deepcopy(obj)
        obj['metadata'].update(uid='operation-uid', resourceVersion='1')
        self.objects[obj['metadata']['name']] = obj
        return copy.deepcopy(obj)

    def replace(self, obj):
        obj = copy.deepcopy(obj)
        if obj['kind'] == 'Lease':
            if obj['metadata']['resourceVersion'] != self.lease['metadata']['resourceVersion']:
                raise ValueError('CAS conflict')
            obj['metadata']['resourceVersion'] = str(int(obj['metadata']['resourceVersion']) + 1)
            self.lease = obj
        else:
            self.objects[obj['metadata']['name']] = obj
        return copy.deepcopy(obj)

    def delete(self, path, obj):
        self.deletes += 1
        if self.fail_delete:
            # Simulate an accepted delete whose response was lost.
            self.snapshot = None
            raise TimeoutError('unknown outcome')
        self.snapshot = None
        return {}


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.kube, self.c = FakeCluster(), config()
        self.r = Recovery(self.kube, self.c, approval(self.c), Path(self.tmp.name) / 'receipt.json')
        self.r.install_barriers = lambda _: None
        self.r.remove_barriers = lambda: None
        self.r.health = lambda: dict(database_restarts=0, database_available_bytes=1 << 30, public_ready=True)
        self.r.wait_native = lambda _: self.r.health()
        self.r.physical = lambda _: dict(free_bytes=54 << 30, available_bytes=45 << 30)

    def test_no_write_when_capacity_fails(self):
        with patch('scripts.recover_longhorn_snapshot.observe', return_value={**review(), 'ready_for_review': False}):
            with self.assertRaises(ValueError):
                self.r.run()
        self.assertEqual(self.kube.objects, {})
        self.assertEqual(self.kube.deletes, 0)
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], '')

    def test_unknown_delete_outcome_retains_lease_and_never_replays(self):
        self.kube.fail_delete = True
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            with self.assertRaises(TimeoutError):
                self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.r.state['phase'], 'delete-intent')
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], self.r.holder)
        self.assertIn(TOKEN, self.kube.lease['metadata']['annotations'])
        # A new invocation only observes native completion, no second DELETE.
        self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.r.state['phase'], 'completed')
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], '')

    def test_replay_completed_operation_is_inert(self):
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            self.r.run()
        self.r.run()
        self.assertEqual(self.kube.deletes, 1)

    def test_no_new_delete_when_an_old_intent_is_unresolved(self):
        self.kube.fail_delete = True
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            with self.assertRaises(TimeoutError):
                self.r.run()
        self.kube.snapshot = dict(metadata=dict(name='old-point', uid='snapshot-uid', resourceVersion='12'))
        with self.assertRaisesRegex(ValueError, 'not terminal'):
            self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], self.r.holder)

    def test_fresh_chain_change_aborts_before_delete(self):
        changed = review()
        changed['snapshot_chain']['new-point'] = dict(parent='kept', children={}, removed=False, usercreated=True)
        with patch('scripts.recover_longhorn_snapshot.observe', side_effect=[review(), changed]):
            with self.assertRaisesRegex(ValueError, 'ancestry'):
                self.r.run()
        self.assertEqual(self.kube.deletes, 0)
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], '')

    def test_lease_theft_does_not_unlock_another_holder(self):
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            self.r.run()
        self.kube.lease['spec']['holderIdentity'] = 'backup/other'
        with self.assertRaisesRegex(ValueError, 'ownership lost'):
            self.r.release()
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], 'backup/other')

    def test_native_failure_retains_protection(self):
        self.r.wait_native = lambda _: (_ for _ in ()).throw(ValueError('native purge error'))
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            with self.assertRaisesRegex(ValueError, 'purge error'):
                self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], self.r.holder)

    def test_pre_delete_barrier_failure_can_retry_without_duplicate_deletion(self):
        self.r.install_barriers = lambda _: (_ for _ in ()).throw(ValueError('fence unavailable'))
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            with self.assertRaisesRegex(ValueError, 'fence unavailable'):
                self.r.run()
        self.assertFalse(self.r.state['deletion_attempted'])
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], '')
        self.r.install_barriers = lambda _: None
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.r.state['phase'], 'completed')

    def test_post_delete_reserve_failure_does_not_unlock(self):
        self.r.physical = lambda _: dict(free_bytes=40 << 30)
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            with self.assertRaisesRegex(ValueError, 'reserve failed'):
                self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], self.r.holder)

    def test_lost_completion_receipt_after_unlock_recovers_without_a_new_delete(self):
        original = self.r.save
        def fail_last(phase, **fields):
            if phase == 'completed':
                raise TimeoutError('receipt response lost')
            return original(phase, **fields)
        self.r.save = fail_last
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            with self.assertRaises(TimeoutError):
                self.r.run()
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], '')
        self.r.save = original
        self.r.run()
        self.assertEqual(self.r.state['phase'], 'completed')
        self.assertEqual(self.kube.deletes, 1)

    def test_wrong_or_unapproved_declaration_is_rejected(self):
        for key, value in [('authorized', False), ('snapshotUID', 'another'), ('declarationSHA256', '0' * 64), ('maxWaitSeconds', 1201), ('readyURL', 'http://example.test')]:
            a = approval(self.c)
            a[key] = value
            with self.assertRaises(ValueError):
                Recovery(self.kube, self.c, a, Path(self.tmp.name) / 'wrong.json')

    def test_raw_delete_has_both_server_preconditions(self):
        cluster = Cluster()
        with patch.object(cluster, 'write', return_value={}) as write:
            cluster.delete('/apis/example/v1/items/old', self.kube.snapshot)
        args, body = write.call_args.args
        self.assertEqual(args, ['delete', '--raw', '/apis/example/v1/items/old', '-f', '-'])
        self.assertEqual(body['preconditions'], dict(uid='snapshot-uid', resourceVersion='12'))

    def test_barriers_cover_independent_cr_writers_without_disabling_status(self):
        objects = barrier_objects(self.c, review(), 'operation-example')
        policies = [o for o in objects if o['kind'] == 'ValidatingAdmissionPolicy']
        self.assertEqual(len(policies), 6)
        resources = {r for p in policies for rule in p['spec']['matchConstraints']['resourceRules'] for r in rule['resources']}
        self.assertEqual(resources, {'snapshots', 'backups', 'volumes', 'persistentvolumeclaims', 'volumesnapshots', 'volumesnapshotcontents'})
        for p in policies:
            self.assertEqual(p['spec']['failurePolicy'], 'Fail')
            self.assertNotIn('/status', canonical(p))
            self.assertIn('oldObject', p['spec']['validations'][0]['expression'])
        text = canonical(policies)
        self.assertIn('snapshot-uid', text)
        self.assertNotIn('backup_run_', text)

    def test_each_independent_reference_writer_has_a_real_denial_probe(self):
        probes = admission_probes(self.c, 'operation-example')
        self.assertEqual({p['kind'] for p in probes}, {'Snapshot', 'Backup', 'Volume', 'PersistentVolumeClaim', 'VolumeSnapshot', 'VolumeSnapshotContent'})
        self.assertEqual(len(probes), 6)
        for obj in probes:
            self.assertNotIn('uid', obj['metadata'])
            self.assertTrue(obj['metadata']['name'].endswith('-probe'))


class NativeFinalizerRecoveryTests(unittest.TestCase):
    """An accepted native delete can finish without replay or forced finalizers."""
    setUp = RecoveryTests.setUp

    def pending(self):
        self.kube.fail_delete = True
        with patch('scripts.recover_longhorn_snapshot.observe', return_value=review()):
            with self.assertRaises(TimeoutError):
                self.r.run()
        self.kube.fail_delete = False
        self.kube.snapshot = dict(apiVersion='longhorn.io/v1beta2', kind='Snapshot',
            metadata=dict(name='old-point', uid='snapshot-uid', resourceVersion='13',
                          deletionTimestamp='2026-01-01T00:00:00Z', finalizers=['longhorn.io']),
            spec=dict(volume='pvc-data', createSnapshot=True))
        identities = []
        for obj in barrier_objects(self.c, review(), self.r.operation, legacy_snapshot_spec=True):
            obj['metadata'].update(uid=obj['metadata']['name'], resourceVersion='1')
            identities.append(dict(kind=obj['kind'], name=obj['metadata']['name'], uid=obj['metadata']['uid']))
            self.kube.objects[obj['metadata']['name'] + obj['kind']] = obj
        self.r.record['data']['barriers'] = canonical(identities)
        self.kube.replace(self.r.record)
        original_get, original_replace = self.kube.get, self.kube.replace
        self.updates, self.dry_runs, self.checks = [], [], []
        def get(kind, name, ns=None):
            kinds = dict(validatingadmissionpolicies='ValidatingAdmissionPolicy',
                         validatingadmissionpolicybindings='ValidatingAdmissionPolicyBinding')
            if kind in kinds:
                return copy.deepcopy(self.kube.objects[name + kinds[kind]])
            if kind == 'snapshots.longhorn.io' and name == 'kept':
                return dict(apiVersion='longhorn.io/v1beta2', kind='Snapshot',
                    metadata=dict(name='kept', uid='kept-uid', finalizers=['longhorn.io']),
                    spec=dict(volume='pvc-data', createSnapshot=True))
            return original_get(kind, name, ns)
        def replace(obj):
            if obj['kind'] in ('ValidatingAdmissionPolicy', 'ValidatingAdmissionPolicyBinding'):
                self.updates.append(copy.deepcopy(obj))
                self.kube.objects[obj['metadata']['name'] + obj['kind']] = copy.deepcopy(obj)
                return copy.deepcopy(obj)
            if obj['kind'] == 'Snapshot':
                self.updates.append(copy.deepcopy(obj))
                # Native controller handles the metadata update, not our code.
                self.kube.snapshot = None
                return copy.deepcopy(obj)
            return original_replace(obj)
        self.kube.get, self.kube.replace = get, replace
        self.kube.command = lambda args, body: self.dry_runs.append((args, json.loads(body))) or '{}'
        self.r.check_barriers = lambda: self.checks.append(True)
        self.denied_updates = []
        self.r.denied_probe = lambda args, body: self.denied_updates.append((args, copy.deepcopy(body)))
        self.r.native_state = lambda _: (copy.deepcopy(self.kube.snapshot), self.kube.snapshot is None, True)

    def test_pending_native_delete_repairs_guard_and_requeues_without_new_delete(self):
        self.pending()
        original = copy.deepcopy(self.kube.snapshot)
        self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.r.state['phase'], 'completed')
        self.assertEqual([x['kind'] for x in self.updates], ['ValidatingAdmissionPolicy', 'Snapshot'])
        kick = self.updates[-1]
        self.assertEqual(kick['metadata']['finalizers'], original['metadata']['finalizers'])
        self.assertEqual(kick['spec'], original['spec'])
        self.assertEqual(kick['metadata']['uid'], original['metadata']['uid'])
        self.assertEqual(kick['metadata']['resourceVersion'], original['metadata']['resourceVersion'])
        self.assertEqual(kick['metadata']['annotations']['fugue.pro/snapshot-recovery-reconcile'], self.r.operation)
        self.assertEqual(len(self.checks), 1)
        self.assertEqual(len(self.dry_runs), 1)
        args, probe = self.dry_runs[0]
        self.assertIn('--dry-run=server', args)
        self.assertEqual(probe['metadata']['finalizers'], ['longhorn.io'])
        self.assertIsNone(probe['spec']['labels'])
        self.assertEqual(len(self.denied_updates), 2)

    def test_changed_guard_spec_keeps_all_protection(self):
        self.pending()
        name = self.r.operation + '-backupsValidatingAdmissionPolicy'
        self.kube.objects[name]['spec']['failurePolicy'] = 'Ignore'
        with self.assertRaisesRegex(ValueError, 'identity or spec'):
            self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.updates, [])
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], self.r.holder)

    def test_purge_still_running_cannot_repair_or_unlock(self):
        self.pending()
        self.r.native_state = lambda _: (self.kube.snapshot, False, False)
        with self.assertRaisesRegex(ValueError, 'not terminal enough'):
            self.r.run()
        self.assertEqual(self.updates, [])
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], self.r.holder)

    def test_unknown_policy_update_can_resume_by_exact_spec_without_redelete(self):
        self.pending()
        original = self.kube.replace
        def lost_response(obj):
            result = original(obj)
            if obj['kind'] == 'ValidatingAdmissionPolicy':
                raise TimeoutError('accepted compatibility update, response lost')
            return result
        self.kube.replace = lost_response
        with self.assertRaises(TimeoutError):
            self.r.run()
        self.assertEqual(self.kube.lease['spec']['holderIdentity'], self.r.holder)
        self.kube.replace = original
        self.r.run()
        self.assertEqual(self.kube.deletes, 1)
        self.assertEqual(self.r.state['phase'], 'completed')
        self.assertEqual(sum(x['kind'] == 'ValidatingAdmissionPolicy' for x in self.updates), 1)


if __name__ == '__main__':
    unittest.main()
