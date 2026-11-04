import copy
import unittest
from scripts.plan_longhorn_snapshot_recovery import collateral_purges, fold_budget, terminal_ledger, validate


class SnapshotReviewTests(unittest.TestCase):
    def observation(self):
        return dict(files=[dict(mapped_bytes=8 << 30), dict(mapped_bytes=6 << 30)],
                    overlap_bytes=5 << 30, new_mapped_bytes=1 << 30,
                    fold_write_bytes=6 << 30, physical_total_bytes=100 << 30,
                    physical_free_bytes=43 << 30, content_bytes_read=0, sync_requested=False)

    def test_requires_peak_work_space_before_reclaim(self):
        value = self.observation()
        report = fold_budget(value, 25, 16 << 30, 192 << 20, 6 << 30)
        self.assertTrue(report['capacity_gate_passed'])
        self.assertEqual(report['new_allocation_estimate_bytes'], 1 << 30)
        self.assertIsNone(report['reclaimable_bytes_exact'])
        value['physical_free_bytes'] = 42 << 30
        self.assertFalse(fold_budget(value, 25, 16 << 30, 192 << 20, 6 << 30)['capacity_gate_passed'])

    def test_rejects_reversed_fold_or_lowered_physical_minimum(self):
        value = self.observation()
        value['new_mapped_bytes'] = 3 << 30
        with self.assertRaises(ValueError):
            fold_budget(value, 25, 16 << 30, 192 << 20, 6 << 30)
        with self.assertRaises(ValueError):
            fold_budget(self.observation(), 24, 16 << 30, 192 << 20, 6 << 30)

    def test_io_bound_does_not_ignore_overwrites(self):
        with self.assertRaises(ValueError):
            fold_budget(self.observation(), 25, 16 << 30, 192 << 20, 2 << 30)

    def test_every_alias_must_be_terminal_unprotected_and_not_restoring(self):
        good = dict(id='artifact', run='run', status='deleted', protected=False,
                    physical_deleted=True, run_terminal=True, run_lease_active=False,
                    restore_references=0, target_type='control-plane-db')
        terminal_ledger([good], 'artifact', 'run')
        for field, value in [('protected', True), ('status', 'active'), ('physical_deleted', False),
                             ('run_terminal', None), ('run_lease_active', True), ('restore_references', 1)]:
            alias = {**good, 'id': 'other', field: value}
            with self.assertRaises(ValueError):
                terminal_ledger([good, alias], 'artifact', 'run')
        with self.assertRaises(ValueError):
            terminal_ledger([], 'artifact', 'run')

    def test_native_purge_collateral_is_not_ignored(self):
        chain = {'volume-head': {}, 'candidate': {},
                 'kept': dict(usercreated=True, removed=False),
                 'expansion': dict(usercreated=False, removed=True, children={'volume-head': True})}
        self.assertEqual(collateral_purges(chain, 'candidate'), ['expansion'])
        chain['unknown'] = dict(usercreated=False, removed=False, children={'kept': True})
        with self.assertRaises(ValueError):
            collateral_purges(chain, 'candidate')

    def test_declaration_cannot_smuggle_apply_or_reduce_reserve(self):
        value = dict(schema='fugue.local-snapshot-review/v1', namespace='platform', cluster='db',
                     clusterUID='1111-2222', primary='db-1', primaryUID='3333-4444',
                     pvcUID='5555-6666', volume='pvc-data', volumeUID='7777-8888',
                     longhornNamespace='storage', snapshot='old-backup', snapshotUID='9999-aaaa',
                     artifact='artifact_example', run='run_example', coordinationLease='db-backup',
                     minimumExtraFreeBytes=16 << 30, workingReserveBytes=192 << 20, maxFoldWriteBytes=6 << 30)
        validate(value)
        for key, replacement in [('apply', True), ('snapshot', 'volume-head'),
                                 ('minimumExtraFreeBytes', 15 << 30), ('workingReserveBytes', 191 << 20)]:
            broken = copy.deepcopy(value)
            broken[key] = replacement
            with self.assertRaises(ValueError):
                validate(broken)


if __name__ == '__main__':
    unittest.main()
