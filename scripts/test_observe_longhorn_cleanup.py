import copy
import importlib.util
import unittest
from pathlib import Path


source = Path(__file__).resolve().parents[1] / 'deploy/environments/production/local-snapshot-cleanup/observe.py'
module = importlib.util.spec_from_file_location('observe_longhorn_cleanup', source)
observer = importlib.util.module_from_spec(module)
module.loader.exec_module(observer)


class TestObserver(unittest.TestCase):
    policy = dict(enabled=True, namespace='control', cluster='core-db',
                  longhornNamespace='storage', coordinationLease='backup')
    cluster = '/apis/postgresql.cnpg.io/v1/namespaces/control/clusters/core-db'
    lease = '/apis/coordination.k8s.io/v1/namespaces/control/leases/backup'
    pod = '/api/v1/namespaces/control/pods/core-db-1'
    claim = '/api/v1/namespaces/control/persistentvolumeclaims/core-db-1'
    pv = '/api/v1/persistentvolumes/pv'
    volume = '/apis/longhorn.io/v1beta2/namespaces/storage/volumes/vol'
    snapshots = '/apis/longhorn.io/v1beta2/namespaces/storage/snapshots'

    def setUp(self):
        self.calls = []
        self.live = {
            self.cluster: dict(metadata=dict(uid='cluster'), status=dict(currentPrimary='core-db-1', targetPrimary='core-db-1')),
            self.lease: dict(metadata=dict(uid='lease'), spec=dict(holderIdentity='')),
            self.pod: dict(metadata=dict(uid='pod')),
            self.claim: dict(metadata=dict(uid='claim'), spec=dict(volumeName='pv')),
            self.pv: dict(metadata=dict(uid='pv'), spec=dict(
                csi=dict(driver='driver.longhorn.io', volumeHandle='vol'),
                claimRef=dict(uid='claim', name='core-db-1', namespace='control'))),
            self.volume: dict(metadata=dict(uid='vol')),
            self.snapshots: dict(metadata={}, items=[
                dict(spec=dict(volume='vol'), metadata={}),
                dict(spec=dict(volume='vol'), metadata=dict(deletionTimestamp='now')),
                dict(spec=dict(volume='another'), metadata={}),
            ]),
        }

    def get(self, path, **query):
        self.calls.append((path, query))
        return copy.deepcopy(self.live[path])

    def test_reports_inventory_only_and_uses_bounded_list(self):
        self.live[self.lease]['spec']['holderIdentity'] = 'backup/example'
        result = observer.observe(self.get, self.policy)
        self.assertEqual(result, dict(phase='inventory_observed', policy_enabled=True,
                                      coordination_occupied=True, labelled_snapshots_on_platform_volume=1))
        self.assertIn((self.snapshots, dict(limit=500, labelSelector='fugue.pro/backup-run')), self.calls)
        self.assertEqual(sum(path == self.lease for path, _ in self.calls), 2)

    def test_partial_list_and_wrong_claim_fail_closed(self):
        self.live[self.snapshots]['metadata']['continue'] = 'cursor'
        with self.assertRaisesRegex(observer.Incomplete, 'snapshot_inventory_incomplete'):
            observer.observe(self.get, self.policy)
        self.live[self.pv]['spec']['claimRef']['uid'] = 'foreign'
        with self.assertRaisesRegex(observer.Incomplete, 'claim_binding_changed'):
            observer.observe(self.get, self.policy)

    def test_identity_and_lease_races_are_not_an_observation(self):
        for path, key, replacement, reason in (
            (self.pod, 'metadata', dict(uid='replacement'), 'identity_changed_during_inventory'),
            (self.lease, 'spec', dict(holderIdentity='backup/new'), 'lease_owner_changed_during_inventory'),
            (self.cluster, 'status', dict(currentPrimary='core-db-1', targetPrimary='core-db-2'), 'primary_transition'),
        ):
            with self.subTest(path=path):
                def get(p, **query):
                    result = self.get(p, **query)
                    if p == path and sum(c[0] == path for c in self.calls) == 2:
                        result[key] = replacement
                    return result

                self.calls.clear()
                with self.assertRaisesRegex(observer.Incomplete, reason):
                    observer.observe(get, self.policy)

    def test_does_not_use_untrusted_names_as_api_paths(self):
        self.live[self.cluster]['status']['currentPrimary'] = 'x/exec'
        with self.assertRaisesRegex(observer.Incomplete, 'invalid_resource_name'):
            observer.observe(self.get, self.policy)
        self.assertEqual(self.calls, [(self.cluster, {})])


if __name__ == '__main__':
    unittest.main()
