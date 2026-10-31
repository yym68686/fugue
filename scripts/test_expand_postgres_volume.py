import copy
import unittest

from scripts.expand_postgres_volume import plan, quantity


def fixture():
    config = dict(schema='fugue.postgres-volume-expansion/v1', namespace='system', cluster='control-db',
                  clusterUID='cluster-uid', primary='control-db-1', pvcUID='claim-uid', pvUID='pv-uid',
                  volumeUID='volume-uid', storageClass='expandable', previousSize='10Gi', targetSize='14Gi',
                  minimumFreeAfterReserveGiB=16, longhornNamespace='storage')
    cluster = dict(metadata=dict(name='control-db', namespace='system', uid='cluster-uid'),
                   spec=dict(instances=1, imageName='postgres:18', storage=dict(size='10Gi', storageClass='expandable', resizeInUseVolumes=True)),
                   status=dict(currentPrimary='control-db-1', targetPrimary='control-db-1', phase='Not enough disk space'))
    pvc = dict(metadata=dict(name='control-db-1', namespace='system', uid='claim-uid', resourceVersion='20',
                             ownerReferences=[dict(uid='cluster-uid', kind='Cluster', controller=True)]),
               spec=dict(volumeName='volume', storageClassName='expandable', accessModes=['ReadWriteOnce'], resources=dict(requests=dict(storage='10Gi'))),
               status=dict(phase='Bound', capacity=dict(storage='10Gi')))
    pv = dict(metadata=dict(name='volume', uid='pv-uid'), spec=dict(claimRef=dict(uid='claim-uid'), csi=dict(driver='driver.longhorn.io', volumeHandle='volume')))
    sc = dict(metadata=dict(name='expandable'), provisioner='driver.longhorn.io', allowVolumeExpansion=True)
    volume = dict(metadata=dict(name='volume', uid='volume-uid'), spec=dict(numberOfReplicas=1), status=dict(state='attached', robustness='healthy'))
    replicas = [dict(spec=dict(volumeName='volume', active=True, nodeID='node-a', diskID='disk-a'), status=dict(currentState='running'))]
    nodes = [dict(metadata=dict(name='node-a'), spec=dict(disks=dict(data=dict(storageReserved=quantity('20Gi')))),
                  status=dict(diskStatus=dict(data=dict(diskUUID='disk-a', storageAvailable=quantity('60Gi'),
                                                       storageMaximum=quantity('100Gi'), storageScheduled=quantity('40Gi'),
                                                       conditions=[dict(type='Ready', status='True')]))))]
    settings = {'storage-minimal-available-percentage': '20', 'storage-over-provisioning-percentage': '100'}
    return [config, cluster, pvc, pv, sc, volume, replicas, nodes, settings]


class ExpansionTests(unittest.TestCase):
    def test_recovers_unready_database_without_changing_code_or_identity(self):
        args = fixture()
        before = copy.deepcopy(args)
        patch, evidence = plan(*args)
        self.assertEqual(args, before)
        self.assertEqual([p for p in patch if p['op'] != 'test'], [dict(op='replace', path='/spec/resources/requests/storage', value='14Gi')])
        self.assertEqual({p['path'] for p in patch if p['op'] == 'test'}, {'/metadata/uid', '/metadata/resourceVersion', '/spec'})
        self.assertEqual(evidence['replicaHeadroom'][0]['freeAfterReserveAndGrowthBytes'], quantity('36Gi'))

    def test_rejects_identity_races_shrink_and_unsafe_backend(self):
        changes = [
            lambda a: a[0].update(targetSize='9Gi'),
            lambda a: a[0].update(targetSize='21Gi'),
            lambda a: a[0].update(deleteOldData=True),
            lambda a: a[1]['metadata'].update(uid='replacement'),
            lambda a: a[1]['status'].update(targetPrimary='other'),
            lambda a: a[1]['spec'].update(instances=2),
            lambda a: a[1]['spec']['storage'].update(resizeInUseVolumes=False),
            lambda a: a[2]['metadata'].update(uid='replacement'),
            lambda a: a[2]['metadata'].update(ownerReferences=[]),
            lambda a: a[2]['spec']['resources']['requests'].update(storage='12Gi'),
            lambda a: a[3]['spec']['claimRef'].update(uid='other'),
            lambda a: a[4].update(allowVolumeExpansion=False),
            lambda a: a[5]['status'].update(robustness='degraded'),
            lambda a: a[5]['spec'].update(numberOfReplicas=2),
            lambda a: a[6][0]['spec'].update(failedAt='2026-01-01'),
            lambda a: a[7][0]['status']['diskStatus']['data'].update(storageAvailable=quantity('39Gi')),
        ]
        for mutate in changes:
            with self.subTest(mutate=mutate):
                args = fixture()
                mutate(args)
                with self.assertRaises(ValueError):
                    plan(*args)

    def test_retry_never_shrinks_or_reapplies_claim(self):
        args = fixture()
        args[2]['spec']['resources']['requests']['storage'] = '14Gi'
        self.assertEqual(plan(*args)[0], [])
        args[1]['spec']['storage']['size'] = '14Gi'
        self.assertEqual(plan(*args)[0], [])

    def test_reserved_space_is_a_total_allocation_limit_not_an_unused_free_floor(self):
        args = fixture()
        args[7][0]['spec']['disks']['data']['storageReserved'] = quantity('65Gi')
        args[7][0]['status']['diskStatus']['data']['storageScheduled'] = quantity('20Gi')
        _, evidence = plan(*args)
        self.assertEqual(evidence['replicaHeadroom'][0]['freeAfterReserveAndGrowthBytes'], quantity('36Gi'))
        self.assertEqual(evidence['replicaHeadroom'][0]['scheduledHeadroomBytes'], quantity('11Gi'))

    def test_rejects_physical_pressure_logical_overallocation_and_invalid_settings(self):
        changes = [
            lambda a: a[8].update({'storage-minimal-available-percentage': '41'}),
            lambda a: a[8].clear(),
            lambda a: a[8].update({'storage-minimal-available-percentage': '-1'}),
            lambda a: a[8].update({'storage-over-provisioning-percentage': '0'}),
            lambda a: a[7][0]['status']['diskStatus']['data'].update(storageScheduled=quantity('77Gi')),
            lambda a: a[7][0]['status']['diskStatus']['data'].update(storageMaximum=0),
        ]
        for mutate in changes:
            with self.subTest(mutate=mutate):
                args = fixture()
                mutate(args)
                with self.assertRaises(ValueError):
                    plan(*args)

    def test_multiple_replicas_on_same_disk_reserve_the_combined_growth(self):
        args = fixture()
        args[5]['spec']['numberOfReplicas'] = 2
        args[6].append(copy.deepcopy(args[6][0]))
        args[7][0]['status']['diskStatus']['data']['storageAvailable'] = quantity('43Gi')
        with self.assertRaisesRegex(ValueError, 'capacity after reserve'):
            plan(*args)


if __name__ == '__main__':
    unittest.main()
