import contextlib
import copy
import datetime as dt
import io
import json
import unittest
from unittest.mock import patch

from scripts import cnpg_backup_retention as r


def fixture():
    policy = dict(schema=r.SCHEMA, namespace='platform-test', cluster='sample-db',
                  cluster_uid='cluster-uid', system_id='12345', destination='s3://sample/archive',
                  endpoint='https://objects.example.test', server_name='sample-db', enabled=True, retain_count=3)
    cluster = dict(metadata=dict(namespace=policy['namespace'], name=policy['cluster'], uid=policy['cluster_uid'],
                                 annotations={r.ANNOTATION: 'policy-uid'}),
                   spec=dict(backup=dict(barmanObjectStore=dict(destinationPath=policy['destination'], endpointURL=policy['endpoint'],
                        s3Credentials=dict(accessKeyId=dict(name='sample-secret', key='access'), secretAccessKey=dict(name='sample-secret', key='secret'))))),
                   status=dict(systemID=policy['system_id'], phase='Cluster in healthy state', readyInstances=1,
                               currentPrimary='sample-db-1', targetPrimary='sample-db-1', conditions=[dict(type='ContinuousArchiving', status='True')]))
    return policy, cluster


def catalogue(n):
    return {'backups_list': [dict(backup_id=f'2026010{i}T030000', status='DONE', systemid='12345', parent_backup_id=None) for i in range(1, n + 1)]}


class FakeKube:
    def __init__(self):
        self.policy, self.cluster = fixture()
        self.namespace, self.name = self.policy['namespace'], 'sample-retention'
        self.resources = {
            r.config_path(self.namespace, self.name): dict(metadata=dict(name=self.name, namespace=self.namespace, uid='policy-uid', resourceVersion='1'), data={'policy.json': json.dumps(self.policy)}),
            r.config_path(self.namespace, self.name + '-receipt'): dict(metadata=dict(resourceVersion='1'), data={}),
            r.cluster_path(self.namespace, self.policy['cluster']): self.cluster,
            f'/apis/coordination.k8s.io/v1/namespaces/{self.namespace}/leases/{self.name}': dict(metadata=dict(resourceVersion='1'), spec={}),
            f'/apis/postgresql.cnpg.io/v1/namespaces/{self.namespace}/backups?limit=500': dict(metadata={}, items=[]),
            '/apis/postgresql.cnpg.io/v1/clusters?limit=500': dict(metadata={}, items=[self.cluster]),
        }

    def request(self, method, path, body=None):
        if method == 'PUT':
            self.resources[path] = copy.deepcopy(body)
        return copy.deepcopy(self.resources[path])

    def run(self, dry_run=False):
        with contextlib.redirect_stdout(io.StringIO()):
            return r.run_retention(self, self.namespace, self.name, dry_run)

    def receipt(self):
        return json.loads(self.resources[r.config_path(self.namespace, self.name + '-receipt')]['data']['receipt.json'])


class CountRetentionTest(unittest.TestCase):
    def test_real_barman_json_shape_and_count_order_not_days(self):
        plan = r.catalogue_plan(catalogue(5), 3, '12345')
        self.assertEqual(plan['keep'], ['20260105T030000', '20260104T030000', '20260103T030000'])
        self.assertEqual(plan['older_candidates'], ['20260102T030000', '20260101T030000'])
        with self.assertRaisesRegex(ValueError, 'Invalid Barman catalogue'):
            r.catalogue_plan({'backups': []}, 3, '12345')

    def test_catalogue_fails_closed(self):
        cases = [catalogue(2), {'backups_list': catalogue(3)['backups_list'] * 2}]
        for field, value in [('status', 'FAILED'), ('systemid', '67890'), ('parent_backup_id', '20251201T030000'), ('backup_id', '../other')]:
            bad = catalogue(4)
            bad['backups_list'][0][field] = value
            cases.append(bad)
        for bad in cases:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                r.catalogue_plan(bad, 3, '12345')

    def test_policy_identity_is_pinned(self):
        policy, cluster = fixture()
        r.validate_policy(policy, cluster)
        for field, value in [('cluster_uid', 'other'), ('system_id', '999'), ('destination', 's3://other/archive'), ('endpoint', 'https://other.example.test'), ('retain_count', True), ('enabled', 1), ('extra', 'unknown')]:
            bad = {**policy, field: value}
            with self.subTest(field=field), self.assertRaises(ValueError):
                r.validate_policy(bad, cluster)

    @patch.object(r, 'barman')
    def test_health_active_backup_native_policy_and_ownership_block_delete(self, barman):
        for mutation in ['health', 'primary', 'archive', 'native', 'owner', 'active', 'restore', 'truncated']:
            k = FakeKube()
            if mutation == 'health':
                k.cluster['status']['phase'] = 'Not enough disk space'
            elif mutation == 'primary':
                k.cluster['status']['targetPrimary'] = 'sample-db-2'
            elif mutation == 'archive':
                k.cluster['status']['conditions'] = []
            elif mutation == 'native':
                k.cluster['spec']['backup']['retentionPolicy'] = '7d'
            elif mutation == 'owner':
                k.cluster['metadata']['annotations'][r.ANNOTATION] = 'other-policy'
            elif mutation == 'active':
                k.resources[f'/apis/postgresql.cnpg.io/v1/namespaces/{k.namespace}/backups?limit=500']['items'] = [dict(spec=dict(cluster=dict(name=k.policy['cluster'])), status=dict(phase='running'))]
            elif mutation == 'restore':
                other = copy.deepcopy(k.cluster)
                other['metadata']['uid'] = 'restore-uid'
                other['status']['phase'] = 'Setting up primary'
                other['spec']['externalClusters'] = [dict(name=k.policy['server_name'], barmanObjectStore=dict(destinationPath=k.policy['destination']))]
                k.resources['/apis/postgresql.cnpg.io/v1/clusters?limit=500']['items'].append(other)
            else:
                k.resources['/apis/postgresql.cnpg.io/v1/clusters?limit=500']['metadata']['continue'] = 'next'
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                k.run()
            self.assertEqual(k.receipt()['state'], 'blocked')
            barman.assert_not_called()
            self.assertEqual(k.resources[f'/apis/coordination.k8s.io/v1/namespaces/{k.namespace}/leases/{k.name}']['spec']['holderIdentity'], '')

    @patch.object(r, 'barman')
    def test_engine_receipt_proves_removed_and_preserved_backups(self, barman):
        before, after = catalogue(5), catalogue(5)
        after['backups_list'] = after['backups_list'][2:]
        barman.side_effect = [json.dumps(before), '', json.dumps(after)]
        k = FakeKube()
        receipt = k.run()
        command = barman.call_args_list[1].args[0]
        self.assertEqual(command[command.index('--retention-policy') + 1], 'REDUNDANCY 3')
        self.assertEqual(command[command.index('--minimum-redundancy') + 1], '3')
        self.assertNotIn('--backup-id', command)
        self.assertEqual(receipt['removed'], ['20260101T030000', '20260102T030000'])
        self.assertEqual(len(receipt['remaining']), 3)
        self.assertTrue(receipt['count_satisfied'])
        self.assertTrue(receipt['deleted'])

    @patch.object(r, 'barman')
    def test_dry_run_and_archival_keep_are_not_false_cleanup_success(self, barman):
        for dry in (True, False):
            barman.reset_mock()
            barman.side_effect = [json.dumps(catalogue(5)), '', json.dumps(catalogue(5))]
            result = FakeKube().run(dry)
            command = barman.call_args_list[1].args[0]
            self.assertEqual('--dry-run' in command, dry)
            self.assertFalse(result['deleted'])
            self.assertFalse(result['count_satisfied'])
            self.assertEqual(len(result['remaining_above_count']), 2)

    @patch.object(r, 'barman')
    def test_revalidate_after_listing_and_never_remove_on_policy_change(self, barman):
        k = FakeKube()
        def changed(_):
            k.resources[r.config_path(k.namespace, k.name)]['metadata']['resourceVersion'] = '2'
            return json.dumps(catalogue(5))
        barman.side_effect = changed
        with self.assertRaisesRegex(ValueError, 'Policy changed'):
            k.run()
        self.assertEqual(barman.call_count, 1)

    def test_active_or_malformed_lease_is_not_stolen(self):
        for spec in [dict(holderIdentity='other'), dict(holderIdentity='other', renewTime=dt.datetime.now(dt.timezone.utc).isoformat(), leaseDurationSeconds=1800)]:
            k = FakeKube()
            k.resources[f'/apis/coordination.k8s.io/v1/namespaces/{k.namespace}/leases/{k.name}']['spec'] = spec
            with self.assertRaises(ValueError):
                k.run()

    @patch.object(r, 'barman')
    def test_missing_preserved_backup_cannot_produce_success_receipt(self, barman):
        wrong = catalogue(5)
        wrong['backups_list'] = wrong['backups_list'][:3]
        barman.side_effect = [json.dumps(catalogue(5)), '', json.dumps(wrong)]
        k = FakeKube()
        with self.assertRaisesRegex(ValueError, 'Retained backup verification failed'):
            k.run()
        self.assertEqual(k.receipt()['state'], 'blocked')
        self.assertTrue(k.receipt()['deletion_attempted'])

    @patch.object(r, 'barman')
    def test_disabled_policy_never_deletes(self, barman):
        k = FakeKube()
        k.policy['enabled'] = False
        k.resources[r.config_path(k.namespace, k.name)]['data']['policy.json'] = json.dumps(k.policy)
        self.assertEqual(k.run()['state'], 'disabled')
        barman.assert_not_called()

    def test_install_is_disabled_preserves_intent_and_uses_secret_refs(self):
        policy, cluster = fixture()
        objects = {}
        applied = []
        def kube(*args, body=None):
            if args[0] == 'get' and args[1] == 'clusters.postgresql.cnpg.io':
                return cluster
            if args[0] == 'get':
                return objects.get((args[1], args[2]))
            if args[0] == 'create':
                objects[(body['kind'], body['metadata']['name'])] = copy.deepcopy(body)
                objects[(body['kind'], body['metadata']['name'])]['metadata']['uid'] = 'policy-uid'
            if args[0] == 'apply':
                applied.append(body)
        config = dict(namespace=policy['namespace'], name='sample-retention', cluster=policy['cluster'], clusterUID=policy['cluster_uid'], systemID=policy['system_id'], image='registry.example.test/db@sha256:' + 'a' * 64)
        with contextlib.redirect_stdout(io.StringIO()):
            r.install(config, kube)
            saved = json.loads(objects[('ConfigMap', config['name'])]['data']['policy.json'])
            self.assertFalse(saved['enabled'])
            saved.update(enabled=True, retain_count=5)
            objects[('ConfigMap', config['name'])]['data']['policy.json'] = json.dumps(saved)
            r.install(config, kube)
        self.assertEqual(json.loads(objects[('ConfigMap', config['name'])]['data']['policy.json']), saved)
        cron = next(item for item in applied if item['kind'] == 'CronJob')
        container = cron['spec']['jobTemplate']['spec']['template']['spec']['containers'][0]
        self.assertEqual(container['image'], config['image'])
        env = {v['name']: v for v in container['env']}
        self.assertIn('secretKeyRef', env['AWS_SECRET_ACCESS_KEY']['valueFrom'])
        self.assertNotIn('value', env['AWS_SECRET_ACCESS_KEY'])
        role = next(item for item in applied if item['kind'] == 'ClusterRole')
        self.assertEqual(role['rules'][0]['verbs'], ['get', 'list'])

    @patch.object(r, 'barman')
    def test_barman_shutdown_preserves_archive_after_native_config_is_removed(self, barman):
        k = FakeKube()
        k.policy.update(enabled=False, barman_disabled=True)
        k.resources[r.config_path(k.namespace, k.name)]['data']['policy.json'] = json.dumps(k.policy)
        k.cluster['metadata']['annotations'][r.DISABLED_ANNOTATION] = 'policy-uid'
        del k.cluster['spec']['backup']['barmanObjectStore']
        self.assertEqual(k.run()['state'], 'disabled')
        barman.assert_not_called()
        for owner in ('', 'foreign-uid'):
            k.cluster['metadata']['annotations'][r.DISABLED_ANNOTATION] = owner
            with self.assertRaisesRegex(ValueError, 'owned Barman disable intent'):
                k.run()
        for value in (True, 1, 'true'):
            bad = {**k.policy, 'enabled': value}
            with self.assertRaises(ValueError):
                r.validate_policy(bad, k.cluster, 'policy-uid')

    def test_install_preserves_shutdown_without_requiring_live_credentials(self):
        policy, cluster = fixture()
        policy.update(enabled=False, barman_disabled=True)
        cluster['metadata']['annotations'][r.DISABLED_ANNOTATION] = 'policy-uid'
        del cluster['spec']['backup']['barmanObjectStore']
        saved = dict(metadata=dict(uid='policy-uid'), data={'policy.json': json.dumps(policy)})
        cron = dict(metadata=dict(uid='cron-uid', resourceVersion='10'), spec=dict(suspend=False))
        patches = []
        def kube(*args, body=None):
            if args[0] == 'get':
                return {'clusters.postgresql.cnpg.io': cluster, 'ConfigMap': saved, 'CronJob': cron}[args[1]]
            if args[0] == 'patch' and args[1] == 'CronJob':
                patches.append(json.loads(args[-1]))
                return {}
            self.fail(f'Unexpected write during disabled installation: {args}')
        config = dict(namespace=policy['namespace'], name='sample-retention', cluster=policy['cluster'], clusterUID=policy['cluster_uid'], systemID=policy['system_id'], image='registry.example.test/db@sha256:' + 'a' * 64)
        with contextlib.redirect_stdout(io.StringIO()):
            r.install(config, kube)
        self.assertEqual(patches[0][-1], dict(op='add', path='/spec/suspend', value=True))
        self.assertEqual(patches[0][0]['value'], 'cron-uid')
        self.assertEqual(json.loads(saved['data']['policy.json']), policy)

    @patch.object(r, 'barman')
    def test_shutdown_marker_blocks_old_enabled_retention(self, barman):
        k = FakeKube()
        k.cluster['metadata']['annotations'][r.DISABLED_ANNOTATION] = 'policy-uid'
        with self.assertRaisesRegex(ValueError, 'shutdown blocks retention'):
            k.run()
        barman.assert_not_called()


if __name__ == '__main__':
    unittest.main()
