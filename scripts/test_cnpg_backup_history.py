import contextlib
import copy
import datetime as dt
import io
import json
import unittest
from unittest.mock import patch

from scripts import cnpg_backup_history as h
from scripts import cnpg_backup_retention as r
from scripts.test_cnpg_backup_retention import FakeKube


class HistoryKube(FakeKube):
    def __init__(self):
        super().__init__()
        self.policy.update(enabled=False, barman_disabled=True)
        self.policy['endpoint'] = 'https://' + 'a' * 32 + '.r2.cloudflarestorage.com'
        self.resources[r.config_path(self.namespace, self.name)]['data']['policy.json'] = json.dumps(self.policy)
        self.cluster['metadata']['annotations'][r.DISABLED_ANNOTATION] = 'policy-uid'
        del self.cluster['spec']['backup']['barmanObjectStore']
        self.resources[r.config_path(self.namespace, self.name + '-history-receipt')] = dict(metadata=dict(resourceVersion='1'), data={'receipt.json': '{}'})
        self.resources[self.cron_path] = dict(metadata=dict(annotations={h.CAPABILITY: 'v1'}), spec=dict(suspend=True))
        self.resources[self.schedules_path] = dict(metadata={}, items=[])

    @property
    def cron_path(self):
        return f'/apis/batch/v1/namespaces/{self.namespace}/cronjobs/{self.name}'

    @property
    def schedules_path(self):
        return f'/apis/postgresql.cnpg.io/v1/namespaces/{self.namespace}/scheduledbackups?limit=500'

    def history_receipt(self):
        return json.loads(self.resources[r.config_path(self.namespace, self.name + '-history-receipt')]['data']['receipt.json'])


class FakeS3:
    def __init__(self):
        self.bucket = 'sample'
        self.prefix = 'archive/sample-db/'
        self.objects, self.uploads = {}, {}
        self.deleted, self.aborted = [], []
        self.deletion_error = False
        self.drift = False
        self.keep_after_delete = False
        self.scan_count = 0
        self.extra_page = None
        self.add('base/20260101T030000/backup.info', b'systemid=12345\nstatus=DONE\nparent_backup_id=None\nsnapshots_info=None\n')
        self.add('base/20260101T030000/data.tar.gz', b'compressed-test-data')
        self.add('wals/0000000100000000/000000010000000000000001.gz', b'wal')
        self.uploads[(self.prefix + 'base/20260102T030000/data.tar.gz', 'upload-1')] = [dict(PartNumber=1, Size=12, ETag='"part-etag"', LastModified=self.old())]

    @staticmethod
    def old():
        return dt.datetime(2026, 1, 2, tzinfo=dt.timezone.utc)

    def add(self, key, data):
        self.objects[self.prefix + key] = dict(data=data, ETag='"' + str(len(data)) + '"', LastModified=self.old())

    def get_paginator(self, method):
        owner = self

        class Paginator:
            def paginate(self, **kwargs):
                if kwargs['Bucket'] != owner.bucket:
                    raise AssertionError('wrong bucket')
                if method == 'list_objects_v2':
                    owner.scan_count += 1
                    if owner.drift and owner.scan_count > 1:
                        owner.add('wals/000000010000000000000002.gz', b'new')
                    items = [dict(Key=key, Size=len(obj['data']), ETag=obj['ETag'], LastModified=obj['LastModified']) for key, obj in owner.objects.items()]
                    # Exercise full object pagination, not only the first page.
                    yield dict(Contents=items[:1], IsTruncated=len(items) > 1, NextContinuationToken='next')
                    if len(items) > 1:
                        yield dict(Contents=items[1:], IsTruncated=False)
                    if owner.extra_page:
                        yield owner.extra_page
                elif method == 'list_multipart_uploads':
                    yield dict(Uploads=[dict(Key=key, UploadId=upload, Initiated=owner.old()) for key, upload in owner.uploads], IsTruncated=False)
                elif method == 'list_parts':
                    parts = owner.uploads[(kwargs['Key'], kwargs['UploadId'])]
                    yield dict(Parts=parts[:1], IsTruncated=len(parts) > 1, NextPartNumberMarker=1)
                    if len(parts) > 1:
                        yield dict(Parts=parts[1:], IsTruncated=False)
                else:
                    raise AssertionError(method)

        return Paginator()

    def get_object(self, **kwargs):
        obj = self.objects[kwargs['Key']]
        assert kwargs['IfMatch'] == obj['ETag']
        return dict(Body=io.BytesIO(obj['data']))

    def head_object(self, **kwargs):
        obj = self.objects[kwargs['Key']]
        assert kwargs['IfMatch'] == obj['ETag']
        return dict(ETag=obj['ETag'], LastModified=obj['LastModified'], ContentLength=len(obj['data']))

    def delete_objects(self, **kwargs):
        assert kwargs['Bucket'] == self.bucket
        objects = kwargs['Delete']['Objects']
        if self.deletion_error:
            objects = objects[:1]
        for obj in objects:
            self.deleted.append(obj['Key'])
            if not self.keep_after_delete:
                del self.objects[obj['Key']]
        return dict(Deleted=objects, Errors=[dict(Code='AccessDenied')] if self.deletion_error else [])

    def abort_multipart_upload(self, **kwargs):
        key = (kwargs['Key'], kwargs['UploadId'])
        self.aborted.append(key)
        del self.uploads[key]


class HistoryTest(unittest.TestCase):
    def run_history(self, kube, s3, execute=False, expected=''):
        with contextlib.redirect_stdout(io.StringIO()):
            return h.run(kube, s3, kube.namespace, kube.name, 'a' * 32, 'job-uid', execute, expected)

    def test_preview_counts_complete_objects_and_all_multipart_parts_without_deletion(self):
        k, s = HistoryKube(), FakeS3()
        s.uploads[next(iter(s.uploads))].append(dict(PartNumber=2, Size=31, ETag='"part-2"', LastModified=s.old()))
        preview = self.run_history(k, s)
        self.assertEqual(preview['state'], 'planned')
        self.assertEqual(preview['inventory']['objects'], 3)
        self.assertEqual(preview['inventory']['multipart_bytes'], 43)
        self.assertEqual(preview['inventory']['multipart_parts'], 2)
        self.assertFalse(preview['deletion_attempted'])
        self.assertEqual(len(preview['plan_sha256']), 64)
        self.assertEqual(s.deleted + s.aborted, [])

    def test_execute_deletes_only_exact_archive_and_aborts_failed_upload(self):
        k, s = HistoryKube(), FakeS3()
        plan = self.run_history(k, s)['plan_sha256']
        result = self.run_history(k, s, True, plan)
        self.assertEqual(result['state'], 'purged')
        self.assertTrue(result['verified_empty'])
        self.assertEqual(result['deleted_objects'], 3)
        self.assertEqual(result['aborted_uploads'], 1)
        self.assertTrue(all(key.startswith(s.prefix) for key in s.deleted))
        self.assertEqual(k.history_receipt()['state'], 'purged')

    def test_unknown_tree_keep_and_foreign_database_fail_closed(self):
        for key, data in [('blocks/shared', b'block'), ('base/20260101T030000/annotations/keep', b'full'),
                          ('base/20260101T030000/backup.info', b'systemid=67890\n'),
                          ('base/20260101T030000/backup.info', b'systemid=12345\nparent_backup_id=20251201T030000\n')]:
            with self.subTest(key=key):
                k, s = HistoryKube(), FakeS3()
                s.add(key, data)
                with self.assertRaises(ValueError):
                    self.run_history(k, s)
                self.assertEqual(s.deleted + s.aborted, [])

    def test_unsafe_repository_paths(self):
        k = HistoryKube()
        for dest in ['s3://sample/../other', 's3://sample/archive//other', 's3://sample/%2e%2e', 's3://sample/archive?x=1', 's3://sample/archive#x']:
            with self.subTest(dest=dest), self.assertRaises(ValueError):
                h.repository(dict(k.policy, destination=dest))
        self.assertEqual(h.repository(dict(k.policy, destination='s3://sample')), ('sample', 'sample-db/'))

    def test_disabled_owner_health_schedule_backup_plugin_and_consumer_gates(self):
        mutations = [lambda k: k.cluster['metadata']['annotations'].update({r.DISABLED_ANNOTATION: 'foreign'}),
                     lambda k: k.cluster['status'].update(phase='Failing over'),
                     lambda k: k.cluster['spec'].update(plugins=[dict(isWALArchiver=True)]),
                     lambda k: k.cluster['spec'].update(backup=dict(retentionPolicy='7d')),
                     lambda k: k.resources[k.cron_path]['spec'].update(suspend=False),
                     lambda k: k.resources[k.schedules_path]['items'].append(dict(spec=dict(cluster=dict(name=k.policy['cluster']), suspend=False))),
                     lambda k: k.resources[f'/apis/postgresql.cnpg.io/v1/namespaces/{k.namespace}/backups?limit=500']['items'].append(dict(spec=dict(cluster=dict(name=k.policy['cluster'])), status=dict(phase='running'))),
                     lambda k: k.resources['/apis/postgresql.cnpg.io/v1/clusters?limit=500']['metadata'].update(continue_='unused', **{'continue': 'truncated'})]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                k, s = HistoryKube(), FakeS3()
                mutation(k)
                with self.assertRaises(ValueError):
                    self.run_history(k, s)
                self.assertEqual(s.scan_count, 0)

    def test_preview_reports_consumer_identity_and_size_but_execution_stays_blocked(self):
        for reference in ('external', 'backup'):
            k, s = HistoryKube(), FakeS3()
            store = dict(destinationPath=k.policy['destination'] + '/', serverName=k.policy['server_name'])
            spec = dict(externalClusters=[dict(name='source', barmanObjectStore=store)]) if reference == 'external' else dict(backup=dict(barmanObjectStore=store))
            other = dict(metadata=dict(uid='other', name='restored-db', namespace='restore-test'), spec=spec, status=dict(phase='Cluster in healthy state'))
            k.resources['/apis/postgresql.cnpg.io/v1/clusters?limit=500']['items'].append(other)
            preview = self.run_history(k, s)
            self.assertEqual(preview['inventory']['objects'], 3)
            self.assertFalse(preview['can_execute'])
            self.assertEqual(preview['archive_consumers'], ['restore-test/restored-db'])
            with self.assertRaisesRegex(ValueError, 'restore-test/restored-db'):
                self.run_history(k, s, True, preview['plan_sha256'])
            self.assertEqual(s.deleted + s.aborted, [])

    def test_active_lease_is_not_stolen(self):
        k, s = HistoryKube(), FakeS3()
        path = f'/apis/coordination.k8s.io/v1/namespaces/{k.namespace}/leases/{k.name}'
        k.resources[path]['spec'] = dict(holderIdentity='other', renewTime=dt.datetime.now(dt.timezone.utc).isoformat(), leaseDurationSeconds=1800)
        with self.assertRaises(ValueError):
            self.run_history(k, s)
        self.assertEqual(s.scan_count, 0)

    def test_drift_expired_preview_and_recent_writes_block_execution(self):
        for case in ('drift', 'expired', 'recent', 'wrong-sha', 'between-scans', 'parts-drift'):
            with self.subTest(case=case):
                k, s = HistoryKube(), FakeS3()
                if case == 'recent':
                    next(iter(s.objects.values()))['LastModified'] = dt.datetime.now(dt.timezone.utc)
                plan = self.run_history(k, s)['plan_sha256']
                if case == 'drift':
                    s.add('wals/000000010000000000000002.gz', b'new')
                elif case == 'expired':
                    receipt = k.history_receipt()
                    receipt['observed_at'] = s.old().isoformat()
                    k.resources[r.config_path(k.namespace, k.name + '-history-receipt')]['data']['receipt.json'] = json.dumps(receipt)
                elif case == 'wrong-sha':
                    plan = 'f' * 64
                elif case == 'between-scans':
                    s.drift = True
                    s.scan_count = 0
                elif case == 'parts-drift':
                    next(iter(s.uploads.values()))[0]['Size'] += 1
                with self.assertRaises(ValueError):
                    self.run_history(k, s, True, plan)
                self.assertEqual(s.deleted + s.aborted, [])

    def test_partial_delete_and_nonempty_verification_never_report_success(self):
        for case in ('error', 'not-empty'):
            k, s = HistoryKube(), FakeS3()
            plan = self.run_history(k, s)['plan_sha256']
            s.deletion_error = case == 'error'
            s.keep_after_delete = case == 'not-empty'
            with self.assertRaises(ValueError):
                self.run_history(k, s, True, plan)
            receipt = k.history_receipt()
            self.assertEqual(receipt['state'], 'partial_or_unconfirmed')
            self.assertTrue(receipt['deletion_attempted'])
            self.assertEqual(s.aborted, [])

    def test_policy_change_during_inventory_blocks_deletion(self):
        k, s = HistoryKube(), FakeS3()
        plan = self.run_history(k, s)['plan_sha256']
        original = h.inventory
        def changed(*args):
            value = original(*args)
            k.resources[r.config_path(k.namespace, k.name)]['metadata']['resourceVersion'] = '2'
            return value
        with patch.object(h, 'inventory', side_effect=changed), self.assertRaisesRegex(ValueError, 'Policy changed'):
            self.run_history(k, s, True, plan)
        self.assertEqual(s.deleted + s.aborted, [])

    def test_policy_is_revalidated_between_exact_key_batches(self):
        k, s = HistoryKube(), FakeS3()
        for index in range(110):
            s.add(f'wals/{index + 10:024X}.gz', b'wal')
        plan = self.run_history(k, s)['plan_sha256']
        original = s.delete_objects
        def changed(**kwargs):
            value = original(**kwargs)
            k.resources[r.config_path(k.namespace, k.name)]['metadata']['resourceVersion'] = 'changed'
            return value
        with patch.object(s, 'delete_objects', side_effect=changed), self.assertRaisesRegex(ValueError, 'Policy changed'):
            self.run_history(k, s, True, plan)
        self.assertEqual(len(s.deleted), 100)
        self.assertEqual(s.aborted, [])
        self.assertEqual(k.history_receipt()['state'], 'partial_or_unconfirmed')

    def test_non_r2_endpoints_never_claim_versioned_s3_history_is_empty(self):
        k, s = HistoryKube(), FakeS3()
        k.policy['endpoint'] = 'https://s3.example.test'
        k.resources[r.config_path(k.namespace, k.name)]['data']['policy.json'] = json.dumps(k.policy)
        with self.assertRaisesRegex(ValueError, 'Cloudflare R2'):
            self.run_history(k, s)
        self.assertEqual(s.scan_count, 0)

    def test_incomplete_listings_and_bounds_are_not_complete_previews(self):
        for page in [dict(IsTruncated=True), dict(IsTruncated=True, NextContinuationToken='orphan'), dict(Contents=[]), dict(IsTruncated=True, NextContinuationToken='next')]:
            k, s = HistoryKube(), FakeS3()
            s.extra_page = page
            with self.assertRaises(ValueError):
                self.run_history(k, s)
            self.assertEqual(k.history_receipt()['state'], 'blocked')
        k, s = HistoryKube(), FakeS3()
        with patch.object(h, 'MAX_OBJECTS', 1), self.assertRaises(ValueError):
            self.run_history(k, s)


if __name__ == '__main__':
    unittest.main()
