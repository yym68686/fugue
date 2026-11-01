#!/usr/bin/env python3
"""Explicit retirement of a disabled native Barman archive, not count retention.

R2 does not document atomic conditional DELETE. The operator must exclude all
external writers; Kubernetes gates, age checks and snapshots cannot fence them.
Only exact inventoried keys/upload IDs are removed. Never recurse-delete a bucket.
"""
import datetime as dt
import hashlib
import json
import os
import re
import time
from urllib.parse import urlsplit

try:
    import worker as r
except ImportError:
    from scripts import cnpg_backup_retention as r

CAPABILITY = 'fugue.pro/cnpg-history-purge'
MAX_OBJECTS = 100000
MAX_PARTS = 100000
MAX_UPLOADS = 1000
QUIET_SECONDS = 900
PLAN_SECONDS = 3600


def repository(policy):
    dest = urlsplit(policy['destination'])
    r.require(dest.scheme == 's3' and re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', dest.netloc or '')
              and not dest.query and not dest.fragment, 'Invalid archive bucket')
    path = dest.path.strip('/')
    components = path.split('/') if path else []
    r.require(all(re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]*', p) and p not in ('.', '..') for p in components)
              and '//' not in dest.path and not dest.path.endswith('//'), 'Unsafe archive path')
    server = policy['server_name']
    r.require(re.fullmatch(r'[a-z0-9][a-z0-9.-]*', server) and server not in ('.', '..'), 'Unsafe archive server')
    return dest.netloc, '/'.join(components + [server]) + '/'


def object_kind(key, prefix):
    r.require(isinstance(key, str) and key.startswith(prefix), 'Object escaped pinned archive')
    relative = key[len(prefix):]
    # Barman cloud keep annotations are base/<id>/annotations/keep (3.18).
    r.require('/annotations/' not in relative, 'Archive has retention annotations; separate review required')
    if re.fullmatch(r'base/[0-9]{8}T[0-9]{6}/(?:backup.info|(?:data|[0-9]+)(?:_[0-9]+)?\.tar(?:\.(?:gz|bz2|snappy|lz4|zst))?)', relative):
        return 'base'
    wal = r'(?:[0-9A-F]{24}(?:\.partial|\.[0-9A-F]{8}\.backup)?|[0-9A-F]{8}\.history)(?:\.(?:gz|bz2|snappy|lz4|zst))?'
    if re.fullmatch(r'wals/(?:[0-9A-F]{8,16}/)?' + wal, relative):
        return 'wals'
    raise ValueError('Unknown archive layout; refusing to delete non-Barman data')


def stamp(value):
    r.require(isinstance(value, dt.datetime) and value.tzinfo is not None, 'Missing object timestamp')
    return value.astimezone(dt.timezone.utc).isoformat()


def pages(s3, method, **kwargs):
    # SDK paginators cover all continuation markers; missing/looping markers fail.
    seen, truncated = set(), False
    for i, page in enumerate(s3.get_paginator(method).paginate(**kwargs)):
        r.require(i < 500, 'Archive listing exceeds page bound')
        r.require(isinstance(page, dict), 'Invalid object-store page')
        r.require(type(page.get('IsTruncated')) is bool, 'Missing listing completeness flag')
        truncated = page['IsTruncated']
        if page.get('IsTruncated'):
            marker_fields = {'list_objects_v2': ('NextContinuationToken',),
                             'list_multipart_uploads': ('NextKeyMarker', 'NextUploadIdMarker'),
                             'list_parts': ('NextPartNumberMarker',)}[method]
            marker = tuple(page.get(field) for field in marker_fields)
            r.require(all(value is not None and value != '' for value in marker) and marker not in seen, 'Missing or repeated listing continuation marker')
            seen.add(marker)
        yield page
    r.require(not truncated, 'Object-store listing ended before completion')


def inventory(s3, policy):
    bucket, prefix = repository(policy)
    objects, uploads, parts_count = [], [], 0
    seen, seen_uploads, upload_keys = set(), set(), set()
    for page in pages(s3, 'list_objects_v2', Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            kind = object_kind(key, prefix)
            r.require(key not in seen and type(obj.get('Size')) is int and obj['Size'] >= 0 and obj.get('ETag'), 'Invalid or duplicate object')
            seen.add(key)
            objects.append(dict(key=key, kind=kind, size=obj['Size'], etag=obj['ETag'], modified=stamp(obj['LastModified'])))
            r.require(len(objects) <= MAX_OBJECTS, 'Archive exceeds object bound')
    for page in pages(s3, 'list_multipart_uploads', Bucket=bucket, Prefix=prefix):
        for upload in page.get('Uploads', []):
            key, upload_id = upload['Key'], upload['UploadId']
            kind = object_kind(key, prefix)
            r.require(kind == 'base' and not key.endswith('/backup.info'), 'Unexpected multipart archive object')
            r.require(isinstance(upload_id, str) and upload_id and (key, upload_id) not in seen_uploads, 'Invalid or duplicate upload')
            # Opaque handles cannot disambiguate concurrent uploads of one key
            # across LIST calls. Never guess which one is the previewed upload.
            r.require(key not in upload_keys, 'Multiple uploads for one archive key require separate review')
            seen_uploads.add((key, upload_id))
            upload_keys.add(key)
            parts, numbers = [], set()
            for part_page in pages(s3, 'list_parts', Bucket=bucket, Key=key, UploadId=upload_id):
                for part in part_page.get('Parts', []):
                    number = part['PartNumber']
                    r.require(type(number) is int and 1 <= number <= 10000 and number not in numbers
                              and type(part.get('Size')) is int and part['Size'] >= 0 and part.get('ETag'), 'Invalid upload part')
                    numbers.add(number)
                    parts.append(dict(number=number, size=part['Size'], etag=part['ETag'], modified=stamp(part['LastModified'])))
                    parts_count += 1
                    r.require(parts_count <= MAX_PARTS, 'Archive exceeds part bound')
            uploads.append(dict(key=key, upload_id=upload_id, initiated=stamp(upload['Initiated']), parts=sorted(parts, key=lambda p: p['number'])))
            r.require(len(uploads) <= MAX_UPLOADS, 'Archive exceeds upload bound')
    objects.sort(key=lambda obj: obj['key'])
    # R2 may rotate the opaque UploadId returned for an unchanged multipart
    # upload between LIST calls. The id is required only for the final abort;
    # it is deliberately excluded from the preview digest.
    uploads.sort(key=lambda upload: upload['key'])
    # Validate every available catalogue identity before any destructive action.
    # Orphan/failed tar uploads without backup.info are allowed in a retired archive.
    for obj in objects:
        if not obj['key'].endswith('/backup.info'):
            continue
        r.require(obj['size'] <= 1024 * 1024, 'Oversized Barman metadata')
        response = s3.get_object(Bucket=bucket, Key=obj['key'], IfMatch=obj['etag'])
        try:
            raw = response['Body'].read(1024 * 1024 + 1)
        finally:
            response['Body'].close()
        r.require(len(raw) <= 1024 * 1024, 'Oversized Barman metadata')
        info = raw.decode('utf-8')
        ids = re.findall(r'^systemid\s*=\s*([0-9]+)\s*$', info, re.MULTILINE)
        r.require(ids == [policy['system_id']], 'Archive metadata database system ID mismatch')
        fields = dict(line.split('=', 1) for line in info.splitlines() if '=' in line)
        r.require(all(fields.get(field, 'None').strip() in ('', 'None') for field in ('parent_backup_id', 'snapshots_info')), 'Incremental or snapshot archive requires separate retirement')
    return dict(bucket=bucket, prefix=prefix, objects=objects, uploads=uploads)


def summary(snapshot):
    objects, uploads = snapshot['objects'], snapshot['uploads']
    result = {kind: dict(objects=sum(obj['kind'] == kind for obj in objects),
                         bytes=sum(obj['size'] for obj in objects if obj['kind'] == kind)) for kind in ('base', 'wals')}
    result.update(bucket=snapshot['bucket'], prefix=snapshot['prefix'], objects=len(objects),
                  object_bytes=sum(obj['size'] for obj in objects), multipart_uploads=len(uploads),
                  multipart_parts=sum(len(upload['parts']) for upload in uploads),
                  multipart_bytes=sum(part['size'] for upload in uploads for part in upload['parts']))
    result['total_bytes'] = result['object_bytes'] + result['multipart_bytes']
    return result


def upload_identity(uploads):
    # Keep key, initiated timestamp and EVERY part's number/size/ETag/time. Only
    # the provider's rotating operation handle is excluded from content identity.
    return [{key: value for key, value in upload.items() if key != 'upload_id'} for upload in uploads]


def digest(policy, uid, rv, snapshot):
    normalized = dict(snapshot, uploads=upload_identity(snapshot['uploads']))
    document = dict(schema='fugue.barman-history-plan/v2', policy=policy, policy_uid=uid, policy_resource_version=rv, inventory=normalized)
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def collection(kube, path):
    value = kube.request('GET', path + '?limit=500')
    r.require(isinstance(value.get('items'), list) and len(value['items']) <= 500 and not value.get('metadata', {}).get('continue'), 'Kubernetes inventory is invalid or truncated')
    return value['items']


def check_live(kube, policy, name, uid, rv, execute=True):
    cm = kube.request('GET', r.config_path(policy['namespace'], name))
    r.require(cm['metadata']['uid'] == uid and cm['metadata']['resourceVersion'] == rv, 'Policy changed during archive retirement')
    cluster = kube.request('GET', r.cluster_path(policy['namespace'], policy['cluster']))
    r.validate_policy(policy, cluster, uid)
    spec, status = cluster['spec'], cluster.get('status', {})
    r.require(policy.get('barman_disabled') and not policy['enabled'] and cluster['metadata'].get('annotations', {}).get(r.DISABLED_ANNOTATION) == uid,
              'Barman must be explicitly disabled before retiring history')
    r.require(cluster['metadata'].get('annotations', {}).get(r.ANNOTATION, uid) == uid, 'A different count policy owns the archive')
    r.require(not spec.get('backup', {}).get('barmanObjectStore') and not spec.get('backup', {}).get('retentionPolicy'), 'Barman shutdown is not complete')
    r.require(not any(plugin.get('isWALArchiver') for plugin in spec.get('plugins', [])), 'Plugin WAL archiver is unsupported')
    r.require(status.get('phase') == 'Cluster in healthy state' and status.get('readyInstances', 0) >= 1
              and status.get('currentPrimary') and status.get('currentPrimary') == status.get('targetPrimary'), 'Database is unhealthy or changing primary')
    ns = policy['namespace']
    cron = kube.request('GET', f'/apis/batch/v1/namespaces/{ns}/cronjobs/{name}')
    r.require(cron['spec'].get('suspend') is True and cron['metadata'].get('annotations', {}).get(CAPABILITY) == 'v1', 'Retention executor must be upgraded and suspended')
    for schedule in collection(kube, f'/apis/postgresql.cnpg.io/v1/namespaces/{ns}/scheduledbackups'):
        s = schedule.get('spec', {})
        if s.get('cluster', {}).get('name') == policy['cluster'] and s.get('method', 'barmanObjectStore') == 'barmanObjectStore':
            r.require(s.get('suspend') is True, 'A native Barman schedule is not suspended')
    for backup in collection(kube, f'/apis/postgresql.cnpg.io/v1/namespaces/{ns}/backups'):
        if backup.get('spec', {}).get('cluster', {}).get('name') == policy['cluster']:
            r.require(backup.get('status', {}).get('phase') in ('completed', 'failed'), 'A database backup is pending or running')
    consumers = []
    for other in collection(kube, '/apis/postgresql.cnpg.io/v1/clusters'):
        if other['metadata']['uid'] == policy['cluster_uid']:
            continue
        stores = [(other.get('spec', {}).get('backup', {}).get('barmanObjectStore', {}), other['metadata']['name'])]
        stores += [(e.get('barmanObjectStore', {}), e.get('name')) for e in other.get('spec', {}).get('externalClusters', [])]
        for store, fallback in stores:
            if not store:
                continue
            other_policy = dict(policy, destination=store.get('destinationPath', ''), server_name=store.get('serverName', fallback) or '')
            # Compare physical bucket/key identity, not endpoint aliases or trailing slashes.
            if repository(other_policy) == repository(policy):
                target = other['metadata']['namespace'] + '/' + other['metadata']['name']
                if target not in consumers:
                    consumers.append(target)
    consumers.sort()
    r.require(not execute or not consumers, 'Another cluster references this archive; history retirement is blocked: ' + ', '.join(consumers))
    return consumers


def quiet(snapshot):
    times = [obj['modified'] for obj in snapshot['objects']]
    for upload in snapshot['uploads']:
        times.append(upload['initiated'])
        times += [part['modified'] for part in upload['parts']]
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=QUIET_SECONDS)
    r.require(all(dt.datetime.fromisoformat(value) <= cutoff for value in times), 'Archive was written within the last 15 minutes; wait and preview again')


def run(kube, s3, namespace, name, operation, job_uid, execute=False, expected=''):
    r.require(re.fullmatch(r'[a-f0-9]{32}', operation) and job_uid, 'Missing history Job identity')
    config = kube.request('GET', r.config_path(namespace, name))
    policy = json.loads(config['data']['policy.json'])
    meta = config['metadata']
    r.require(meta['namespace'] == namespace and meta['name'] == name and policy['namespace'] == namespace, 'Policy identity changed')
    uid, rv = meta['uid'], meta['resourceVersion']
    repository(policy)
    # ListObjectsV2 cannot prove removal of versioned S3 history. This retired
    # archive path deliberately supports R2, not arbitrary S3-compatible stores.
    endpoint = urlsplit(policy['endpoint'])
    r.require(endpoint.scheme == 'https' and re.fullmatch(r'[a-f0-9]{32}(?:\.(?:eu|fedramp))?\.r2\.cloudflarestorage\.com', endpoint.hostname or '')
              and endpoint.path in ('', '/') and not endpoint.port and not endpoint.query and not endpoint.fragment and not endpoint.username,
              'History retirement currently supports native unversioned Cloudflare R2 endpoints only')
    receipt_path = r.config_path(namespace, name + '-history-receipt')
    path, lock = r.acquire(kube, namespace, name, 'history/' + operation)
    started = time.monotonic()
    evidence = dict(state='blocked', operation_id=operation, job_uid=job_uid, dry_run=not execute, deletion_attempted=False,
                    deleted_objects=0, aborted_uploads=0, observed_at=dt.datetime.now(dt.timezone.utc).isoformat())

    def save():
        receipt = kube.request('GET', receipt_path)
        receipt['data'] = {'receipt.json': json.dumps(evidence)}
        kube.request('PUT', receipt_path, receipt)

    def guard():
        r.require(time.monotonic() - started < 1000, 'History operation exceeded time bound; preview remaining data again')
        current_lock = kube.request('GET', path)
        r.require(current_lock.get('spec', {}).get('holderIdentity') == 'history/' + operation, 'History operation lost its Lease')
        return check_live(kube, policy, name, uid, rv, execute)

    try:
        guard()
        if s3 is None:
            import boto3
            from botocore.config import Config
            s3 = boto3.client('s3', endpoint_url=policy['endpoint'], region_name='auto',
                              config=Config(connect_timeout=10, read_timeout=30, retries={'max_attempts': 2}))
        if execute:
            r.require(re.fullmatch(r'[a-f0-9]{64}', expected), 'Execution requires a preview SHA256')
            previous = json.loads(kube.request('GET', receipt_path)['data']['receipt.json'])
            r.require(previous.get('state') == 'planned' and previous.get('can_execute') is True and previous.get('plan_sha256') == expected, 'Matching executable preview is required')
            age = dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(previous['observed_at'])
            r.require(0 <= age.total_seconds() <= PLAN_SECONDS, 'Preview expired; create a fresh preview')
        snapshot = inventory(s3, policy)
        plan_hash = digest(policy, uid, rv, snapshot)
        evidence.update(inventory=summary(snapshot), plan_sha256=plan_hash, plan_schema='fugue.barman-history-plan/v2',
                        warning='Permanent removal of ALL Barman restore points. Does not remove Backup CRs, Secrets, local WAL or Longhorn. External writers must be excluded; object-store DELETE is not atomically fenced.')
        consumers = guard()
        evidence.update(can_execute=not consumers, archive_consumers=consumers,
                        execution_blockers=['Another cluster references this archive: ' + consumer for consumer in consumers])
        if not execute:
            evidence['state'] = 'planned'
            return evidence
        r.require(plan_hash == expected, 'Archive changed since preview; no deletion attempted')
        quiet(snapshot)
        latest = inventory(s3, policy)
        r.require(digest(policy, uid, rv, latest) == plan_hash, 'Archive changed during validation; no deletion attempted')
        # Use the newest opaque UploadId, while retaining the preview's content
        # and age checks. R2's UploadId is not a stable object identity.
        snapshot['uploads'] = latest['uploads']
        guard()
        evidence['state'] = 'deleting'
        save()  # A killed Job never leaves a previous successful-preview receipt.
        bucket = snapshot['bucket']
        for offset in range(0, len(snapshot['objects']), 100):
            guard()
            batch = snapshot['objects'][offset:offset + 100]
            for obj in batch:
                head = s3.head_object(Bucket=bucket, Key=obj['key'], IfMatch=obj['etag'])
                r.require(head['ETag'] == obj['etag'] and head['ContentLength'] == obj['size'] and stamp(head['LastModified']) == obj['modified'], 'Object changed; cleanup stopped, preview remaining data again')
            guard()
            evidence['deletion_attempted'] = True
            save()
            response = s3.delete_objects(Bucket=bucket, Delete={'Objects': [{'Key': obj['key']} for obj in batch], 'Quiet': False})
            deleted = response.get('Deleted', [])
            evidence['deleted_objects'] += len(deleted)
            evidence['last_batch_unconfirmed'] = len(batch) - len(deleted)
            save()
            r.require(not response.get('Errors') and sorted(obj.get('Key', '') for obj in deleted) == sorted(obj['key'] for obj in batch), 'Object deletion was partial or unconfirmed; preview remaining data again')
        # Re-list exact pending uploads, including part ETags/timestamps, before abort.
        guard()
        remaining = inventory(s3, policy)
        r.require(not remaining['objects'] and upload_identity(remaining['uploads']) == upload_identity(snapshot['uploads']), 'Archive changed before multipart cleanup; preview remaining data again')
        for upload in remaining['uploads']:
            guard()
            evidence['deletion_attempted'] = True
            save()
            s3.abort_multipart_upload(Bucket=bucket, Key=upload['key'], UploadId=upload['upload_id'])
            evidence['aborted_uploads'] += 1
            save()
        guard()
        after = inventory(s3, policy)
        guard()
        evidence['remaining'] = summary(after)
        r.require(not after['objects'] and not after['uploads'], 'Archive is not empty after cleanup; preview remaining data again')
        evidence.update(state='purged', verified_empty=True)
        return evidence
    except Exception as error:
        evidence['state'] = 'partial_or_unconfirmed' if evidence['deletion_attempted'] else 'blocked'
        evidence['reason'] = str(error) if isinstance(error, ValueError) else 'History operation failed; inspect access and retry with a fresh preview. Raw service errors are not logged.'
        raise
    finally:
        try:
            save()
        finally:
            lock['spec']['holderIdentity'] = ''
            kube.request('PUT', path, lock)
            print(json.dumps(evidence))


def main():
    kube = r.Kube()
    ns, name = os.environ['POLICY_NAMESPACE'], os.environ['POLICY_NAME']
    try:
        run(kube, None, ns, name, os.environ['HISTORY_OPERATION_ID'], os.environ['HISTORY_JOB_UID'],
            os.environ.get('HISTORY_EXECUTE') == 'true', os.environ.get('HISTORY_PLAN_SHA256', ''))
    except Exception:
        raise SystemExit('History operation did not complete; inspect its receipt. Raw credentials are not logged.')


if __name__ == '__main__':
    main()
