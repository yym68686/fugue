#!/usr/bin/env python3
"""One explicitly authorized, isolated, budgeted copy. No route or retirement.

The trusted main CI runner alone decrypts two role sessions. The worker gets
neither platform keys nor a Kubernetes token; all output is allowlisted.
"""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import time

from scripts import compiler_input_readonly as r
from scripts.compiler_input_archive import credential_key
from scripts.compiler_input_storage import command, get, kube

PURPOSE = 'compiler-input-copy'
OPTIONS = '-c statement_timeout=5s -c lock_timeout=250ms -c work_mem=1MB -c idle_in_transaction_session_timeout=10s'
require = r.require


def validate(c):
    extra = {'copy', 'storage', 'maxWalGrowthBytes', 'maxAvailableDropBytes', 'maxNewDeadlineFailures'}
    base = {k: v for k, v in c.items() if k not in extra}
    require(type(base.get('version')) is int and base['version'] == 1, 'invalid copy declaration')
    base.update(version=3, readCount=0)
    r.validate(base)
    require(set(c) == (set(base)-{'readCount'}) | extra, 'invalid copy declaration fields')
    require(c['minimumFreeBytes'] >= 3 << 30, 'copy reserve is below 3GiB')
    for key, low, high in [('maxWalGrowthBytes', 1, 1 << 30), ('maxAvailableDropBytes', 1, 2 << 30), ('maxNewDeadlineFailures', 0, 8)]:
        require(type(c[key]) is int and low <= c[key] <= high, 'invalid copy impact bound')
    x = c['copy']
    fields = {'version', 'digest', 'source_format', 'raw_bytes', 'backend_hash', 'segment_bytes', 'max_put_bytes', 'max_get_bytes', 'max_put_requests', 'max_get_requests', 'payload_bytes_per_second', 'max_seconds'}
    require(isinstance(x, dict) and set(x) == fields and type(x['version']) is int and x['version'] == 1, 'invalid native copy contract')
    require(isinstance(x['digest'], str) and re.fullmatch('sha256:[a-f0-9]{64}', x['digest'])
            and isinstance(x['backend_hash'], str) and re.fullmatch('sha256:[a-f0-9]{64}', x['backend_hash'])
            and x['source_format'] in ('runtime-snapshot-json-v1', 'runtime-snapshot-json-chunk-codec-v2'), 'invalid copy source or backend')
    for key, low, high in [('raw_bytes', 1, 1 << 30), ('segment_bytes', 64 << 10, 1 << 20), ('max_put_bytes', 1, 2 << 30),
                           ('max_get_bytes', 1, 4 << 30), ('max_put_requests', 1, 16384), ('max_get_requests', 1, 32768),
                           ('payload_bytes_per_second', 64 << 10, 8 << 20), ('max_seconds', 60, 600)]:
        require(type(x[key]) is int and low <= x[key] <= high, 'invalid copy payload bound')
    segments = (x['raw_bytes']+x['segment_bytes']-1)//x['segment_bytes']
    require(x['max_put_requests'] == segments and x['max_get_requests'] == 2*segments, 'copy request budget differs from two-pass verification')
    s = c['storage']
    names = {'longhorn_namespace', 'volume', 'replica', 'instance_manager', 'node'}
    uids = {'volume_uid', 'pvc_uid', 'pv_uid', 'replica_uid', 'instance_manager_uid', 'disk_id'}
    require(isinstance(s, dict) and set(s) == names | uids | {'disk_path'}, 'invalid copy backing-storage declaration')
    require(all(isinstance(s[k], str) and re.fullmatch('[a-z0-9][a-z0-9.-]{0,127}', s[k]) for k in names)
            and all(isinstance(s[k], str) and re.fullmatch('[a-f0-9-]{36}', s[k]) for k in uids)
            and re.fullmatch('/[a-zA-Z0-9/_-]{1,200}', s['disk_path']), 'invalid copy backing-storage identity')
    return c


def job_manifest(c, name, session_name):
    # Reuse proven UID-cleanup and isolation shapes, never the readonly command
    # or its one-shot identity. The source/byte contract lives in a mounted
    # immutable Secret item; credentials are not command arguments.
    job = r.job_manifest({**c, 'readCount': 0}, name, session_name)
    job['metadata']['labels']['fugue.pro/purpose'] = PURPOSE
    template = job['spec']['template']
    template['metadata']['labels']['fugue.pro/purpose'] = PURPOSE
    spec = template['spec']
    spec['terminationGracePeriodSeconds'] = 30
    spec['volumes'] = [{'name': 'contract', 'secret': {'secretName': session_name, 'defaultMode': 0o444, 'items': [{'key': 'copy.json', 'path': 'copy.json'}]}}]
    container = spec['containers'][0]
    container.update(name='copy', command=['nice', '-n', '19', '/usr/local/bin/fugue-input-storage'],
                     args=['-mode=archive-copy', '-apply', '-copy-contract=/session/copy.json'],
                     volumeMounts=[{'name': 'contract', 'mountPath': '/session', 'readOnly': True}])
    pairs = [('FUGUE_DATABASE_URL', 'database-url')]
    for suffix, key in [('ENDPOINT', 'endpoint'), ('ACCOUNT_ID', 'account-id'), ('REGION', 'region'), ('BUCKET', 'bucket'), ('PREFIX', 'prefix'),
                        ('ACCESS_KEY_ID', 'writer-access-key-id'), ('SECRET_ACCESS_KEY', 'writer-secret-access-key'),
                        ('READER_ACCESS_KEY_ID', 'reader-access-key-id'), ('READER_SECRET_ACCESS_KEY', 'reader-secret-access-key')]:
        pairs.append(('FUGUE_COMPILER_INPUT_ARCHIVE_'+suffix, key))
    container['env'] = [{'name': name, 'valueFrom': {'secretKeyRef': {'name': session_name, 'key': key}}} for name, key in pairs]
    container['env'] += [{'name': name, 'value': value} for name, value in [('PGOPTIONS', OPTIONS), ('GOMAXPROCS', '1'), ('GOMEMLIMIT', '128MiB'),
                                ('FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED', 'true'), ('FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED', 'false')]]
    return job


def metadata(c, credentials=False):
    digest = c['copy']['digest']  # validated exact SHA, never arbitrary SQL
    platform = ", 'platform',(SELECT jsonb_build_object('account_id',body->>'account_id','compiler_input_archive',body->'compiler_input_archive') FROM fugue_object_storage_config WHERE id='platform')" if credentials else ''
    sql = """BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB';
SELECT json_build_object('manifest',(SELECT json_build_object('format',format,'raw_bytes',size_bytes,'chunk_size',chunk_size,'chunk_count',chunk_count,'created_at',created_at,'location',storage_location)
 FROM fugue_platform_input_manifests WHERE content_hash='"""+digest+"""'),
'source_chunks',(SELECT count(*) FROM fugue_platform_input_chunks WHERE content_hash='"""+digest+"""'),
'archive',(SELECT json_build_object('state',state,'digest',verified_digest,'verified',verified_at IS NOT NULL,'raw_bytes',raw_bytes,'segments',segment_count,
 'stored_bytes',(SELECT COALESCE(sum(stored_bytes),0) FROM fugue_platform_input_archive_segments s WHERE s.content_hash=a.content_hash)) FROM fugue_platform_input_archives a WHERE content_hash='"""+digest+"""'),
'other_leases',(SELECT count(*) FROM fugue_platform_input_archive_leases WHERE content_hash<>'"""+digest+"""' AND expires_at>transaction_timestamp()),
'all_leases',(SELECT count(*) FROM fugue_platform_input_archive_leases WHERE expires_at>transaction_timestamp()),
'fenced',(SELECT writes_fenced OR retirement_started_at IS NOT NULL FROM fugue_platform_input_archive_control WHERE singleton),
'epoch',(SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton),
'wal_lsn',pg_current_wal_lsn(), 'wal_bytes',(SELECT wal_bytes::text FROM pg_stat_wal),
'active_clients',(SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND state='active' AND pid<>pg_backend_pid()),
'worker_connections',(SELECT count(*) FROM pg_stat_activity WHERE application_name='compiler-input-copy')"""+platform+");\nCOMMIT;\n"
    argv = ['kubectl', '-n', c['namespace'], 'exec', '-i', c['primary'], '-c', c['postgresContainer'], '--', 'psql', '-U', 'postgres', '-d', c['database'], '-qAt', '-v', 'ON_ERROR_STOP=1']
    p = subprocess.run(argv, input=sql, capture_output=True, text=True, timeout=20)
    require(p.returncode == 0 and len(p.stdout) <= 128 << 10, 'copy metadata observation unavailable')
    try:
        value = json.loads(p.stdout)
    except (ValueError, TypeError):
        raise r.Refusal('copy metadata observation malformed') from None
    require(value['fenced'] is False and value['other_leases'] == 0, 'concurrent archive maintenance exists')
    m = value['manifest']
    require(m and m['format'] == c['copy']['source_format'] and m['raw_bytes'] == c['copy']['raw_bytes']
            and m['location'] == 'legacy' and m['chunk_count'] == value['source_chunks'], 'copy source identity or row count changed')
    return value


def copy_session(c, cluster, pod, deployment, executor, observed):
    # Only in the trusted runner, never in the isolated Job, arguments or logs.
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    container = next(x for x in deployment['spec']['template']['spec']['containers'] if x['name'] == executor['container'])
    key = credential_key(c['namespace'], container, {})
    platform = observed['platform']
    data = r.reader_session(platform, key, c['copy']['backend_hash'])
    try:
        raw = base64.b64decode(platform['compiler_input_archive']['writer']['ciphertext'], validate=True)
        writer = json.loads(AESGCM(key).decrypt(raw[:12], raw[12:], None))
    except Exception:
        raise r.Refusal('copy writer session decryption failed') from None
    require(set(writer) <= {'access_key_id', 'secret_access_key', 'token'} and writer.get('access_key_id') and writer.get('secret_access_key')
            and not writer.get('token') and writer['access_key_id'] != data['reader-access-key-id'], 'copy role session is invalid')
    data.update({'writer-access-key-id': writer['access_key_id'], 'writer-secret-access-key': writer['secret_access_key']})
    database = r.session({**c, 'version': 3, 'readCount': 0}, cluster, pod, {}, {}, {})['database-readonly-url']
    # r.session uses the ordinary application role, not a new grant. Override
    # only this short-lived session's READ ONLY default; the exact native mode
    # limits writes to archive leases/receipts and preserves the source.
    import urllib.parse
    parsed = urllib.parse.urlsplit(database)
    query = urllib.parse.urlencode({'sslmode': 'require', 'connect_timeout': '5', 'application_name': PURPOSE, 'options': OPTIONS})
    data['database-url'] = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ''))
    data['copy.json'] = json.dumps(c['copy'], sort_keys=True, separators=(',', ':'))
    return data


def node_gate(c, job_uid=''):
    summary = r.node_gate(c, job_uid)
    stats = json.loads(r.cluster_kube('get', '--raw', '/api/v1/nodes/'+c['node']+'/proxy/stats/summary'))
    fs = stats['node']['fs']
    # The original 25% physical-data-disk floor is enforced on the exact
    # Longhorn backing filesystem below, not on an unrelated worker root.
    # This root stores at most the full 256MiB Job ephemeral limit; preserve
    # an additional 16GiB plus 192MiB and the existing DiskPressure gate.
    require(type(fs['capacityBytes']) is int and type(fs['availableBytes']) is int
            and fs['availableBytes'] >= (16 << 30)+(192 << 20)+(256 << 20), 'copy worker filesystem reserve unavailable')
    summary.update(filesystem_available_bytes=fs['availableBytes'], filesystem_capacity_bytes=fs['capacityBytes'])
    return summary


def backing_gate(c, primary, starting=False):
    # Existing Longhorn instance-manager, metadata/statvfs only. No helper Pod,
    # snapshot traversal, file contents, trim, sync, or storage mutations.
    s = c['storage']
    pvc = get(c['namespace'], 'pvc', c['primary'])
    pv = r.cluster_get('pv', pvc['spec']['volumeName'])
    require(pvc['metadata']['uid'] == s['pvc_uid'] and pv['metadata']['uid'] == s['pv_uid']
            and pv['spec']['claimRef']['uid'] == s['pvc_uid'] and pv['spec']['csi']['driver'] == 'driver.longhorn.io'
            and pv['spec']['csi']['volumeHandle'] == s['volume'], 'copy backing-volume binding changed')
    volume = get(s['longhorn_namespace'], 'volumes.longhorn.io', s['volume'])
    require(volume['metadata']['uid'] == s['volume_uid'] and volume['spec'].get('dataEngine') == 'v1'
            and volume['spec'].get('numberOfReplicas') == 1 and volume['status'].get('state') == 'attached'
            and volume['status'].get('robustness') == 'healthy' and not volume['status'].get('expansionRequired')
            and volume['status'].get('currentNodeID') == primary['spec']['nodeName'] == s['node'], 'copy backing-volume topology or health changed')
    replicas = json.loads(kube(s['longhorn_namespace'], 'get', 'replicas.longhorn.io', '-l', 'longhornvolume='+s['volume'], '-o', 'json'))['items']
    require(len(replicas) == 1, 'copy backing-replica inventory is ambiguous')
    replica = replicas[0]
    require(replica['metadata']['name'] == s['replica'] and replica['metadata']['uid'] == s['replica_uid']
            and replica['spec']['volumeName'] == s['volume'] and replica['spec']['nodeID'] == s['node']
            and replica['spec']['diskID'] == s['disk_id'] and replica['spec']['diskPath'] == s['disk_path']
            and replica['spec'].get('active') is True and not replica['spec'].get('failedAt')
            and replica['status'].get('currentState') == 'running' and replica['status']['instanceManagerName'] == s['instance_manager'], 'copy backing-replica identity changed')
    manager = get(s['longhorn_namespace'], 'pod', s['instance_manager'])
    require(manager['metadata']['uid'] == s['instance_manager_uid'] and manager['spec']['nodeName'] == s['node']
            and not manager['metadata'].get('deletionTimestamp')
            and any(x['name'] == 'instance-manager' and x['ready'] for x in manager['status']['containerStatuses']), 'copy backing observer changed')
    program = 'import json,os,sys; s=os.statvfs(sys.argv[1]); print(json.dumps(dict(total_bytes=s.f_blocks*s.f_frsize,free_bytes=s.f_bfree*s.f_frsize,available_bytes=s.f_bavail*s.f_frsize)))'
    fs = json.loads(kube(s['longhorn_namespace'], 'exec', s['instance_manager'], '-c', 'instance-manager', '--', 'python3', '-c', program, '/host'+s['disk_path']))
    require(set(fs) == {'total_bytes', 'free_bytes', 'available_bytes'} and all(type(x) is int and x >= 0 for x in fs.values())
            and 0 <= fs['available_bytes'] <= fs['free_bytes'] <= fs['total_bytes'] and fs['total_bytes'] > 0, 'copy backing-space observation malformed')
    require(get(s['longhorn_namespace'], 'pod', s['instance_manager'])['metadata']['uid'] == s['instance_manager_uid'], 'copy backing observer changed during stat')
    minimum = fs['total_bytes']//4+(16 << 30)+(192 << 20)
    # Reserve the Job's full ephemeral-storage limit on the backing disk, plus
    # the separately authorized maximum PG-volume drop for the initial gate.
    required = minimum+(256 << 20)+ (c['maxAvailableDropBytes'] if starting else 0)
    return {**fs, 'volume_uid': s['volume_uid'], 'replica_uid': s['replica_uid'], 'instance_manager_uid': s['instance_manager_uid'],
            'protected_floor_bytes': minimum, 'required_bytes': required, 'remaining_budget_bytes': fs['free_bytes']-required,
            'capacity_gate_passed': fs['free_bytes'] >= required}


def api_activity(c, serving):
    observed = {}
    for name, uid in serving['api_pods'].items():
        pod = get(c['namespace'], 'pod', name)
        require(pod['metadata']['uid'] == uid, 'copy API owner changed')
        raw = json.loads(kube(c['namespace'], 'exec', name, '-c', 'api', '--', '/usr/local/bin/fugue-input-storage', '-mode=observe'))
        fields = raw['compiler_input_separated_activity']
        selected = {k: fields[k] for k in ('write_failures', 'budget_fallbacks')}
        require(all(type(v) is int and v >= 0 for v in selected.values()), 'copy error baseline malformed')
        selected['category'] = fields['last_failure_category']
        require(selected['category'] in ('', 'deadline'), 'copy baseline has a new writer failure category')
        observed[uid] = selected
    return observed


def api_log_signals(c, serving):
    # Exact Deployment owner Pods only. Read a small recent tail, retain only
    # aggregate categories, then recheck UID/image/container/restart identity.
    result = {}
    for name, uid in serving['api_pods'].items():
        before = get(c['namespace'], 'pod', name)
        container = next(x for x in before['status']['containerStatuses'] if x['name'] == 'api')
        body = kube(c['namespace'], 'logs', name, '-c', 'api', '--since=1m', '--tail=200')
        require(len(body) <= 1 << 20, 'copy bounded API log sample exceeded')
        lines = body.splitlines()
        counts = {'critical': sum(bool(re.search(r'panic|fatal', x, re.I)) for x in lines),
                  'archive_error': sum(bool(re.search(r'archive', x, re.I) and re.search(r'error|fail', x, re.I)) for x in lines)}
        after = get(c['namespace'], 'pod', name)
        current = next(x for x in after['status']['containerStatuses'] if x['name'] == 'api')
        require(before['metadata']['uid'] == after['metadata']['uid'] == uid
                and all(current.get(k) == container.get(k) for k in ('containerID', 'imageID', 'restartCount')),
                'copy API owner changed during bounded log sampling')
        require(counts['critical'] == counts['archive_error'] == 0, 'copy API bounded log error appeared')
        result[uid] = counts
    return result


def check_impact(c, initial, current):
    require(current['epoch'] == initial['epoch'] and current['manifest'] == initial['manifest'] and current['source_chunks'] == initial['source_chunks'], 'copy source or maintenance epoch changed')
    require(0 <= int(current['wal_bytes'])-int(initial['wal_bytes']) <= c['maxWalGrowthBytes'], 'copy WAL growth budget exceeded or counter reset')
    require(current['other_leases'] == 0 and 0 <= current['all_leases'] <= 1, 'concurrent compiler-input archive worker appeared')
    require(current['active_clients'] <= initial['active_clients']+4 and current['worker_connections'] <= 1, 'copy database connection pressure increased')


def valid_error_category(category):
    return isinstance(category, str) and (category in {'deadline', 'canceled', 'conflict', 'invalid-input', 'network', 'budget', 'integrity', 'unclassified'}
                                         or re.fullmatch('[A-Z0-9]{5}', category) is not None)


def error_category(body):
    # Preserve only the executor's fixed bounded category before exact-UID
    # cleanup removes Pod logs. No arbitrary stderr reaches CI evidence.
    categories = re.findall(r'^compiler input maintenance completion not confirmed; inspect state before retrying; category=([A-Z0-9]{5}|[a-z0-9-]{1,32})$', body, re.MULTILINE)
    require(len(categories) <= 1, 'copy error category is ambiguous')
    require(not categories or valid_error_category(categories[0]), 'copy error category is invalid')
    return categories[0] if categories else None


def result_contract(body, c, success=True):
    require(len(body) <= 16384, 'copy output exceeds budget')
    lines = [json.loads(x) for x in body.splitlines() if x.startswith('{')]
    require(len(lines) == 1, 'copy native receipt missing or ambiguous')
    v = lines[0]
    fields = {'action', 'digest', 'source_format', 'raw_bytes', 'verified_raw_bytes', 'segments', 'state', 'source_preserved', 'route_unchanged', 'usage', 'elapsed_ms'}
    require(set(v) in (fields, fields | {'failure'}) and v['action'] == 'archive-copy' and v['digest'] == c['copy']['digest'] and v['source_format'] == c['copy']['source_format'], 'copy result identity differs')
    if 'failure' in v:
        f = v['failure']
        require(not success and isinstance(f, dict) and set(f) == {'stage', 'category', 'segment_ordinal', 'segment_bytes_read'}
                and f['stage'] in ('source-read', 'unclassified') and valid_error_category(f['category'])
                and type(f['segment_ordinal']) is int and type(f['segment_bytes_read']) is int
                and ((f['stage'] == 'source-read' and 0 <= f['segment_ordinal'] < c['copy']['max_put_requests']
                      and 0 <= f['segment_bytes_read'] < min(c['copy']['segment_bytes'], c['copy']['raw_bytes']-f['segment_ordinal']*c['copy']['segment_bytes']))
                     or (f['stage'] == 'unclassified' and f['segment_ordinal'] == -1 and f['segment_bytes_read'] == 0)), 'copy failure receipt differs')
    require(v['raw_bytes'] == c['copy']['raw_bytes'] and type(v['verified_raw_bytes']) is int and v['verified_raw_bytes'] in (0, c['copy']['raw_bytes'])
            and type(v['source_preserved']) is bool and type(v['route_unchanged']) is bool and v['state'] in ('', 'verified')
            and type(v['segments']) is int and 0 <= v['segments'] <= c['copy']['max_put_requests']
            and type(v['elapsed_ms']) is int and 0 <= v['elapsed_ms'] <= 1000*c['copy']['max_seconds']+5000, 'copy result shape differs')
    u = v['usage']
    require(set(u) == {'put_payload_bytes', 'get_payload_bytes', 'put_requests', 'get_requests', 'source_raw_bytes'}
            and all(type(x) is int and x >= 0 for x in u.values()), 'copy usage receipt differs')
    require(u['put_requests'] <= c['copy']['max_put_requests'] and u['get_requests'] <= c['copy']['max_get_requests']
            and u['put_payload_bytes'] <= c['copy']['max_put_bytes'] and u['get_payload_bytes'] <= c['copy']['max_get_bytes']
            and u['source_raw_bytes'] <= c['copy']['raw_bytes'], 'copy partial usage exceeds contract')
    if not success: return v
    require(v['state'] == 'verified' and v['source_preserved'] is True and v['route_unchanged'] is True
            and v['verified_raw_bytes'] == c['copy']['raw_bytes'], 'copy did not fully verify the original bytes')
    require(u['put_requests'] == c['copy']['max_put_requests'] and u['get_requests'] == c['copy']['max_get_requests']
            and u['source_raw_bytes'] == c['copy']['raw_bytes'] and u['get_payload_bytes'] == 2*u['put_payload_bytes']
            and u['put_payload_bytes'] <= c['copy']['max_put_bytes'] and u['get_payload_bytes'] <= c['copy']['max_get_bytes']
            and v['segments'] == u['put_requests'] and type(v['elapsed_ms']) is int and 0 <= v['elapsed_ms'] <= 1000*c['copy']['max_seconds'], 'copy payload/request bounds differ')
    return v


def execute(c, evidence, apply=False):
    declaration_hash = r.identity(c)
    name = 'fugue-input-copy-'+declaration_hash[:20]
    secret_name = name+'-session'
    registry = json.loads(Path('deploy/releases/components.json').read_text())
    manifest = job_manifest(c, name, secret_name)
    report = {'declaration_sha256': declaration_hash, 'job': name, 'status': 'preflight', 'cleanup_confirmed': False,
              'retirement_authorized': False, 'route_changes_authorized': False}
    job_uid = secret_uid = priority_uid = None
    def save(): evidence.write_text(json.dumps(report, separators=(',', ':'))+'\n')
    with evidence.open('x') as output: output.write(json.dumps(report)+'\n')
    try:
        require(os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REF') == 'refs/heads/main'
                and os.environ.get('GITHUB_SHA') == command(['git', 'rev-parse', 'HEAD']).strip(), 'copy requires exact main CI checkout')
        report['node_before'] = node_gate(c)
        cluster, primary, available = r.primary_gate(c)
        report['backing_before'] = backing_gate(c, primary, starting=True); save()
        require(report['backing_before']['capacity_gate_passed'], 'copy backing filesystem reserve unavailable')
        executor, deployment, serving = r.serving_gate(c, registry)
        code = command(['git', 'show', c['sourceCommit']+':cmd/fugue-input-storage/main.go'])
        require('store.RunCompilerInputArchiveCopy' in code, 'copy worker executable lacks bounded mode')
        initial = metadata(c, credentials=True)
        require(initial['archive'] is None and initial['all_leases'] == 0 and initial['worker_connections'] == 0, 'copy source already attempted or worker active')
        data = copy_session(c, cluster, primary, deployment, executor, initial)
        initial.pop('platform')
        report.update(before=initial, available_before=available, serving=serving, errors_before=api_activity(c, serving),
                      bounded_logs_before=api_log_signals(c, serving), impact_samples=[])
        primary_restarts = {x['name']: x['restartCount'] for x in primary['status']['containerStatuses']}
        def watch():
            _, pod, free = r.primary_gate(c)
            backing = backing_gate(c, pod)
            report['last_backing_observation'] = backing; save()
            require(backing['capacity_gate_passed'], 'copy backing filesystem reserve exhausted')
            require({x['name']: x['restartCount'] for x in pod['status']['containerStatuses']} == primary_restarts, 'copy primary restarted')
            require(available-free <= c['maxAvailableDropBytes'], 'copy filesystem drop budget exceeded')
            _, _, now = r.serving_gate(c, registry)
            require(now == serving, 'copy serving identity or restart changed')
            current = metadata(c)
            check_impact(c, initial, current)
            errors = api_activity(c, serving)
            bounded_logs = api_log_signals(c, serving)
            for uid, item in errors.items():
                baseline = report['errors_before'][uid]
                require(item['budget_fallbacks'] == baseline['budget_fallbacks'] and 0 <= item['write_failures']-baseline['write_failures'] <= c['maxNewDeadlineFailures'], 'copy writer error impact budget exceeded')
            node = node_gate(c, job_uid or '')
            sample = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'available_bytes': free, 'metadata': current, 'errors': errors, 'bounded_logs': bounded_logs, 'node': node, 'backing': backing}
            report['impact_samples'].append(sample)
            require(len(report['impact_samples']) <= 50, 'copy watchdog sample bound exhausted')
            save()
            return current
        require(not json.loads(kube(c['namespace'], 'get', 'jobs', '-l', 'fugue.pro/purpose='+PURPOSE, '-o', 'json'))['items'], 'another copy execution resource exists')
        priority = {'apiVersion': 'scheduling.k8s.io/v1', 'kind': 'PriorityClass', 'metadata': {'name': name}, 'value': -1000000,
                    'globalDefault': False, 'preemptionPolicy': 'Never', 'description': 'One isolated bounded compiler-input copy'}
        r.create(priority, dry=True); r.create(manifest, dry=True)
        report['status'] = 'preview-passed'; save()
        if not apply: data.clear(); return report
        r.create({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'namespace': c['namespace'], 'name': name+'-receipt'}, 'immutable': True,
                  'data': {'declaration-sha256': declaration_hash, 'intent': 'single-copy-attempt-no-retry'}})
        priority_uid = r.create(priority)['metadata']['uid']
        r.create({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'namespace': c['namespace'], 'name': name+'-admission', **manifest['spec']['template']['metadata']}, 'spec': manifest['spec']['template']['spec']}, dry=True)
        job_uid = r.create(manifest)['metadata']['uid']; report.update(job_uid=job_uid, status='reserved'); save()
        secret = {'apiVersion': 'v1', 'kind': 'Secret', 'immutable': True, 'metadata': {'namespace': c['namespace'], 'name': secret_name,
                  'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': job_uid, 'controller': True}]}, 'type': 'Opaque',
                  'data': {k: base64.b64encode(v.encode()).decode() for k, v in data.items()}}
        secret_uid = r.create(secret)['metadata']['uid']; data.clear(); secret.clear()
        watch()
        r.api_write(['-n', c['namespace'], 'patch', 'job', name, '--type=json', '--patch-file=/dev/stdin', '-o', 'json'],
                    [{'op': 'test', 'path': '/metadata/uid', 'value': job_uid}, {'op': 'test', 'path': '/spec/suspend', 'value': True}, {'op': 'replace', 'path': '/spec/suspend', 'value': False}])
        report['status'] = 'running'; save()
        deadline = time.monotonic()+630
        completed = None
        while time.monotonic() < deadline:
            job = get(c['namespace'], 'job', name)
            require(job['metadata']['uid'] == job_uid, 'copy Job identity changed')
            pods = json.loads(kube(c['namespace'], 'get', 'pods', '-l', 'job-name='+name, '-o', 'json'))['items']
            require(len(pods) <= 1, 'copy has multiple Pods')
            if pods and pods[0]['status'].get('containerStatuses'):
                pod = pods[0]; status = pod['status']['containerStatuses'][0]
                if status.get('imageID'):
                    require(any(x.get('controller') and x.get('uid') == job_uid for x in pod['metadata'].get('ownerReferences', []))
                            and pod['spec'].get('nodeName') == c['node'] and pod['spec'].get('priority') == -1000000 and pod['spec'].get('preemptionPolicy') == 'Never'
                            and status['name'] == 'copy' and status['restartCount'] == 0 and status['imageID'].removeprefix('docker-pullable://') == c['image'], 'copy actual execution identity differs')
                    report['execution'] = {'pod': pod['metadata']['name'], 'pod_uid': pod['metadata']['uid'], 'image_id': status['imageID'], 'restart_count': 0, 'node_uid': c['nodeUID']}
                    if pod['status']['phase'] in ('Succeeded', 'Failed'): completed = pod; break
            watch()
            time.sleep(15)
        require(completed is not None, 'copy worker did not finish within deadline')
        body = kube(c['namespace'], 'logs', completed['metadata']['name'], '-c', 'copy', '--limit-bytes=16385')
        report['native_error_category'] = error_category(body)
        save()
        report['native_result'] = result_contract(body, c, success=False)
        save()
        require(completed['status']['phase'] == 'Succeeded', 'copy worker failed; no automatic retry')
        report['result'] = result_contract(body, c)
        final = watch()
        receipt = final['archive']
        require(receipt and receipt['state'] == 'verified' and receipt['verified'] is True and receipt['digest'] == c['copy']['digest']
                and receipt['stored_bytes'] == report['result']['usage']['put_payload_bytes'] and receipt['raw_bytes'] == c['copy']['raw_bytes']
                and final['all_leases'] == 0, 'copy durable verification receipt differs')
        report.update(after=final, status='copy-verified-source-preserved')
        return report
    except Exception as error:
        report.update(status='completion-not-confirmed', error_type=type(error).__name__)
        if isinstance(error, r.Refusal): report['refusal_reason'] = str(error)
        raise
    finally:
        if job_uid:
            try:
                r.delete_exact(c, 'Job', name, job_uid)
                for _ in range(18):
                    pods = json.loads(kube(c['namespace'], 'get', 'pods', '-l', 'job-name='+name, '-o', 'json'))['items']
                    if not any(any(x.get('uid') == job_uid for x in p['metadata'].get('ownerReferences', [])) for p in pods): break
                    time.sleep(5)
                else: raise r.Refusal('copy Pod cleanup remains pending')
                secrets = json.loads(kube(c['namespace'], 'get', 'secret', '--field-selector=metadata.name='+secret_name, '-o', 'json'))['items']
                if secrets:
                    require(len(secrets) == 1 and (secret_uid is None or secrets[0]['metadata']['uid'] == secret_uid)
                            and any(x.get('uid') == job_uid and x.get('controller') for x in secrets[0]['metadata'].get('ownerReferences', [])), 'copy session identity changed')
                    r.delete_exact(c, 'Secret', secret_name, secrets[0]['metadata']['uid'])
                for _ in range(12):
                    secrets = json.loads(kube(c['namespace'], 'get', 'secret', '--field-selector=metadata.name='+secret_name, '-o', 'json'))['items']
                    jobs = json.loads(kube(c['namespace'], 'get', 'jobs', '--field-selector=metadata.name='+name, '-o', 'json'))['items']
                    if not secrets and not jobs: report['cleanup_confirmed'] = True; break
                    time.sleep(5)
                require(report['cleanup_confirmed'], 'copy session cleanup remains pending')
            except Exception as error:
                report.update(cleanup_confirmed=False, cleanup_error_type=type(error).__name__)
        if priority_uid and (not job_uid or report['cleanup_confirmed']):
            try:
                r.delete_exact(c, 'PriorityClass', name, priority_uid)
                require(not json.loads(r.cluster_kube('get', 'priorityclasses', '--field-selector=metadata.name='+name, '-o', 'json'))['items'], 'copy priority cleanup remains pending')
            except Exception as error: report.update(cleanup_confirmed=False, cleanup_error_type=type(error).__name__)
        report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat(); save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('declaration'); parser.add_argument('--apply', action='store_true'); parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    try:
        c = validate(json.loads(Path(args.declaration).read_text()))
        report = execute(c, args.evidence, args.apply)
        if args.apply: require(report['cleanup_confirmed'] and report['status'] == 'copy-verified-source-preserved', 'copy execution or cleanup incomplete')
        print(json.dumps({k: report[k] for k in ('job', 'status', 'cleanup_confirmed', 'retirement_authorized', 'route_changes_authorized')}))
    except Exception as error:
        raise SystemExit('bounded copy completion not confirmed; inspect evidence; category='+type(error).__name__) from None


if __name__ == '__main__': main()
