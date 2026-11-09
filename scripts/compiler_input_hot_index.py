#!/usr/bin/env python3
"""One receipted dependency-index preparation page, never a retention proof.

Only exact main CI may create execution resources. The isolated worker receives
one ordinary DB session, not platform signing, archive or Kubernetes credentials.
Preview checks admission and resources without running the WAL-producing native
rehearsal. A consumed intent is never retried, even if completion is uncertain.
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
import urllib.parse

from scripts import compiler_input_deadline as deadline
from scripts import compiler_input_copy as impact
from scripts import compiler_input_readonly as r
from scripts.compiler_input_storage import command, get, kube

PURPOSE = 'compiler-input-hot-index'
OPTIONS = '-c statement_timeout=5s -c lock_timeout=250ms -c work_mem=1MB -c idle_in_transaction_session_timeout=10s'
require = r.require
COMMON = {'version', 'executionAuthorized', 'namespace', 'node', 'nodeUID', 'sourceCommit', 'image',
          'cluster', 'clusterUID', 'primary', 'primaryUID', 'postgresContainer', 'dataPath', 'database',
          'apiService', 'readyURL', 'minimumFreeBytes'}


def validate(c):
    fields = COMMON | {'storage', 'page', 'maxWalGrowthBytes', 'maxAvailableDropBytes', 'maxNewDeadlineFailures'}
    require(type(c) is dict and set(c) == fields and type(c['version']) is int and c['version'] == 1,
            'invalid dependency index declaration')
    r.validate({**{k: c[k] for k in COMMON}, 'version': 3, 'readCount': 0})
    require(type(c['minimumFreeBytes']) is int and c['minimumFreeBytes'] >= 3 << 30,
            'index reserve is below 3GiB')
    for key, high in [('maxWalGrowthBytes', 256 << 20), ('maxAvailableDropBytes', 128 << 20), ('maxNewDeadlineFailures', 4)]:
        require(type(c[key]) is int and 0 <= c[key] <= high and (key == 'maxNewDeadlineFailures' or c[key] > 0),
                'dependency index impact budget is invalid')
    page = c['page']
    require(type(page) is dict and set(page) == {'afterID', 'limit'}
            and type(page['afterID']) is str and re.fullmatch('[A-Za-z0-9_.:-]{0,512}', page['afterID'])
            and type(page['limit']) is int and 1 <= page['limit'] <= 32, 'dependency index page is not bounded')
    s = c['storage']
    names = {'longhorn_namespace', 'volume', 'replica', 'instance_manager', 'node'}
    uids = {'volume_uid', 'pvc_uid', 'pv_uid', 'replica_uid', 'instance_manager_uid', 'disk_id'}
    require(type(s) is dict and set(s) == names | uids | {'disk_path'}, 'dependency index storage binding is invalid')
    require(all(type(s[k]) is str and re.fullmatch('[a-z0-9][a-z0-9.-]{0,127}', s[k]) for k in names)
            and all(type(s[k]) is str and re.fullmatch('[a-f0-9]{8}-([a-f0-9]{4}-){3}[a-f0-9]{12}', s[k]) for k in uids)
            and type(s['disk_path']) is str and re.fullmatch('/[a-zA-Z0-9/_-]{1,200}', s['disk_path']),
            'dependency index storage identity is not exact')
    return c


def session(c, cluster, primary):
    # Reuse the managed application-role credential path without fetching or
    # decrypting any R2/platform key. This role is not permission-reduced; the
    # pinned native mode and immutable page declaration bound its operations.
    data = r.session({**c, 'version': 3, 'readCount': 0}, cluster, primary, {}, {}, {})
    parsed = urllib.parse.urlsplit(data.pop('database-readonly-url'))
    query = urllib.parse.urlencode({'sslmode': 'require', 'connect_timeout': '5',
                                   'application_name': PURPOSE, 'options': OPTIONS})
    return {'database-url': urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, ''))}


def job_manifest(c, name, secret_name):
    job = r.job_manifest({**c, 'readCount': 0}, name, secret_name)
    job['metadata']['labels']['fugue.pro/purpose'] = PURPOSE
    template = job['spec']['template']
    template['metadata']['labels']['fugue.pro/purpose'] = PURPOSE
    spec = template['spec']
    container = spec['containers'][0]
    container.update(name='index', command=['nice', '-n', '19', '/usr/local/bin/fugue-input-storage'],
                     args=['-mode=archive-hot-index', '-apply', '-hot-index-after='+c['page']['afterID'],
                           '-hot-index-limit='+str(c['page']['limit'])])
    container['env'] = [{'name': 'FUGUE_DATABASE_URL', 'valueFrom': {'secretKeyRef': {'name': secret_name, 'key': 'database-url'}}}]
    container['env'] += [{'name': k, 'value': v} for k, v in [('PGOPTIONS', OPTIONS), ('GOMAXPROCS', '1'), ('GOMEMLIMIT', '128MiB')]]
    return job


def metadata(c):
    # No source body or object credentials. WAL distance includes concurrent
    # traffic; stats_reset and LSN guard against silently accepting a reset.
    sql = """BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB';
SELECT json_build_object('epoch',(SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton),
'fenced',(SELECT writes_fenced OR retirement_started_at IS NOT NULL FROM fugue_platform_input_archive_control WHERE singleton),
'leases',(SELECT count(*) FROM fugue_platform_input_archive_leases WHERE expires_at>transaction_timestamp()),
'wal_lsn',pg_current_wal_insert_lsn()::text,'wal_reset',(SELECT stats_reset::text FROM pg_stat_wal),
'active_clients',(SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND state='active' AND pid<>pg_backend_pid()),
'worker_connections',(SELECT count(*) FROM pg_stat_activity WHERE application_name='compiler-input-hot-index'),
'index_present',to_regclass('fugue_platform_input_hot_dependency_index_v1') IS NOT NULL);
COMMIT;
"""
    p = subprocess.run(['kubectl', '-n', c['namespace'], 'exec', '-i', c['primary'], '-c', c['postgresContainer'], '--',
                        'psql', '-U', 'postgres', '-d', c['database'], '-qAt', '-v', 'ON_ERROR_STOP=1'],
                       input=sql, capture_output=True, text=True, timeout=20)
    require(p.returncode == 0 and len(p.stdout) <= 16384, 'dependency index metadata unavailable')
    try:
        v = json.loads(p.stdout)
    except (TypeError, ValueError):
        raise r.Refusal('dependency index metadata malformed') from None
    require(set(v) == {'epoch', 'fenced', 'leases', 'wal_lsn', 'wal_reset', 'active_clients', 'worker_connections', 'index_present'}
            and all(type(v[k]) is int and v[k] >= 0 for k in ('epoch', 'leases', 'active_clients', 'worker_connections'))
            and type(v['wal_reset']) is str and v['wal_reset'] and type(v['wal_lsn']) is str
            and re.fullmatch('[A-F0-9]{1,8}/[A-F0-9]{1,8}', v['wal_lsn'])
            and v['fenced'] is False and v['leases'] == 0 and v['index_present'] is True,
            'dependency index maintenance identity or state unavailable')
    return v


def check_metadata(c, baseline, now):
    def lsn(value):
        high, low = value.split('/')
        return (int(high, 16) << 32) + int(low, 16)
    require(now['epoch'] == baseline['epoch'] and now['wal_reset'] == baseline['wal_reset'],
            'dependency index maintenance epoch or WAL reset changed')
    require(0 <= lsn(now['wal_lsn'])-lsn(baseline['wal_lsn']) <= c['maxWalGrowthBytes'],
            'dependency index WAL budget exceeded or reset')
    require(now['leases'] == 0 and now['fenced'] is False and now['index_present'] is True
            and now['active_clients'] <= baseline['active_clients']+4 and now['worker_connections'] <= 1,
            'dependency index database pressure or maintenance changed')


def result_contract(body, c):
    from scripts.compiler_input_copy_resume import strict_json
    require(type(body) is str and len(body.encode()) <= 16384, 'dependency index result exceeds budget')
    value = strict_json(body)
    fields = {'action', 'observed_at', 'selected', 'next_after_id', 'source_bytes', 'index_bytes', 'wal_bytes', 'applied', 'retirement_authorized'}
    require(type(value) is dict and set(value) == fields and value['action'] == 'archive-hot-index'
            and value['applied'] is True and value['retirement_authorized'] is False, 'dependency index result contract differs')
    require(all(type(value[k]) is int and value[k] >= 0 for k in ('selected', 'source_bytes', 'index_bytes', 'wal_bytes'))
            and value['selected'] <= c['page']['limit'] and value['source_bytes']+value['index_bytes'] <= 32 << 20
            and value['wal_bytes'] <= c['maxWalGrowthBytes'], 'dependency index result budget differs')
    cursor = value['next_after_id']
    require(type(cursor) is str and re.fullmatch('[A-Za-z0-9_.:-]{0,512}', cursor)
            and ((value['selected'] == 0 and cursor == '' and value['source_bytes'] == value['index_bytes'] == 0)
                 or (value['selected'] > 0 and cursor > c['page']['afterID'])), 'dependency index result cursor differs')
    require(type(value['observed_at']) is str and len(value['observed_at']) <= 40, 'dependency index timestamp differs')
    try:
        observed = datetime.datetime.fromisoformat(value['observed_at'].replace('Z', '+00:00'))
        require(observed.tzinfo is not None, 'dependency index timestamp is not UTC bound')
    except (ValueError, TypeError):
        raise r.Refusal('dependency index timestamp malformed') from None
    return value


def terminal_receipt(c, report):
    require(report['cleanup_confirmed'] and report.get('job_uid'), 'dependency index cleanup unconfirmed')
    fields = ('declaration_sha256', 'job', 'job_uid', 'status', 'cleanup_confirmed',
              'source_preserved', 'route_changes_authorized', 'retirement_authorized', 'finished_at')
    value = {k: report[k] for k in fields}
    for key in ('execution', 'result', 'native_error_category', 'refusal_reason'):
        if key in report:
            value[key] = report[key]
    payload = json.dumps(value, separators=(',', ':'))
    require(len(payload.encode()) <= 32768, 'dependency index terminal receipt exceeds budget')
    item = {'apiVersion': 'v1', 'kind': 'ConfigMap',
            'metadata': {'namespace': c['namespace'], 'name': report['job']+'-terminal'},
            'immutable': True, 'data': {'declaration-sha256': report['declaration_sha256'], 'receipt.json': payload}}
    created = r.create(item)
    observed = get(c['namespace'], 'configmap', item['metadata']['name'])
    require(created['metadata']['uid'] == observed['metadata']['uid'] and observed.get('immutable') is True
            and observed.get('data') == item['data'], 'dependency index terminal receipt readback differs')
    return {'uid': created['metadata']['uid'], 'confirmed': True}


def cleanup(c, name, secret_name, job_uid, secret_uid, priority_uid):
    if job_uid:
        r.delete_exact(c, 'Job', name, job_uid)
        for _ in range(18):
            pods = json.loads(kube(c['namespace'], 'get', 'pods', '-l', 'job-name='+name, '-o', 'json'))['items']
            # A replacement with this name is not ours to delete or ignore.
            require(all(any(x.get('uid') == job_uid for x in p['metadata'].get('ownerReferences', [])) for p in pods),
                    'dependency index cleanup found unrelated Pod')
            if not pods:
                break
            deadline.pause(5)
        else:
            raise r.Refusal('dependency index Pod cleanup pending')
        secrets = json.loads(kube(c['namespace'], 'get', 'secret', '--field-selector=metadata.name='+secret_name, '-o', 'json'))['items']
        if secrets:
            require(len(secrets) == 1 and secrets[0]['metadata']['uid'] == secret_uid
                    and any(x.get('controller') and x.get('uid') == job_uid for x in secrets[0]['metadata'].get('ownerReferences', [])),
                    'dependency index session ownership changed')
            r.delete_exact(c, 'Secret', secret_name, secret_uid)
        for _ in range(12):
            remaining = [json.loads(kube(c['namespace'], 'get', kind, '--field-selector=metadata.name='+target, '-o', 'json'))['items']
                         for kind, target in [('secrets', secret_name), ('jobs', name)]]
            if not any(remaining):
                break
            deadline.pause(5)
        else:
            raise r.Refusal('dependency index resources cleanup pending')
    if priority_uid:
        r.delete_exact(c, 'PriorityClass', name, priority_uid)
        require(not json.loads(r.cluster_kube('get', 'priorityclasses', '--field-selector=metadata.name='+name, '-o', 'json'))['items'],
                'dependency index priority cleanup pending')


def execute(c, evidence, apply=False, cumulative_watch=None, evidence_limit=None):
    require(evidence_limit is None or type(evidence_limit) is int and evidence_limit >= 16384,
            'dependency index evidence budget invalid')
    validate(c)
    declaration_hash = r.identity(c)
    name = 'fugue-input-index-'+declaration_hash[:20]
    secret_name = name+'-session'
    registry = json.loads(Path('deploy/releases/components.json').read_text())
    manifest = job_manifest(c, name, secret_name)
    report = {'declaration_sha256': declaration_hash, 'job': name, 'status': 'preflight', 'cleanup_confirmed': False,
              'source_preserved': True, 'route_changes_authorized': False, 'retirement_authorized': False}
    job_uid = secret_uid = priority_uid = None
    data = {}
    def save():
        body = json.dumps(report, separators=(',', ':'))+'\n'
        require(evidence_limit is None or len(body.encode()) <= evidence_limit,
                'dependency index evidence budget exceeded')
        evidence.write_text(body)
    with evidence.open('x') as output:
        output.write(json.dumps(report)+'\n')
    try:
        require(os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REF') == 'refs/heads/main'
                and os.environ.get('GITHUB_SHA') == command(['git', 'rev-parse', 'HEAD']).strip(),
                'dependency index requires exact main CI checkout')
        report['node_before'] = impact.node_gate(c)
        cluster, primary, available = r.primary_gate(c)
        report['backing_before'] = impact.backing_gate(c, primary, starting=True)
        require(report['backing_before']['capacity_gate_passed'], 'dependency index backing reserve unavailable')
        _, _, serving = r.serving_gate(c, registry)
        source = command(['git', 'show', c['sourceCommit']+':cmd/fugue-input-storage/main.go'])
        require('store.RunCompilerInputHotIndex' in source, 'dependency index serving executable unavailable')
        baseline = metadata(c)
        require(baseline['worker_connections'] == 0, 'dependency index worker already exists')
        report.update(before=baseline, available_before=available, serving=serving,
                      errors_before=impact.api_activity(c, serving), bounded_logs_before=impact.api_log_signals(c, serving), impact_samples=[])
        primary_restarts = {x['name']: x['restartCount'] for x in primary['status']['containerStatuses']}
        def watch():
            _, pod, free = r.primary_gate(c)
            backing = impact.backing_gate(c, pod)
            require(backing['capacity_gate_passed'], 'dependency index backing reserve exhausted')
            require({x['name']: x['restartCount'] for x in pod['status']['containerStatuses']} == primary_restarts
                    and available-free <= c['maxAvailableDropBytes'], 'dependency index primary restart or space budget changed')
            _, _, current_serving = r.serving_gate(c, registry)
            require(current_serving == serving, 'dependency index serving identity changed')
            now = metadata(c)
            check_metadata(c, baseline, now)
            errors = impact.api_activity(c, serving)
            require(set(errors) == set(report['errors_before']), 'dependency index writer population changed')
            for uid, value in errors.items():
                old = report['errors_before'][uid]
                impact.check_writer_impact(c, old, value)
                require(value['budget_fallbacks'] == old['budget_fallbacks']
                        and 0 <= value['write_failures']-old['write_failures'] <= c['maxNewDeadlineFailures'],
                        'dependency index inner writer impact exceeded')
            sample = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'available_bytes': free,
                      'metadata': now, 'errors': errors, 'backing': backing, 'node': impact.node_gate(c, job_uid or ''),
                      'bounded_logs': impact.api_log_signals(c, serving)}
            report['impact_samples'].append(sample)
            require(len(report['impact_samples']) <= 50, 'dependency index watchdog bound exhausted')
            save()
            if cumulative_watch is not None:
                cumulative_watch(job_uid or '')
        # Do not race a copy, readonly inventory or another preparation worker.
        for purpose in (PURPOSE, impact.PURPOSE, r.PURPOSE):
            require(not json.loads(kube(c['namespace'], 'get', 'jobs', '-l', 'fugue.pro/purpose='+purpose, '-o', 'json'))['items'],
                    'another compiler input maintenance resource exists')
        priority = {'apiVersion': 'scheduling.k8s.io/v1', 'kind': 'PriorityClass', 'metadata': {'name': name},
                    'value': -1000000, 'globalDefault': False, 'preemptionPolicy': 'Never', 'description': 'One bounded dependency index page'}
        r.create(priority, dry=True)
        r.create(manifest, dry=True)
        report['status'] = 'preview-passed'
        save()
        if not apply:
            return report
        if cumulative_watch is not None:
            cumulative_watch()
        intent = r.create({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'namespace': c['namespace'], 'name': name+'-receipt'},
                           'immutable': True, 'data': {'declaration-sha256': declaration_hash, 'intent': 'single-index-page-attempt-no-retry'}})
        report['intent_uid'] = intent['metadata']['uid']
        save()
        data = session(c, cluster, primary)
        priority_uid = r.create(priority)['metadata']['uid']
        r.create({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'namespace': c['namespace'], 'name': name+'-admission',
                  **manifest['spec']['template']['metadata']}, 'spec': manifest['spec']['template']['spec']}, dry=True)
        job_uid = r.create(manifest)['metadata']['uid']
        report.update(job_uid=job_uid, status='reserved')
        save()
        secret = {'apiVersion': 'v1', 'kind': 'Secret', 'immutable': True,
                  'metadata': {'namespace': c['namespace'], 'name': secret_name, 'ownerReferences': [
                      {'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': job_uid, 'controller': True}]},
                  'type': 'Opaque', 'data': {k: base64.b64encode(v.encode()).decode() for k, v in data.items()}}
        secret_uid = r.create(secret)['metadata']['uid']
        data.clear()
        secret.clear()
        watch()
        r.api_write(['-n', c['namespace'], 'patch', 'job', name, '--type=json', '--patch-file=/dev/stdin', '-o', 'json'],
                    [{'op': 'test', 'path': '/metadata/uid', 'value': job_uid}, {'op': 'test', 'path': '/spec/suspend', 'value': True},
                     {'op': 'replace', 'path': '/spec/suspend', 'value': False}])
        report['status'] = 'running'
        save()
        deadline = time.monotonic()+630
        completed = None
        while time.monotonic() < deadline:
            job = get(c['namespace'], 'job', name)
            require(job['metadata']['uid'] == job_uid, 'dependency index Job identity changed')
            observed_priority = r.cluster_get('priorityclass', name)
            require(observed_priority['metadata']['uid'] == priority_uid and observed_priority['value'] == -1000000
                    and observed_priority['preemptionPolicy'] == 'Never' and not observed_priority.get('globalDefault'),
                    'dependency index priority changed')
            pods = json.loads(kube(c['namespace'], 'get', 'pods', '-l', 'job-name='+name, '-o', 'json'))['items']
            require(len(pods) <= 1, 'dependency index worker population changed')
            if pods and pods[0].get('status', {}).get('containerStatuses'):
                pod = pods[0]
                statuses = pod['status']['containerStatuses']
                require(len(statuses) == 1, 'dependency index worker container population changed')
                status = statuses[0]
                if status.get('imageID'):
                    require(any(x.get('controller') and x.get('uid') == job_uid for x in pod['metadata'].get('ownerReferences', []))
                            and pod['metadata'].get('annotations', {}).get('fugue.pro/source-commit') == c['sourceCommit']
                            and pod['spec'].get('nodeName') == c['node'] and pod['spec'].get('priority') == -1000000
                            and pod['spec'].get('preemptionPolicy') == 'Never' and status['name'] == 'index' and status['restartCount'] == 0
                            and status['imageID'].removeprefix('docker-pullable://') == c['image'], 'dependency index actual worker identity changed')
                    report['execution'] = {'pod': pod['metadata']['name'], 'pod_uid': pod['metadata']['uid'], 'image_id': status['imageID']}
                    if pod['status']['phase'] in ('Succeeded', 'Failed'):
                        completed = pod
                        report['execution']['terminal'] = impact.worker_terminal(pod, 'index')
                        save()
                        break
            watch()
            time.sleep(15)
        require(completed is not None, 'dependency index worker deadline exceeded')
        body = kube(c['namespace'], 'logs', completed['metadata']['name'], '-c', 'index', '--limit-bytes=16385')
        report['native_error_category'] = impact.error_category(body)
        save()
        require(completed['status']['phase'] == 'Succeeded', 'dependency index worker failed; no automatic retry')
        terminal = report['execution']['terminal']
        require(terminal['metadata_complete'] and terminal['exit_code'] == 0 and terminal['signal'] == 0,
                'dependency index successful termination unconfirmed')
        report['result'] = result_contract(body, c)
        watch()
        report['status'] = 'index-page-committed'
        return report
    except Exception as error:
        report.update(status='completion-not-confirmed', error_type=type(error).__name__)
        if isinstance(error, r.Refusal):
            report['refusal_reason'] = str(error)
        raise
    finally:
        data.clear()
        try:
            cleanup(c, name, secret_name, job_uid, secret_uid, priority_uid)
            report['cleanup_confirmed'] = bool(job_uid)
        except Exception as error:
            report.update(cleanup_confirmed=False, cleanup_error_type=type(error).__name__)
        report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        evidence_error = None
        try:
            save()
        except Exception as error:
            evidence_error = error
            report.update(status='completion-not-confirmed', error_type=type(error).__name__)
        if job_uid and report['cleanup_confirmed']:
            try:
                report['terminal_receipt'] = terminal_receipt(c, report)
            except Exception as error:
                report['terminal_receipt'] = {'confirmed': False, 'error_type': type(error).__name__}
            try:
                save()
            except Exception as error:
                evidence_error = error
        if evidence_error is not None:
            raise evidence_error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('declaration')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    try:
        from scripts.compiler_input_copy_resume import strict_json
        c = validate(strict_json(Path(args.declaration).read_bytes()))
        report = execute(c, args.evidence, args.apply)
        if args.apply:
            require(report['status'] == 'index-page-committed' and report['cleanup_confirmed']
                    and report.get('terminal_receipt', {}).get('confirmed') is True, 'dependency index execution or receipt unconfirmed')
        print(json.dumps({k: report[k] for k in ('job', 'status', 'cleanup_confirmed', 'retirement_authorized')}))
    except Exception as error:
        raise SystemExit('dependency index completion not confirmed; inspect evidence; category='+type(error).__name__) from None


if __name__ == '__main__':
    main()
