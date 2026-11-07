#!/usr/bin/env python3
"""Shared receipted input restoration, never business activation or retirement.

The exact serving API signs and confirms over bounded stdin. Only a public
ticket, an ordinary DB session and the dedicated GET role enter the isolated
worker. An intent is consumed before credentials or a worker are created; an
uncertain attempt is not replayed. No operation declaration means no work.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

from scripts import compiler_input_copy as impact
from scripts import compiler_input_hot_index as index
from scripts import compiler_input_readonly as r
from scripts.compiler_input_copy_resume import strict_json
from scripts.compiler_input_storage import command, get, kube

MAINTENANCE_PURPOSES = ('compiler-input-hot-index', 'compiler-input-readonly', 'compiler-input-copy',
                        'compiler-input-task-prepare', 'compiler-input-publication-prepare', 'compiler-input-consumer-prepare')
require = r.require
BOUNDS = {'max_inputs': 32, 'max_raw_bytes': 128 << 20, 'max_stored_bytes': 128 << 20,
          'max_get_bytes': 448 << 20, 'max_get_requests': 32768, 'max_seconds': 540}


def validate_common(c, subject):
    require(type(c) is dict and set(c) == index.COMMON | {
        'storage', 'maxWalGrowthBytes', 'maxAvailableDropBytes', 'maxNewDeadlineFailures',
        subject, 'backendHash', 'expectedPlanSHA256'}, 'invalid input preparation declaration')
    index.validate({**{k: v for k, v in c.items() if k not in (subject, 'backendHash', 'expectedPlanSHA256')},
                    'page': {'afterID': '', 'limit': 1}})
    require(type(c['backendHash']) is str and re.fullmatch('sha256:[0-9a-f]{64}', c['backendHash'])
            and type(c['expectedPlanSHA256']) is str and re.fullmatch('[0-9a-f]{64}', c['expectedPlanSHA256']),
            'input preparation backend or physical plan is not exact')


def maintenance_gate(c, own_uid='', reader=kube):
    seen = []
    for purpose in MAINTENANCE_PURPOSES:
        jobs = strict_json(reader(c['namespace'], 'get', 'jobs', '-l', 'fugue.pro/purpose='+purpose, '-o', 'json'))['items']
        require(all(job['metadata']['uid'] == own_uid for job in jobs), 'another input maintenance resource exists')
        seen.extend(job['metadata']['uid'] for job in jobs)
    require(len(seen) <= 1, 'input maintenance population changed')

def native(c, executor, mode, payload, adapter):
    raw = json.dumps(payload, separators=(',', ':'))
    require(len(raw.encode()) <= (adapter.CONTRACT_LIMIT if mode == 'plan' else 513 << 10), 'input preparation coordination input exceeds bound')
    flag = '-'+adapter.NATIVE_PREFIX+'-contract=-' if mode == 'plan' else '-'+adapter.NATIVE_PREFIX+'-plan=-'
    argv = ['kubectl', '-n', c['namespace'], 'exec', '-i', executor['pod'], '-c', executor['container'], '--',
            '/usr/local/bin/fugue-input-storage', '-mode='+adapter.NATIVE_PREFIX+'-'+mode, flag]
    argv += ['-'+adapter.NATIVE_PREFIX+'-backend='+c['backendHash']] if mode == 'plan' else ['-apply']
    p = subprocess.run(argv, input=raw, capture_output=True, text=True, timeout=75)
    require(p.returncode == 0 and len(p.stdout.encode()) <= 513 << 10, 'trusted task coordination not confirmed')
    return strict_json(p.stdout)

def instant(value):
    require(type(value) is str and len(value) <= 40, 'input preparation ticket timestamp invalid')
    try:
        at = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(at.tzinfo is not None, 'input preparation ticket timestamp lacks timezone')
        return at
    except ValueError:
        raise r.Refusal('input preparation ticket timestamp invalid') from None

def exact_json(actual, expected):
    if type(actual) is not type(expected):
        return False
    if type(expected) is dict:
        return set(actual) == set(expected) and all(exact_json(actual[k], v) for k, v in expected.items())
    if type(expected) is list:
        return len(actual) == len(expected) and all(exact_json(a, b) for a, b in zip(actual, expected))
    return actual == expected

def plan_contract(value, c, adapter):
    budget = c[adapter.SUBJECT]
    require(type(value) is dict and set(value) == {'ticket', 'verification_key'}, 'input preparation ticket envelope differs')
    t = value['ticket']
    require(type(t) is dict and set(t) == {'version', 'contract', 'backend_hash', 'closure_sha256', 'epoch',
            'horizon', 'issued_at', 'expires_at', 'inputs', 'tuples', 'signature'}, 'input preparation ticket fields differ')
    require(type(t['version']) is int and t['version'] == 1
            and exact_json(t['contract'], {**adapter.contract(c), 'expected_plan_sha256': c['expectedPlanSHA256']})
            and t['backend_hash'] == c['backendHash'] and type(t['closure_sha256']) is str
            and re.fullmatch('[0-9a-f]{64}', t['closure_sha256'])
            and type(t['epoch']) is int and t['epoch'] >= 0 and type(t['horizon']) is int and 0 < t['horizon'] < 1 << 63,
            'input preparation ticket identity or budgets differ')
    issued, expires = instant(t['issued_at']), instant(t['expires_at'])
    now = datetime.datetime.now(datetime.timezone.utc)
    require(datetime.timedelta(0) <= now-issued <= datetime.timedelta(seconds=30)
            and expires-issued == datetime.timedelta(minutes=10), 'input preparation ticket is stale')
    try:
        require(len(base64.b64decode(value['verification_key'], validate=True)) == 32
                and len(base64.b64decode(t['signature'], validate=True)) == 64, 'input preparation public signature malformed')
    except (ValueError, TypeError):
        raise r.Refusal('input preparation public signature malformed') from None
    require(type(t['inputs']) is list and len(t['inputs']) <= budget['max_inputs']
            and type(t['tuples']) is list and 1 <= len(t['tuples']) <= 32768, 'input preparation ticket population exceeds bound')
    previous, total = '', 0
    for m in t['inputs']:
        require(type(m) is dict and set(m) == {'digest', 'format', 'location', 'raw_bytes', 'chunk_size', 'chunks', 'created_at'}
                and type(m['digest']) is str and re.fullmatch('sha256:[0-9a-f]{64}', m['digest'])
                and m['digest'] > previous and type(m['raw_bytes']) is int and 0 < m['raw_bytes'] <= budget['max_raw_bytes']
                and type(m['chunk_size']) is int and 0 < m['chunk_size'] <= 262144
                and type(m['chunks']) is int and m['chunks'] == (m['raw_bytes']+m['chunk_size']-1)//m['chunk_size']
                and m['format'] in ('runtime-snapshot-json-v1', 'runtime-snapshot-json-chunk-codec-v2',
                                    'runtime-snapshot-shared-v3', 'runtime-snapshot-separated-v4')
                and m['location'] in ('legacy', 'hot'), 'input preparation input witness differs')
        instant(m['created_at'])
        require(m['format'] != 'runtime-snapshot-json-chunk-codec-v2' or m['chunk_size'] <= 262139,
                'input preparation codec chunk exceeds encoded geometry')
        require(m['format'] not in ('runtime-snapshot-shared-v3', 'runtime-snapshot-separated-v4')
                or m['chunk_size'] == 262144, 'input preparation shared input geometry differs')
        previous, total = m['digest'], total+m['raw_bytes']
    require(total <= budget['max_raw_bytes'], 'input preparation raw budget exceeded')
    previous, owner_seen = '', False
    tables = {'fugue_platform_artifacts', 'fugue_platform_artifact_releases',
              'fugue_platform_expected_consumer_sets', 'fugue_node_update_tasks', 'fugue_platform_lkg_snapshots'}
    if adapter.SUBJECT == 'publication' and c[adapter.SUBJECT].get('producer'):
        tables.add('fugue_platform_release_lanes')
    for v in t['tuples']:
        require(type(v) is dict and set(v) == {'table', 'key', 'xmin', 'ctid', 'relation', 'filenode'}
                and v['table'] in tables,
                'input preparation physical witness table differs')
        require(type(v['key']) is str and 0 < len(v['key'].encode()) <= (4096 if v['table'] == 'fugue_platform_lkg_snapshots' else 2048 if v['table'] == 'fugue_platform_release_lanes' else 512)
                and type(v['xmin']) is str and re.fullmatch('[0-9]{1,10}', v['xmin'])
                and type(v['ctid']) is str and re.fullmatch(r'\([0-9]{1,10},[0-9]{1,5}\)', v['ctid'])
                and all(type(v[k]) is int and 0 < v[k] < 1 << 63 for k in ('relation', 'filenode'))
                and v['table']+'/'+v['key'] > previous, 'input preparation physical witness differs')
        require((v['xmin'] == '0') == (v['ctid'] == '(0,0)')
                and (v['table'] in ('fugue_platform_lkg_snapshots', 'fugue_platform_release_lanes') or v['xmin'] != '0'),
                'input physical absence witness differs')
        owner_seen |= adapter.physical_owner(c, v)
        previous = v['table']+'/'+v['key']
    require(owner_seen, 'input preparation physical owner witness missing')
    return value

def metadata(c, adapter, reader=False):
    platform = ", 'platform',(SELECT jsonb_build_object('account_id',body->>'account_id','compiler_input_archive',(body->'compiler_input_archive')-'writer') FROM fugue_object_storage_config WHERE id='platform')" if reader else ''
    sql = """BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB';
SELECT json_build_object('epoch',(SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton),
'fenced',(SELECT writes_fenced OR retirement_started_at IS NOT NULL FROM fugue_platform_input_archive_control WHERE singleton),
'leases',(SELECT count(*) FROM fugue_platform_input_archive_leases WHERE expires_at>transaction_timestamp()),
'wal_lsn',pg_current_wal_insert_lsn()::text,'wal_reset',(SELECT stats_reset::text FROM pg_stat_wal),
'active_clients',(SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND state='active' AND pid<>pg_backend_pid()),
'worker_connections',(SELECT count(*) FROM pg_stat_activity WHERE application_name IN ('compiler-input-task-prepare','compiler-input-publication-prepare','compiler-input-consumer-prepare')),
'index_present',to_regclass('fugue_platform_input_hot_dependency_index_v1') IS NOT NULL"""+platform+");\nCOMMIT;\n"
    p = subprocess.run(['kubectl', '-n', c['namespace'], 'exec', '-i', c['primary'], '-c', c['postgresContainer'], '--',
                        'psql', '-U', 'postgres', '-d', c['database'], '-qAt', '-v', 'ON_ERROR_STOP=1'],
                       input=sql, capture_output=True, text=True, timeout=20)
    require(p.returncode == 0 and len(p.stdout.encode()) <= 128 << 10, 'input preparation metadata unavailable')
    value = strict_json(p.stdout)
    require(type(value) is dict and set(value) == {'epoch', 'fenced', 'leases', 'wal_lsn', 'wal_reset',
            'active_clients', 'worker_connections', 'index_present'} | ({'platform'} if reader else set()),
            'input preparation metadata fields differ')
    require(all(type(value[k]) is int and value[k] >= 0 for k in ('epoch', 'leases', 'active_clients', 'worker_connections'))
            and value['fenced'] is False and value['leases'] == 0 and value['index_present'] is True
            and type(value['wal_reset']) is str and value['wal_reset'] and type(value['wal_lsn']) is str
            and re.fullmatch('[A-F0-9]{1,8}/[A-F0-9]{1,8}', value['wal_lsn']), 'input preparation metadata unsafe')
    return value

def session(c, cluster, primary, deployment, executor, adapter):
    private = adapter.metadata(c, reader=True)
    key = None
    try:
        container = next(x for x in deployment['spec']['template']['spec']['containers'] if x['name'] == executor['container'])
        key = r.credential_key(c['namespace'], container, {})
        data = r.reader_session(private['platform'], key, c['backendHash'])
    finally:
        private.clear()
        key = None
    try:
        data.update(index.session(c, cluster, primary))
    except Exception:
        data.clear()
        raise
    # Keep resource-watch accounting distinct from the index runner.
    data['database-url'] = data['database-url'].replace('application_name='+index.PURPOSE, 'application_name='+adapter.PURPOSE)
    return data

def job_manifest(c, name, secret_name, adapter):
    job = index.job_manifest({**c, 'page': {'afterID': '', 'limit': 1}}, name, secret_name)
    job['metadata']['labels']['fugue.pro/purpose'] = adapter.PURPOSE
    template = job['spec']['template']
    template['metadata']['labels']['fugue.pro/purpose'] = adapter.PURPOSE
    pod, container = template['spec'], template['spec']['containers'][0]
    container.update(name='restore', args=['-mode='+adapter.NATIVE_PREFIX+'-restore', '-apply', '-'+adapter.NATIVE_PREFIX+'-plan=/preparation/plan.json'])
    env = {'FUGUE_DATABASE_URL': 'database-url', adapter.VERIFICATION_ENV: 'verification-key'}
    for suffix, field in [('ENDPOINT', 'endpoint'), ('ACCOUNT_ID', 'account-id'), ('REGION', 'region'),
                          ('BUCKET', 'bucket'), ('PREFIX', 'prefix'), ('READER_ACCESS_KEY_ID', 'reader-access-key-id'),
                          ('READER_SECRET_ACCESS_KEY', 'reader-secret-access-key')]:
        env['FUGUE_COMPILER_INPUT_ARCHIVE_'+suffix] = field
    container['env'] = [{'name': k, 'valueFrom': {'secretKeyRef': {'name': secret_name, 'key': v}}} for k, v in env.items()]
    container['env'] += [{'name': k, 'value': v} for k, v in [('PGOPTIONS', index.OPTIONS), ('GOMAXPROCS', '1'),
                                ('GOMEMLIMIT', '128MiB'), ('FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED', 'true')]]
    pod['volumes'] = [{'name': 'preparation', 'secret': {'secretName': secret_name, 'defaultMode': 292,
                                                      'items': [{'key': 'plan.json', 'path': 'plan.json'}]}}]
    container['volumeMounts'] = [{'name': 'preparation', 'mountPath': '/preparation', 'readOnly': True}]
    return job

def result_contract(value, c, plan, adapter, confirmed=False):
    budget = c[adapter.SUBJECT]
    identity = adapter.receipt_identity(c)
    fields = {'action', 'plan_sha256', 'input_count', 'raw_bytes', 'stored_bytes', 'pins', 'usage',
              'prepared', 'retirement_authorized'} | set(identity) | {adapter.ACTIVATED_FIELD}
    require(type(value) is dict and set(value) == fields and value['action'] == adapter.NATIVE_PREFIX+('-confirm' if confirmed else '-restore')
            and all(type(value[k]) is type(v) and value[k] == v for k, v in identity.items()) and value['plan_sha256'] == c['expectedPlanSHA256']
            and value['prepared'] is confirmed and value[adapter.ACTIVATED_FIELD] is False and value['retirement_authorized'] is False,
            'input preparation native completion contract differs')
    inputs = plan['ticket']['inputs']
    require(all(type(value[k]) is int and value[k] >= 0 for k in ('input_count', 'raw_bytes', 'stored_bytes'))
            and value['input_count'] == len(inputs) and value['raw_bytes'] == sum(m['raw_bytes'] for m in inputs)
            and value['stored_bytes'] <= budget['max_stored_bytes']
            and type(value['pins']) is list and len(value['pins']) == len(inputs), 'input preparation completion bytes or population differ')
    usage = value['usage']
    require(type(usage) is dict and set(usage) == {'put_payload_bytes', 'get_payload_bytes', 'put_requests', 'get_requests', 'source_raw_bytes'}
            and all(type(v) is int and v >= 0 for v in usage.values())
            and usage['put_payload_bytes'] == usage['put_requests'] == usage['source_raw_bytes'] == 0
            and usage['get_payload_bytes'] <= budget['max_get_bytes'] and usage['get_requests'] <= budget['max_get_requests'],
            'input preparation object usage exceeds GET-only budget')
    if confirmed:
        require(not any(usage.values()) and value['stored_bytes'] == 0, 'input preparation confirmation unexpectedly copied data')
    for pin, m in zip(value['pins'], inputs, strict=True):
        require(type(pin) is dict and set(pin) == {'action', 'key', 'digest', 'token', 'state', 'applied', 'verified', 'raw_bytes', 'retirement_authorized'}
                and pin['key'] == budget['operation_key']+'/'+hashlib.sha256(m['digest'].encode()).hexdigest()
                and pin['digest'] == m['digest'] and type(pin['token']) is int and 0 < pin['token'] < 1 << 63
                and pin['action'] == ('' if confirmed else 'archive-local-pin') and pin['applied'] is (not confirmed)
                and pin['state'] == 'verified' and pin['verified'] is True and pin['retirement_authorized'] is False
                and type(pin['raw_bytes']) is int and pin['raw_bytes'] == m['raw_bytes'], 'input preparation pin receipt differs')
    return value

def execute(c, evidence, apply, adapter):
    adapter.validate(c)
    command, get, kube = adapter.command, adapter.get, adapter.kube
    native, metadata, session = adapter.native, adapter.metadata, adapter.session
    job_manifest, plan_contract, result_contract = adapter.job_manifest, adapter.plan_contract, adapter.result_contract
    contract, terminal_receipt = adapter.contract, adapter.terminal_receipt
    budget = c[adapter.SUBJECT]
    declaration_hash = r.identity(c)
    name = adapter.NAME_PREFIX+declaration_hash[:20]
    secret_name = name+'-session'
    registry = json.loads(Path('deploy/releases/components.json').read_text())
    manifest = job_manifest(c, name, secret_name)
    report = {'declaration_sha256': declaration_hash, 'job': name, 'status': 'preflight', 'cleanup_confirmed': False,
              'source_preserved': True, 'route_changes_authorized': False, 'retirement_authorized': False, adapter.ACTIVATED_FIELD: False,
              'preparation_purpose': adapter.PURPOSE, 'subject': adapter.receipt_identity(c)}
    job_uid = secret_uid = priority_uid = None
    data = {}
    def save():
        payload = json.dumps(report, separators=(',', ':'))+'\n'
        require(len(payload.encode()) <= 32 << 20, 'input preparation evidence exceeds budget')
        evidence.write_text(payload)
    with evidence.open('x') as output:
        output.write(json.dumps(report)+'\n')
    try:
        require(os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REF') == 'refs/heads/main'
                and os.environ.get('GITHUB_SHA') == command(['git', 'rev-parse', 'HEAD']).strip(), 'input preparation preparation requires exact main CI')
        report['node_before'] = impact.node_gate(c)
        cluster, primary, available = r.primary_gate(c)
        report['backing_before'] = impact.backing_gate(c, primary, starting=True)
        require(report['backing_before']['capacity_gate_passed'], 'input preparation preparation backing reserve unavailable')
        executor, deployment, serving = r.serving_gate(c, registry)
        baseline = metadata(c)
        require(baseline['worker_connections'] == 0, 'input preparation restoration worker already exists')
        report.update(before=baseline, available_before=available, serving=serving, errors_before=impact.api_activity(c, serving), impact_samples=[])
        plan = plan_contract(native(c, executor, 'plan', contract(c)), c)
        plan_json = json.dumps(plan, separators=(',', ':'))
        plan_hash = hashlib.sha256(plan_json.encode()).hexdigest()
        report.update(ticket_sha256=plan_hash, verification_key=plan['verification_key'])
        primary_restarts = {x['name']: x['restartCount'] for x in primary['status']['containerStatuses']}
        def watch():
            # One cumulative allowance covers reservation, execution and cleanup.
            # Exclude only this attempt's exact Job UID, never a name or label.
            adapter.maintenance_gate(c, job_uid or '')
            _, pod, free = r.primary_gate(c)
            backing = impact.backing_gate(c, pod)
            require(backing['capacity_gate_passed'] and available-free <= c['maxAvailableDropBytes']
                    and {x['name']: x['restartCount'] for x in pod['status']['containerStatuses']} == primary_restarts,
                    'input preparation preparation backing, restart or PG space changed')
            _, _, current = r.serving_gate(c, registry)
            require(current == serving, 'input preparation preparation serving identity changed')
            now = metadata(c)
            index.check_metadata(c, baseline, now)
            errors = impact.api_activity(c, serving)
            require(set(errors) == set(report['errors_before']), 'input preparation preparation writer population changed')
            for uid, value in errors.items():
                old = report['errors_before'][uid]
                impact.check_writer_impact(c, old, value)
                require(value['budget_fallbacks'] == old['budget_fallbacks']
                        and 0 <= value['write_failures']-old['write_failures'] <= c['maxNewDeadlineFailures'], 'input preparation preparation inner writer impact exceeded')
            report['impact_samples'].append({'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'available_bytes': free,
                'metadata': now, 'errors': errors, 'backing': backing, 'node': impact.node_gate(c, job_uid or ''),
                'bounded_logs': impact.api_log_signals(c, serving)})
            require(len(report['impact_samples']) <= 50, 'input preparation preparation watchdog exhausted')
            save()
        priority = {'apiVersion': 'scheduling.k8s.io/v1', 'kind': 'PriorityClass', 'metadata': {'name': name},
                    'value': -1000000, 'globalDefault': False, 'preemptionPolicy': 'Never', 'description': 'One bounded '+adapter.SUBJECT+' input restoration'}
        r.create(priority, dry=True)
        r.create(manifest, dry=True)
        watch()
        report['status'] = 'preview-passed'
        save()
        if not apply:
            return report
        intent_data = {'declaration-sha256': declaration_hash, 'ticket-sha256': plan_hash, 'verification-key': plan['verification_key'],
                       'intent': 'single-'+adapter.SUBJECT+'-restore-attempt-no-retry', 'preparation-purpose': adapter.PURPOSE,
                       'subject.json': json.dumps(adapter.receipt_identity(c), separators=(',', ':'))}
        intent_name = name+'-receipt'
        intent = r.create({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'namespace': c['namespace'], 'name': intent_name},
                           'immutable': True, 'data': intent_data})
        report['intent_uid'] = intent['metadata']['uid']
        save()
        observed = get(c['namespace'], 'configmap', intent_name)
        require(observed['metadata']['uid'] == report['intent_uid'] and observed.get('immutable') is True
                and observed.get('data') == intent_data, 'input preparation preparation intent readback differs')
        data = session(c, cluster, primary, deployment, executor)
        data.update({'plan.json': plan_json, 'verification-key': plan['verification_key']})
        priority_uid = r.create(priority)['metadata']['uid']
        r.create({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'namespace': c['namespace'], 'name': name+'-admission',
                  **manifest['spec']['template']['metadata']}, 'spec': manifest['spec']['template']['spec']}, dry=True)
        job_uid = r.create(manifest)['metadata']['uid']
        report.update(job_uid=job_uid, status='reserved')
        save()
        secret = {'apiVersion': 'v1', 'kind': 'Secret', 'immutable': True, 'type': 'Opaque',
                  'metadata': {'namespace': c['namespace'], 'name': secret_name, 'ownerReferences': [
                      {'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': job_uid, 'controller': True}]},
                  'data': {k: base64.b64encode(v.encode()).decode() for k, v in data.items()}}
        secret_uid = r.create(secret)['metadata']['uid']
        data.clear()
        secret.clear()
        watch()
        require((instant(plan['ticket']['expires_at'])-datetime.datetime.now(datetime.timezone.utc)).total_seconds()
                >= budget['max_seconds']+30, 'input preparation ticket setup exhausted execution window')
        r.api_write(['-n', c['namespace'], 'patch', 'job', name, '--type=json', '--patch-file=/dev/stdin', '-o', 'json'],
                    [{'op': 'test', 'path': '/metadata/uid', 'value': job_uid}, {'op': 'test', 'path': '/spec/suspend', 'value': True},
                     {'op': 'replace', 'path': '/spec/suspend', 'value': False}])
        report['status'] = 'running'
        save()
        deadline, completed = time.monotonic()+630, None
        while time.monotonic() < deadline:
            require(get(c['namespace'], 'job', name)['metadata']['uid'] == job_uid, 'input preparation preparation Job identity changed')
            actual_priority = r.cluster_get('priorityclass', name)
            require(actual_priority['metadata']['uid'] == priority_uid and actual_priority['value'] == -1000000
                    and actual_priority['preemptionPolicy'] == 'Never' and not actual_priority.get('globalDefault'), 'input preparation preparation priority changed')
            pods = json.loads(kube(c['namespace'], 'get', 'pods', '-l', 'job-name='+name, '-o', 'json'))['items']
            require(len(pods) <= 1, 'input preparation preparation worker population changed')
            if pods and pods[0].get('status', {}).get('containerStatuses'):
                pod = pods[0]
                statuses = pod['status']['containerStatuses']
                require(len(statuses) == 1, 'input preparation preparation container population changed')
                status = statuses[0]
                if status.get('imageID'):
                    require(any(x.get('controller') and x.get('uid') == job_uid for x in pod['metadata'].get('ownerReferences', []))
                            and pod['metadata'].get('annotations', {}).get('fugue.pro/source-commit') == c['sourceCommit']
                            and pod['spec'].get('nodeName') == c['node'] and pod['spec'].get('priority') == -1000000
                            and pod['spec'].get('preemptionPolicy') == 'Never' and status['name'] == 'restore' and status['restartCount'] == 0
                            and status['imageID'].removeprefix('docker-pullable://') == c['image'], 'input preparation preparation actual worker identity changed')
                    report['execution'] = {'pod': pod['metadata']['name'], 'pod_uid': pod['metadata']['uid'], 'image_id': status['imageID']}
                    if pod['status']['phase'] in ('Succeeded', 'Failed'):
                        completed = pod
                        report['execution']['terminal'] = impact.worker_terminal(pod, 'restore')
                        save()
                        break
            watch()
            time.sleep(15)
        require(completed is not None, 'input preparation preparation worker deadline exceeded')
        body = kube(c['namespace'], 'logs', completed['metadata']['name'], '-c', 'restore', '--limit-bytes=131073')
        require(len(body.encode()) <= 128 << 10, 'input preparation preparation result exceeds bound')
        report['native_error_category'] = impact.error_category(body)
        save()
        terminal = report['execution']['terminal']
        require(completed['status']['phase'] == 'Succeeded' and terminal['metadata_complete'] and terminal['exit_code'] == terminal['signal'] == 0,
                'input preparation preparation worker failed or termination unconfirmed; no retry')
        report['restore_result'] = result_contract(strict_json(body), c, plan)
        watch()
        confirmed = result_contract(native(c, executor, 'confirm', plan), c, plan, confirmed=True)
        require([(p['key'], p['token']) for p in confirmed['pins']] == [(p['key'], p['token']) for p in report['restore_result']['pins']],
                'trusted confirmation pin tokens differ from restoration')
        report['confirmation'] = confirmed
        watch()
        report['status'] = adapter.SUCCESS
        return report
    except Exception as error:
        report.update(status='completion-not-confirmed', error_type=type(error).__name__)
        if isinstance(error, r.Refusal):
            report['refusal_reason'] = str(error)
        raise
    finally:
        data.clear()
        try:
            index.cleanup(c, name, secret_name, job_uid, secret_uid, priority_uid)
            report['cleanup_confirmed'] = bool(job_uid)
        except Exception as error:
            report.update(status='completion-not-confirmed', cleanup_confirmed=False, cleanup_error_type=type(error).__name__)
        if job_uid and report['cleanup_confirmed']:
            try:
                watch()
            except Exception as error:
                report.update(status='completion-not-confirmed', cleanup_impact_error_type=type(error).__name__)
        report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:
            save()
        except Exception as error:
            report.update(status='completion-not-confirmed', evidence_error_type=type(error).__name__)
            raise
        finally:
            # Local evidence failure must not suppress an independent terminal.
            if report.get('intent_uid'):
                try:
                    report['terminal_receipt'] = terminal_receipt(c, report)
                except Exception as error:
                    report['terminal_receipt'] = {'confirmed': False, 'error_type': type(error).__name__}
                save()

def terminal_receipt(c, report, adapter):
    get = adapter.get
    # Failed-before-Job attempts are terminal too: the consumed intent remains.
    fields = ('declaration_sha256', 'intent_uid', 'ticket_sha256', 'verification_key', 'job', 'status',
              'cleanup_confirmed', 'source_preserved', 'route_changes_authorized', 'retirement_authorized', adapter.ACTIVATED_FIELD, 'finished_at')
    value = {k: report[k] for k in fields}
    value.update(preparation_purpose=adapter.PURPOSE, subject=adapter.receipt_identity(c))
    for k in ('job_uid', 'execution', 'restore_result', 'confirmation', 'refusal_reason',
              'cleanup_error_type', 'cleanup_impact_error_type', 'evidence_error_type'):
        if k in report:
            value[k] = report[k]
    payload = json.dumps(value, separators=(',', ':'))
    require(len(payload.encode()) <= 128 << 10, 'input preparation terminal receipt exceeds budget')
    item = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'namespace': c['namespace'], 'name': report['job']+'-terminal'},
            'immutable': True, 'data': {'declaration-sha256': report['declaration_sha256'], 'receipt.json': payload}}
    created = r.create(item)
    actual = get(c['namespace'], 'configmap', item['metadata']['name'])
    require(created['metadata']['uid'] == actual['metadata']['uid'] and actual.get('immutable') is True and actual.get('data') == item['data'],
            'input preparation terminal receipt readback differs')
    return {'uid': created['metadata']['uid'], 'confirmed': True}

def main(adapter):
    parser = argparse.ArgumentParser()
    parser.add_argument('declaration')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    try:
        report = adapter.execute(adapter.validate(strict_json(Path(args.declaration).read_bytes())), args.evidence, args.apply)
        if args.apply:
            require(report['status'] == adapter.SUCCESS and report['cleanup_confirmed']
                    and report.get('terminal_receipt', {}).get('confirmed') is True, 'input preparation preparation or terminal receipt unconfirmed')
        print(json.dumps({k: report[k] for k in ('job', 'status', 'cleanup_confirmed', adapter.ACTIVATED_FIELD, 'retirement_authorized')}))
    except Exception as error:
        raise SystemExit('input preparation preparation not confirmed; inspect evidence; category='+type(error).__name__) from None
