#!/usr/bin/env python3
"""Bounded storage maintenance and reader/LKG compatibility gate for CI.

No credentials, input bodies, manifests or subprocess error text are printed.
The database transaction owns the writer fence and reference barriers.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


FORMAT = 'runtime-snapshot-separated-v4'


def command(args, timeout=30):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        stage = 'cluster-read'
        if args[0] == 'git':
            stage = 'source-compatibility'
        elif '/usr/local/bin/fugue-input-storage' in args:
            stage = 'maintenance-executor'
        elif 'df' in args:
            stage = 'database-space-observation'
        # Extract only the executor's bounded category, never SQL, connection
        # strings, kubectl stderr, evidence, or arbitrary subprocess output.
        category = re.search(r'^compiler input maintenance completion not confirmed; inspect state before retrying; category=([A-Z0-9]{5}|[a-z0-9-]{1,32})$', result.stderr, re.MULTILINE)
        suffix = ' category='+category[1] if category else ''
        raise ValueError('storage command failed stage='+stage+suffix)
    return result.stdout


def kube(namespace, *args):
    return command(['kubectl', '-n', namespace, *args])


def get(namespace, kind, name):
    return json.loads(kube(namespace, 'get', kind, name, '-o', 'json'))


def compatible_revision(revision, archive_required=False):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('reader source revision is missing')
    body = command(['git', 'show', revision+':internal/store/platform_input_separated.go'])
    archive_reader = command(['git', 'show', revision+':internal/store/platform_input_archive_reader.go'])
    if FORMAT not in body or 'sql.LevelRepeatableRead' not in archive_reader:
        raise ValueError('reader source does not support separated inputs or Repeatable Read snapshots')
    if not archive_required:
        return
    if ('func (s *Store) BeginCompilerInputReadLease' not in archive_reader or
            'pg_try_advisory_lock_shared' not in archive_reader or
            's.compilerInputArchiveReadLeaseEnabled = configured' not in archive_reader or
            'compilerInputArchiveRouted(ctx' not in archive_reader or
            'verifyCompilerInputArchiveStartupCompatibility' not in archive_reader or
            'compiler input chunks relation is retired; archive backend is required' not in archive_reader):
        raise ValueError('current or rollback reader lacks archive routing or reader barrier support')


def primary(resources, workload):
    items = [x for x in resources['items'] if x.get('kind') == workload['kind'] and x['metadata']['name'] == workload['name'] and x['metadata'].get('namespace') == workload['namespace']]
    if len(items) != 1:
        raise ValueError('release primary workload is ambiguous')
    return items[0]


def owned_pods(namespace, deployment):
    # Legacy Deployment selectors can also match unrelated workloads. Bind the
    # population by controller UID through ReplicaSets, never by labels alone.
    uid = deployment['metadata'].get('uid')
    selector = deployment['spec']['selector']
    if not uid or set(selector) != {'matchLabels'} or not selector['matchLabels']:
        raise ValueError('reader ownership or selector is not exact')
    labels = ','.join(k+'='+v for k, v in sorted(selector['matchLabels'].items()))
    sets = json.loads(kube(namespace, 'get', 'replicasets', '-l', labels, '-o', 'json'))
    owners = {x['metadata']['uid'] for x in sets['items'] if any(
        r.get('controller') is True and r.get('kind') == 'Deployment' and r.get('uid') == uid
        for r in x['metadata'].get('ownerReferences', []))}
    pods = json.loads(kube(namespace, 'get', 'pods', '-l', labels, '-o', 'json'))
    return {'items': [x for x in pods['items'] if any(
        r.get('controller') is True and r.get('kind') == 'ReplicaSet' and r.get('uid') in owners
        for r in x['metadata'].get('ownerReferences', []))]}


def validate_reader(component, deployment, pods, record, archive_required=False):
    w = component['workload']
    template = deployment['spec']['template']
    revision = template['metadata']['annotations']['fugue.pro/source-commit']
    compatible_revision(revision, archive_required)
    count = deployment['spec'].get('replicas', 1)
    status = deployment['status']
    if count < 1 or status.get('observedGeneration', 0) < deployment['metadata']['generation'] or any(status.get(k, 0) != count for k in ('updatedReplicas', 'readyReplicas', 'replicas', 'availableReplicas')):
        raise ValueError('reader rollout is not stable')
    containers = [x for x in template['spec']['containers'] if x['name'] == w['container']]
    if len(containers) != 1 or not re.fullmatch(r'.+@sha256:[0-9a-f]{64}', containers[0]['image']):
        raise ValueError('reader image is not immutable')
    active = [x for x in pods['items'] if not x['metadata'].get('deletionTimestamp')]
    if len(active) != count or len(active) != len(pods['items']):
        raise ValueError('reader pod population is changing')
    for pod in active:
        statuses = [x for x in pod['status'].get('containerStatuses', []) if x['name'] == w['container']]
        images = [x['image'] for x in pod['spec']['containers'] if x['name'] == w['container']]
        if pod['metadata'].get('annotations', {}).get('fugue.pro/source-commit') != revision or len(statuses) != 1 or not statuses[0]['ready'] or images != [containers[0]['image']] or not any(c['type'] == 'Ready' and c['status'] == 'True' for c in pod['status'].get('conditions', [])):
            raise ValueError('reader pod identity or readiness differs')
    if record.get('immutable') is not True:
        raise ValueError('reader release record is mutable')
    envelope = json.loads(record['data']['record.json'])
    if envelope['component'] != component['id'] or envelope['configSha'] != revision:
        raise ValueError('reader monitor does not bind the running revision')
    rollback_revision = ''
    for key, digest_key in (('forward.json', 'forwardManifestDigest'), ('lkg.json', 'lkgManifestDigest')):
        body = record['data'][key].strip()
        if 'sha256:'+hashlib.sha256(body.encode()).hexdigest() != envelope[digest_key]:
            raise ValueError('reader release manifest digest differs')
        item = primary(json.loads(body), w)
        bound = item['spec']['template']['metadata']['annotations']['fugue.pro/source-commit']
        compatible_revision(bound, archive_required)
        if key == 'forward.json' and bound != revision:
            raise ValueError('reader forward source differs')
        if key == 'lkg.json':
            rollback_revision = bound
    return {'component': component['id'], 'revision': revision, 'rollback_revision': rollback_revision, 'pod': active[0]['metadata']['name'], 'namespace': w['namespace'], 'container': w['container'], 'image': containers[0]['image']}


def readers(registry, archive_required=False):
    result = []
    for component in registry['components']:
        if not {'internal/store', 'internal/schemamigrate'}.intersection(component.get('sourceRoots', [])):
            continue
        w = component['workload']
        if w['kind'] != 'Deployment':
            raise ValueError('unsupported store reader workload; explicit review required')
        namespace = w['namespace']
        deployment = get(namespace, 'deployment', w['name'])
        pods = owned_pods(namespace, deployment)
        pointer = get(namespace, 'configmap', 'fugue-release-monitor-'+component['id'])
        state = json.loads(pointer['data']['state.json'])
        if state.get('consecutiveFailures') or state.get('rollbackStatus'):
            raise ValueError('reader monitor has an unresolved failure or rollback')
        name = pointer['data']['recordName']
        if not re.fullmatch('fugue-release-record-'+re.escape(component['id'])+'-[0-9a-f]{16}', name):
            raise ValueError('reader monitor record name is invalid')
        result.append(validate_reader(component, deployment, pods, get(namespace, 'configmap', name), archive_required))
    if not result:
        raise ValueError('store reader inventory is empty')
    return result


def free_bytes(database):
    namespace = database['namespace']
    cluster = get(namespace, 'clusters.postgresql.cnpg.io', database['cluster'])
    if cluster['status'].get('phase') != 'Cluster in healthy state':
        raise ValueError('database cluster is not healthy')
    pod = cluster['status']['currentPrimary']
    current = get(namespace, 'pod', pod)
    if not any(c['type'] == 'Ready' and c['status'] == 'True' for c in current['status'].get('conditions', [])):
        raise ValueError('database primary is not ready')
    result = kube(namespace, 'exec', pod, '-c', database['container'], '--', 'df', '-Pk', database['dataPath'])
    lines = result.strip().splitlines()
    if len(lines) != 2 or len(lines[-1].split()) != 6:
        raise ValueError('database filesystem evidence is ambiguous')
    return int(lines[-1].split()[3])*1024


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', help='exact reviewed operation declaration from main')
    parser.add_argument('--mode', choices=['check-readers', 'check-archive-readers', 'check-history-compatibility', 'check-api-candidate', 'inspect', 'migrate', 'rollback', 'collect'], default='check-readers')
    parser.add_argument('--candidate-revision', default='')
    parser.add_argument('--digest', default='')
    parser.add_argument('--expected-format', default='')
    parser.add_argument('--max-input-bytes', type=int, default=128 << 20)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    # Reject before an operation can replace the requested diagnostic mode or
    # even read an operation file. A read-only invocation must never dispatch a
    # mutation because --operation normally supplies its own mode.
    if args.mode == 'check-history-compatibility' and (args.operation or args.apply):
        raise ValueError('history compatibility is read-only and does not accept an operation or apply')
    expected_revision = None
    if args.operation:
        value = json.loads(Path(args.operation).read_text())
        required = {'version', 'mode', 'digest', 'expectedFormat', 'maxInputBytes', 'apply', 'expectedExecutorRevision'}
        if set(value) != required or type(value['version']) is not int or value['version'] != 1 or value['mode'] not in ('inspect', 'migrate', 'rollback', 'collect') or type(value['apply']) is not bool or type(value['maxInputBytes']) is not int or not re.fullmatch('[0-9a-f]{40}', value['expectedExecutorRevision']):
            raise ValueError('invalid bounded storage operation declaration')
        args.mode, args.digest, args.expected_format = value['mode'], value['digest'], value['expectedFormat']
        args.max_input_bytes, args.apply = value['maxInputBytes'], value['apply']
        expected_revision = value['expectedExecutorRevision']
    read_modes = ('check-readers', 'check-archive-readers', 'check-history-compatibility', 'check-api-candidate')
    if not 0 < args.max_input_bytes <= 128 << 20 or args.mode not in (*read_modes, 'collect') and not re.fullmatch(r'sha256:[0-9a-f]{64}', args.digest):
        raise ValueError('invalid maintenance input bound or identity')
    if args.mode == 'check-api-candidate':
        if args.operation or args.apply:
            raise ValueError('API candidate check is read-only and does not accept an operation')
        compatible_revision(args.candidate_revision, archive_required=True)
        print(json.dumps({'archive_reader_candidate': args.candidate_revision, 'startup_guard': 'present'}))
        return
    if args.mode in ('migrate', 'rollback') and args.expected_format not in ('runtime-snapshot-json-v1', 'runtime-snapshot-json-chunk-codec-v2', 'runtime-snapshot-shared-v3', FORMAT):
        raise ValueError('exact expected format is required')
    if args.mode in read_modes or args.mode == 'inspect':
        if args.apply:
            raise ValueError('read-only operation cannot apply')
    registry = json.loads(Path('deploy/releases/components.json').read_text())
    if args.mode == 'check-history-compatibility':
        if args.operation:
            raise ValueError('history compatibility is read-only and does not accept an operation')
        from scripts.compiler_input_history_compatibility import check_history_compatibility
        print(json.dumps(check_history_compatibility(registry)))
        return
    current = readers(registry, archive_required=args.mode == 'check-archive-readers')
    if args.mode == 'check-readers':
        print(json.dumps({'compatible_readers_and_rollback': current}))
        return
    if args.mode == 'check-archive-readers':
        print(json.dumps({'archive_compatible_readers_and_rollback': current}))
        return
    config = json.loads(Path('deploy/environments/production/storage-policy/maintenance.json').read_text())
    if set(config) != {'version', 'database'} or config['version'] != 1 or set(config['database']) != {'namespace', 'cluster', 'container', 'dataPath'}:
        raise ValueError('invalid database maintenance declaration')
    database = config['database']
    before = free_bytes(database)
    # Both a rehearsal and apply consume WAL. Yield before starting if headroom
    # cannot cover the bounded operation plus an untouched 1GiB reserve.
    if args.mode != 'inspect' and before < (1 << 30) + 8*args.max_input_bytes:
        raise ValueError('database free-space reserve is insufficient')
    executor = next(x for x in current if x['component'] == 'api')
    if expected_revision is not None and executor['revision'] != expected_revision:
        raise ValueError('maintenance executor revision differs from reviewed operation')
    command_args = ['exec', executor['pod'], '-c', executor['container'], '--', '/usr/local/bin/fugue-input-storage', '--mode', args.mode, '--digest', args.digest, '--max-input-bytes', str(args.max_input_bytes)]
    if args.expected_format:
        command_args += ['--expected-format', args.expected_format]
    if args.mode != 'inspect':
        command_args += ['--apply' if args.apply else '--rehearse']
    result = command(['kubectl', '-n', executor['namespace'], *command_args], timeout=150)
    if len(result) > 16384:
        raise ValueError('maintenance receipt exceeds budget')
    # Emit the durable outcome before post-checks: a later failed check must not
    # disguise a transaction which has already committed.
    print(json.dumps({'operation': json.loads(result), 'free_bytes_before': before}), flush=True)
    readers(registry)
    print(json.dumps({'postcheck': 'healthy', 'free_bytes_after': free_bytes(database)}))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, StopIteration, subprocess.SubprocessError) as exc:
        # Static ValueError messages contain no subprocess data or SQL details.
        raise SystemExit(str(exc) if type(exc) is ValueError else 'storage preflight failed') from None
