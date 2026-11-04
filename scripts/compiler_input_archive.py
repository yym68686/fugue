#!/usr/bin/env python3
"""Reviewed main-only archive operations. Never prints credentials or bodies."""
import argparse
import base64
import binascii
import hashlib
import json
from pathlib import Path
import re
import time
import subprocess

from scripts.compiler_input_storage import command, readers, free_bytes, get, primary


def validate_operation(value):
    required = {'version', 'mode', 'digest', 'limit', 'apply', 'expectedExecutorRevision', 'minimumFreeBytes'}
    if 'maxDurationSeconds' in value:
        required.add('maxDurationSeconds')
        if type(value['maxDurationSeconds']) is not int or not 60 <= value['maxDurationSeconds'] <= 1800:
            raise ValueError('archive duration must be between 60 and 1800 seconds')
    if value.get('mode') == 'archive-hot-copy':
        required.add('maxStoredBytes')
    if set(value) != required or value['version'] != 1 or type(value['version']) is not int:
        raise ValueError('invalid archive operation declaration')
    if value['mode'] not in ('archive', 'archive-batch', 'archive-route', 'archive-route-batch', 'archive-unroute', 'archive-hot-copy'):
        raise ValueError('unsupported archive operation')
    if type(value['apply']) is not bool or type(value['limit']) is not int or not 1 <= value['limit'] <= 1000:
        raise ValueError('invalid archive batch bound')
    if value['mode'] not in ('archive-batch', 'archive-route-batch') and (value['limit'] != 1 or not re.fullmatch(r'sha256:[0-9a-f]{64}', value['digest'])):
        raise ValueError('archive requires exact input identity')
    if value['mode'] in ('archive-batch', 'archive-route-batch') and value['digest'] != '':
        raise ValueError('batch cannot accept a hidden identity selector')
    if value['mode'] in ('archive-route', 'archive-route-batch', 'archive-unroute') and not value['apply']:
        raise ValueError('route changes require explicit apply')
    if not re.fullmatch('[0-9a-f]{40}', value['expectedExecutorRevision']):
        raise ValueError('archive executor revision is not pinned')
    if type(value['minimumFreeBytes']) is not int or value['minimumFreeBytes'] < 512 << 20:
        raise ValueError('archive must preserve at least 512MiB free space')
    if value['mode'] == 'archive-hot-copy':
        budget = value['maxStoredBytes']
        if type(budget) is not int or not 0 < budget <= 2 << 30:
            raise ValueError('hot copy requires an explicit stored-byte bound')
        # Source rows remain during promotion. Reserve for the extra hot copy
        # and WAL, in addition to the ordinary migration stop line.
        if value['minimumFreeBytes'] < (512 << 20)+4*budget:
            raise ValueError('hot copy reserve must include conservative copy and WAL headroom')
    return value


def managed_reader(revision):
    body = command(['git', 'show', revision+':internal/store/platform_input_archive_reader.go'])
    config = command(['git', 'show', revision+':internal/store/platform_input_archive_config.go'])
    if 'resolveCompilerInputArchiveConfig(ctx, db, false)' not in body or 'configured || s.db != nil' not in body or 'decryptDataBackendSecret(sealed)' not in config:
        raise ValueError('reader lacks managed archive configuration or activation barrier')


def hot_reader(revision):
    body = command(['git', 'show', revision+':internal/store/platform_input_archive_reader.go'])
    function = body.split('func (s *Store) compilerInputArchiveRouted(', 1)[-1].split('\nfunc ', 1)[0]
    if not re.search(r'if m\.Location == "hot"\s*\{\s*return false, nil\s*\}', function):
        raise ValueError('reader does not prioritize the verified local hot copy')
    chunks = command(['git', 'show', revision+':internal/store/platform_input_chunks.go'])
    table = chunks.split('func compilerInputChunksTable(', 1)[-1].split('\n}', 1)[0]
    if 'case "hot":' not in table or 'fugue_platform_input_hot_chunks' not in table:
        raise ValueError('reader release does not support hot chunk locations')


def credential_key(namespace, container, secrets):
    """Match dataBackendSecretKey's precedence without printing key material.

    Inspect the actual running/LKG manifest and existing Secret, rather than
    assuming that a safe HEAD template proves an older rollback is safe.
    """
    names = ('FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'FUGUE_BUNDLE_SIGNING_KEY', 'FUGUE_WORKLOAD_IDENTITY_SIGNING_KEY')
    if container.get('envFrom'):
        raise ValueError('archive credential key has an unreviewed environment source')
    env = {}
    for item in container.get('env', []):
        if item['name'] in names:
            if item['name'] in env:
                raise ValueError('archive credential key environment is ambiguous')
            env[item['name']] = item
    for name in names:
        item = env.get(name, {})
        value = item.get('value', '')
        if 'valueFrom' in item:
            ref = item['valueFrom'].get('secretKeyRef')
            if not ref:
                raise ValueError('archive credential key source is unsupported')
            identity = namespace, ref['name']
            if identity not in secrets:
                secrets[identity] = get(namespace, 'secret', ref['name'])
            encoded = secrets[identity].get('data', {}).get(ref['key'])
            if encoded is None and not ref.get('optional', False):
                raise ValueError('archive credential key Secret is incomplete')
            try:
                value = base64.b64decode(encoded or '', validate=True).decode()
            except (binascii.Error, UnicodeError):
                raise ValueError('archive credential key Secret encoding is invalid') from None
        value = value.strip()
        if not value:
            continue
        raw = value.encode()
        try:
            # Go's StdEncoding accepts CR/LF within a base64 string.
            decoded = base64.b64decode(value.replace('\r', '').replace('\n', ''), validate=True)
            if len(decoded) >= 32:
                raw = decoded
        except binascii.Error:
            pass
        return hashlib.sha256(raw).digest()
    raise ValueError('archive reader lacks a configured credential encryption key')


def shared_reader_keys(registry, current):
    workloads = []
    for item in current:
        component = next(x for x in registry['components'] if x['id'] == item['component'])
        if 'internal/store' not in component.get('sourceRoots', []):
            continue  # schema-only jobs do not open archived compiler inputs
        w = component['workload']
        live = get(w['namespace'], 'deployment', w['name'])
        pointer = get(w['namespace'], 'configmap', 'fugue-release-monitor-'+component['id'])
        record = get(w['namespace'], 'configmap', pointer['data']['recordName'])
        envelope = json.loads(record['data']['record.json'])
        body = record['data']['lkg.json'].strip()
        if record.get('immutable') is not True or envelope['component'] != component['id'] or envelope['configSha'] != item['revision'] or 'sha256:'+hashlib.sha256(body.encode()).hexdigest() != envelope['lkgManifestDigest']:
            raise ValueError('archive reader release record changed during credential validation')
        lkg = primary(json.loads(body), w)
        for label, workload, revision in [('current', live, item['revision']), ('rollback', lkg, item['rollback_revision'])]:
            if workload['spec']['template']['metadata']['annotations']['fugue.pro/source-commit'] != revision:
                raise ValueError('archive reader changed during credential validation')
            containers = [x for x in workload['spec']['template']['spec']['containers'] if x['name'] == w['container']]
            if len(containers) != 1:
                raise ValueError('archive reader container is ambiguous')
            workloads.append((component['id'], label, w['namespace'], containers[0]))
    secrets = {}
    executor = next(x for x in workloads if x[0] == 'api' and x[1] == 'current')
    expected = credential_key(executor[2], executor[3], secrets)
    for component, label, namespace, container in workloads:
        if credential_key(namespace, container, secrets) != expected:
            raise ValueError('archive credential encryption key differs for '+component+' '+label)


def execute_item(argv, database, minimum_free_bytes, deadline, max_duration_seconds=None):
    # A SQLSTATE describes the failing transaction, not earlier transactions
    # in this command. A copy may already own a durable lease or segment even
    # when a later source-page/checkpoint lock fails. The executor alone can
    # retry a rolled-back acquisition before proceeding; never replay the
    # whole command on 55P03 or conceal it behind a subsequent lease conflict.
    # Resume needs inspection of receipts and natural lease expiry, then a new
    # reviewed operation. Unknown commit outcomes are not automatic retries.
    if time.monotonic() >= deadline:
        raise ValueError('archive paused at time budget; prior receipts retained')
    if free_bytes(database) < minimum_free_bytes:
        raise ValueError('archive paused at free-space reserve; prior receipts retained')
    if max_duration_seconds is not None:
        # The timer lives beside the executor, not on kubectl: disconnecting
        # exec alone could leave a remote copy running. TERM cancels the Go
        # signal context and preserves resumable receipts; never send KILL.
        marker = argv.index('/usr/local/bin/fugue-input-storage')
        argv = argv[:marker]+['timeout', '-s', 'TERM', str(max_duration_seconds)]+argv[marker:]
        return command(argv, timeout=max_duration_seconds+30)
    return command(argv, timeout=1850)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', required=True)
    args = parser.parse_args()
    value = validate_operation(json.loads(Path(args.operation).read_text()))
    registry = json.loads(Path('deploy/releases/components.json').read_text())
    hot_mode = value['mode'] == 'archive-hot-copy'
    route_mode = value['mode'] in ('archive-route', 'archive-route-batch', 'archive-unroute') or hot_mode
    current = readers(registry, archive_required=route_mode)
    executor = next(x for x in current if x['component'] == 'api')
    if executor['revision'] != value['expectedExecutorRevision']:
        raise ValueError('archive executor differs from reviewed revision')
    managed_reader(executor['revision'])
    if route_mode:
        for item in current:
            managed_reader(item['revision'])
            managed_reader(item['rollback_revision'])
        shared_reader_keys(registry, current)
    if hot_mode:
        for item in current:
            hot_reader(item['revision'])
            hot_reader(item['rollback_revision'])
    database = json.loads(Path('deploy/environments/production/storage-policy/maintenance.json').read_text())['database']
    before = free_bytes(database)
    if before < value['minimumFreeBytes']:
        raise ValueError('archive free-space reserve is insufficient')
    # Small segments and receipts replace re-encoding: old whole-input WAL
    # budgets do not apply. The reserve is checked before every copy below.
    count = value['limit'] if value['mode'] in ('archive-batch', 'archive-route-batch') else 1
    max_duration = value.get('maxDurationSeconds')
    deadline = time.monotonic()+(max_duration or 80*60)
    for _ in range(count):
        if time.monotonic() >= deadline:
            print(json.dumps({'batch_paused_time_budget': True}), flush=True)
            break
        # One-item batches permit a primary/space check between inputs.
        argv = ['kubectl', '-n', executor['namespace'], 'exec', executor['pod'], '-c', executor['container'], '--', 'env',
                'FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED=true', 'FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED=true',
                '/usr/local/bin/fugue-input-storage', '--mode', value['mode']]
        if value['mode'] in ('archive-batch', 'archive-route-batch'):
            argv += ['--archive-limit', '1']
        else:
            argv += ['--digest', value['digest']]
        if hot_mode:
            argv += ['--hot-copy-max-stored-bytes', str(value['maxStoredBytes'])]
        if value['apply']:
            argv += ['--apply']
        remaining = min(max_duration, int(deadline-time.monotonic())) if max_duration is not None else None
        if remaining is not None and remaining <= 0:
            raise ValueError('archive paused at time budget; prior receipts retained')
        result = execute_item(argv, database, value['minimumFreeBytes'], deadline, remaining)
        if len(result) > 16384:
            raise ValueError('archive receipt exceeds bound')
        if not result.strip():
            print(json.dumps({'batch_exhausted': True}), flush=True)
            break
        for line in result.splitlines():
            print(json.dumps({'operation': json.loads(line), 'free_bytes_initial': before}), flush=True)
        if not value['apply']:
            break
    readers(registry)
    print(json.dumps({'postcheck': 'healthy', 'free_bytes_after': free_bytes(database)}), flush=True)


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, StopIteration, subprocess.SubprocessError) as exc:
        raise SystemExit(str(exc) if type(exc) is ValueError else 'archive preflight failed') from None
