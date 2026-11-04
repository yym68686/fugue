#!/usr/bin/env python3
"""Reviewed main-only archive operations. Never prints credentials or bodies."""
import argparse
import json
from pathlib import Path
import re
import time
import subprocess

from scripts.compiler_input_storage import command, readers, free_bytes


def validate_operation(value):
    required = {'version', 'mode', 'digest', 'limit', 'apply', 'expectedExecutorRevision', 'minimumFreeBytes'}
    if set(value) != required or value['version'] != 1 or type(value['version']) is not int:
        raise ValueError('invalid archive operation declaration')
    if value['mode'] not in ('archive', 'archive-batch', 'archive-route', 'archive-unroute'):
        raise ValueError('unsupported archive operation')
    if type(value['apply']) is not bool or type(value['limit']) is not int or not 1 <= value['limit'] <= 1000:
        raise ValueError('invalid archive batch bound')
    if value['mode'] != 'archive-batch' and (value['limit'] != 1 or not re.fullmatch(r'sha256:[0-9a-f]{64}', value['digest'])):
        raise ValueError('archive requires exact input identity')
    if value['mode'] == 'archive-batch' and value['digest'] != '':
        raise ValueError('batch cannot accept a hidden identity selector')
    if value['mode'] in ('archive-route', 'archive-unroute') and not value['apply']:
        raise ValueError('route changes require explicit apply')
    if not re.fullmatch('[0-9a-f]{40}', value['expectedExecutorRevision']):
        raise ValueError('archive executor revision is not pinned')
    if type(value['minimumFreeBytes']) is not int or value['minimumFreeBytes'] < 512 << 20:
        raise ValueError('archive must preserve at least 512MiB free space')
    return value


def managed_reader(revision):
    body = command(['git', 'show', revision+':internal/store/platform_input_archive_reader.go'])
    config = command(['git', 'show', revision+':internal/store/platform_input_archive_config.go'])
    if 'resolveCompilerInputArchiveConfig(ctx, db, false)' not in body or 'configured || s.db != nil' not in body or 'decryptDataBackendSecret(sealed)' not in config:
        raise ValueError('reader lacks managed archive configuration or activation barrier')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', required=True)
    args = parser.parse_args()
    value = validate_operation(json.loads(Path(args.operation).read_text()))
    registry = json.loads(Path('deploy/releases/components.json').read_text())
    current = readers(registry, archive_required=value['mode'] in ('archive-route', 'archive-unroute'))
    executor = next(x for x in current if x['component'] == 'api')
    if executor['revision'] != value['expectedExecutorRevision']:
        raise ValueError('archive executor differs from reviewed revision')
    managed_reader(executor['revision'])
    if value['mode'] in ('archive-route', 'archive-unroute'):
        for item in current:
            managed_reader(item['revision'])
            managed_reader(item['rollback_revision'])
    database = json.loads(Path('deploy/environments/production/storage-policy/maintenance.json').read_text())['database']
    before = free_bytes(database)
    if before < value['minimumFreeBytes']:
        raise ValueError('archive free-space reserve is insufficient')
    # Small segments and receipts replace re-encoding: old whole-input WAL
    # budgets do not apply. The reserve is checked before every copy below.
    count = value['limit'] if value['mode'] == 'archive-batch' else 1
    deadline = time.monotonic()+80*60
    for _ in range(count):
        if time.monotonic() >= deadline:
            print(json.dumps({'batch_paused_time_budget': True}), flush=True)
            break
        if free_bytes(database) < value['minimumFreeBytes']:
            raise ValueError('archive paused at free-space reserve; prior receipts retained')
        # One-item batches permit a primary/space check between inputs.
        argv = ['kubectl', '-n', executor['namespace'], 'exec', executor['pod'], '-c', executor['container'], '--', 'env',
                'FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED=true', 'FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED=true',
                '/usr/local/bin/fugue-input-storage', '--mode', value['mode']]
        if value['mode'] == 'archive-batch':
            argv += ['--archive-limit', '1']
        else:
            argv += ['--digest', value['digest']]
        if value['apply']:
            argv += ['--apply']
        result = command(argv, timeout=1850)
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
