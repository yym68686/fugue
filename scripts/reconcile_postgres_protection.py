#!/usr/bin/env python3
"""Reconcile database protection without coupling it to an application release."""
import argparse
import json
import os
import shlex
import subprocess
import re


def validate(config, live):
    spec = config['spec']
    if set(spec) - {'resources', 'backup', 'postgresql', 'primaryUpdateStrategy'}:
        raise ValueError('Protection configuration cannot change identity, image or volumes')
    if str(live['status']['systemID']) != config['expectedSystemID']:
        raise ValueError('Database system ID mismatch')
    if live['status'].get('readyInstances', 0) < 2:
        raise ValueError('At least two healthy instances are required')
    if live['status'].get('currentPrimary') != live['status'].get('targetPrimary'):
        raise ValueError('Primary transition is in progress')
    if spec['postgresql']['synchronous'] != {'method': 'any', 'number': 1, 'dataDurability': 'required'}:
        raise ValueError('Protection requires one durable synchronous standby')
    if spec['primaryUpdateStrategy'] != 'supervised':
        raise ValueError('Primary updates must remain supervised')
    if not spec['backup']['barmanObjectStore']['destinationPath'].startswith('s3://'):
        raise ValueError('A durable WAL archive is required')
    if spec['backup']['target'] != 'prefer-standby':
        raise ValueError('Backups must prefer a standby')
    old_archive = live['spec'].get('backup', {}).get('barmanObjectStore', {}).get('destinationPath')
    if old_archive and old_archive != spec['backup']['barmanObjectStore']['destinationPath']:
        raise ValueError('Cannot replace the existing WAL archive in a protection rollout')
    storage = config.get('newInstanceStorage')
    if storage:
        if set(storage) != {'size', 'storageClass', 'resizeInUseVolumes'} or storage['resizeInUseVolumes'] is not False:
            raise ValueError('New-volume defaults must not resize existing volumes')
        if not re.fullmatch(r'[1-9][0-9]*Gi', storage['size']) or not storage['storageClass']:
            raise ValueError('Invalid new-volume size or storage class')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    with open(args.config) as stream:
        config = json.load(stream)
    command = shlex.split(os.environ.get('KUBECTL', 'kubectl'))
    def kube(*args, stdin=None):
        return subprocess.check_output(command + list(args), input=stdin, text=True)
    ns, name = config['namespace'], config['cluster']
    live = json.loads(kube('-n', ns, 'get', 'cluster', name, '-o', 'json'))
    validate(config, live)
    desired = dict(config['spec'])
    if config.get('newInstanceStorage'):
        storage = config['newInstanceStorage']
        pvcs = json.loads(kube('-n', ns, 'get', 'pvc', '-l', 'cnpg.io/cluster=' + name, '-o', 'json'))
        if any(pvc['spec']['storageClassName'] != storage['storageClass'] for pvc in pvcs['items']):
            raise ValueError('New storage class differs from existing database volumes')
        desired['storage'] = storage
    patch = json.dumps({'metadata': {'resourceVersion': live['metadata']['resourceVersion']}, 'spec': desired})
    flags = [] if args.apply else ['--dry-run=server']
    print(kube('-n', ns, 'patch', 'cluster', name, '--type=merge', '-p', patch, *flags))
    schedule = config['scheduledBackup']
    backup = {'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'ScheduledBackup',
              'metadata': {'name': schedule['name'], 'namespace': ns},
              'spec': {'schedule': schedule['schedule'], 'immediate': False,
                       'backupOwnerReference': 'self', 'method': 'barmanObjectStore',
                       'cluster': {'name': name}, 'target': 'prefer-standby'}}
    print(kube('apply', '--server-side', '--field-manager=fugue-postgres-protection', '-f', '-', *flags,
               stdin=json.dumps(backup)))


if __name__ == '__main__':
    main()
