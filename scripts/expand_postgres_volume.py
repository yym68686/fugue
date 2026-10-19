#!/usr/bin/env python3
"""Expand one declared existing CNPG data volume through the CSI controller.

No database connection, replacement Pod, data deletion, image change or failover
is involved. Recovery can run while the database and application API are down.
"""
import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time


def quantity(value):
    match = re.fullmatch(r'([1-9][0-9]*)(Gi|Mi|Ki)?', str(value))
    if not match:
        raise ValueError('Unsupported storage quantity')
    return int(match[1]) * {'Gi': 2**30, 'Mi': 2**20, 'Ki': 2**10, None: 1}[match[2]]


def plan(config, cluster, pvc, pv, storage_class, volume, replicas, nodes):
    required = {'schema', 'namespace', 'cluster', 'clusterUID', 'primary', 'pvcUID',
                'pvUID', 'volumeUID', 'storageClass', 'previousSize', 'targetSize',
                'minimumFreeAfterReserveGiB', 'longhornNamespace'}
    if set(config) != required or config['schema'] != 'fugue.postgres-volume-expansion/v1':
        raise ValueError('Unsupported expansion declaration')
    before, target = quantity(config['previousSize']), quantity(config['targetSize'])
    if target <= before or target > 2 * before or type(config['minimumFreeAfterReserveGiB']) is not int or config['minimumFreeAfterReserveGiB'] < 16:
        raise ValueError('Expansion must be bounded and preserve disk headroom')
    meta, spec, status = cluster['metadata'], cluster['spec'], cluster.get('status', {})
    if meta['uid'] != config['clusterUID'] or meta['name'] != config['cluster'] or meta['namespace'] != config['namespace'] or meta.get('deletionTimestamp'):
        raise ValueError('Cluster identity changed')
    if status.get('currentPrimary') != config['primary'] or status.get('targetPrimary') != config['primary'] or spec.get('instances') != 1:
        raise ValueError('This bounded recovery requires the declared single primary')
    if spec['storage'].get('resizeInUseVolumes') is not True or spec['storage'].get('storageClass') != config['storageClass'] or quantity(spec['storage']['size']) not in (before, target):
        raise ValueError('Storage policy changed')
    pm, ps = pvc['metadata'], pvc['spec']
    if pm['uid'] != config['pvcUID'] or pm['name'] != config['primary'] or pm['namespace'] != config['namespace'] or pm.get('deletionTimestamp') or pvc['status']['phase'] != 'Bound':
        raise ValueError('PVC identity or binding changed')
    if not any(o.get('uid') == meta['uid'] and o.get('kind') == 'Cluster' and o.get('controller') for o in pm.get('ownerReferences', [])):
        raise ValueError('PVC is not owned by the declared database')
    if ps.get('volumeMode', 'Filesystem') != 'Filesystem' or ps.get('storageClassName') != config['storageClass'] or ps.get('accessModes') != ['ReadWriteOnce']:
        raise ValueError('PVC storage properties changed')
    current = quantity(ps['resources']['requests']['storage'])
    if current not in (before, target):
        raise ValueError('PVC size changed outside this expansion')
    if pv['metadata']['uid'] != config['pvUID'] or pv['metadata']['name'] != ps['volumeName'] or pv['spec']['claimRef']['uid'] != pm['uid']:
        raise ValueError('PV identity changed')
    if storage_class['metadata']['name'] != config['storageClass'] or storage_class.get('allowVolumeExpansion') is not True or storage_class.get('provisioner') != 'driver.longhorn.io' or pv['spec']['csi']['driver'] != 'driver.longhorn.io':
        raise ValueError('An expandable Longhorn CSI volume is required')
    if volume['metadata']['uid'] != config['volumeUID'] or pv['spec']['csi']['volumeHandle'] != volume['metadata']['name'] or volume['status'].get('state') != 'attached' or volume['status'].get('robustness') != 'healthy':
        raise ValueError('Volume identity or health changed')
    active = [r for r in replicas if r['spec'].get('volumeName') == volume['metadata']['name'] and r['spec'].get('active')]
    if len(active) != volume['spec']['numberOfReplicas'] or not active:
        raise ValueError('Replica inventory is incomplete')
    headroom = []
    for replica in active:
        rs = replica['spec']
        if rs.get('failedAt') or replica['status'].get('currentState') != 'running':
            raise ValueError('Replica is not healthy')
        matches = [(node, key, disk) for node in nodes if node['metadata']['name'] == rs['nodeID']
                   for key, disk in node['status'].get('diskStatus', {}).items() if disk.get('diskUUID') == rs['diskID']]
        if len(matches) != 1:
            raise ValueError('Replica disk inventory is ambiguous')
        node, key, disk = matches[0]
        if not any(c['type'] == 'Ready' and c['status'] == 'True' for c in disk.get('conditions', [])):
            raise ValueError('Replica disk is not ready')
        # Reserve the full declared growth even on a retry: sparse volume usage
        # and an in-progress filesystem resize must not overstate headroom.
        free = disk['storageAvailable'] - node['spec']['disks'][key]['storageReserved'] - (target - before)
        if free < config['minimumFreeAfterReserveGiB'] * 2**30:
            raise ValueError('Insufficient replica disk capacity after reserve')
        headroom.append({'node': rs['nodeID'], 'disk': rs['diskID'], 'freeAfterReserveAndGrowthBytes': free})
    patch = []
    if current != target:
        patch = [{'op': 'test', 'path': '/metadata/uid', 'value': pm['uid']},
                 {'op': 'test', 'path': '/metadata/resourceVersion', 'value': pm['resourceVersion']},
                 {'op': 'test', 'path': '/spec', 'value': ps},
                 {'op': 'replace', 'path': '/spec/resources/requests/storage', 'value': config['targetSize']}]
    return patch, {'clusterUID': meta['uid'], 'pvcUID': pm['uid'], 'previousSize': config['previousSize'],
                   'targetSize': config['targetSize'], 'replicaHeadroom': headroom}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    command = shlex.split(os.environ.get('KUBECTL', 'kubectl'))
    def kube(*argv):
        return subprocess.check_output(command + list(argv), text=True, timeout=30)
    def get(kind, name, namespace=None):
        return json.loads(kube(*(['-n', namespace] if namespace else []), 'get', kind, *([name] if name else []), '-o', 'json'))
    ns, name = config['namespace'], config['cluster']
    cluster = get('cluster', name, ns)
    pvc = get('pvc', config['primary'], ns)
    pv = get('pv', pvc['spec']['volumeName'])
    lh = config['longhornNamespace']
    patch, evidence = plan(config, cluster, pvc, pv, get('storageclass', config['storageClass']),
                           get('volumes.longhorn.io', pv['spec']['csi']['volumeHandle'], lh),
                           get('replicas.longhorn.io', '', lh)['items'], get('nodes.longhorn.io', '', lh)['items'])
    evidence.update(mode='apply' if args.apply else 'preview', patch=patch)
    def save():
        Path(args.evidence).write_text(json.dumps(evidence, indent=2) + '\n')
    save()
    if patch:
        kube('-n', ns, 'patch', 'pvc', config['primary'], '--type=json', '-p', json.dumps(patch), *([] if args.apply else ['--dry-run=server']))
    if args.apply:
        deadline = time.monotonic() + 300
        while True:
            pvc = get('pvc', config['primary'], ns)
            if pvc['metadata']['uid'] != config['pvcUID']:
                raise ValueError('PVC was replaced while expanding')
            if quantity(pvc['status']['capacity']['storage']) >= quantity(config['targetSize']) and not pvc['status'].get('conditions'):
                break
            if time.monotonic() >= deadline:
                raise RuntimeError('CSI expansion has not converged; retain the increased request')
            time.sleep(5)
        # Preserve the larger desired size after CSI has expanded the one named
        # active claim. Old detached claims are neither deleted nor patched.
        cluster = get('cluster', name, ns)
        if cluster['metadata']['uid'] != config['clusterUID'] or cluster['status'].get('currentPrimary') != config['primary'] or cluster['status'].get('targetPrimary') != config['primary']:
            raise ValueError('Database identity changed during expansion')
        if cluster['spec']['storage']['size'] != config['targetSize']:
            if cluster['spec']['storage']['size'] != config['previousSize']:
                raise ValueError('Database storage intent changed during expansion')
            intent = [{'op': 'test', 'path': '/metadata/uid', 'value': config['clusterUID']},
                      {'op': 'test', 'path': '/metadata/resourceVersion', 'value': cluster['metadata']['resourceVersion']},
                      {'op': 'test', 'path': '/spec', 'value': cluster['spec']},
                      {'op': 'replace', 'path': '/spec/storage/size', 'value': config['targetSize']}]
            kube('-n', ns, 'patch', 'cluster', name, '--type=json', '-p', json.dumps(intent))
        evidence['csiCapacityVerified'] = pvc['status']['capacity']['storage']
        save()
    print(json.dumps(evidence, sort_keys=True))


if __name__ == '__main__':
    main()
