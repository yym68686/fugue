"""Read-only, exact-identity review plan for one obsolete local PG snapshot.

No --apply mode exists. This does not retire a backup, acquire a Lease, change
retention, call DELETE, or mutate PostgreSQL. A later executor must coordinate
with backup/release writers and revalidate this plan before native deletion.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess

from scripts import longhorn_snapshot_map

NAME = re.compile(r'[a-z0-9][a-z0-9.-]{0,252}')
TERMINAL = {'deleted', 'expired'}


def require(value, message):
    if not value:
        raise ValueError(message)


def validate(config):
    required = {'schema', 'namespace', 'cluster', 'clusterUID', 'primary', 'primaryUID',
                'pvcUID', 'volume', 'volumeUID', 'longhornNamespace', 'snapshot',
                'snapshotUID', 'artifact', 'run', 'coordinationLease',
                'minimumExtraFreeBytes', 'workingReserveBytes', 'maxFoldWriteBytes'}
    require(set(config) == required and config['schema'] == 'fugue.local-snapshot-review/v1', 'invalid snapshot review declaration')
    for key in required - {'schema', 'minimumExtraFreeBytes', 'workingReserveBytes', 'maxFoldWriteBytes', 'artifact', 'run'}:
        require(isinstance(config[key], str) and NAME.fullmatch(config[key]), 'invalid snapshot review identity')
    for key in ('artifact', 'run'):
        require(isinstance(config[key], str) and re.fullmatch(r'[a-zA-Z0-9_-]{1,200}', config[key]), 'invalid backup ledger identity')
    for key, minimum in [('minimumExtraFreeBytes', 16 << 30), ('workingReserveBytes', 192 << 20), ('maxFoldWriteBytes', 1)]:
        require(type(config[key]) is int and minimum <= config[key] <= 1 << 40, 'invalid snapshot working-space budget')
    require(config['snapshot'] != 'volume-head', 'the live head cannot be reviewed for removal')


def fold_budget(observation, minimal_percent, extra, reserve, max_write):
    require(type(minimal_percent) is int and 25 <= minimal_percent < 100, 'physical minimum must remain at least 25 percent')
    parent, child = observation['files']
    overlap = observation['overlap_bytes']
    require(0 <= overlap <= min(parent['mapped_bytes'], child['mapped_bytes']), 'invalid overlap observation')
    growth = child['mapped_bytes'] - overlap
    require(observation['new_mapped_bytes'] == growth and observation['fold_write_bytes'] == child['mapped_bytes'], 'fold direction disagrees with pinned engine')
    require(observation['fold_write_bytes'] <= max_write, 'fold exceeds the reviewed I/O bound')
    require(observation['content_bytes_read'] == 0 and observation['sync_requested'] is False, 'unexpected data read or sync')
    free = observation['physical_free_bytes']
    minimum = observation['physical_total_bytes'] * minimal_percent // 100
    remaining = free - minimum - extra - reserve - growth
    return dict(new_allocation_estimate_bytes=growth, fold_write_bytes=child['mapped_bytes'],
                overlapping_mapped_bytes=overlap, physical_free_bytes=free,
                minimum_physical_free_bytes=minimum, extra_free_bytes=extra,
                working_reserve_bytes=reserve, remaining_budget_bytes=remaining,
                capacity_gate_passed=remaining >= 0,
                reclaimable_bytes_exact=None)


class ReadOnlyCluster:
    def __init__(self):
        self.kubectl = shlex.split(os.environ.get('KUBECTL', 'kubectl'))

    def command(self, args, stdin=None, limit=8 << 20):
        command = self.kubectl + args
        host = os.environ.get('FUGUE_REVIEW_SSH_HOST')
        if host:
            require(re.fullmatch(r'[a-zA-Z0-9._-]+', host), 'invalid read-only SSH host')
            command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', host, shlex.join(command)]
        result = subprocess.run(command, input=stdin, text=True,
                                capture_output=True, timeout=30)
        require(result.returncode == 0 and len(result.stdout) <= limit, 'bounded read-only cluster observation failed')
        return result.stdout

    def get(self, resource, name, namespace=None):
        prefix = ['-n', namespace] if namespace else []
        return json.loads(self.command(prefix + ['get', resource, name, '-o', 'json']))

    def items(self, path):
        # Explicit pagination is unnecessary for these bounded inventories:
        # fail closed rather than accept a partial cross-reference list.
        response = json.loads(self.command(['get', '--raw', path + '?limit=500']))
        require(not response.get('metadata', {}).get('continue') and len(response['items']) <= 500, 'cross-reference inventory is truncated')
        return response['items']

    def ledger(self, config):
        literal = lambda value: "'" + value.replace("'", "''") + "'"
        volume, snapshot = literal(config['volume']), literal(config['snapshot'])
        sql = f"""BEGIN READ ONLY;
SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms';
SET LOCAL work_mem='1MB'; SET LOCAL temp_file_limit=0;
SET LOCAL max_parallel_workers_per_gather=0;
SELECT json_build_object('id',a.id,'run',a.run_id,'status',a.status,'protected',a.protected,
 'kind',a.kind,'target_type',a.target_type,'physical_deleted',a.physical_deleted_at IS NOT NULL,
 'run_terminal',r.status IN ('succeeded','failed','canceled'),
 'run_lease_active',COALESCE(r.locked_until>now(),false),
 'restore_references',(SELECT count(*) FROM fugue_backup_restore_plans p WHERE p.artifact_id=a.id AND p.status NOT IN ('succeeded','failed','canceled'))
  +(SELECT count(*) FROM fugue_backup_restore_runs u WHERE u.artifact_id=a.id AND u.status NOT IN ('succeeded','failed','canceled')))
FROM fugue_backup_artifacts a LEFT JOIN fugue_backup_runs r ON r.id=a.run_id
WHERE a.kind='longhorn-snapshot' AND a.manifest_json#>>'{{metadata,volume_name}}'={volume}
 AND a.manifest_json#>>'{{metadata,snapshot_name}}'={snapshot} LIMIT 101;
COMMIT;
"""
        raw = self.command(['-n', config['namespace'], 'exec', '-i', config['primary'], '-c', 'postgres', '--',
                            'psql', '-U', 'postgres', '-d', 'fugue', '-qAt', '-v', 'ON_ERROR_STOP=1'], stdin=sql, limit=1 << 20)
        rows = [json.loads(line) for line in raw.splitlines() if line]
        require(0 < len(rows) <= 100, 'snapshot ledger references missing or exceed budget')
        return rows


def same(obj, name, uid):
    require(obj['metadata']['name'] == name and obj['metadata']['uid'] == uid and not obj['metadata'].get('deletionTimestamp'), 'reviewed object identity changed')


def terminal_ledger(ledger, artifact, run):
    require(any(x['id'] == artifact and x['run'] == run for x in ledger), 'exact artifact receipt is missing')
    require(all(x['status'] in TERMINAL and x['protected'] is False and x['physical_deleted'] is True and x['run_terminal'] is True and x['run_lease_active'] is False and x['restore_references'] == 0 and x['target_type'] == 'control-plane-db' for x in ledger), 'live, protected, unverified or restoring artifact references this snapshot')


def collateral_purges(chain, selected):
    collateral = []
    for name, item in chain.items():
        if name in ('volume-head', selected) or (not item.get('removed') and item.get('usercreated') is True):
            continue
        require(set(item.get('children', {})) == {'volume-head'}, 'native purge would coalesce additional snapshots')
        collateral.append(name)
    return collateral


def observe(kube, config):
    validate(config)
    ns, lh, volume_name = config['namespace'], config['longhornNamespace'], config['volume']
    cluster = kube.get('clusters.postgresql.cnpg.io', config['cluster'], ns)
    same(cluster, config['cluster'], config['clusterUID'])
    cs = cluster['status']
    require(cluster['spec']['instances'] == cs.get('readyInstances') == 1 and cs.get('phase') == 'Cluster in healthy state', 'database is not healthy')
    require(cs.get('currentPrimary') == cs.get('targetPrimary') == config['primary'], 'primary changed')
    pod = kube.get('pod', config['primary'], ns)
    same(pod, config['primary'], config['primaryUID'])
    require(any(c['type'] == 'Ready' and c['status'] == 'True' for c in pod['status'].get('conditions', [])), 'primary is not Ready')
    pvc = kube.get('pvc', config['primary'], ns)
    same(pvc, config['primary'], config['pvcUID'])
    pv = kube.get('pv', pvc['spec']['volumeName'])
    require(pv['spec']['claimRef']['uid'] == config['pvcUID'] and pv['spec']['csi']['driver'] == 'driver.longhorn.io' and pv['spec']['csi']['volumeHandle'] == volume_name, 'database binding changed')
    volume = kube.get('volumes.longhorn.io', volume_name, lh)
    same(volume, volume_name, config['volumeUID'])
    require(volume['spec'].get('dataEngine') == 'v1' and volume['spec'].get('numberOfReplicas') == 1 and volume['status'].get('state') == 'attached' and volume['status'].get('robustness') == 'healthy', 'unsupported volume health or topology')
    snapshot = kube.get('snapshots.longhorn.io', config['snapshot'], lh)
    same(snapshot, config['snapshot'], config['snapshotUID'])
    require(snapshot['spec']['volume'] == volume_name and snapshot['metadata'].get('labels', {}).get('fugue.pro/backup-run') == config['run'], 'snapshot ownership changed')
    require(snapshot['status'].get('readyToUse') is True and not snapshot['status'].get('markRemoved'), 'snapshot is not an intact candidate')
    base = '/apis/longhorn.io/v1beta2/namespaces/' + lh
    engines = [x for x in kube.items(base + '/engines') if x['spec'].get('volumeName') == volume_name]
    replicas = [x for x in kube.items(base + '/replicas') if x['spec'].get('volumeName') == volume_name]
    require(len(engines) == len(replicas) == 1, 'engine or replica inventory is ambiguous')
    engine, replica = engines[0], replicas[0]
    es, rs = engine['status'], replica['status']
    require(es.get('currentState') == rs.get('currentState') == 'running' and replica['spec'].get('active') is True and not replica['spec'].get('failedAt'), 'replica is not healthy')
    require(es.get('currentImage', '').endswith(':v1.12.1') and engine['spec'].get('image') == es.get('currentImage'), 'engine implementation differs from the reviewed fold semantics')
    require(list(es.get('replicaModeMap', {}).values()) == ['RW'], 'replica is not exclusively RW')
    require(es.get('purgeStatus') and all(not x.get('isPurging') and not x.get('error') for x in es['purgeStatus'].values()), 'purge is active or health is unknown')
    for key in ('restoreStatus', 'rebuildStatus', 'cloneStatus', 'backupStatus'):
        require(not any(x.get('isRestoring') or x.get('isCloning') or x.get('isRebuilding') or x.get('state') in ('in_progress', 'in-progress') for x in (es.get(key) or {}).values()), 'another storage operation may be active')
    chain = es.get('snapshots', {})
    selected = chain.get(config['snapshot'], {})
    children = [n for n, present in selected.get('children', {}).items() if present]
    require(len(children) == 1 and children[0] != 'volume-head', 'candidate must have one immutable child')
    child = children[0]
    require(child in chain and chain[child].get('parent') == config['snapshot'], 'engine chain binding differs')
    # Native purge also touches already removed/system-generated snapshots.
    # Include them explicitly; only a head-parent prune can be reviewed here.
    collateral = collateral_purges(chain, config['snapshot'])
    ledger = kube.ledger(config)
    terminal_ledger(ledger, config['artifact'], config['run'])
    backups = kube.items(base + '/backups')
    require(not any(x.get('spec', {}).get('snapshotName') == config['snapshot'] for x in backups), 'remote Backup CR still refers to the candidate')
    volumes = kube.items(base + '/volumes')
    require(not any(volume_name in str(x['spec'].get('dataSource', '')) or x.get('status', {}).get('cloneStatus', {}).get('sourceVolume') == volume_name for x in volumes), 'a clone refers to this volume')
    contents = kube.items('/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents')
    require(not any(volume_name in str(x.get('spec', {}).get('source', {})) or volume_name in str(x.get('status', {}).get('snapshotHandle', '')) for x in contents), 'CSI snapshot references this volume')
    lease = kube.get('leases.coordination.k8s.io', config['coordinationLease'], ns)
    require(not lease['spec'].get('holderIdentity'), 'backup or release coordination Lease is occupied')
    manager = kube.get('pod', rs['instanceManagerName'], lh)
    require(manager['spec'].get('nodeName') == replica['spec']['nodeID'] == pod['spec']['nodeName'], 'replica observer is on another node')
    require(any(c['name'] == 'instance-manager' and c.get('ready') for c in manager['status'].get('containerStatuses', [])), 'instance manager is not Ready')
    request = dict(directory='/host' + replica['spec']['diskPath'] + '/replicas/' + replica['spec']['dataDirectoryName'], parent=config['snapshot'], child=child)
    source = Path(longhorn_snapshot_map.__file__).read_text()
    observation = json.loads(kube.command(['-n', lh, 'exec', '-i', manager['metadata']['name'], '-c', 'instance-manager', '--',
                                          'python3', '-c', source], json.dumps(request), limit=1 << 20))
    setting = kube.get('settings.longhorn.io', 'storage-minimal-available-percentage', lh)
    budget = fold_budget(observation, int(setting['value']), config['minimumExtraFreeBytes'], config['workingReserveBytes'], config['maxFoldWriteBytes'])
    evidence = dict(schema='fugue.local-snapshot-review/v1', observed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                    read_only=True, ready_for_review=budget['capacity_gate_passed'], execution_authorized=False,
                    deletion_attempted=False, declaration=config,
                    snapshot_resource_version=snapshot['metadata']['resourceVersion'],
                    engine_uid=engine['metadata']['uid'], replica_uid=replica['metadata']['uid'],
                    instance_manager_uid=manager['metadata']['uid'], child=child,
                    lease_uid=lease['metadata']['uid'], collateral_head_parent_prunes=collateral,
                    ledger=ledger, allocation=observation, budget=budget,
                    remaining_execution_gates=['shared backup/release Lease held until native purge quiesces',
                                              'fresh object UID/resourceVersion and full reference revalidation',
                                              'bounded native deletion, no manual replica-file changes',
                                              'database/API latency and post-purge physical free-space verification'])
    canonical = json.dumps(evidence, sort_keys=True, separators=(',', ':')).encode()
    evidence['review_sha256'] = hashlib.sha256(canonical).hexdigest()
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('declaration')
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.declaration).read_text())
    evidence = observe(ReadOnlyCluster(), config)
    Path(args.evidence).write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({k: evidence[k] for k in ('observed_at', 'ready_for_review', 'execution_authorized', 'deletion_attempted', 'budget', 'review_sha256')}))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, KeyError, TypeError, OSError, subprocess.TimeoutExpired) as error:
        # Provider failures and raw manifests are never printed.
        print('snapshot review incomplete: ' + str(error) if isinstance(error, ValueError) else 'snapshot review incomplete; no action taken')
        raise SystemExit(1) from None
