"""Read-only, exact-identity review plan for one obsolete local PG snapshot.

No --apply mode exists. This does not retire a backup, acquire a Lease, change
retention, call DELETE, or mutate PostgreSQL. A later executor must coordinate
with backup/release writers and revalidate this plan before native deletion.
"""
import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import urllib.parse

from scripts import longhorn_snapshot_map

NAME = re.compile(r'[a-z0-9][a-z0-9.-]{0,252}')
TERMINAL = {'deleted', 'expired'}
FOLD_IMAGE = 'docker.io/longhornio/longhorn-engine:v1.12.1'
FOLD_COMMIT = '1fa902f0061b84b92af445c38392bfeb99faffb2'


def require(value, message):
    if not value:
        raise ValueError(message)


def validate(config):
    required = {'schema', 'namespace', 'cluster', 'clusterUID', 'primary', 'primaryUID',
                'pvcUID', 'volume', 'volumeUID', 'longhornNamespace', 'snapshot',
                'snapshotUID', 'artifact', 'run', 'coordinationLease',
                'minimumExtraFreeBytes', 'workingReserveBytes', 'maxFoldWriteBytes'}
    ledger_keys = {'ledgerNamespace', 'ledgerPrimary', 'ledgerPrimaryUID', 'coordinationNamespace', 'targetType'}
    version = config.get('schema')
    if version == 'fugue.local-snapshot-review/v2':
        required = required | {'recoveryBudget'}
        validate_recovery_budget(config['recoveryBudget'])
    require(set(config) in (required, required | ledger_keys) and version in ('fugue.local-snapshot-review/v1', 'fugue.local-snapshot-review/v2'), 'invalid snapshot review declaration')
    for key in (set(config) - {'schema', 'minimumExtraFreeBytes', 'workingReserveBytes', 'maxFoldWriteBytes', 'artifact', 'run', 'targetType', 'recoveryBudget'}):
        require(isinstance(config[key], str) and NAME.fullmatch(config[key]), 'invalid snapshot review identity')
    require(config.get('targetType', 'control-plane-db') in ('control-plane-db', 'app-database'), 'unsupported snapshot target type')
    for key in ('artifact', 'run'):
        require(isinstance(config[key], str) and re.fullmatch(r'[a-zA-Z0-9_-]{1,200}', config[key]), 'invalid backup ledger identity')
    for key, minimum in [('minimumExtraFreeBytes', 16 << 30), ('workingReserveBytes', 192 << 20), ('maxFoldWriteBytes', 1)]:
        require(type(config[key]) is int and minimum <= config[key] <= 1 << 40, 'invalid snapshot working-space budget')
    require(config['snapshot'] != 'volume-head', 'the live head cannot be reviewed for removal')


def validate_recovery_budget(value):
    require(isinstance(value, dict) and set(value) == {'schema', 'foregroundReserveBytes', 'growthSampleSeconds', 'operationWindowSeconds', 'minimumFoldBytesPerSecond', 'maxDatabaseProbeSeconds', 'maxAPIProbeSeconds'}, 'invalid fixed-capacity recovery budget')
    require(value['schema'] == 'fugue.fixed-capacity-ext4/v1', 'unsupported recovery budget')
    for key, low, high in [('foregroundReserveBytes', 16 << 30, 1 << 40),
                           ('growthSampleSeconds', 2, 5), ('operationWindowSeconds', 300, 21600),
                           ('minimumFoldBytesPerSecond', 1 << 20, 1 << 30),
                           ('maxDatabaseProbeSeconds', 1, 15), ('maxAPIProbeSeconds', 1, 15)]:
        require(type(value[key]) is int and low <= value[key] <= high, 'invalid fixed-capacity budget bound')


def recovery_budget(observation, config):
    """No future reclaim credit and no dependence on new-replica scheduling.

    Whole-child mapped writes bound new data allocation on the supported ext4
    path, including overwrites; working reserve covers metadata/alignment. The
    smaller overlap-derived estimate is informational, NOT admission authority.
    """
    value = config['recoveryBudget']
    validate_recovery_budget(value)
    require(observation.get('filesystem', {}).get('type') == 'ext4' and
            observation['filesystem'].get('read_only') is False, 'unsupported recovery filesystem')
    elapsed = observation.get('growth_sample_seconds', 0)
    require(isinstance(elapsed, (int, float)) and math.isfinite(elapsed) and
            value['growthSampleSeconds'] <= elapsed <= 30, 'filesystem growth sample unavailable')
    observed = observation.get('observed_growth_bytes')
    require(type(observed) is int and observed >= 0, 'filesystem growth sample unavailable')
    parent, child = observation['files']
    overlap, written = observation['overlap_bytes'], observation['fold_write_bytes']
    for number in (overlap, written, parent['mapped_bytes'], child['mapped_bytes'], observation['new_mapped_bytes'],
                   observation['physical_free_bytes'], observation['physical_available_bytes'], observation['physical_total_bytes']):
        require(type(number) is int and number >= 0, 'invalid physical allocation observation')
    require(observation['physical_available_bytes'] <= observation['physical_free_bytes'] <= observation['physical_total_bytes'],
            'inconsistent physical allocation counters')
    require(0 <= overlap <= min(parent['mapped_bytes'], child['mapped_bytes']) and
            written == child['mapped_bytes'] and observation['new_mapped_bytes'] == written - overlap,
            'fold direction disagrees with pinned engine')
    require(written <= config['maxFoldWriteBytes'], 'fold exceeds the declared operation write budget')
    estimated_seconds = math.ceil(written / value['minimumFoldBytesPerSecond'])
    require(estimated_seconds <= value['operationWindowSeconds'], 'estimated fold duration exceeds foreground growth window')
    require(observation['content_bytes_read'] == 0 and observation['sync_requested'] is False, 'unexpected data read or sync')
    free = observation['physical_available_bytes']
    foreground = max(value['foregroundReserveBytes'], math.ceil(2 * observed / elapsed * value['operationWindowSeconds']))
    floor = config['minimumExtraFreeBytes'] + config['workingReserveBytes'] + foreground
    return dict(schema=value['schema'], counter='available_bytes',
                new_allocation_estimate_bytes=written - overlap, peak_allocation_bound_bytes=written,
                fold_write_bytes=written, overlapping_mapped_bytes=overlap,
                estimated_native_seconds=estimated_seconds, minimum_fold_bytes_per_second=value['minimumFoldBytesPerSecond'],
                physical_free_bytes=observation['physical_free_bytes'], physical_available_bytes=free,
                foreground_reserve_bytes=foreground, working_reserve_bytes=config['workingReserveBytes'],
                extra_free_bytes=config['minimumExtraFreeBytes'], completion_floor_bytes=floor,
                remaining_budget_bytes=free - floor - written,
                capacity_gate_passed=free >= floor + written, reclaimable_bytes_exact=None)


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

    def items(self, path, **selectors):
        # Explicit pagination is unnecessary for these bounded inventories:
        # fail closed rather than accept a partial cross-reference list.
        response = json.loads(self.command(['get', '--raw', path + '?' + urllib.parse.urlencode(dict(limit=500, **selectors))]))
        require(not response.get('metadata', {}).get('continue') and len(response['items']) <= 500, 'cross-reference inventory is truncated')
        return response['items']

    def ledger(self, config):
        rows = self.volume_ledger(config, snapshot=config['snapshot'])
        if not rows and config['artifact'] == config['run']:
            rows = self.failed_snapshot_run(config)
        require(0 < len(rows) <= 100, 'snapshot ledger references missing or exceed budget')
        return rows

    def failed_snapshot_run(self, config):
        # A failed create can leave a local snapshot without ANY artifact.
        # Such a source has no remote-retirement receipt; keep that proof
        # distinct rather than pretending a remote backup was deleted.
        run = config['run']
        require(re.fullmatch(r'[a-zA-Z0-9_-]{1,200}', run), 'invalid failed run identity')
        namespace = config.get('ledgerNamespace', config['namespace'])
        primary = config.get('ledgerPrimary', config['primary'])
        if config.get('ledgerPrimaryUID'):
            same(self.get('pod', primary, namespace), primary, config['ledgerPrimaryUID'])
        sql = f"""BEGIN READ ONLY;
SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL temp_file_limit=0;
SELECT json_build_object('id',r.id,'run',r.id,'kind','failed-run','status',r.status,
 'protected',false,'no_artifacts',true,'run_terminal',true,'run_lease_active',false,
 'terminal_source',true,'restore_references',0,'target_type',r.target_type,
 'service_name',r.target_json->>'service_name')
FROM fugue_backup_runs r WHERE r.id='{run}' AND r.target_json->>'engine'='longhorn-snapshot'
 AND r.status IN ('failed','canceled') AND COALESCE(r.finished_at,r.updated_at)<now()-interval '24 hours'
 AND COALESCE(r.locked_until<now(),true) AND (r.next_retry_at IS NULL OR r.next_retry_at<=now())
 AND NOT EXISTS (SELECT 1 FROM fugue_backup_artifacts a WHERE a.run_id=r.id);
COMMIT;
"""
        raw = self.command(['-n', namespace, 'exec', '-i', primary, '-c', 'postgres', '--',
                            'psql', '-U', 'postgres', '-d', 'fugue', '-qAt', '-v', 'ON_ERROR_STOP=1'], stdin=sql, limit=1 << 20)
        rows = [json.loads(line) for line in raw.splitlines() if line]
        require(len(rows) <= 1, 'failed run proof is ambiguous')
        return [{**r, 'snapshot': config['snapshot']} for r in rows]

    def volume_ledger(self, config, *, snapshot=None):
        literal = lambda value: "'" + value.replace("'", "''") + "'"
        volume = literal(config['volume'])
        predicate = ''
        if snapshot is not None:
            name = literal(snapshot)
            predicate = f" AND (a.manifest_json#>>'{{metadata,snapshot_name}}'={name} OR a.manifest_json#>>'{{metadata,longhorn_snapshot_name}}'={name})"
        namespace = config.get('ledgerNamespace', config['namespace'])
        primary = config.get('ledgerPrimary', config['primary'])
        if config.get('ledgerPrimaryUID'):
            same(self.get('pod', primary, namespace), primary, config['ledgerPrimaryUID'])
        sql = f"""BEGIN READ ONLY;
SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms';
SET LOCAL work_mem='1MB'; SET LOCAL temp_file_limit=0;
SET LOCAL max_parallel_workers_per_gather=0;
SELECT json_build_object('id',a.id,'run',a.run_id,'status',a.status,'protected',a.protected,
 'kind',a.kind,'target_type',a.target_type,'physical_deleted',a.physical_deleted_at IS NOT NULL,
 'snapshot',a.manifest_json#>>'{{metadata,snapshot_name}}',
 'alternate_snapshot',a.manifest_json#>>'{{metadata,longhorn_snapshot_name}}',
 'longhorn_namespace',a.manifest_json#>>'{{metadata,longhorn_namespace}}',
 'service_name',a.target_json->>'service_name',
 'run_terminal',r.status IN ('succeeded','failed','canceled'),
 'run_lease_active',COALESCE(r.locked_until>now(),false),
 'restore_references',(SELECT count(*) FROM fugue_backup_restore_plans p WHERE p.artifact_id=a.id AND p.status NOT IN ('succeeded','failed','canceled'))
  +(SELECT count(*) FROM fugue_backup_restore_runs u WHERE u.artifact_id=a.id AND u.status NOT IN ('succeeded','failed','canceled')))
FROM fugue_backup_artifacts a LEFT JOIN fugue_backup_runs r ON r.id=a.run_id
WHERE a.kind='longhorn-snapshot' AND a.manifest_json#>>'{{metadata,volume_name}}'={volume}
 {predicate} LIMIT 501;
COMMIT;
"""
        raw = self.command(['-n', namespace, 'exec', '-i', primary, '-c', 'postgres', '--',
                            'psql', '-U', 'postgres', '-d', 'fugue', '-qAt', '-v', 'ON_ERROR_STOP=1'], stdin=sql, limit=1 << 20)
        rows = [json.loads(line) for line in raw.splitlines() if line]
        require(len(rows) <= 500, 'volume ledger references exceed budget')
        return rows


def same(obj, name, uid):
    require(obj['metadata']['name'] == name and obj['metadata']['uid'] == uid and not obj['metadata'].get('deletionTimestamp'), 'reviewed object identity changed')


def engine_implementation(kube, namespace, image, node, expected=None):
    """The allocation model applies to this reviewed native build, not a suffix."""
    require(image == FOLD_IMAGE, 'native fold image is not the reviewed implementation')
    if expected:
        item = kube.get('engineimages.longhorn.io', expected['name'], namespace)
        same(item, expected['name'], expected['uid'])
    else:
        items = [x for x in kube.items('/apis/longhorn.io/v1beta2/namespaces/' + namespace + '/engineimages')
                 if x['spec'].get('image') == image]
        require(len(items) == 1, 'native engine image identity is ambiguous')
        item = items[0]
    status = item['status']
    require(not item['metadata'].get('deletionTimestamp') and item['spec']['image'] == image and
            status.get('state') == 'deployed' and status.get('version') == 'v1.12.1' and
            status.get('gitCommit') == FOLD_COMMIT and status.get('nodeDeploymentMap', {}).get(node) is True,
            'native fold build or node deployment differs from the reviewed implementation')
    return dict(name=item['metadata']['name'], uid=item['metadata']['uid'], image=image, git_commit=FOLD_COMMIT)


def terminal_ledger(ledger, artifact, run, target_type='control-plane-db'):
    require(target_type in ('control-plane-db', 'app-database'), 'unsupported snapshot target type')
    require(any(x['id'] == artifact and x['run'] == run for x in ledger), 'exact artifact receipt is missing')
    if len(ledger) == 1 and ledger[0].get('kind') == 'failed-run':
        r = ledger[0]
        require(artifact == run and r['id'] == run and r['status'] in ('failed', 'canceled') and
                r.get('no_artifacts') is True and r.get('terminal_source') is True and
                r.get('protected', False) is False and
                r['run_terminal'] is True and r['run_lease_active'] is False and
                r['target_type'] == target_type and r['restore_references'] == 0, 'failed run is not safe to retire')
        return
    require(all(x['status'] in TERMINAL and x['protected'] is False and x['physical_deleted'] is True and x['run_terminal'] is True and x['run_lease_active'] is False and x['restore_references'] == 0 and x['target_type'] == target_type for x in ledger), 'live, protected, unverified or restoring artifact references this snapshot')


def collateral_purges(chain, selected):
    collateral = []
    for name, item in chain.items():
        if name in ('volume-head', selected) or (not item.get('removed') and item.get('usercreated') is True):
            continue
        require(set(item.get('children', {})) == {'volume-head'}, 'native purge would coalesce additional snapshots')
        collateral.append(name)
    return collateral


def check_pending_snapshot_deletions(snapshot_crs, volume, chain, collateral):
    for item in snapshot_crs:
        if item['spec'].get('volume') != volume or not item['metadata'].get('deletionTimestamp'):
            continue
        name = item['metadata']['name']
        native = chain.get(name, {})
        # Longhorn retains the removed live-head parent until an immutable
        # child exists. This quiescent, already-authorized head-parent prune
        # is included in collateral_purges, NOT a concurrent unknown delete.
        require(name in collateral and native.get('removed') is True and
                set(native.get('children', {})) == {'volume-head'} and
                item.get('status', {}).get('markRemoved') is True,
                'another snapshot deletion is already pending')


def observe(kube, config, *, lease_holder=None):
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
    snapshot_crs = kube.items(base + '/snapshots')
    check_pending_snapshot_deletions(snapshot_crs, volume_name, chain, collateral)
    collateral_uids = [x['metadata']['uid'] for x in snapshot_crs
                       if x['metadata']['name'] in collateral and x['spec'].get('volume') == volume_name]
    ledger = kube.ledger(config)
    terminal_ledger(ledger, config['artifact'], config['run'], config.get('targetType', 'control-plane-db'))
    if config.get('targetType') == 'app-database':
        require(all(x.get('service_name') == config['cluster'] and x['run'] == config['run'] for x in ledger), 'managed database ledger binding differs')
    backups = kube.items(base + '/backups')
    require(not any(x.get('spec', {}).get('snapshotName') == config['snapshot'] for x in backups), 'remote Backup CR still refers to the candidate')
    require(not any(x.get('metadata', {}).get('labels', {}).get('backup-volume') == volume_name and
                    (x.get('status', {}).get('state') != 'Completed' or x['metadata'].get('deletionTimestamp')) for x in backups),
            'another backup operation on the volume is pending')
    volumes = kube.items(base + '/volumes')
    require(not any(volume_name in str(x['spec'].get('dataSource', '')) or x.get('status', {}).get('cloneStatus', {}).get('sourceVolume') == volume_name for x in volumes), 'a clone refers to this volume')
    contents = kube.items('/apis/snapshot.storage.k8s.io/v1/volumesnapshotcontents')
    require(not any(volume_name in str(x.get('spec', {}).get('source', {})) or volume_name in str(x.get('status', {}).get('snapshotHandle', '')) for x in contents), 'CSI snapshot references this volume')
    snapshots = kube.items('/apis/snapshot.storage.k8s.io/v1/volumesnapshots')
    require(not any(x['metadata'].get('namespace') == ns and x.get('spec', {}).get('source', {}).get('persistentVolumeClaimName') == config['primary'] for x in snapshots), 'a pending CSI snapshot references this claim')
    claims = kube.items('/api/v1/persistentvolumeclaims')
    for claim in claims:
        for field in ('dataSource', 'dataSourceRef'):
            source = claim.get('spec', {}).get(field) or {}
            require(not (source.get('kind') == 'PersistentVolumeClaim' and source.get('name') == config['primary'] and
                         source.get('namespace', claim['metadata'].get('namespace')) == ns), 'a PVC clone references this claim')
    lease = kube.get('leases.coordination.k8s.io', config['coordinationLease'], config.get('coordinationNamespace', ns))
    require(lease['spec'].get('holderIdentity', '') == (lease_holder or ''), 'backup or release coordination Lease is occupied')
    manager = kube.get('pod', rs['instanceManagerName'], lh)
    require(manager['spec'].get('nodeName') == replica['spec']['nodeID'] == pod['spec']['nodeName'], 'replica observer is on another node')
    require(any(c['name'] == 'instance-manager' and c.get('ready') for c in manager['status'].get('containerStatuses', [])), 'instance manager is not Ready')
    request = dict(directory='/host' + replica['spec']['diskPath'] + '/replicas/' + replica['spec']['dataDirectoryName'], parent=config['snapshot'], child=child)
    if 'recoveryBudget' in config:
        request['growthSampleSeconds'] = config['recoveryBudget']['growthSampleSeconds']
        implementation = engine_implementation(kube, lh, es['currentImage'], replica['spec']['nodeID'])
    source = Path(longhorn_snapshot_map.__file__).read_text()
    observation = json.loads(kube.command(['-n', lh, 'exec', '-i', manager['metadata']['name'], '-c', 'instance-manager', '--',
                                          'python3', '-c', source], json.dumps(request), limit=1 << 20))
    if 'recoveryBudget' in config:
        budget = recovery_budget(observation, config)
    else:
        setting = kube.get('settings.longhorn.io', 'storage-minimal-available-percentage', lh)
        budget = fold_budget(observation, int(setting['value']), config['minimumExtraFreeBytes'], config['workingReserveBytes'], config['maxFoldWriteBytes'])
    evidence = dict(schema=config['schema'], observed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
                    read_only=True, ready_for_review=budget['capacity_gate_passed'], execution_authorized=False,
                    deletion_attempted=False, declaration=config,
                    snapshot_resource_version=snapshot['metadata']['resourceVersion'],
                    engine_uid=engine['metadata']['uid'], replica_uid=replica['metadata']['uid'],
                    engine_name=engine['metadata']['name'], replica_name=replica['metadata']['name'],
                    snapshot_chain=chain,
                    instance_manager_uid=manager['metadata']['uid'], child=child,
                    instance_manager_name=manager['metadata']['name'], replica_directory=request['directory'],
                    lease_uid=lease['metadata']['uid'], collateral_head_parent_prunes=collateral,
                    collateral_snapshot_uids=collateral_uids,
                    ledger=ledger, allocation=observation, budget=budget,
                    remaining_execution_gates=['shared backup/release Lease held until native purge quiesces',
                                              'fresh object UID/resourceVersion and full reference revalidation',
                                              'bounded native deletion, no manual replica-file changes',
                                              'database/API latency and post-purge physical free-space verification'])
    if 'recoveryBudget' in config:
        evidence['engine_implementation'] = implementation
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
