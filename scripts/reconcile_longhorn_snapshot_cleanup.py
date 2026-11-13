"""Bounded, restart-safe local snapshot GC through the production CI lane.

The committed policy authorizes only remotely retired, unprotected ledger
aliases. Reuse the native executor, including its durable intent, fences,
capacity gates and completion receipt. No replica files or remote backups are
modified. An unknown DELETE outcome is never blindly replayed.
"""
import argparse
import copy
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import time

from scripts.plan_longhorn_snapshot_recovery import NAME, observe, require, terminal_ledger, validate, validate_recovery_budget
from scripts.recover_longhorn_snapshot import Cluster, Recovery, canonical, timestamp

POLICY_LABEL = 'fugue.pro/snapshot-cleanup-policy'


class PreflightInventory:
    """Reuse read-only discovery within a pass, NEVER executor authorization.

    Recovery.run() always receives the original client and repeats fresh gates
    under its Lease/fences. Ledger queries and allocation probes stay fresh.
    """
    def __init__(self, kube):
        self.kube, self.cache = kube, {}

    def get(self, resource, name, namespace=None):
        key = ('get', resource, name, namespace)
        if key not in self.cache:
            self.cache[key] = self.kube.get(resource, name, namespace)
        return copy.deepcopy(self.cache[key])

    def items(self, path):
        key = ('items', path)
        if key not in self.cache:
            self.cache[key] = self.kube.items(path)
        return copy.deepcopy(self.cache[key])

    def ledger(self, config):
        return self.kube.ledger(config)

    def command(self, *args, **kwargs):
        return self.kube.command(*args, **kwargs)


def validate_policy(policy):
    fields = {'schema', 'enabled', 'namespace', 'cluster', 'longhornNamespace',
                            'coordinationLease', 'minimumExtraFreeBytes', 'workingReserveBytes',
                            'maxFoldWriteBytes', 'maxWaitSeconds', 'readyURL'}
    if policy.get('schema') == 'fugue.local-snapshot-cleanup/v2':
        fields |= {'recoveryBudget', 'maxOperationsPerPass', 'maxPassSeconds'}
        validate_recovery_budget(policy['recoveryBudget'])
        for key, low, high in [('maxOperationsPerPass', 1, 20), ('maxPassSeconds', 240, 600)]:
            require(type(policy[key]) is int and low <= policy[key] <= high, 'invalid cleanup pass bound')
        require(type(policy['maxWaitSeconds']) is int and policy['maxWaitSeconds'] + 180 <= policy['maxPassSeconds'],
                'native observation window leaves no discovery headroom')
    require(set(policy) == fields, 'invalid cleanup policy')
    require(policy['schema'] in ('fugue.local-snapshot-cleanup/v1', 'fugue.local-snapshot-cleanup/v2') and type(policy['enabled']) is bool, 'invalid cleanup policy version')
    for key in ('namespace', 'cluster', 'longhornNamespace', 'coordinationLease'):
        require(isinstance(policy[key], str) and NAME.fullmatch(policy[key]), 'invalid cleanup policy identity')
    # Validate the shared budget/URL contracts without relaxing executor gates.
    c = dict(schema='fugue.local-snapshot-review/v1', **{k: policy[k] for k in
             ('namespace', 'cluster', 'longhornNamespace', 'coordinationLease', 'minimumExtraFreeBytes', 'workingReserveBytes', 'maxFoldWriteBytes')},
             clusterUID='uid', primary='primary', primaryUID='uid', pvcUID='uid', volume='volume',
             volumeUID='uid', snapshot='snapshot', snapshotUID='uid', artifact='artifact', run='run')
    if 'recoveryBudget' in policy:
        c.update(schema='fugue.local-snapshot-review/v2', recoveryBudget=policy['recoveryBudget'])
    validate(c)
    Recovery(None, c, approval(policy, c), '/dev/null')


def approval(policy, declaration):
    return dict(schema='fugue.local-snapshot-recovery/v1', authorized=True,
                declarationSHA256=hashlib.sha256(canonical(declaration).encode()).hexdigest(),
                snapshotUID=declaration['snapshotUID'], maxWaitSeconds=policy['maxWaitSeconds'], readyURL=policy['readyURL'])


def candidate_ledger(rows, snapshot, run):
    aliases = [r for r in rows if snapshot in (r.get('snapshot'), r.get('alternate_snapshot'))]
    require(aliases and all(r['run'] == run for r in aliases), 'snapshot ledger ownership is missing or ambiguous')
    types = {r['target_type'] for r in aliases}
    require(len(types) == 1, 'snapshot ledger target types differ')
    target = next(iter(types))
    terminal_ledger(aliases, aliases[0]['id'], run, target)
    return aliases


def database_binding(kube, policy, namespace, cluster_name):
    cluster = kube.get('clusters.postgresql.cnpg.io', cluster_name, namespace)
    primary = cluster['status']['currentPrimary']
    pod = kube.get('pod', primary, namespace)
    pvc = kube.get('pvc', primary, namespace)
    pv = kube.get('pv', pvc['spec']['volumeName'])
    require(pv['spec']['claimRef']['uid'] == pvc['metadata']['uid'] and
            pv['spec']['csi']['driver'] == 'driver.longhorn.io', 'database is not bound to Longhorn')
    volume = kube.get('volumes.longhorn.io', pv['spec']['csi']['volumeHandle'], policy['longhornNamespace'])
    return dict(namespace=namespace, cluster=cluster_name, clusterUID=cluster['metadata']['uid'],
                primary=primary, primaryUID=pod['metadata']['uid'], pvcUID=pvc['metadata']['uid'],
                volume=volume['metadata']['name'], volumeUID=volume['metadata']['uid'])


def declaration(kube, policy, platform, snapshot, rows, pod):
    meta = snapshot['metadata']
    run = meta.get('labels', {}).get('fugue.pro/backup-run', '')
    aliases = candidate_ledger(rows, meta['name'], run)
    target = aliases[0]['target_type']
    if target == 'control-plane-db':
        binding = platform
    else:
        # Derive the database from the actual CNPG Pod owning this attached
        # volume, then require every ledger alias to agree on its service.
        cluster = pod['metadata'].get('labels', {}).get('cnpg.io/cluster')
        require(cluster and all(r.get('service_name') == cluster for r in aliases), 'managed database service binding differs')
        binding = database_binding(kube, policy, pod['metadata']['namespace'], cluster)
        require(binding['primary'] == pod['metadata']['name'], 'volume consumer is not the current database primary')
    require(snapshot['spec']['volume'] == binding['volume'], 'snapshot volume differs from current database')
    require(all(r.get('longhorn_namespace') in (None, '', policy['longhornNamespace']) for r in aliases), 'ledger storage namespace differs')
    c = dict(schema='fugue.local-snapshot-review/v1', **binding,
             longhornNamespace=policy['longhornNamespace'], snapshot=meta['name'], snapshotUID=meta['uid'],
             artifact=aliases[0]['id'], run=run, coordinationLease=policy['coordinationLease'],
             ledgerNamespace=platform['namespace'], ledgerPrimary=platform['primary'], ledgerPrimaryUID=platform['primaryUID'],
             coordinationNamespace=platform['namespace'], targetType=target,
             **{k: policy[k] for k in ('minimumExtraFreeBytes', 'workingReserveBytes', 'maxFoldWriteBytes')})
    if 'recoveryBudget' in policy:
        c.update(schema='fugue.local-snapshot-review/v2', recoveryBudget=policy['recoveryBudget'])
    validate(c)
    return c


def reconcile(kube, policy, evidence_path, *, execute=False):
    validate_policy(policy)
    digest = hashlib.sha256(canonical(policy).encode()).hexdigest()
    result = dict(schema=policy['schema'], observed_at=timestamp(), preview=not execute,
                  policy_sha256=digest, completed=0, candidates=0, blocked=[], retained=[], phase='disabled')
    path = Path(evidence_path)

    def save():
        path.write_text(json.dumps(result, indent=2) + '\n')

    save()
    if not policy['enabled']:
        pending = [r for r in kube.items('/api/v1/configmaps', labelSelector=POLICY_LABEL)
                   if json.loads(r['data']['receipt.json'])['phase'] != 'completed']
        if pending:
            result.update(phase='blocked', blocked=[dict(reason='policy disabled with unfinished native operation; protection retained')])
            save()
        return result
    result['phase'] = 'inspecting'
    save()
    platform = database_binding(kube, policy, policy['namespace'], policy['cluster'])
    base = '/apis/longhorn.io/v1beta2/namespaces/' + policy['longhornNamespace']
    # Resume durable operations before discovery, even if the Snapshot CR or
    # its engine layer is already gone. A crash must not lose completion work.
    operations = kube.items('/api/v1/configmaps', labelSelector=POLICY_LABEL)
    pending = []
    for record in operations:
        data = record.get('data', {})
        receipt = json.loads(data['receipt.json'])
        if receipt['phase'] == 'completed':
            continue
        if data.get('cleanup_policy_sha256') != digest:
            require('cleanup_policy.json' in data, 'unfinished cleanup belongs to a different policy; review required')
            original = json.loads(data['cleanup_policy.json'])
            validate_policy(original)
            require(hashlib.sha256(canonical(original).encode()).hexdigest() == data.get('cleanup_policy_sha256') and
                    all(original[k] == policy[k] for k in ('namespace', 'cluster', 'longhornNamespace', 'coordinationLease')),
                    'unfinished cleanup belongs to a different policy; review required')
        pending.append(record)
    require(len(pending) <= 1, 'multiple unfinished local snapshot operations; review required')
    if pending:
        record = pending[0]
        original = json.loads(record['data']['cleanup_policy.json']) if 'cleanup_policy.json' in record['data'] else policy
        validate_policy(original)
        c, a = json.loads(record['data']['declaration.json']), json.loads(record['data']['approval.json'])
        require(a == approval(original, c) and c['ledgerNamespace'] == platform['namespace'] and
                c['coordinationNamespace'] == platform['namespace'] and c['coordinationLease'] == policy['coordinationLease'] and
                c['longhornNamespace'] == original['longhornNamespace'] and
                all(c[k] == original[k] for k in ('minimumExtraFreeBytes', 'workingReserveBytes', 'maxFoldWriteBytes')) and
                c.get('recoveryBudget') == original.get('recoveryBudget') and
                record['metadata']['namespace'] == c['namespace'], 'durable cleanup authorization differs')
        r = Recovery(kube, c, a, str(path) + '.operation.json', cleanup_policy_sha256=record['data']['cleanup_policy_sha256'],
                     cleanup_policy=original if 'cleanup_policy.json' in record['data'] else None)
        require(record['metadata']['name'] == r.operation, 'durable cleanup operation identity differs')
        result.update(phase='pending-native-operation', operation=r.operation)
        save()
        if execute:
            r.run()
            result.update(phase=r.state['phase'], completed=int(r.state['phase'] == 'completed'))
            save()
        return result
    lease = kube.get('leases.coordination.k8s.io', policy['coordinationLease'], policy['namespace'])
    if lease['spec'].get('holderIdentity'):
        result.update(phase='blocked', blocked=[dict(reason='backup/release coordination Lease is occupied')])
        save()
        return result
    snapshots = kube.items(base + '/snapshots', labelSelector='fugue.pro/backup-run')
    volumes = {v['metadata']['name']: v for v in kube.items(base + '/volumes')}
    pods = kube.items('/api/v1/pods', labelSelector='cnpg.io/cluster')
    preflight = PreflightInventory(kube)
    ledger_cache = {}
    ready = []
    # Bounded observations, at most ONE native deletion per fresh discovery.
    # Sorting is deterministic; blocked candidates do not starve smaller ones.
    for snapshot in sorted(snapshots, key=lambda s: (s['metadata'].get('creationTimestamp', ''), s['metadata']['name'])):
        if snapshot['metadata'].get('deletionTimestamp') or snapshot.get('status', {}).get('markRemoved'):
            continue
        name, volume_name = snapshot['metadata']['name'], snapshot['spec']['volume']
        if volume_name not in ledger_cache:
            require(len(ledger_cache) < 32, 'snapshot volume inventory exceeds observation budget')
            ledger_cache[volume_name] = kube.volume_ledger({**platform, 'volume': volume_name})
        try:
            if not any(name in (r.get('snapshot'), r.get('alternate_snapshot')) for r in ledger_cache[volume_name]):
                failed = kube.failed_snapshot_run({**platform, 'run': snapshot['metadata']['labels']['fugue.pro/backup-run'], 'snapshot': name})
                ledger_cache[volume_name].extend(failed)
            aliases = candidate_ledger(ledger_cache[volume_name], name, snapshot['metadata']['labels']['fugue.pro/backup-run'])
        except ValueError:
            refs = [r for r in ledger_cache[volume_name] if name in (r.get('snapshot'), r.get('alternate_snapshot'))]
            owned = refs and all(r['run'] == snapshot['metadata']['labels']['fugue.pro/backup-run'] for r in refs)
            if owned and any(r.get('protected') is True or (r.get('kind') != 'failed-run' and r['status'] not in ('deleted', 'expired')) for r in refs):
                result['retained'].append(dict(snapshot=name, reason='live or protected recovery point'))
            else:
                result['blocked'].append(dict(snapshot=name, reason='remote not retired, restoring, or ownership unverified'))
            save()
            continue
        result['candidates'] += 1
        if result['candidates'] > 20:
            result['blocked'].append(dict(reason='candidate observation budget reached; retry next pass'))
            break
        try:
            require(volume_name in volumes, 'snapshot volume is unavailable')
            consumers = volumes[volume_name]['status'].get('kubernetesStatus', {}).get('workloadsStatus', [])
            owners = [p for p in pods if any(w.get('podName') == p['metadata']['name'] and
                      volumes[volume_name]['status']['kubernetesStatus'].get('namespace') == p['metadata']['namespace'] for w in consumers)]
            if aliases[0]['target_type'] == 'app-database':
                require(len(owners) == 1, 'managed database volume ownership is ambiguous')
            c = declaration(kube, policy, platform, snapshot, ledger_cache[volume_name], owners[0] if len(owners) == 1 else {})
            review = observe(preflight, c)
            if not review['ready_for_review']:
                result['blocked'].append(dict(snapshot=name, reason='working-space gate failed; no deletion permitted', budget=review['budget']))
                save()
                continue
        except ValueError as error:
            result['blocked'].append(dict(snapshot=name, reason=str(error)))
            save()
            continue
        ready.append((snapshot, c, review))
        if 'recoveryBudget' not in policy:
            break
    if ready:
        def cost(item):
            snapshot, _, review = item
            created = snapshot['metadata'].get('creationTimestamp', timestamp())
            age = max(0, (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(created.replace('Z', '+00:00'))).total_seconds())
            written = review['budget'].get('fold_write_bytes', 0)
            # Age weighting prevents small new jobs indefinitely starving old
            # layers, without putting a large, unsafe layer ahead of all work.
            return (written / (1 + age / (7 * 86400)), created, snapshot['metadata']['name'])
        snapshot, c, review = min(ready, key=cost)
        result.update(phase='ready', selected_snapshot=snapshot['metadata']['name'], budget=review['budget'],
                      executable_candidates=len(ready))
        save()
        if execute:
            r = Recovery(kube, c, approval(policy, c), str(path) + '.operation.json', cleanup_policy_sha256=digest,
                         cleanup_policy=policy)
            r.run()
            result.update(phase=r.state['phase'], operation=r.operation, completed=int(r.state['phase'] == 'completed'))
            save()
        return result
    result['phase'] = 'blocked' if result['blocked'] else 'idle'
    save()
    return result


def drain(kube, policy, evidence_path, *, execute=False):
    """Bounded sequence; every operation gets a new, fresh discovery pass."""
    validate_policy(policy)
    if not execute or 'recoveryBudget' not in policy:
        return reconcile(kube, policy, evidence_path, execute=execute)
    started, passes = time.monotonic(), []
    result = dict(schema=policy['schema'], observed_at=timestamp(), preview=False, phase='inspecting',
                  completed=0, candidates=0, passes=passes)
    path = Path(evidence_path)
    for index in range(policy['maxOperationsPerPass']):
        # Discovery can inspect twenty two-second samples, barriers and native
        # observation add more work. Leave headroom inside the 15-minute job.
        if index and time.monotonic() - started + policy['maxWaitSeconds'] + 180 >= policy['maxPassSeconds']:
            result['yield_reason'] = 'pass time budget; continue on next scheduled reconciliation'
            path.write_text(json.dumps(result, indent=2) + '\n')
            break
        path.write_text(json.dumps(result, indent=2) + '\n')
        current = reconcile(kube, policy, str(path) + '.pass-' + str(index) + '.json', execute=True)
        passes.append(current)
        result.update(phase=current['phase'], completed=sum(p['completed'] for p in passes), candidates=current['candidates'])
        path.write_text(json.dumps(result, indent=2) + '\n')
        if current['phase'] != 'completed' or time.monotonic() - started >= policy['maxPassSeconds']:
            break
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('policy')
    parser.add_argument('--evidence', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if args.apply:
        require(os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REF') == 'refs/heads/main' and
                re.fullmatch('[0-9]+', os.environ.get('GITHUB_RUN_ID', '')) and not os.environ.get('FUGUE_REVIEW_SSH_HOST'),
                'apply requires the standard main CI lane')
    result = drain(Cluster(), json.loads(Path(args.policy).read_text()), args.evidence, execute=args.apply)
    print(canonical({k: result[k] for k in ('phase', 'completed', 'candidates', 'preview')}))


if __name__ == '__main__':
    main()
