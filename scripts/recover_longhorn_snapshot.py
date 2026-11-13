"""One declared local snapshot, native deletion, fail-closed durable recovery.

Execution is restricted to the normal production CI lane. A non-expiring-owner
shared Lease and exact-volume admission barriers survive runner loss. After the
delete intent is durable, errors NEVER release those barriers automatically.
Recovery only observes completion; it never sends a second DELETE.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import urllib.request
import urllib.parse

from scripts.plan_longhorn_snapshot_recovery import ReadOnlyCluster, engine_implementation, observe, require, same, validate

TOKEN = 'fugue.pro/coordination-token'
OPERATION = 'fugue.pro/snapshot-recovery'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def timestamp():
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


class Cluster(ReadOnlyCluster):
    def write(self, args, body):
        require(not os.environ.get('FUGUE_REVIEW_SSH_HOST'), 'maintenance writes cannot run through SSH')
        return json.loads(self.command(args, canonical(body)))

    def create(self, obj):
        return self.write(['create', '-f', '-', '-o', 'json'], obj)

    def replace(self, obj):
        return self.write(['replace', '-f', '-', '-o', 'json'], obj)

    def maybe(self, resource, name, namespace=None):
        prefix = ['-n', namespace] if namespace else []
        raw = self.command(prefix + ['get', resource, name, '--ignore-not-found', '-o', 'json'])
        return json.loads(raw) if raw.strip() else None

    def delete(self, path, obj):
        # kubectl raw DELETE forwards the filename as the DeleteOptions body.
        return self.write(['delete', '--raw', path, '-f', '-'],
                          dict(apiVersion='v1', kind='DeleteOptions',
                               preconditions={k: obj['metadata'][k] for k in ('uid', 'resourceVersion')}))


def barrier_objects(config, review, operation, *, legacy_snapshot_spec=False, map_snapshot_spec=False, typed_presence_snapshot_spec=False):
    """Deny NEW reference writers; native finalizers/status remain operational.

    The shared Lease covers Fugue backups/releases, not CSI or Longhorn native
    recurring writers. Admission fences these CR-based writers independently.
    Existing aliases are already terminal and cannot be restored or re-adopted
    through the supported Fugue store contracts. No database row is changed.
    """
    volume, namespace, pvc = map(canonical, (config['volume'], config['namespace'], config['primary']))
    snapshot_uids = canonical([config['snapshotUID']] + review.get('collateral_snapshot_uids', []))
    chain = canonical(sorted(review['snapshot_chain']))
    # Removed head-parent system snapshots may be reconciled by native cleanup.
    same_spec = "request.operation == 'UPDATE' && object.spec == oldObject.spec"
    # Longhorn's typed SnapshotSpec always serializes nullable labels, even
    # when the stored JSON omitted it. Finalizer Update therefore adds null.
    # Kubernetes exposes the structural spec as a typed object, not a map.
    # Pin its complete schema before using the explicit field comparison.
    # Preserve the old map expression solely for exact durable-state recovery.
    map_same_spec = (
        "request.operation == 'UPDATE' && "
        "object.spec.all(k, k == 'labels' || (k in oldObject.spec && object.spec[k] == oldObject.spec[k])) && "
        "oldObject.spec.all(k, k == 'labels' || (k in object.spec && object.spec[k] == oldObject.spec[k])) && "
        "(has(object.spec.labels) ? object.spec.labels : null) == (has(oldObject.spec.labels) ? oldObject.spec.labels : null)")
    typed_same_spec = (
        "request.operation == 'UPDATE' && object.spec.volume == oldObject.spec.volume && "
        "(has(object.spec.createSnapshot) ? object.spec.createSnapshot : false) == "
        "(has(oldObject.spec.createSnapshot) ? oldObject.spec.createSnapshot : false) && "
        "((!has(object.spec.labels) && !has(oldObject.spec.labels)) || "
        "(has(object.spec.labels) && has(oldObject.spec.labels) && object.spec.labels == oldObject.spec.labels))")
    # VAP evaluation uses dynamic objects: has(labels) is true for explicit
    # null. Only this nullable field needs dyn to type-check map/null equality.
    nullable_same_spec = (
        "request.operation == 'UPDATE' && object.spec.volume == oldObject.spec.volume && "
        "(has(object.spec.createSnapshot) ? object.spec.createSnapshot : false) == "
        "(has(oldObject.spec.createSnapshot) ? oldObject.spec.createSnapshot : false) && "
        "(has(object.spec.labels) ? dyn(object.spec.labels) : null) == "
        "(has(oldObject.spec.labels) ? dyn(oldObject.spec.labels) : null)")
    snapshot_same_spec = (same_spec if legacy_snapshot_spec else map_same_spec if map_snapshot_spec else
                          typed_same_spec if typed_presence_snapshot_spec else nullable_same_spec)
    rules = [
        ('snapshots', 'longhorn.io', 'v1beta2', ['snapshots'],
         f'o.spec.volume == {volume}',
         f"({snapshot_same_spec}) || (request.operation == 'DELETE' && oldObject.metadata.uid in {snapshot_uids})"),
        ('backups', 'longhorn.io', 'v1beta2', ['backups'],
         f"(has(o.spec.snapshotName) && o.spec.snapshotName in {chain}) || (has(o.metadata.labels) && 'backup-volume' in o.metadata.labels && o.metadata.labels['backup-volume'] == {volume})",
         same_spec),
        ('volumes', 'longhorn.io', 'v1beta2', ['volumes'],
         f"o.metadata.name == {volume} || (has(o.spec.dataSource) && o.spec.dataSource.contains({volume}))",
         same_spec),
        ('csi-source', '', 'v1', ['persistentvolumeclaims'],
         # Prevent new PVCs cloning this source through core Volume dataSourceRef.
         f"(o.metadata.namespace == {namespace} && has(o.spec.dataSource) && o.spec.dataSource.kind == 'PersistentVolumeClaim' && o.spec.dataSource.name == {pvc}) || (has(o.spec.dataSourceRef) && o.spec.dataSourceRef.kind == 'PersistentVolumeClaim' && o.spec.dataSourceRef.name == {pvc} && ((has(o.spec.dataSourceRef.namespace) && o.spec.dataSourceRef.namespace == {namespace}) || (!has(o.spec.dataSourceRef.namespace) && o.metadata.namespace == {namespace})))",
         same_spec),
        ('csi', 'snapshot.storage.k8s.io', 'v1', ['volumesnapshots'],
         f"o.metadata.namespace == {namespace} && has(o.spec.source.persistentVolumeClaimName) && o.spec.source.persistentVolumeClaimName == {pvc}", same_spec),
        ('csi-content', 'snapshot.storage.k8s.io', 'v1', ['volumesnapshotcontents'],
         f"(has(o.spec.source.volumeHandle) && o.spec.source.volumeHandle == {volume}) || (has(o.spec.source.snapshotHandle) && o.spec.source.snapshotHandle.contains({volume}))", same_spec),
    ]
    objects = []
    for suffix, group, version, resources, reference, permitted in rules:
        name = operation + '-' + suffix
        involved = f"[object, oldObject].exists(o, o != null && ({reference}))"
        policy = dict(apiVersion='admissionregistration.k8s.io/v1', kind='ValidatingAdmissionPolicy',
                      metadata=dict(name=name, labels={OPERATION: operation}),
                      spec=dict(failurePolicy='Fail', matchConstraints=dict(
                          matchPolicy='Equivalent', namespaceSelector={}, objectSelector={}, resourceRules=[dict(
                          apiGroups=[group], apiVersions=[version], operations=['CREATE', 'UPDATE', 'DELETE'], resources=resources, scope='*')]),
                          validations=[dict(expression=f'!({involved}) || ({permitted})',
                                            message='snapshot-maintenance-fence/' + operation)]))
        binding = dict(apiVersion='admissionregistration.k8s.io/v1', kind='ValidatingAdmissionPolicyBinding',
                       metadata=dict(name=name, labels={OPERATION: operation}),
                       spec=dict(policyName=name, validationActions=['Deny']))
        objects.extend([policy, binding])
    return objects


def stable_chain(review):
    # The live head grows with normal PostgreSQL writes. Immutable ancestry may
    # not change between the review and DELETE; sizes/checksums aren't authority.
    return {n: {k: v.get(k) for k in ('parent', 'children', 'removed', 'usercreated')}
            for n, v in review['snapshot_chain'].items()}


def admission_probes(config, operation):
    name = operation + '-probe'
    def obj(group, kind, namespace, spec, **metadata):
        return dict(apiVersion=group, kind=kind, metadata=dict(name=name, **({'namespace': namespace} if namespace else {}), **metadata), spec=spec)
    lh, ns = config['longhornNamespace'], config['namespace']
    return [
        obj('longhorn.io/v1beta2', 'Snapshot', lh, dict(volume=config['volume'], createSnapshot=True)),
        obj('longhorn.io/v1beta2', 'Backup', lh, dict(snapshotName=config['snapshot']), labels={'backup-volume': config['volume']}),
        obj('longhorn.io/v1beta2', 'Volume', lh, dict(size='1073741824', numberOfReplicas=1, dataEngine='v1', frontend='blockdev',
                                                   dataSource='snapshot://' + config['volume'] + '/' + config['snapshot'])),
        obj('v1', 'PersistentVolumeClaim', ns, dict(accessModes=['ReadWriteOnce'], resources=dict(requests=dict(storage='1Gi')),
                                                  dataSource=dict(apiGroup='', kind='PersistentVolumeClaim', name=config['primary']))),
        obj('snapshot.storage.k8s.io/v1', 'VolumeSnapshot', ns, dict(source=dict(persistentVolumeClaimName=config['primary']))),
        obj('snapshot.storage.k8s.io/v1', 'VolumeSnapshotContent', None, dict(deletionPolicy='Retain', driver='driver.longhorn.io',
             volumeSnapshotRef=dict(name=name, namespace=ns), source=dict(volumeHandle=config['volume']))),
    ]


class Recovery:
    def __init__(self, kube, config, approval, evidence_path, *, cleanup_policy_sha256=None, cleanup_policy=None):
        validate(config)
        require(set(approval) == {'schema', 'declarationSHA256', 'snapshotUID', 'authorized', 'maxWaitSeconds', 'readyURL'}, 'invalid recovery approval')
        require(approval['schema'] == 'fugue.local-snapshot-recovery/v1' and approval['authorized'] is True,
                'single snapshot deletion is not authorized')
        self.digest = hashlib.sha256(canonical(config).encode()).hexdigest()
        require(approval['declarationSHA256'] == self.digest and approval['snapshotUID'] == config['snapshotUID'], 'approval targets another snapshot')
        require(type(approval['maxWaitSeconds']) is int and 60 <= approval['maxWaitSeconds'] <= 1200, 'invalid completion wait budget')
        url = urllib.parse.urlparse(approval['readyURL'])
        require(url.scheme == 'https' and url.hostname and not url.username and not url.password and not url.fragment and not url.query, 'an explicit HTTPS health URL is required')
        self.kube, self.config, self.approval = kube, config, approval
        require(cleanup_policy_sha256 is None or re.fullmatch('[a-f0-9]{64}', cleanup_policy_sha256), 'invalid cleanup policy identity')
        self.cleanup_policy_sha256 = cleanup_policy_sha256
        require(cleanup_policy is None or hashlib.sha256(canonical(cleanup_policy).encode()).hexdigest() == cleanup_policy_sha256,
                'cleanup policy bytes differ from authorization')
        self.cleanup_policy = cleanup_policy
        self.operation = 'fugue-snapshot-recovery-' + self.digest[:20]
        self.holder = 'release/' + self.operation
        self.path = Path(evidence_path)
        self.record = None
        self.state = dict(schema=approval['schema'], operation=self.operation, declaration_sha256=self.digest,
                          authorized=True, deletion_attempted=False, phase='not-started', observed_at=timestamp())

    def save(self, phase, **fields):
        self.state.update(phase=phase, observed_at=timestamp(), **fields)
        self.path.write_text(json.dumps(self.state, indent=2) + '\n')
        if self.record:
            self.record['data']['receipt.json'] = canonical(self.state)
            self.record = self.kube.replace(self.record)

    def lease(self):
        return self.kube.get('leases.coordination.k8s.io', self.config['coordinationLease'], self.config.get('coordinationNamespace', self.config['namespace']))

    def own_lease(self):
        lease = self.lease()
        require(lease['metadata']['uid'] == self.record['data']['lease_uid'] and
                lease['spec'].get('holderIdentity') == self.holder and
                lease['metadata'].get('annotations', {}).get(TOKEN) == self.record['data']['token'], 'coordination fence ownership lost')
        lease['spec']['renewTime'] = timestamp()
        return self.kube.replace(lease)

    def release(self):
        lease = self.own_lease()
        lease['spec'].update(holderIdentity='', renewTime=timestamp())
        for key in (TOKEN, OPERATION, 'fugue.pro/recovery-required'):
            lease['metadata']['annotations'].pop(key, None)
        self.kube.replace(lease)

    def remove_barriers(self):
        for obj in reversed(json.loads(self.record['data']['barriers'])):
            resource = 'validatingadmissionpolicies' if obj['kind'] == 'ValidatingAdmissionPolicy' else 'validatingadmissionpolicybindings'
            current = self.kube.maybe(resource, obj['name'])
            if current is None:
                continue
            require(current['metadata']['uid'] == obj['uid'] and current['metadata'].get('labels', {}).get(OPERATION) == self.operation, 'admission barrier identity changed')
            self.kube.delete('/apis/admissionregistration.k8s.io/v1/' + resource + '/' + obj['name'], current)

    def install_barriers(self, review):
        self.check_snapshot_schema()
        self.record['data']['barriers'] = '[]'
        for obj in barrier_objects(self.config, review, self.operation):
            self.own_lease()
            created = self.kube.create(obj)
            identities = json.loads(self.record['data']['barriers'])
            identities.append(dict(kind=obj['kind'], name=created['metadata']['name'], uid=created['metadata']['uid']))
            self.record['data']['barriers'] = canonical(identities)
            self.save('installing-barriers')
        self.check_barriers()
        self.check_snapshot_updates(review)

    def denied_probe(self, args, body):
        deadline = time.monotonic() + 30
        while True:
            self.own_lease()
            result = subprocess.run(self.kube.kubectl + args,
                                    input=canonical(body), text=True, capture_output=True, timeout=20)
            if result.returncode and ('snapshot-maintenance-fence/' + self.operation) in result.stderr:
                return
            require(time.monotonic() < deadline, 'admission fence was not observed; no deletion permitted')
            time.sleep(1)

    def check_barriers(self):
        # Admission is cached. A bounded, server-side dry-run MUST observe the
        # denial marker before any destructive request is eligible.
        for probe in admission_probes(self.config, self.operation):
            self.denied_probe(['create', '--dry-run=server', '-f', '-', '-o', 'json'], probe)
        for obj in json.loads(self.record['data']['barriers']):
            if obj['kind'] != 'ValidatingAdmissionPolicy':
                continue
            deadline = time.monotonic() + 60
            while True:
                self.own_lease()
                live = self.kube.get('validatingadmissionpolicies', obj['name'])
                require(live['metadata']['uid'] == obj['uid'] and not live['metadata'].get('deletionTimestamp'),
                        'admission policy identity changed')
                status = live.get('status', {})
                if status.get('observedGeneration') == live['metadata']['generation']:
                    # The status controller acknowledges the checked generation.
                    # After clearing old warnings SSA can omit typeChecking;
                    # requiring that optional empty object rejects a valid rule.
                    require(not (status.get('typeChecking') or {}).get('expressionWarnings'),
                            'admission policy has expression type warnings')
                    break
                require(time.monotonic() < deadline, 'admission policy type check incomplete')
                time.sleep(1)

    def check_snapshot_schema(self):
        crd = self.kube.get('customresourcedefinitions', 'snapshots.longhorn.io')
        versions = [v for v in crd['spec']['versions'] if v['name'] == 'v1beta2' and v.get('served')]
        require(len(versions) == 1, 'Snapshot API schema unavailable')
        spec = versions[0]['schema']['openAPIV3Schema']['properties']['spec']
        properties = spec.get('properties', {})
        require(spec.get('type') == 'object' and set(properties) == {'volume', 'createSnapshot', 'labels'} and
                not spec.get('additionalProperties') and not spec.get('x-kubernetes-preserve-unknown-fields') and
                properties['volume'].get('type') == 'string' and
                properties['createSnapshot'].get('type') == 'boolean' and
                properties['labels'].get('type') == 'object' and properties['labels'].get('nullable') is True and
                properties['labels'].get('additionalProperties') == {'type': 'string'}, 'unsupported SnapshotSpec schema')

    def resume_native_finalizer(self, review):
        """Repair only the known nullable-field fence, never DELETE/finalizers.

        The old native layer must already be gone and purge idle. Every durable
        guard UID and its complete old/new spec must still match before CAS.
        An annotation merely requeues Longhorn's own finalizer reconciliation;
        it neither removes a finalizer nor invokes an engine operation.
        """
        self.own_lease()
        snapshot, _, engine_quiescent = self.native_state(review)
        require(snapshot and snapshot['metadata'].get('deletionTimestamp') and engine_quiescent,
                'native deletion is not terminal enough for finalizer compatibility recovery')
        self.health()
        self.check_snapshot_schema()
        expected = barrier_objects(self.config, review, self.operation)
        legacy = barrier_objects(self.config, review, self.operation, legacy_snapshot_spec=True)
        map_legacy = barrier_objects(self.config, review, self.operation, map_snapshot_spec=True)
        presence_legacy = barrier_objects(self.config, review, self.operation, typed_presence_snapshot_spec=True)
        identities = json.loads(self.record['data']['barriers'])
        require(len(identities) == len(expected) and
                {(x['kind'], x['name']) for x in identities} == {(x['kind'], x['metadata']['name']) for x in expected},
                'durable admission barrier set changed')
        repair = None
        for wanted, old, previous, presence in zip(expected, legacy, map_legacy, presence_legacy):
            self.own_lease()
            kind, name = wanted['kind'], wanted['metadata']['name']
            resource = 'validatingadmissionpolicies' if kind == 'ValidatingAdmissionPolicy' else 'validatingadmissionpolicybindings'
            live = self.kube.get(resource, name)
            identity = next(x for x in identities if x['kind'] == kind and x['name'] == name)
            require(live['metadata']['uid'] == identity['uid'] and
                    live['metadata'].get('labels', {}).get(OPERATION) == self.operation and
                    live['spec'] in (wanted['spec'], old['spec'], previous['spec'], presence['spec']), 'admission barrier identity or spec changed')
            if live['spec'] != wanted['spec']:
                require(kind == 'ValidatingAdmissionPolicy' and name.endswith('-snapshots'), 'unexpected barrier repair')
                repair = (live, wanted['spec'])
        self.save('repairing-native-finalizer-compatibility', barrier_revision='snapshot-null-dynamic-labels-v3')
        if repair:
            live, spec = repair
            self.own_lease()
            live['spec'] = spec
            self.kube.replace(live)
        self.check_barriers()
        self.check_snapshot_updates(review)
        self.own_lease()
        snapshot, _, engine_quiescent = self.native_state(review)
        if snapshot is None:
            return
        require(snapshot['metadata'].get('deletionTimestamp') and engine_quiescent, 'native state changed before reconciliation')
        annotations = snapshot['metadata'].setdefault('annotations', {})
        key = 'fugue.pro/snapshot-recovery-reconcile'
        require(annotations.get(key) in (None, self.operation), 'another recovery requested reconciliation')
        if annotations.get(key) != self.operation:
            annotations[key] = self.operation
            self.save('native-reconcile-requested')
            self.kube.replace(snapshot)

    def check_snapshot_updates(self, review):
        # Test a real, retained immutable child's typed normalization without
        # persisting any change or touching its finalizers. Deny probes above
        # independently prove that new reference writers remain fenced.
        child = next(n for n, present in review['snapshot_chain'][self.config['snapshot']]['children'].items() if present)
        probe = self.kube.get('snapshots.longhorn.io', child, self.config['longhornNamespace'])
        require(probe['spec']['volume'] == self.config['volume'] and probe['spec'].get('labels') is None,
                'a nullable SnapshotSpec normalization probe is unavailable')
        probe['spec']['labels'] = None
        self.kube.command(['replace', '--dry-run=server', '-f', '-', '-o', 'json'], canonical(probe))
        for key, value in [('createSnapshot', not probe['spec'].get('createSnapshot', False)),
                           ('labels', {'maintenance-probe': 'changed'})]:
            changed = json.loads(canonical(probe))
            changed['spec'][key] = value
            self.denied_probe(['replace', '--dry-run=server', '-f', '-', '-o', 'json'], changed)

    def health(self):
        c = self.config
        cluster = self.kube.get('clusters.postgresql.cnpg.io', c['cluster'], c['namespace'])
        same(cluster, c['cluster'], c['clusterUID'])
        require(cluster['status'].get('phase') == 'Cluster in healthy state' and cluster['status'].get('readyInstances') == 1 and
                cluster['status'].get('currentPrimary') == cluster['status'].get('targetPrimary') == c['primary'], 'database health changed')
        pod = self.kube.get('pod', c['primary'], c['namespace'])
        same(pod, c['primary'], c['primaryUID'])
        require(any(x.get('type') == 'Ready' and x.get('status') == 'True' for x in pod['status'].get('conditions', [])), 'database primary not Ready')
        raw = self.kube.command(['-n', c['namespace'], 'exec', c['primary'], '-c', 'postgres', '--', 'df', '-Pk', '/var/lib/postgresql/data'])
        free = int(raw.splitlines()[-1].split()[3]) * 1024
        require(free >= 512 << 20, 'database space is below the hard stop')
        probes = {}
        if 'recoveryBudget' in c:
            started = time.monotonic()
            answer = self.kube.command(['-n', c['namespace'], 'exec', c['primary'], '-c', 'postgres', '--',
                                        'psql', '-U', 'postgres', '-d', 'postgres', '-qAt', '-v', 'ON_ERROR_STOP=1',
                                        '-c', 'SELECT 1'])
            probes['database_probe_seconds'] = time.monotonic() - started
            require(answer.strip() == '1' and probes['database_probe_seconds'] <= c['recoveryBudget']['maxDatabaseProbeSeconds'],
                    'database latency gate failed')
        started = time.monotonic()
        with urllib.request.urlopen(self.approval['readyURL'], timeout=15) as response:
            ready = json.load(response)
        if 'recoveryBudget' in c:
            probes['api_probe_seconds'] = time.monotonic() - started
            require(probes['api_probe_seconds'] <= c['recoveryBudget']['maxAPIProbeSeconds'], 'public API latency gate failed')
        require(ready.get('status') == 'ok' and all(x.get('status') == 'ok' for x in ready.get('checks', {}).values()) and ready.get('checks'), 'public API readiness failed')
        return dict(database_available_bytes=free,
                    database_restarts=sum(x.get('restartCount', 0) for x in pod['status'].get('containerStatuses', [])),
                    public_ready=True, **probes)

    def physical(self, review):
        manager = self.kube.get('pod', review['instance_manager_name'], self.config['longhornNamespace'])
        same(manager, review['instance_manager_name'], review['instance_manager_uid'])
        # Only statvfs; no snapshot file or data is opened, and no sync/trim.
        source = 'import json,os,sys; p=sys.stdin.read(); s=os.statvfs(p); print(json.dumps(dict(free_bytes=s.f_bfree*s.f_frsize,available_bytes=s.f_bavail*s.f_frsize,total_bytes=s.f_blocks*s.f_frsize,io_pressure=open("/proc/pressure/io").read() if os.path.exists("/proc/pressure/io") else None)))'
        return json.loads(self.kube.command(['-n', self.config['longhornNamespace'], 'exec', '-i', review['instance_manager_name'],
                                            '-c', 'instance-manager', '--', 'python3', '-c', source], review['replica_directory']))

    def native_state(self, review):
        c = self.config
        volume = self.kube.get('volumes.longhorn.io', c['volume'], c['longhornNamespace'])
        same(volume, c['volume'], c['volumeUID'])
        require(volume['status'].get('robustness') == 'healthy' and volume['status'].get('state') == 'attached', 'volume health changed during native deletion')
        engine = self.kube.get('engines.longhorn.io', review['engine_name'], c['longhornNamespace'])
        same(engine, review['engine_name'], review['engine_uid'])
        es = engine['status']
        require(es.get('currentState') == 'running' and list(es.get('replicaModeMap', {}).values()) == ['RW'], 'native engine health changed')
        require(es.get('currentImage', '').endswith(':v1.12.1') and es.get('currentImage') == engine['spec'].get('image'), 'native engine implementation changed')
        replica = self.kube.get('replicas.longhorn.io', review['replica_name'], c['longhornNamespace'])
        same(replica, review['replica_name'], review['replica_uid'])
        require(replica['spec'].get('active') is True and not replica['spec'].get('failedAt') and
                replica['status'].get('currentState') == 'running', 'native replica health changed')
        if 'recoveryBudget' in c:
            engine_implementation(self.kube, c['longhornNamespace'], es['currentImage'], replica['spec']['nodeID'],
                                  expected=review.get('engine_implementation'))
        chain, purge = es.get('snapshots', {}), es.get('purgeStatus', {})
        require(purge and all(not x.get('error') for x in purge.values()), 'native purge reported an error or unknown state')
        self.native_progress = sorted([dict(progress=x.get('progress'), is_purging=x.get('isPurging'), state=x.get('state'))
                                       for x in purge.values()], key=canonical)
        kept = set(review['snapshot_chain']) - {c['snapshot']} - set(review['collateral_head_parent_prunes'])
        require(kept <= set(chain), 'another recovery point disappeared')
        snapshot = self.kube.maybe('snapshots.longhorn.io', c['snapshot'], c['longhornNamespace'])
        if snapshot:
            require(snapshot['metadata']['uid'] == c['snapshotUID'], 'snapshot name was reused')
        engine_quiescent = c['snapshot'] not in chain and all(not x.get('isPurging') for x in purge.values())
        return snapshot, not snapshot and engine_quiescent, engine_quiescent

    def wait_native(self, review):
        deadline = time.monotonic() + self.approval['maxWaitSeconds']
        stable = 0
        while True:
            self.own_lease()
            _, completed, _ = self.native_state(review)
            stable = stable + 1 if completed else 0
            fields = dict(native_quiescent=completed)
            if 'recoveryBudget' in self.config:
                progress = getattr(self, 'native_progress', [])
                changed = progress != self.state.get('native_progress')
                last = timestamp() if changed else self.state.get('last_native_progress_at', timestamp())
                stalled = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(last.replace('Z', '+00:00'))).total_seconds() >= 600
                fields.update(native_progress=progress, last_native_progress_at=last, progress_stalled=stalled,
                              next_observation_after=timestamp())
                physical, health = self.physical(review), self.health()
                floor = review['budget']['completion_floor_bytes']
                require(physical['available_bytes'] >= floor, 'native operation physical reserve failed; protection retained')
                fields.update(physical_current=physical, health_current=health)
            self.save('awaiting-native-quiescence', **fields)
            if stable >= 2:
                return self.health()
            if 'recoveryBudget' in self.config and time.monotonic() >= deadline:
                # An observation window ending does NOT cancel the engine or
                # authorize a second DELETE/unlock. Persist and resume next pass.
                self.save('native-running', **fields)
                return None
            require(time.monotonic() < deadline, 'native operation has not quiesced; protection retained')
            time.sleep(5)

    def recover_known_predelete(self):
        # Only the standing reconciler may retry a known pre-delete crash.
        # Durable false intent proves DELETE was never issued. Every fence
        # must match the journal; an unjournaled/ambiguous create still stops.
        require(self.cleanup_policy_sha256 is not None and self.state.get('deletion_attempted') is False,
                'previous pre-delete attempt needs explicit recovery review')
        expected = json.loads(self.record['data']['barriers'])
        actual = []
        for resource, kind in [('validatingadmissionpolicies', 'ValidatingAdmissionPolicy'),
                               ('validatingadmissionpolicybindings', 'ValidatingAdmissionPolicyBinding')]:
            for obj in self.kube.items('/apis/admissionregistration.k8s.io/v1/' + resource, labelSelector=OPERATION + '=' + self.operation):
                actual.append(dict(kind=kind, name=obj['metadata']['name'], uid=obj['metadata']['uid']))
        require(sorted(actual, key=canonical) == sorted(expected, key=canonical), 'pre-delete fence inventory differs; protection retained')
        lease = self.lease()
        require(lease['metadata']['uid'] == self.record['data']['lease_uid'], 'coordination Lease identity changed')
        if lease['spec'].get('holderIdentity'):
            self.own_lease()
            self.remove_barriers()
            self.release()
        else:
            require(not actual, 'unowned pre-delete admission fences require review')
        self.save('aborted-before-delete')

    def run(self):
        c = self.config
        existing = self.kube.maybe('configmaps', self.operation, c['namespace'])
        retry_aborted = False
        if existing:
            self.record = existing
            require(existing['data']['declaration_sha256'] == self.digest and existing['metadata'].get('labels', {}).get(OPERATION) == self.operation,
                    'operation record identity differs')
            self.state = json.loads(existing['data']['receipt.json'])
            if self.state['phase'] == 'completed':
                self.path.write_text(json.dumps(self.state, indent=2) + '\n')
                return
            if self.state['phase'] == 'native-completed' and self.state.get('native_quiescent') is True and not self.lease()['spec'].get('holderIdentity'):
                require(self.kube.maybe('snapshots.longhorn.io', c['snapshot'], c['longhornNamespace']) is None,
                        'deleted snapshot name is present again')
                for obj in json.loads(existing['data']['barriers']):
                    resource = 'validatingadmissionpolicies' if obj['kind'] == 'ValidatingAdmissionPolicy' else 'validatingadmissionpolicybindings'
                    require(self.kube.maybe(resource, obj['name']) is None, 'native-completed operation still has an admission fence')
                # The last receipt commit can be lost after successful unlock.
                # Record that already-proven completion; never reacquire/delete.
                self.save('completed', after=self.health())
                return
            if self.state.get('deletion_attempted') is False and self.state['phase'] != 'aborted-before-delete':
                self.recover_known_predelete()
            retry_aborted = self.state['phase'] == 'aborted-before-delete' and self.state.get('deletion_attempted') is False
            if retry_aborted:
                require(not self.lease()['spec'].get('holderIdentity'), 'coordination Lease remains occupied')
                for obj in json.loads(existing['data']['barriers']):
                    resource = 'validatingadmissionpolicies' if obj['kind'] == 'ValidatingAdmissionPolicy' else 'validatingadmissionpolicybindings'
                    require(self.kube.maybe(resource, obj['name']) is None, 'an aborted operation still has an admission fence')
            else:
                require(self.state.get('deletion_attempted') is True, 'previous pre-delete attempt needs explicit recovery review')
                self.own_lease()
                review = json.loads(existing['data']['review.json'])
                snapshot = self.kube.maybe('snapshots.longhorn.io', c['snapshot'], c['longhornNamespace'])
                if snapshot is not None:
                    require(snapshot['metadata']['uid'] == c['snapshotUID'] and snapshot['metadata'].get('deletionTimestamp'),
                            'delete outcome is not terminal; keep existing fences')
                    # Ordinary asynchronous purge may still be running. The
                    # finalizer compatibility repair is only for a gone layer.
                    _, _, quiescent = self.native_state(review)
                    if quiescent or 'recoveryBudget' not in c:
                        self.resume_native_finalizer(review)
        if not existing or retry_aborted:
            review = observe(self.kube, c)
            require(review['ready_for_review'], 'working-space gate failed; no deletion permitted')
            before = self.health()
            lease = self.lease()
            require(not lease['spec'].get('holderIdentity'), 'coordination Lease is occupied')
            token = secrets.token_hex(16)
            data = dict(declaration_sha256=self.digest, token=token, lease_uid=lease['metadata']['uid'],
                        **{'receipt.json': canonical(self.state), 'review.json': canonical(review), 'barriers': '[]'})
            labels = {OPERATION: self.operation}
            if self.cleanup_policy_sha256:
                data.update(cleanup_policy_sha256=self.cleanup_policy_sha256,
                            **{'declaration.json': canonical(c), 'approval.json': canonical(self.approval)})
                if self.cleanup_policy is not None:
                    data['cleanup_policy.json'] = canonical(self.cleanup_policy)
                labels['fugue.pro/snapshot-cleanup-policy'] = self.cleanup_policy_sha256[:40]
            if retry_aborted:
                self.record['data'] = data
                self.record = self.kube.replace(self.record)
            else:
                self.record = self.kube.create(dict(apiVersion='v1', kind='ConfigMap',
                    metadata=dict(name=self.operation, namespace=c['namespace'], labels=labels), data=data))
            lease['spec'].update(holderIdentity=self.holder, leaseDurationSeconds=120, acquireTime=timestamp(), renewTime=timestamp())
            lease['metadata'].setdefault('annotations', {}).update({TOKEN: token, OPERATION: self.operation, 'fugue.pro/recovery-required': 'true'})
            # An expired release/* holder is NOT recoverable by backup workers.
            self.kube.replace(lease)
            try:
                self.own_lease()
                self.install_barriers(review)
                fresh = observe(self.kube, c, lease_holder=self.holder)
                require(fresh['ready_for_review'] and stable_chain(fresh) == stable_chain(review), 'capacity or immutable ancestry changed')
                self.health()
                self.record['data']['review.json'] = canonical(fresh)
                self.save('delete-intent', deletion_attempted=True, native_started_at=timestamp(), before=before, budget=fresh['budget'])
                # This is the sole destructive call. Unknown outcome is durable;
                # no finally block releases protection after this point.
                target = self.kube.get('snapshots.longhorn.io', c['snapshot'], c['longhornNamespace'])
                same(target, c['snapshot'], c['snapshotUID'])
                require(target['metadata']['resourceVersion'] == fresh['snapshot_resource_version'], 'snapshot changed after review')
                self.own_lease()
                self.kube.delete('/apis/longhorn.io/v1beta2/namespaces/' + c['longhornNamespace'] + '/snapshots/' + c['snapshot'], target)
                review = fresh
            except Exception:
                if not self.state['deletion_attempted']:
                    self.remove_barriers()
                    self.release()
                    self.save('aborted-before-delete')
                raise
        after = self.wait_native(review)
        if after is None:
            return
        require(after['database_restarts'] == self.state['before']['database_restarts'], 'database restarted; retain protection for review')
        physical = self.physical(review)
        if 'recoveryBudget' in c:
            require(physical['available_bytes'] >= review['budget']['completion_floor_bytes'],
                    'post-operation physical reserve failed; keep protection')
        else:
            require(physical['free_bytes'] >= review['budget']['minimum_physical_free_bytes'] + c['minimumExtraFreeBytes'],
                    'post-operation physical reserve failed; keep protection')
        self.save('native-completed', native_quiescent=True, after=after, physical_after=physical,
                  physical_free_change_bytes=physical['free_bytes'] - review['budget']['physical_free_bytes'])
        self.remove_barriers()
        self.release()
        self.save('completed', native_quiescent=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('declaration')
    parser.add_argument('approval')
    parser.add_argument('--evidence', required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    if args.apply:
        require(os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REF') == 'refs/heads/main' and
                re.fullmatch(r'[0-9]+', os.environ.get('GITHUB_RUN_ID', '')) and not os.environ.get('FUGUE_REVIEW_SSH_HOST'), 'apply requires the standard main CI lane')
    recovery = Recovery(Cluster(), json.loads(Path(args.declaration).read_text()),
                        json.loads(Path(args.approval).read_text()), args.evidence)
    if not args.apply:
        existing = recovery.kube.maybe('configmaps', recovery.operation, recovery.config['namespace'])
        evidence = dict(read_only=True, operation=recovery.operation, observed_at=timestamp())
        if existing:
            require(existing['data']['declaration_sha256'] == recovery.digest, 'operation identity changed')
            receipt = json.loads(existing['data']['receipt.json'])
            evidence.update(phase=receipt['phase'], deletion_attempted=receipt['deletion_attempted'])
        else:
            evidence.update(observe(recovery.kube, recovery.config))
        recovery.path.write_text(json.dumps(evidence, indent=2) + '\n')
        print(canonical({k: evidence[k] for k in ('read_only', 'operation', 'observed_at')}))
        return
    try:
        recovery.run()
    except Exception:
        recovery.path.write_text(json.dumps({**recovery.state, 'error': 'maintenance incomplete; inspect durable fences before retry'}, indent=2) + '\n')
        raise
    print(canonical({k: recovery.state[k] for k in ('operation', 'phase', 'deletion_attempted', 'observed_at')}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        reason = str(error) if isinstance(error, ValueError) else 'bounded maintenance observation or request failed'
        print('snapshot recovery incomplete: ' + reason + '; no automatic replay or unlock')
        raise SystemExit(1) from None
