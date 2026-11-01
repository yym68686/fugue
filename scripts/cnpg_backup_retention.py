#!/usr/bin/env python3
"""Independent, fail-closed Barman count retention. Never deletes S3 keys itself."""
import argparse
import datetime as dt
import hashlib
import json
import os
import re
import ssl
import subprocess
import urllib.request
import uuid
from pathlib import Path
from urllib.parse import urlsplit

LABEL = 'fugue.pro/cnpg-count-retention'
ANNOTATION = 'fugue.pro/cnpg-count-policy'
DISABLED_ANNOTATION = 'fugue.pro/barman-disabled'
SCHEMA = 'fugue.cnpg-count-retention/v1'
NAME = re.compile(r'^[a-z0-9]([a-z0-9.-]*[a-z0-9])?$')


def require(value, message):
    if not value:
        raise ValueError(message)


def validate_policy(policy, cluster, policy_uid=None):
    required = {'schema', 'namespace', 'cluster', 'cluster_uid', 'system_id', 'destination', 'endpoint', 'server_name', 'enabled', 'retain_count'}
    require(required <= set(policy) <= required | {'barman_disabled'}, 'Invalid policy fields')
    require(policy['schema'] == SCHEMA, 'Unsupported policy schema')
    for key in ('namespace', 'cluster', 'server_name'):
        require(isinstance(policy[key], str) and NAME.fullmatch(policy[key]) and len(policy[key]) <= 253, 'Invalid resource identity')
    require(type(policy['enabled']) is bool and type(policy['retain_count']) is int and 1 <= policy['retain_count'] <= 100, 'Invalid count policy')
    require(type(policy.get('barman_disabled', False)) is bool, 'Invalid Barman disable intent')
    require(not policy.get('barman_disabled') or not policy['enabled'], 'Barman disable intent cannot enable retention')
    require(isinstance(policy['cluster_uid'], str) and policy['cluster_uid'] and re.fullmatch(r'[0-9]+', policy['system_id']), 'Invalid database identity')
    endpoint = urlsplit(policy['endpoint'])
    require(endpoint.scheme == 'https' and endpoint.hostname and not endpoint.username and not endpoint.password and not endpoint.query and not endpoint.fragment, 'Invalid backup endpoint')
    destination = urlsplit(policy['destination'])
    require(destination.scheme == 's3' and destination.netloc and not destination.username and not destination.query and not destination.fragment, 'Invalid backup destination')
    meta = cluster['metadata']
    require(meta['namespace'] == policy['namespace'] and meta['name'] == policy['cluster'] and meta['uid'] == policy['cluster_uid'], 'Cluster identity changed')
    require(str(cluster.get('status', {}).get('systemID', '')) == policy['system_id'], 'Database system ID changed')
    store = cluster['spec'].get('backup', {}).get('barmanObjectStore')
    if not store:
        require(policy.get('barman_disabled') and policy_uid and meta.get('annotations', {}).get(DISABLED_ANNOTATION) == policy_uid, 'Backup repository missing without owned Barman disable intent')
        return
    require(store['destinationPath'] == policy['destination'] and policy['destination'].startswith('s3://'), 'Backup destination changed')
    require(store.get('endpointURL') == policy['endpoint'], 'Backup endpoint changed')
    require(store.get('serverName', meta['name']) == policy['server_name'], 'Barman server identity changed')


def catalogue_plan(catalogue, count, system_id):
    # This is the public barman-cloud-backup-list JSON shape, including 3.18.
    require(isinstance(catalogue, dict) and isinstance(catalogue.get('backups_list'), list), 'Invalid Barman catalogue')
    backups = catalogue['backups_list']
    require(len(backups) <= 10000, 'Backup catalogue exceeds safety bound')
    ids = []
    for backup in backups:
        require(isinstance(backup, dict) and re.fullmatch(r'[0-9]{8}T[0-9]{6}', backup.get('backup_id', '')), 'Invalid Barman backup identity')
        require(backup.get('status') == 'DONE', 'Incomplete or failed Barman catalogue entry; cleanup blocked')
        require(str(backup.get('systemid', '')) == system_id, 'Archive contains a different database system ID')
        require(not backup.get('parent_backup_id'), 'Incremental backup chains require a separate retention policy')
        ids.append(backup['backup_id'])
    require(len(ids) == len(set(ids)), 'Duplicate Barman backup IDs')
    ids.sort(reverse=True)
    require(len(ids) >= count, 'Fewer successful backups than the requested minimum')
    # Barman additionally honours archival/keep annotations. Do not bypass them.
    return {'keep': ids[:count], 'older_candidates': ids[count:]}


def cluster_path(namespace, cluster):
    return f'/apis/postgresql.cnpg.io/v1/namespaces/{namespace}/clusters/{cluster}'


def config_path(namespace, name):
    return f'/api/v1/namespaces/{namespace}/configmaps/{name}'


class Kube:
    def __init__(self):
        root = Path('/var/run/secrets/kubernetes.io/serviceaccount')
        self.token = (root / 'token').read_text().strip()
        self.context = ssl.create_default_context(cafile=str(root / 'ca.crt'))
        self.base = 'https://kubernetes.default.svc'

    def request(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.base + path, data=data, method=method,
                                         headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, context=self.context, timeout=15) as response:
            raw = response.read(8 * 1024 * 1024 + 1)
            require(len(raw) <= 8 * 1024 * 1024, 'Kubernetes response exceeds safety bound')
            return json.loads(raw)


def acquire(kube, namespace, name, holder):
    path = f'/apis/coordination.k8s.io/v1/namespaces/{namespace}/leases/{name}'
    lease = kube.request('GET', path)
    spec = lease.get('spec', {})
    now = dt.datetime.now(dt.timezone.utc)
    renewed = spec.get('renewTime')
    if spec.get('holderIdentity'):
        require(renewed and type(spec.get('leaseDurationSeconds')) is int and spec['leaseDurationSeconds'] > 0, 'Invalid existing retention lock')
        expires = dt.datetime.fromisoformat(renewed.replace('Z', '+00:00')) + dt.timedelta(seconds=spec.get('leaseDurationSeconds', 600))
        require(expires < now, 'Retention policy is locked by another operation')
    lease['spec'] = {'holderIdentity': holder, 'leaseDurationSeconds': 1800, 'renewTime': now.isoformat().replace('+00:00', 'Z')}
    return path, kube.request('PUT', path, lease)


def backup_command(binary, cluster, policy):
    store = cluster['spec']['backup']['barmanObjectStore']
    return [binary, '--cloud-provider', 'aws-s3', '--endpoint-url', store['endpointURL'], '--read-timeout', '30']


def barman(command):
    # Credentials are injected from existing Secret refs, never command arguments.
    result = subprocess.run(command, capture_output=True, text=True, timeout=180)
    require(result.returncode == 0, 'Barman operation failed; verify object-store access and worker compatibility')
    require(len(result.stdout) <= 8 * 1024 * 1024, 'Barman response exceeds safety bound')
    return result.stdout


def check_live(kube, policy, name, config_uid, resource_version):
    config = kube.request('GET', config_path(policy['namespace'], name))
    require(config['metadata']['uid'] == config_uid and config['metadata']['resourceVersion'] == resource_version, 'Policy changed during reconciliation')
    cluster = kube.request('GET', cluster_path(policy['namespace'], policy['cluster']))
    validate_policy(policy, cluster, config_uid)
    require(not policy.get('barman_disabled') and not cluster['metadata'].get('annotations', {}).get(DISABLED_ANNOTATION), 'Barman shutdown blocks retention')
    require(cluster['metadata'].get('annotations', {}).get(ANNOTATION) == config_uid, 'Count policy ownership has not been activated')
    require(not cluster['spec']['backup'].get('retentionPolicy'), 'Native recovery-window policy is still enabled')
    status = cluster.get('status', {})
    require(status.get('phase') == 'Cluster in healthy state' and status.get('readyInstances', 0) >= 1, 'Database is not healthy; no remote backups will be removed')
    require(status.get('currentPrimary') and status.get('currentPrimary') == status.get('targetPrimary'), 'Primary transition is in progress')
    require(any(c.get('type') == 'ContinuousArchiving' and c.get('status') == 'True' for c in status.get('conditions', [])), 'Continuous WAL archiving is not healthy')
    backups = kube.request('GET', f"/apis/postgresql.cnpg.io/v1/namespaces/{policy['namespace']}/backups?limit=500")
    require(not backups.get('metadata', {}).get('continue'), 'Backup inventory is truncated')
    require(not any(b.get('spec', {}).get('cluster', {}).get('name') == policy['cluster'] and b.get('status', {}).get('phase') not in ('completed', 'failed') for b in backups['items']), 'A database backup is pending or running')
    # A restoration using this archive must finish before retention can advance.
    clusters = kube.request('GET', '/apis/postgresql.cnpg.io/v1/clusters?limit=500')
    require(not clusters.get('metadata', {}).get('continue'), 'Restore inventory is truncated')
    for other in clusters['items']:
        if other['metadata']['uid'] == policy['cluster_uid']:
            continue
        for external in other.get('spec', {}).get('externalClusters', []):
            store = external.get('barmanObjectStore', {})
            if store.get('destinationPath') == policy['destination'] and store.get('serverName', external.get('name')) == policy['server_name']:
                require(other.get('status', {}).get('phase') == 'Cluster in healthy state', 'An archive-dependent recovery is in progress')
    return cluster


def run_retention(kube, namespace, name, dry_run=False):
    config = kube.request('GET', config_path(namespace, name))
    policy = json.loads(config['data']['policy.json'])
    require(config['metadata']['namespace'] == namespace and config['metadata']['name'] == name and policy['namespace'] == namespace, 'Policy identity changed')
    cluster = kube.request('GET', cluster_path(namespace, policy['cluster']))
    validate_policy(policy, cluster, config['metadata']['uid'])
    if not policy['enabled']:
        return {'state': 'disabled', 'deleted': False}
    path, lock = acquire(kube, namespace, name, 'retention/' + str(uuid.uuid4()))
    evidence = {'state': 'blocked', 'deleted': False, 'deletion_attempted': False, 'dry_run': dry_run, 'observed_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'policy_resource_version': config['metadata']['resourceVersion']}
    try:
        rv, uid = config['metadata']['resourceVersion'], config['metadata']['uid']
        cluster = check_live(kube, policy, name, uid, rv)
        command = backup_command('barman-cloud-backup-list', cluster, policy) + ['--format', 'json', policy['destination'], policy['server_name']]
        plan = catalogue_plan(json.loads(barman(command)), policy['retain_count'], policy['system_id'])
        evidence.update(plan)
        check_live(kube, policy, name, uid, rv)
        delete = backup_command('barman-cloud-backup-delete', cluster, policy) + ['--retention-policy', f"REDUNDANCY {policy['retain_count']}", '--minimum-redundancy', str(policy['retain_count'])]
        if dry_run:
            delete.append('--dry-run')
        delete += [policy['destination'], policy['server_name']]
        evidence['deletion_attempted'] = not dry_run
        barman(delete)
        after = catalogue_plan(json.loads(barman(command)), policy['retain_count'], policy['system_id'])
        require(set(plan['keep']).issubset(set(after['keep']) | set(after['older_candidates'])), 'Retained backup verification failed')
        before_ids, after_ids = set(plan['keep'] + plan['older_candidates']), set(after['keep'] + after['older_candidates'])
        require(not dry_run or before_ids == after_ids, 'Catalogue changed during dry-run; retry preview')
        removed = sorted(before_ids - after_ids)
        evidence.update(state='planned' if dry_run else 'reconciled', deleted=bool(removed), removed=removed, remaining=sorted(after_ids, reverse=True), remaining_above_count=after['older_candidates'], count_satisfied=len(after_ids) == policy['retain_count'])
    except Exception as error:
        # Never put subprocess stderr, credentials or arbitrary remote data in a receipt.
        evidence['reason'] = str(error) if isinstance(error, ValueError) else 'Operator or Barman request failed; no complete cleanup receipt'
        raise
    finally:
        try:
            receipt_path = config_path(namespace, name + '-receipt')
            receipt = kube.request('GET', receipt_path)
            receipt['data'] = {'receipt.json': json.dumps(evidence)}
            kube.request('PUT', receipt_path, receipt)
        finally:
            lock['spec']['holderIdentity'] = ''
            kube.request('PUT', path, lock)
            print(json.dumps(evidence))
    return evidence


def worker():
    return run_retention(Kube(), os.environ['POLICY_NAMESPACE'], os.environ['POLICY_NAME'], os.environ.get('RETENTION_DRY_RUN') == 'true')


def install(config, kube):
    namespace, name = config['namespace'], config['name']
    require(NAME.fullmatch(namespace) and len(namespace) <= 63 and NAME.fullmatch(name) and len(name) <= 40, 'Invalid installation names')
    cluster = kube('get', 'clusters.postgresql.cnpg.io', config['cluster'], '-n', namespace, '-o', 'json')
    require(cluster['metadata']['uid'] == config['clusterUID'] and str(cluster['status']['systemID']) == config['systemID'], 'Installation target identity mismatch')
    image = config['image']
    require(re.fullmatch(r'[^\s]+@sha256:[a-f0-9]{64}', image), 'Worker image must be digest pinned')
    existing_policy = kube('get', 'ConfigMap', name, '-n', namespace, '--ignore-not-found', '-o', 'json')
    if existing_policy:
        saved = json.loads(existing_policy['data']['policy.json'])
        validate_policy(saved, cluster, existing_policy['metadata']['uid'])
        if saved.get('barman_disabled'):
            require(cluster['metadata'].get('annotations', {}).get(DISABLED_ANNOTATION) == existing_policy['metadata']['uid'], 'Barman shutdown ownership is not activated; retry CLI disable')
            cron = kube('get', 'CronJob', name, '-n', namespace, '--ignore-not-found', '-o', 'json')
            if cron and not cron['spec'].get('suspend'):
                patch = [{'op': 'test', 'path': '/metadata/uid', 'value': cron['metadata']['uid']},
                         {'op': 'test', 'path': '/metadata/resourceVersion', 'value': cron['metadata']['resourceVersion']},
                         {'op': 'add', 'path': '/spec/suspend', 'value': True}]
                kube('patch', 'CronJob', name, '-n', namespace, '--type=json', '-p', json.dumps(patch))
            print(json.dumps({'installed': False, 'barman_disabled': True, 'intent_preserved': True}))
            return
    require(not cluster['metadata'].get('annotations', {}).get(DISABLED_ANNOTATION), 'Barman shutdown intent must not be overwritten by an installation')
    store = cluster['spec']['backup']['barmanObjectStore']
    policy = {'schema': SCHEMA, 'namespace': namespace, 'cluster': config['cluster'], 'cluster_uid': config['clusterUID'], 'system_id': config['systemID'], 'destination': store['destinationPath'], 'endpoint': store['endpointURL'], 'server_name': store.get('serverName', config['cluster']), 'enabled': False, 'retain_count': 3}
    validate_policy(policy, cluster)
    meta = {'name': name, 'namespace': namespace, 'labels': {LABEL: name}}
    # Intent is operator-owned after initial creation. A code release never resets it.
    for resource in [dict(apiVersion='v1', kind='ConfigMap', metadata=meta, data={'policy.json': json.dumps(policy)}),
                     dict(apiVersion='v1', kind='ConfigMap', metadata={**meta, 'name': name + '-receipt'}, data={'receipt.json': '{}'}),
                     dict(apiVersion='coordination.k8s.io/v1', kind='Lease', metadata=meta, spec={})]:
        existing = kube('get', resource['kind'], resource['metadata']['name'], '-n', namespace, '--ignore-not-found', '-o', 'json')
        if not existing:
            kube('create', '-f', '-', body=resource)
        elif resource['metadata']['name'] == name and resource['kind'] == 'ConfigMap':
            validate_policy(json.loads(existing['data']['policy.json']), cluster, existing['metadata']['uid'])
    code = Path(__file__).read_text()
    code_name = name + '-' + hashlib.sha256(code.encode()).hexdigest()[:12]
    resources = [dict(apiVersion='v1', kind='ConfigMap', metadata={**meta, 'name': code_name}, immutable=True, data={'worker.py': code}),
                 dict(apiVersion='v1', kind='ServiceAccount', metadata=meta),
                 dict(apiVersion='rbac.authorization.k8s.io/v1', kind='Role', metadata=meta, rules=[
                     {'apiGroups': [''], 'resources': ['configmaps'], 'resourceNames': [name, name + '-receipt'], 'verbs': ['get', 'update']},
                     {'apiGroups': ['coordination.k8s.io'], 'resources': ['leases'], 'resourceNames': [name], 'verbs': ['get', 'update']},
                     {'apiGroups': ['postgresql.cnpg.io'], 'resources': ['backups'], 'verbs': ['list']}]),
                 dict(apiVersion='rbac.authorization.k8s.io/v1', kind='RoleBinding', metadata=meta, subjects=[{'kind': 'ServiceAccount', 'name': name, 'namespace': namespace}], roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': name}),
                 dict(apiVersion='rbac.authorization.k8s.io/v1', kind='ClusterRole', metadata={'name': namespace + '-' + name}, rules=[{'apiGroups': ['postgresql.cnpg.io'], 'resources': ['clusters'], 'verbs': ['get', 'list']}]),
                 dict(apiVersion='rbac.authorization.k8s.io/v1', kind='ClusterRoleBinding', metadata={'name': namespace + '-' + name}, subjects=[{'kind': 'ServiceAccount', 'name': name, 'namespace': namespace}], roleRef={'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'ClusterRole', 'name': namespace + '-' + name})]
    credentials = store['s3Credentials']
    env = [{'name': 'POLICY_NAMESPACE', 'value': namespace}, {'name': 'POLICY_NAME', 'value': name}, {'name': 'RETENTION_DRY_RUN', 'value': 'false'}]
    for variable, key in [('AWS_ACCESS_KEY_ID', 'accessKeyId'), ('AWS_SECRET_ACCESS_KEY', 'secretAccessKey')]:
        ref = credentials[key]
        env.append({'name': variable, 'valueFrom': {'secretKeyRef': {'name': ref['name'], 'key': ref['key']}}})
    resources.append(dict(apiVersion='batch/v1', kind='CronJob', metadata=meta, spec={
        'schedule': '*/15 * * * *', 'concurrencyPolicy': 'Forbid', 'successfulJobsHistoryLimit': 2, 'failedJobsHistoryLimit': 2,
        'jobTemplate': {'spec': {'backoffLimit': 0, 'activeDeadlineSeconds': 600, 'ttlSecondsAfterFinished': 86400,
            'template': {'metadata': {'labels': {LABEL: name}}, 'spec': {'serviceAccountName': name, 'restartPolicy': 'Never',
                'securityContext': {'runAsNonRoot': True, 'runAsUser': 26, 'runAsGroup': 26},
                'containers': [{'name': 'retention', 'image': image, 'command': ['python3', '/retention/worker.py', '--worker'], 'env': env,
                    'resources': {'requests': {'cpu': '50m', 'memory': '128Mi'}, 'limits': {'cpu': '500m', 'memory': '512Mi'}},
                    'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']}},
                    'volumeMounts': [{'name': 'code', 'mountPath': '/retention', 'readOnly': True}]}],
                'volumes': [{'name': 'code', 'configMap': {'name': code_name}}]}}}}}))
    for resource in resources:
        kube('apply', '--server-side', '--field-manager=fugue-cnpg-count-retention', '-f', '-', body=resource)
    print(json.dumps({'installed': True, 'policy': name, 'intent_preserved': True, 'code_configmap': code_name}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', action='store_true')
    parser.add_argument('--install')
    args = parser.parse_args()
    if args.worker:
        try:
            worker()
        except Exception:
            raise SystemExit('Retention did not complete; inspect the policy receipt. No raw credentials are logged.')
    elif args.install:
        def kube(*args, body=None):
            result = subprocess.run(['kubectl', *args], input=None if body is None else json.dumps(body), capture_output=True, text=True, check=True, timeout=40)
            return json.loads(result.stdout) if result.stdout.strip().startswith('{') else None
        install(json.loads(Path(args.install).read_text()), kube)
    else:
        parser.error('--worker or --install is required')


if __name__ == '__main__':
    main()
