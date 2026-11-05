#!/usr/bin/env python3
"""One reviewed, isolated metadata/readback diagnostic; no storage mutations.

Only CI may create the short-lived execution resources. The trusted runner
decrypts the managed reader in memory; platform signing and writer keys never
enter the Job, arguments, logs, or evidence. A durable one-shot receipt prevents
workflow retries from silently repeating the diagnostic.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.parse
import urllib.request

from scripts.compiler_input_archive import credential_key
from scripts.compiler_input_storage import command, get, kube, readers, owned_pods


PURPOSE = 'compiler-input-readonly'
OPTIONS = '-c default_transaction_read_only=on -c statement_timeout=5s -c lock_timeout=250ms -c work_mem=1MB'


class Refusal(ValueError):
    """Only controlled, credential-free messages may reach evidence."""


def require(condition, message):
    if not condition:
        raise Refusal(message)


def cluster_kube(*args):
    """Cluster-scoped kubectl read; unlike kube(), never emits an empty -n."""
    return command(['kubectl', *args])


def cluster_get(kind, name):
    return json.loads(cluster_kube('get', kind, name, '-o', 'json'))


def validate(c):
    fields = {'version', 'executionAuthorized', 'namespace', 'node', 'nodeUID', 'sourceCommit', 'image',
              'cluster', 'clusterUID', 'primary', 'primaryUID', 'postgresContainer', 'dataPath',
              'database', 'digest', 'rawBytes', 'storedBytes', 'segments', 'readCount',
              'apiService', 'readyURL', 'minimumFreeBytes'}
    require(set(c) == fields and type(c['version']) is int and c['version'] == 1, 'invalid readonly declaration')
    require(c['executionAuthorized'] is True, 'readonly execution is not authorized')
    for key in ('namespace', 'node', 'cluster', 'primary', 'postgresContainer', 'database', 'apiService'):
        require(isinstance(c[key], str) and re.fullmatch('[a-z0-9][a-z0-9.-]{0,127}', c[key]), 'invalid readonly identity')
    for key in ('nodeUID', 'clusterUID', 'primaryUID'):
        require(isinstance(c[key], str) and re.fullmatch('[a-f0-9-]{36}', c[key]), 'invalid readonly UID')
    require(re.fullmatch('[a-f0-9]{40}', c['sourceCommit']) and re.fullmatch(r'[a-z0-9./_-]+@sha256:[a-f0-9]{64}', c['image']), 'readonly executable is not pinned')
    require(re.fullmatch('sha256:[a-f0-9]{64}', c['digest']), 'readonly digest is not exact')
    for key, upper in (('rawBytes', 1 << 30), ('storedBytes', 1 << 30), ('segments', 65536), ('readCount', 3)):
        require(type(c[key]) is int and 1 <= c[key] <= upper, 'invalid readonly read budget')
    require(type(c['minimumFreeBytes']) is int and c['minimumFreeBytes'] >= 1 << 30, 'readonly database reserve is below 1GiB')
    require(re.fullmatch('/[a-zA-Z0-9/_-]{1,200}', c['dataPath']), 'invalid database data path')
    url = urllib.parse.urlsplit(c['readyURL'])
    require(url.scheme == 'https' and url.hostname and not url.username and not url.password and not url.query and not url.fragment and url.path == '/readyz', 'invalid public readiness URL')
    return c


def identity(c):
    return hashlib.sha256(json.dumps(c, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def quantity(value):
    value = str(value)
    match = re.fullmatch(r'([0-9]+(?:\.[0-9]+)?)([a-zA-Z]*)', value)
    require(match is not None, 'unsupported resource quantity')
    units = {'': 1, 'n': 1e-9, 'u': 1e-6, 'm': 1e-3, 'Ki': 1 << 10, 'Mi': 1 << 20, 'Gi': 1 << 30,
             'Ti': 1 << 40, 'K': 1000, 'M': 1000000, 'G': 1000000000}
    require(match[2] in units, 'unsupported resource unit')
    return float(match[1]) * units[match[2]]


def node_gate(c, existing_job=''):
    node = cluster_get('node', c['node'])
    require(node['metadata']['uid'] == c['nodeUID'] and not node['spec'].get('unschedulable') and not node['spec'].get('taints'), 'readonly node identity or scheduling changed')
    conditions = {x['type']: x['status'] for x in node['status']['conditions']}
    require(conditions.get('Ready') == 'True' and all(conditions.get(x) == 'False' for x in ('DiskPressure', 'MemoryPressure', 'PIDPressure')), 'readonly node is unhealthy')
    metric = cluster_get('nodes.metrics.k8s.io', c['node'])
    age = (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(metric['timestamp'].replace('Z', '+00:00'))).total_seconds()
    require(-5 <= age <= 120, 'readonly node metrics are stale')
    alloc = node['status']['allocatable']
    cpu = quantity(alloc['cpu']) - quantity(metric['usage']['cpu'])
    memory = quantity(alloc['memory']) - quantity(metric['usage']['memory'])
    require(cpu >= 1.5 and memory >= 512 << 20, 'readonly node usage headroom unavailable')
    pods = json.loads(cluster_kube('get', 'pods', '-A', '--field-selector=spec.nodeName='+c['node'], '-o', 'json'))['items']
    reservations = {k: 0 for k in ('cpu', 'memory', 'ephemeral-storage')}
    for pod in pods:
        if pod.get('status', {}).get('phase') in ('Succeeded', 'Failed'):
            continue
        if existing_job and any(x.get('controller') and x.get('uid') == existing_job for x in pod['metadata'].get('ownerReferences', [])):
            continue
        # Sum init containers conservatively rather than undercounting native
        # sidecars or a still-running init. Include terminating reservations.
        for container in pod['spec'].get('containers', []) + pod['spec'].get('initContainers', []):
            for key in reservations:
                reservations[key] += quantity(container.get('resources', {}).get('requests', {}).get(key, '0'))
        for key in reservations:
            reservations[key] += quantity(pod['spec'].get('overhead', {}).get(key, '0'))
    require(all(quantity(alloc[k]) - reservations[k] >= v for k, v in [('cpu', 1), ('memory', 512 << 20), ('ephemeral-storage', 256 << 20)]), 'readonly node reservation headroom unavailable')
    return {'node_uid': c['nodeUID'], 'metric_timestamp': metric['timestamp'], 'cpu_headroom_cores': cpu,
            'memory_headroom_bytes': int(memory), 'reserved_cpu_cores': reservations['cpu']}


def primary_gate(c):
    ns = c['namespace']
    cluster = get(ns, 'clusters.postgresql.cnpg.io', c['cluster'])
    status = cluster['status']
    require(cluster['metadata']['uid'] == c['clusterUID'] and cluster['spec']['instances'] == 1 and status.get('readyInstances') == 1
            and status.get('phase') == 'Cluster in healthy state' and status.get('currentPrimary') == c['primary']
            and status.get('targetPrimary') == c['primary'], 'readonly primary cluster changed or unhealthy')
    pod = get(ns, 'pod', c['primary'])
    require(pod['metadata']['uid'] == c['primaryUID'] and not pod['metadata'].get('deletionTimestamp')
            and any(x.get('controller') and x.get('uid') == c['clusterUID'] for x in pod['metadata'].get('ownerReferences', [])), 'readonly primary identity changed')
    require(any(x['type'] == 'Ready' and x['status'] == 'True' for x in pod['status']['conditions'])
            and all(x['ready'] for x in pod['status']['containerStatuses']), 'readonly primary is not ready')
    result = kube(ns, 'exec', c['primary'], '-c', c['postgresContainer'], '--', 'df', '-Pk', c['dataPath'])
    lines = result.strip().splitlines()
    require(len(lines) == 2 and len(lines[1].split()) == 6 and lines[1].split()[-1] == c['dataPath'], 'readonly primary filesystem is ambiguous')
    available = int(lines[1].split()[3]) * 1024
    require(available >= c['minimumFreeBytes'], 'readonly primary free-space gate failed')
    return cluster, pod, available


def public_gate(c):
    with urllib.request.urlopen(c['readyURL'], timeout=10) as response:
        require(response.status == 200, 'public readiness failed')
        body = response.read(16385)
    require(len(body) <= 16384 and json.loads(body).get('status') == 'ok', 'public readiness failed')


def serving_gate(c, registry):
    current = readers(registry, archive_required=True)
    executor = next(x for x in current if x['component'] == 'api')
    require(executor['namespace'] == c['namespace'] and executor['revision'] == c['sourceCommit'] and executor['image'] == c['image'], 'readonly image is not the reviewed serving API')
    component = next(x for x in registry['components'] if x['id'] == 'api')
    deployment = get(c['namespace'], 'deployment', component['workload']['name'])
    pods = owned_pods(c['namespace'], deployment)['items']
    bound, restarts = {}, {}
    for pod in pods:
        status = next(x for x in pod['status']['containerStatuses'] if x['name'] == executor['container'])
        require(status['imageID'].removeprefix('docker-pullable://') == c['image'], 'readonly serving imageID differs')
        bound[pod['metadata']['name']] = pod['metadata']['uid']
        restarts[pod['metadata']['name']] = status['restartCount']
    endpoints = json.loads(kube(c['namespace'], 'get', 'endpointslices', '-l', 'kubernetes.io/service-name='+c['apiService'], '-o', 'json'))
    targets = []
    for item in endpoints['items']:
        for endpoint in item.get('endpoints', []):
            condition = endpoint.get('conditions', {})
            ref = endpoint.get('targetRef', {})
            require(condition.get('ready') is True and condition.get('serving') is True and not condition.get('terminating')
                    and ref.get('uid') == bound.get(ref.get('name')), 'readonly API endpoint changed')
            targets.append(ref['name'])
    require(set(targets) == set(bound) and len(targets) == len(bound), 'readonly API endpoint coverage incomplete')
    public_gate(c)
    return executor, deployment, {'api_source': executor['revision'], 'api_image': executor['image'], 'api_pods': bound, 'api_restarts': restarts}


def sql_metadata(c):
    # Select only the archive reader (never the writer/platform account token),
    # in the same bounded read-only snapshot as its verified archive receipt.
    digest = c['digest']  # validated SHA-256; no free-form SQL arguments
    sql = """BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='250ms'; SET LOCAL work_mem='1MB';
SELECT json_build_object('platform', (SELECT jsonb_build_object('account_id',body->>'account_id',
 'compiler_input_archive',(body->'compiler_input_archive')-'writer') FROM fugue_object_storage_config WHERE id='platform'),
 'archive',(SELECT json_build_object('backend_hash',backend_hash,'raw_bytes',raw_bytes,'segments',segment_count,
 'stored_bytes',(SELECT sum(stored_bytes) FROM fugue_platform_input_archive_segments s WHERE s.content_hash=a.content_hash),
 'verified',state IN ('verified','routed') AND verified_digest=content_hash AND verified_at IS NOT NULL)
 FROM fugue_platform_input_archives a WHERE content_hash='"""+digest+"""'),
 'active_leases',(SELECT count(*) FROM fugue_platform_input_archive_leases WHERE expires_at>transaction_timestamp()),
 'fenced',(SELECT writes_fenced OR retirement_started_at IS NOT NULL FROM fugue_platform_input_archive_control WHERE singleton));
COMMIT;
"""
    argv = ['kubectl', '-n', c['namespace'], 'exec', '-i', c['primary'], '-c', c['postgresContainer'], '--',
            'psql', '-U', 'postgres', '-d', c['database'], '-qAt', '-v', 'ON_ERROR_STOP=1']
    p = subprocess.run(argv, input=sql, capture_output=True, text=True, timeout=20)
    require(p.returncode == 0 and len(p.stdout) <= 128 << 10, 'readonly credential metadata query unavailable')
    try:
        value = json.loads(p.stdout)
    except (ValueError, TypeError):
        raise ValueError('readonly credential metadata is invalid') from None
    require(value['active_leases'] == 0 and value['fenced'] is False, 'archive maintenance is active')
    archive = value['archive']
    require(archive and archive['verified'] is True and all(archive[k] == c[k2] for k, k2 in [('raw_bytes', 'rawBytes'), ('stored_bytes', 'storedBytes'), ('segments', 'segments')]), 'readonly sample receipt changed')
    return value


def reader_session(platform, key, expected_backend):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    v = platform.get('compiler_input_archive')
    require(v and v['state'] == 'active' and v['account_id'] == platform['account_id'] and v['reader_configured'] is True
            and v['writer_configured'] is True and v.get('protection_verified_at') and re.fullmatch('[a-f0-9]{64}', v['retention_policy_sha256']), 'managed archive protection is incomplete')
    require(re.fullmatch('[a-f0-9]{32}', v['account_id']) and re.fullmatch('[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', v['bucket'])
            and re.fullmatch('[a-zA-Z0-9/_-]{1,200}', v['prefix']), 'managed archive identity is invalid')
    raw = base64.b64decode(v['reader']['ciphertext'], validate=True)
    try:
        plain = AESGCM(key).decrypt(raw[:12], raw[12:], None)
        credentials = json.loads(plain)
    except Exception:
        raise ValueError('managed archive reader decryption failed') from None
    require(set(credentials) <= {'access_key_id', 'secret_access_key', 'token'} and not credentials.get('token')
            and credentials.get('access_key_id') and credentials.get('secret_access_key'), 'managed archive reader credential is invalid')
    endpoint = 'https://'+v['account_id']+'.r2.cloudflarestorage.com'
    canonical = {'Endpoint': endpoint, 'AccountID': v['account_id'], 'Region': 'auto', 'Bucket': v['bucket'], 'Prefix': v['prefix'].strip('/')}
    fingerprint = 'sha256:'+hashlib.sha256(json.dumps(canonical, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
    require(fingerprint == expected_backend, 'managed archive backend fingerprint differs')
    return {'endpoint': endpoint, 'account-id': v['account_id'], 'region': 'auto', 'bucket': v['bucket'], 'prefix': v['prefix'],
            'reader-access-key-id': credentials['access_key_id'], 'reader-secret-access-key': credentials['secret_access_key']}


def session(c, cluster, pod, deployment, executor, metadata):
    container = next(x for x in deployment['spec']['template']['spec']['containers'] if x['name'] == executor['container'])
    key = credential_key(c['namespace'], container, {})
    data = reader_session(metadata['platform'], key, metadata['archive']['backend_hash'])
    secret = get(c['namespace'], 'secret', cluster['spec']['bootstrap']['initdb']['secret']['name'])
    user = base64.b64decode(secret['data']['username'], validate=True).decode()
    password = base64.b64decode(secret['data']['password'], validate=True).decode()
    require(cluster['spec']['bootstrap']['initdb']['database'] == c['database'] and user == cluster['spec']['bootstrap']['initdb']['owner'] and password, 'application database identity differs')
    host = pod['status']['podIP']
    # This ordinary application role is not permission-reduced. Explicit
    # default READ ONLY plus the exact CLI's READ ONLY transactions enforce
    # the operation; do not describe this as a new database grant.
    import ipaddress
    address = ipaddress.ip_address(host)
    host = '['+host+']' if address.version == 6 else host
    query = urllib.parse.urlencode({'sslmode': 'require', 'connect_timeout': '5', 'application_name': PURPOSE, 'options': OPTIONS})
    data['database-readonly-url'] = 'postgresql://'+urllib.parse.quote(user, safe='')+':'+urllib.parse.quote(password, safe='')+'@'+host+':5432/'+c['database']+'?'+query
    return data


def job_manifest(c, name, session_name):
    env = [{'name': 'FUGUE_DATABASE_URL', 'valueFrom': {'secretKeyRef': {'name': session_name, 'key': 'database-readonly-url'}}}]
    for suffix, key in [('ENDPOINT', 'endpoint'), ('ACCOUNT_ID', 'account-id'), ('REGION', 'region'), ('BUCKET', 'bucket'), ('PREFIX', 'prefix'), ('ACCESS_KEY_ID', 'reader-access-key-id'), ('SECRET_ACCESS_KEY', 'reader-secret-access-key')]:
        env.append({'name': 'FUGUE_COMPILER_INPUT_ARCHIVE_'+suffix, 'valueFrom': {'secretKeyRef': {'name': session_name, 'key': key}}})
    env += [{'name': 'PGOPTIONS', 'value': OPTIONS}, {'name': 'GOMAXPROCS', 'value': '1'}, {'name': 'GOMEMLIMIT', 'value': '128MiB'}]
    script = 'set -eu; graph_status=0; nice -n 19 /usr/local/bin/fugue-input-storage -mode=archive-hot-inventory || graph_status=$?; '
    for _ in range(c['readCount']):
        script += 'nice -n 19 /usr/local/bin/fugue-input-storage -mode=archive-read-check -digest='+c['digest']+' -read-check-max-bytes='+str(c['rawBytes'])+'; '
    script += 'exit "$graph_status"'
    resources = {'cpu': '1', 'memory': '512Mi', 'ephemeral-storage': '256Mi'}
    return {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': name, 'namespace': c['namespace'], 'labels': {'fugue.pro/purpose': PURPOSE}},
            'spec': {'suspend': True, 'backoffLimit': 0, 'activeDeadlineSeconds': 600, 'parallelism': 1, 'completions': 1, 'ttlSecondsAfterFinished': 3600,
                     'template': {'metadata': {'labels': {'fugue.pro/purpose': PURPOSE}, 'annotations': {'fugue.pro/source-commit': c['sourceCommit']}},
                                  'spec': {'restartPolicy': 'Never', 'terminationGracePeriodSeconds': 15, 'automountServiceAccountToken': False,
                                           'nodeSelector': {'kubernetes.io/hostname': c['node']}, 'preemptionPolicy': 'Never',
                                           'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532, 'seccompProfile': {'type': 'RuntimeDefault'}},
                                           'containers': [{'name': 'readonly', 'image': c['image'], 'imagePullPolicy': 'IfNotPresent',
                                                           'resources': {'requests': dict(resources), 'limits': dict(resources)},
                                                           'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}},
                                                           'env': env, 'command': ['/bin/sh', '-c'], 'args': [script]}]}}}}


def api_write(argv, value, timeout=30):
    # Credentials travel on stdin only. Never relay kubectl errors: they may
    # embed the submitted Secret or request body.
    require(os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REF') == 'refs/heads/main', 'readonly resource writes require the main CI lane')
    p = subprocess.run(['kubectl', *argv], input=json.dumps(value), capture_output=True, text=True, timeout=timeout)
    require(p.returncode == 0, 'readonly execution resource write was not confirmed')
    try:
        return json.loads(p.stdout) if p.stdout.strip() else {}
    except ValueError:
        raise ValueError('readonly execution resource receipt is invalid') from None


def create(value, dry=False):
    return api_write(['create', '-f', '-', '-o', 'json'] + (['--dry-run=server'] if dry else []), value)


def delete_exact(c, kind, name, uid):
    plural = {'Job': ('/apis/batch/v1', 'jobs'), 'Secret': ('/api/v1', 'secrets')}[kind]
    return api_write(['delete', '--raw', plural[0]+'/namespaces/'+c['namespace']+'/'+plural[1]+'/'+name, '-f', '-'],
                     {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'preconditions': {'uid': uid}, 'propagationPolicy': 'Foreground'})


def result_contract(body, c):
    require(len(body) <= 64 << 20, 'readonly results exceed output budget')
    values = []
    for line in body.splitlines():
        if not line.startswith('{'):
            continue  # Go error text is classified; never persist raw stderr.
        try:
            values.append(json.loads(line))
        except ValueError:
            raise ValueError('readonly result JSON is incomplete') from None
    require(1 <= len(values) <= c['readCount']+1, 'readonly result count differs')
    graph = values[0]
    progress = graph.get('progress')
    require(isinstance(progress, dict) and set(progress) == {'phase', 'resolved_nodes', 'unresolved_nodes', 'edges', 'root_bindings', 'resolver_calls'}
            and re.fullmatch('[a-z-]{0,48}', progress['phase']) and all(type(v) is int and 0 <= v <= 524289 for k, v in progress.items() if k != 'phase'), 'readonly graph progress contract differs')
    counts = graph.get('root_counts') or {}
    require(isinstance(counts, dict) and len(counts) <= 64 and all(re.fullmatch('[a-z-]{1,48}', k) and type(v) is int and 0 <= v <= 16385 for k, v in counts.items()), 'readonly root count contract differs')
    if graph.get('schema') == 'fugue.compiler-input-hot-inventory-failure/v1':
        fields = {'schema', 'inspection_complete', 'metadata_complete', 'local_copies_verified', 'retirement_authorized', 'plan_sha256',
                  'stage', 'reason', 'category', 'coherent_metadata', 'root_inventory_complete', 'root_counts', 'progress'}
        require(set(graph) == fields and all(graph[k] is False for k in ('inspection_complete', 'metadata_complete', 'local_copies_verified', 'retirement_authorized'))
                and graph['plan_sha256'] == '' and all(re.fullmatch('[a-zA-Z0-9_-]{1,64}', graph[k]) for k in ('stage', 'reason', 'category')), 'readonly graph failure contract differs')
        complete = False
    else:
        fields = {'action', 'observed_at', 'database_snapshot', 'coherent_metadata', 'root_inventory_complete', 'traversal_performed',
                  'root_counts', 'progress', 'body_verification', 'reactivation_protected', 'retirement_authorized', 'uncovered_sources', 'closure'}
        require(set(graph) == fields and graph['action'] == 'archive-hot-inventory' and graph['coherent_metadata'] is True
                and graph['root_inventory_complete'] is True and graph['traversal_performed'] is True
                and all(graph[k] is False for k in ('body_verification', 'reactivation_protected', 'retirement_authorized')), 'readonly graph result contract differs')
        complete = True
    fields = {'action', 'digest', 'read_only', 'verified', 'retirement_authorized', 'raw_bytes', 'stored_bytes', 'segments', 'metadata_elapsed_ms', 'readback_elapsed_ms', 'total_elapsed_ms'}
    for v in values[1:]:
        require(set(v) == fields and v['action'] == 'archive-read-check' and v['digest'] == c['digest'] and v['read_only'] is True
                and v['verified'] is True and v['retirement_authorized'] is False and v['raw_bytes'] == c['rawBytes']
                and v['stored_bytes'] == c['storedBytes'] and v['segments'] == c['segments']
                and all(type(v[k]) is int and v[k] >= 0 for k in ('raw_bytes', 'stored_bytes', 'segments', 'metadata_elapsed_ms', 'readback_elapsed_ms', 'total_elapsed_ms')), 'readonly read result contract differs')
    return {'graph_command_completed': complete, 'graph': graph, 'reads': values[1:], 'all_reads_verified': len(values) == c['readCount']+1,
            'retirement_authorized': False}


def pod_check(c, job_uid, pod):
    require(any(x.get('controller') and x.get('uid') == job_uid and x.get('kind') == 'Job' for x in pod['metadata'].get('ownerReferences', [])), 'readonly Pod owner differs')
    require(pod['spec'].get('nodeName') == c['node'] and pod['metadata'].get('annotations', {}).get('fugue.pro/source-commit') == c['sourceCommit'], 'readonly Pod node or source differs')
    statuses = pod['status'].get('containerStatuses', [])
    require(len(statuses) == 1 and statuses[0]['name'] == 'readonly' and statuses[0]['restartCount'] == 0
            and statuses[0]['imageID'].removeprefix('docker-pullable://') == c['image'], 'readonly Pod actual image or restart differs')
    return {'pod': pod['metadata']['name'], 'pod_uid': pod['metadata']['uid'], 'node_uid': c['nodeUID'],
            'image_id': statuses[0]['imageID'], 'restart_count': statuses[0]['restartCount'], 'phase': pod['status']['phase']}


def execute(c, evidence, apply=False):
    registry = json.loads(Path('deploy/releases/components.json').read_text())
    declaration_hash = identity(c)
    name = 'fugue-input-readonly-'+declaration_hash[:20]
    secret_name = name+'-session'
    manifest = job_manifest(c, name, secret_name)
    report = {'declaration_sha256': declaration_hash, 'execution_authorized': True, 'job': name,
              'production_storage_mutations': False, 'retirement_authorized': False, 'status': 'preflight', 'cleanup_confirmed': False}
    def save():
        evidence.write_text(json.dumps(report, separators=(',', ':'))+'\n')
    # Reserve the evidence filename even for failed preflight; no overwrite.
    with evidence.open('x') as output:
        output.write(json.dumps(report)+'\n')
    job_uid = secret_uid = None
    try:
        if apply:
            require(os.environ.get('GITHUB_ACTIONS') == 'true' and os.environ.get('GITHUB_REF') == 'refs/heads/main'
                    and os.environ.get('GITHUB_SHA') == command(['git', 'rev-parse', 'HEAD']).strip(), 'readonly execution requires exact main CI checkout')
        report['node'] = node_gate(c)
        cluster, primary, available = primary_gate(c)
        executor, deployment, serving = serving_gate(c, registry)
        report.update(serving=serving, initial_available_bytes=available)
        primary_restarts = {x['name']: x['restartCount'] for x in primary['status']['containerStatuses']}
        def health_watch():
            _, observed, _ = primary_gate(c)
            require({x['name']: x['restartCount'] for x in observed['status']['containerStatuses']} == primary_restarts, 'primary restarted during readonly diagnostic')
            _, _, current = serving_gate(c, registry)
            require(current == serving, 'API identity or restarts changed during readonly diagnostic')
        metadata = sql_metadata(c)
        # Even plan mode validates the credential path, but never persists it.
        data = session(c, cluster, primary, deployment, executor, metadata)
        active = json.loads(kube(c['namespace'], 'get', 'jobs', '-l', 'fugue.pro/purpose='+PURPOSE, '-o', 'json'))['items']
        require(not active, 'another readonly execution resource exists; inspect before retrying')
        create(manifest, dry=True)
        report['status'] = 'preview-passed'
        if not apply:
            return report
        # Current-main parity is also checked in CI immediately before this
        # call. The immutable receipt survives cleanup/TTL and rejects replay.
        receipt = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'namespace': c['namespace'], 'name': name+'-receipt'},
                   'immutable': True, 'data': {'declaration-sha256': declaration_hash, 'intent': 'single-readonly-attempt-no-retry'}}
        create(receipt)
        created = create(manifest)
        job_uid = created['metadata']['uid']
        report.update(job_uid=job_uid, status='reserved')
        save()
        secret = {'apiVersion': 'v1', 'kind': 'Secret', 'immutable': True, 'metadata': {'namespace': c['namespace'], 'name': secret_name,
                  'ownerReferences': [{'apiVersion': 'batch/v1', 'kind': 'Job', 'name': name, 'uid': job_uid, 'controller': True}]},
                  'type': 'Opaque', 'data': {k: base64.b64encode(v.encode()).decode() for k, v in data.items()}}
        secret_uid = create(secret)['metadata']['uid']
        data.clear()
        # Recheck after all writes and immediately before releasing the Pod.
        node_gate(c)
        health_watch()
        api_write(['-n', c['namespace'], 'patch', 'job', name, '--type=json', '--patch-file=/dev/stdin', '-o', 'json'],
                  [{'op': 'test', 'path': '/metadata/uid', 'value': job_uid}, {'op': 'test', 'path': '/spec/suspend', 'value': True}, {'op': 'replace', 'path': '/spec/suspend', 'value': False}])
        report['status'] = 'running'
        save()
        deadline = time.monotonic()+630
        last_pod = None
        while time.monotonic() < deadline:
            job = get(c['namespace'], 'job', name)
            require(job['metadata']['uid'] == job_uid, 'readonly Job identity changed')
            pods = json.loads(kube(c['namespace'], 'get', 'pods', '-l', 'job-name='+name, '-o', 'json'))['items']
            require(len(pods) <= 1, 'readonly Job has multiple Pods')
            if pods and pods[0]['status'].get('containerStatuses') and pods[0]['status']['containerStatuses'][0].get('imageID'):
                last_pod = pods[0]
                report['execution'] = pod_check(c, job_uid, last_pod)
                if last_pod['status']['phase'] in ('Succeeded', 'Failed'):
                    break
            health_watch()
            node_gate(c, job_uid)
            save()
            time.sleep(10)
        require(last_pod is not None and last_pod['status']['phase'] in ('Succeeded', 'Failed'), 'readonly Job did not finish within deadline')
        body = kube(c['namespace'], 'logs', last_pod['metadata']['name'], '-c', 'readonly', '--limit-bytes=67108864')
        report['result'] = result_contract(body, c)
        report['execution'] = pod_check(c, job_uid, get(c['namespace'], 'pod', last_pod['metadata']['name']))
        health_watch()
        report.update(status='diagnostic-finished', execution_identity_rechecked=True)
        return report
    except Exception as error:
        # Never stringify arbitrary subprocess/driver/crypto exceptions.
        report.update(status='completion-not-confirmed', error_type=type(error).__name__)
        if isinstance(error, Refusal):
            report['refusal_reason'] = str(error)
        raise
    finally:
        if job_uid:
            try:
                delete_exact(c, 'Job', name, job_uid)
                # Foreground deletion is asynchronous. Confirm no exact-owned
                # Pod before deleting its session. Do not force kill anything.
                for _ in range(12):
                    remaining = json.loads(kube(c['namespace'], 'get', 'pods', '-l', 'job-name='+name, '-o', 'json'))['items']
                    if not any(any(x.get('uid') == job_uid for x in p['metadata'].get('ownerReferences', [])) for p in remaining):
                        break
                    time.sleep(5)
                else:
                    raise ValueError('readonly Pod cleanup remains pending')
                # Job owner GC may already have removed this exact Secret.
                secrets = json.loads(kube(c['namespace'], 'get', 'secret', '--field-selector=metadata.name='+secret_name, '-o', 'json'))['items']
                if secrets:
                    require(len(secrets) == 1 and (secret_uid is None or secrets[0]['metadata']['uid'] == secret_uid)
                            and any(x.get('uid') == job_uid and x.get('controller') for x in secrets[0]['metadata'].get('ownerReferences', [])), 'readonly session identity changed')
                    delete_exact(c, 'Secret', secret_name, secrets[0]['metadata']['uid'])
                for _ in range(12):
                    remaining_secrets = json.loads(kube(c['namespace'], 'get', 'secret', '--field-selector=metadata.name='+secret_name, '-o', 'json'))['items']
                    remaining_jobs = json.loads(kube(c['namespace'], 'get', 'jobs', '--field-selector=metadata.name='+name, '-o', 'json'))['items']
                    if not remaining_secrets and not remaining_jobs:
                        break
                    time.sleep(5)
                else:
                    raise ValueError('readonly session or Job cleanup remains pending')
                report['cleanup_confirmed'] = True
            except Exception as error:
                report.update(cleanup_confirmed=False, cleanup_error_type=type(error).__name__)
        report['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('declaration')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    try:
        c = validate(json.loads(Path(args.declaration).read_text()))
        report = execute(c, args.evidence, args.apply)
        print(json.dumps({k: report[k] for k in ('job', 'status', 'cleanup_confirmed', 'retirement_authorized')}))
        if args.apply:
            require(report['cleanup_confirmed'], 'readonly execution cleanup not confirmed')
            require(report.get('result', {}).get('all_reads_verified'), 'readonly native archive readback incomplete')
    except Exception as error:
        raise SystemExit('readonly diagnostic completion not confirmed; inspect evidence; category='+type(error).__name__) from None


if __name__ == '__main__':
    main()
