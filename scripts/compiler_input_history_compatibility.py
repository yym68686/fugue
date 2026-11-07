"""Bounded binary/current/LKG protocol check; never retirement permission.

This is a necessary gate for the registered store/schema releases. It does not
certify unregistered database clients, retained inputs, a confidential restore,
or the future admission/enforcement transition. It performs no mutations.
"""
import hashlib
import json
import re
import time

from scripts.compiler_input_storage import command, get, kube, primary, validate_reader


ARGUMENT = '--compiler-input-history-capabilities'
CONTRACT_PATH = 'internal/inputhistory/capabilities.json'
REQUIRED = {
    'store': frozenset((
        'input-formats-v1-v2-v3-v4',
        'archive-reader-lease-and-startup-guard-v1',
        'all-input-writers-transaction-barrier-v2',
        'input-writer-caller-cancellation-v1',
        'fenced-new-inputs-independent-local-storage-v1',
        'cold-input-writer-explicit-preparation-v1',
        'durable-history-admission-phases-v1',
    )),
    'schema': frozenset(('compiler-input-additive-migrations-v1',)),
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def strict_json(raw):
    require(isinstance(raw, str), 'invalid history capability evidence')
    try:
        size = len(raw.encode())
    except UnicodeError:
        raise ValueError('invalid history capability evidence') from None
    require(size <= 16384, 'history capability evidence exceeds bound')
    def pairs(items):
        value = {}
        for key, item in items:
            require(key not in value, 'duplicate history capability field')
            value[key] = item
        return value
    try:
        return json.loads(raw, object_pairs_hook=pairs)
    except (json.JSONDecodeError, UnicodeError):
        raise ValueError('invalid history capability evidence') from None


def source_contract(revision, role, package, cache):
    require(re.fullmatch('[0-9a-f]{40}', revision) is not None and role in REQUIRED,
            'history source identity or role is invalid')
    require(re.fullmatch(r'\./cmd/[a-z0-9-]+', package or '') is not None,
            'history executable package is not exact')
    key = revision, role, package
    if key in cache:
        return cache[key]
    raw = command(['git', 'show', revision+':'+CONTRACT_PATH])
    value = strict_json(raw)
    require(type(value) is dict and set(value) == {'schema', 'version', 'roles'} and
            value['schema'] == 'fugue.compiler-input-history.capability-contract.v1' and
            type(value['version']) is int and value['version'] == 1 and
            type(value['roles']) is dict and set(value['roles']) == set(REQUIRED),
            'current or rollback lacks the history capability contract')
    for name, capabilities in value['roles'].items():
        require(type(capabilities) is list and 0 < len(capabilities) <= 64 and
                all(isinstance(x, str) and re.fullmatch('[a-z0-9-]{1,128}', x) for x in capabilities) and
                len(set(capabilities)) == len(capabilities) and REQUIRED[name] <= set(capabilities),
                'current or rollback lacks required history protocols')
    # Bind the executable's own diagnostic entry, not merely a shared file
    # present somewhere in its source revision. The trusted CI image/record
    # binding and live executable report are checked independently below.
    entry = command(['git', 'show', revision+':'+package[2:]+'/main.go'])
    require('inputhistory.WriteCapabilities(os.Args[1:], "'+role+'", os.Stdout)' in entry,
            'history executable does not expose its compiled role')
    result = {'schema': 'fugue.compiler-input-history.binary-capabilities.v1',
              'role': role, 'capabilities': value['roles'][role],
              'contract_sha256': hashlib.sha256(raw.encode()).hexdigest()}
    cache[key] = result
    return result


def exact_owned_pods(namespace, deployment):
    """Pod -> exact controller RS UID -> Deployment UID; no RS history dump."""
    uid = deployment['metadata'].get('uid')
    selector = deployment['spec'].get('selector', {})
    require(uid and set(selector) == {'matchLabels'} and selector['matchLabels'],
            'history deployment ownership is ambiguous')
    labels = ','.join(k+'='+v for k, v in sorted(selector['matchLabels'].items()))
    value = json.loads(kube(namespace, 'get', 'pods', '-l', labels, '-o', 'json'))
    require(not value.get('metadata', {}).get('continue') and
            type(value.get('items')) is list and len(value['items']) <= 256,
            'history pod population exceeds bound')
    owned, sets = [], {}
    for pod in value['items']:
        owners = [x for x in pod['metadata'].get('ownerReferences', []) if x.get('controller') is True]
        require(len(owners) == 1, 'history pod controller is ambiguous')
        owner = owners[0]
        if owner.get('kind') != 'ReplicaSet':
            continue  # Shared selectors can include a separately owned janitor.
        name, rs_uid = owner.get('name'), owner.get('uid')
        require(name and rs_uid, 'history pod ReplicaSet identity is missing')
        if name not in sets:
            sets[name] = get(namespace, 'replicaset', name)
        rs = sets[name]
        require(rs['metadata'].get('uid') == rs_uid, 'history ReplicaSet was replaced')
        parents = [x for x in rs['metadata'].get('ownerReferences', []) if x.get('controller') is True]
        require(len(parents) == 1, 'history ReplicaSet controller is ambiguous')
        if parents[0].get('kind') == 'Deployment' and parents[0].get('uid') == uid:
            owned.append(pod)
    return {'items': owned}


def _components(registry):
    require(type(registry) is dict and type(registry.get('components')) is list and
            0 < len(registry['components']) <= 128, 'history component registry is invalid')
    selected, seen, workloads = [], set(), set()
    for component in registry['components']:
        require(type(component) is dict and type(component.get('sourceRoots')) is list and
                all(type(x) is str for x in component['sourceRoots']), 'history source roots are missing')
        roots = set(component['sourceRoots'])
        if not roots.intersection({'internal/store', 'internal/schemamigrate'}):
            continue
        w = component['workload']
        identity = w['kind'], w['namespace'], w['name'], w['container']
        require(component['id'] not in seen and w['kind'] == 'Deployment' and not component.get('artifactTargets'),
                'history store workload needs explicit complete target support')
        require(identity not in workloads, 'history store workload is declared more than once')
        seen.add(component['id'])
        workloads.add(identity)
        selected.append((component, 'store' if 'internal/store' in roots else 'schema'))
    require(selected and len(selected) <= 16, 'history store inventory is empty or exceeds bound')
    return selected


def snapshot(registry, cache, deadline):
    result = []
    for component, role in _components(registry):
        require(time.monotonic() < deadline, 'history compatibility observation exceeded deadline')
        w = component['workload']; namespace = w['namespace']
        deployment = get(namespace, 'deployment', w['name'])
        require(type(deployment['spec'].get('replicas', 1)) is int and
                0 < deployment['spec'].get('replicas', 1) <= 64,
                'history deployment replica count is invalid')
        pods = exact_owned_pods(namespace, deployment)
        pointer = get(namespace, 'configmap', 'fugue-release-monitor-'+component['id'])
        state = strict_json(pointer['data']['state.json'])
        require(type(state) is dict and type(state.get('consecutiveFailures')) is int and
                state['consecutiveFailures'] == 0 and state.get('rollbackStatus', '') == '',
                'history release has an unresolved failure or rollback')
        name = pointer['data']['recordName']
        require(re.fullmatch('fugue-release-record-'+re.escape(component['id'])+'-[0-9a-f]{16}', name) is not None,
                'history release record name is invalid')
        record = get(namespace, 'configmap', name)
        reader = validate_reader(component, deployment, pods, record, archive_required=role == 'store')
        current = source_contract(reader['revision'], role, component['artifact']['buildPackage'], cache)
        rollback = source_contract(reader['rollback_revision'], role, component['artifact']['buildPackage'], cache)
        envelope = strict_json(record['data']['record.json'])
        rollback_workload = primary(strict_json(record['data']['lkg.json']), w)
        rollback_containers = [x for x in rollback_workload['spec']['template']['spec']['containers'] if x['name'] == w['container']]
        require(len(rollback_containers) == 1 and re.fullmatch(r'.+@sha256:[0-9a-f]{64}', rollback_containers[0]['image']),
                'history rollback image is not immutable')
        # The trusted immutable forward record must bind the running IMAGE as
        # well as the revision. A copied source annotation is insufficient.
        forward_workload = primary(strict_json(record['data']['forward.json']), w)
        forward_containers = [x for x in forward_workload['spec']['template']['spec']['containers'] if x['name'] == w['container']]
        require(len(forward_containers) == 1 and forward_containers[0]['image'] == reader['image'],
                'history forward image differs from live image')
        identities = []
        for pod in pods['items']:
            status = next(x for x in pod['status']['containerStatuses'] if x['name'] == w['container'])
            image_id = status.get('imageID', '')
            image_match = re.fullmatch(r'(?:[a-z0-9+.-]+://)?(?:[^@\s]+@)?(sha256:[0-9a-f]{64})', image_id) if isinstance(image_id, str) else None
            require(image_match and image_match[1] == reader['image'].rsplit('@', 1)[-1] and
                    type(status.get('restartCount')) is int and status['restartCount'] >= 0 and status.get('containerID'),
                    'history running image or container identity is unavailable')
            require(pod['metadata'].get('uid'), 'history Pod UID is missing')
            identities.append({'name': pod['metadata']['name'], 'uid': pod['metadata']['uid'],
                               'container_id': status['containerID'], 'image_id': image_id,
                               'restarts': status['restartCount']})
        require(deployment['metadata'].get('uid') and pointer['metadata'].get('uid') and record['metadata'].get('uid'),
                'history release object UID is missing')
        result.append({'component': component['id'], 'role': role, 'namespace': namespace, 'container': w['container'],
                       'deployment_uid': deployment['metadata']['uid'], 'generation': deployment['metadata']['generation'],
                       'revision': reader['revision'], 'image': reader['image'], 'current_capabilities': current,
                       'rollback_revision': reader['rollback_revision'], 'rollback_image': rollback_containers[0]['image'],
                       'rollback_capabilities': rollback, 'monitor_uid': pointer['metadata']['uid'],
                       'record_name': name, 'record_uid': record['metadata']['uid'],
                       'forward_manifest_digest': envelope['forwardManifestDigest'],
                       'lkg_manifest_digest': envelope['lkgManifestDigest'],
                       'pods': sorted(identities, key=lambda x: x['uid'])})
    require(sum(len(x['pods']) for x in result) <= 64, 'history owned pod inventory exceeds bound')
    return sorted(result, key=lambda x: x['component'])


def check_history_compatibility(registry):
    started = time.monotonic(); deadline = started+60
    cache = {}
    before = snapshot(registry, cache, deadline)
    for component in before:
        for pod in component['pods']:
            require(time.monotonic() < deadline, 'history compatibility observation exceeded deadline')
            # Execute the exact running PID1 executable, not a newer helper on
            # PATH or a configurable command. Its early diagnostic never opens
            # the DB/configuration or starts a second service.
            raw = command(['kubectl', '-n', component['namespace'], 'exec', pod['name'], '-c', component['container'],
                           '--', '/proc/1/exe', ARGUMENT], timeout=5)
            require(strict_json(raw) == component['current_capabilities'],
                    'live executable capabilities differ from its reviewed source')
    after = snapshot(registry, cache, deadline)
    require(time.monotonic() <= deadline and before == after,
            'history release, rollback, pod or container changed during observation')
    return {'schema': 'fugue.compiler-input-history.registry-compatibility.v1', 'registered_releases': after,
            'scope': 'exact registered live binaries and immutable forward/LKG source/image bindings',
            'retirement_ready': False,
            'remaining_proofs': ['unregistered-and-maintenance-database-client-drain',
                                 'complete-retained-dependency-readback-and-durable-pins',
                                 'controlled-enforcement-transition',
                                 'confidential-coldstart-current-and-promised-rollback-e2e',
                                 'all-retirement-resource-retention-and-physical-release-gates']}
