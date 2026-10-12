import argparse
import datetime
import hashlib
import json
from pathlib import Path
import re

from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json


MANAGER = 'fugue-fixed-node-retention'
TAINTS = {'node.kubernetes.io/not-ready', 'node.kubernetes.io/unreachable'}


def digest(value):
    return 'sha256:' + hashlib.sha256(canonical(value).encode()).hexdigest()


def validate(config):
    if set(config) != {'schema', 'generation', 'expected_previous_digest', 'namespace', 'state_configmap', 'owner_manager', 'pod_selector', 'nodes'} or config['schema'] != 'fugue.fixed-node-retention/v1':
        raise ValueError('complete fixed-node retention declaration required')
    if type(config['generation']) is not int or config['generation'] < 1 or config['generation'] > 2**53-1:
        raise ValueError('positive retention generation required')
    if config['expected_previous_digest'] and not re.fullmatch(r'sha256:[a-f0-9]{64}', config['expected_previous_digest']):
        raise ValueError('exact retention predecessor required')
    for field in ['namespace', 'state_configmap', 'owner_manager']:
        if not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', config[field]):
            raise ValueError('canonical retention identity required')
    selector = config['pod_selector']
    if not isinstance(selector, dict) or not 1 <= len(selector) <= 8 or any(not isinstance(key, str) or not isinstance(value, str) or not key or not value or any(character in key + value for character in ',= \r\n') for key, value in selector.items()):
        raise ValueError('explicit bounded retention selector required')
    if selector.get('app.kubernetes.io/managed-by') != config['owner_manager']:
        raise ValueError('retention selector must bind the declared owner')
    if not isinstance(config['nodes'], list) or not 1 <= len(config['nodes']) <= 64:
        raise ValueError('bounded retention node set required')
    seen = set()
    for node in config['nodes']:
        if set(node) != {'id', 'uid', 'reason', 'expected_recovery_date'} or node['id'] in seen or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', node['id']):
            raise ValueError('unique canonical retention node required')
        seen.add(node['id'])
        if not re.fullmatch(r'[a-f0-9-]{36}', node['uid']) or node['reason'] not in {'quota_exhausted', 'maintenance', 'network_unavailable'}:
            raise ValueError('exact retention UID and typed reason required')
        if node['expected_recovery_date']:
            datetime.date.fromisoformat(node['expected_recovery_date'])
    return config


def retain_tolerations(tolerations):
    result = [dict(value) for value in tolerations]
    for key in sorted(TAINTS):
        matches = [item for item in result if item.get('key') == key and item.get('effect') == 'NoExecute']
        if len(matches) > 1 or matches and matches[0].get('operator', 'Equal') != 'Exists':
            raise ValueError('ambiguous fixed-node NoExecute toleration')
        if matches:
            matches[0].pop('tolerationSeconds', None)
        else:
            result.append({'key': key, 'operator': 'Exists', 'effect': 'NoExecute'})
    return result


def pod_patch(pod, node, selector):
    metadata, spec, status = pod['metadata'], pod['spec'], pod.get('status', {})
    if metadata.get('deletionTimestamp'):
        return None
    if spec.get('nodeName') != node or not all(metadata.get('labels', {}).get(key) == value for key, value in selector.items()):
        raise ValueError('fixed-node Pod identity changed')
    if status.get('phase') != 'Pending' or status.get('containerStatuses') or status.get('initContainerStatuses'):
        raise ValueError('live fixed-node container requires separate session-preserving maintenance')
    previous = spec.get('tolerations', [])
    desired = retain_tolerations(previous)
    if previous == desired:
        return None
    return [{'op': 'test', 'path': '/metadata/uid', 'value': metadata['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': metadata['resourceVersion']},
            {'op': 'test', 'path': '/spec/nodeName', 'value': node},
            {'op': 'test', 'path': '/status', 'value': status},
            {'op': 'add', 'path': '/spec/tolerations', 'value': desired}]


def reconcile(config, evidence_path, apply=False, kube=kubectl):
    validate(config)
    namespace = config['namespace']
    read = lambda arguments: strict_json(kube(['get'] + arguments + ['-o', 'json']))
    raw = kube(['get', 'configmap', config['state_configmap'], '-n', namespace, '--ignore-not-found', '-o', 'json'])
    current = strict_json(raw) if raw.strip() else None
    if current:
        metadata = current['metadata']
        previous = strict_json(current.get('data', {}).get('declaration.json', ''))
        if metadata.get('labels', {}).get('app.kubernetes.io/managed-by') != MANAGER or metadata.get('deletionTimestamp'):
            raise ValueError('retention configuration ownership changed')
        if previous != config and (config['expected_previous_digest'] != digest(previous) or config['generation'] != previous['generation']+1):
            raise ValueError('retention configuration predecessor changed')
    elif config['generation'] != 1 or config['expected_previous_digest']:
        raise ValueError('missing retention configuration requires explicit recovery')
    operations, observations = [], []
    query = ','.join(key + '=' + value for key, value in sorted(config['pod_selector'].items()))
    for declared in config['nodes']:
        node = read(['node', declared['id']])
        if node['metadata']['uid'] != declared['uid'] or node['metadata'].get('deletionTimestamp'):
            raise ValueError('retention node identity changed')
        ready = any(condition.get('type') == 'Ready' and condition.get('status') == 'True' for condition in node.get('status', {}).get('conditions', []))
        if ready:
            raise ValueError('node recovered; fresh admission and serving evidence must be reviewed before changing the retention declaration')
        pods = read(['pods', '-n', namespace, '-l', query, '--field-selector', 'spec.nodeName=' + declared['id']])
        if pods.get('metadata', {}).get('continue') or len(pods['items']) > 4096:
            raise ValueError('retention inventory exceeds bound')
        selected, retained = 0, 0
        for pod in pods['items']:
            operation = pod_patch(pod, declared['id'], config['pod_selector'])
            if pod['metadata'].get('deletionTimestamp'):
                retained += 1
                continue
            owners = [owner for owner in pod['metadata'].get('ownerReferences', []) if owner.get('controller') is True and owner.get('kind') == 'ReplicaSet']
            if len(owners) != 1:
                raise ValueError('retention Pod must have one ReplicaSet owner')
            replica = read(['replicaset', owners[0]['name'], '-n', namespace])
            if replica['metadata']['uid'] != owners[0]['uid'] or replica['spec']['template']['spec'].get('nodeName') != declared['id'] or replica['metadata'].get('labels', {}).get('app.kubernetes.io/managed-by') != config['owner_manager']:
                raise ValueError('retention Pod owner is not fixed to the declared node')
            selected += 1
            if operation:
                command = ['patch', 'pod', pod['metadata']['name'], '-n', namespace, '--type=json', '--field-manager=' + MANAGER, '--patch-file=/dev/stdin']
                kube(command + ['--dry-run=server'], operation)
                operations.append((command, operation))
        observations.append({'node': declared['id'], 'node_uid': declared['uid'], 'pending_instances': selected, 'terminating_materials_retained': retained, 'expected_recovery_date': declared['expected_recovery_date'], 'automatic_readmission': False})
    report = {'schema': config['schema'], 'declaration_digest': digest(config), 'observations': observations,
              'patches': [{'command': command, 'patch': operation} for command, operation in operations], 'applied': []}
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    evidence_path.touch(mode=0o600, exist_ok=True)
    evidence_path.chmod(0o600)
    def save():
        evidence_path.write_text(canonical(report) + '\n')
    save()
    if apply:
        for command, operation in operations:
            kube(command, operation)
            report['applied'].append(operation[0]['value'])
            save()
        metadata = {'name': config['state_configmap'], 'namespace': namespace, 'labels': {'app.kubernetes.io/managed-by': MANAGER}}
        if current:
            metadata.update(uid=current['metadata']['uid'], resourceVersion=current['metadata']['resourceVersion'])
        desired = {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': metadata,
                   'data': {'declaration.json': canonical(config), 'observations.json': canonical(observations)}}
        kube(['replace' if current else 'create', '-f', '-'], desired)
        persisted = read(['configmap', config['state_configmap'], '-n', namespace])
        if persisted.get('data') != desired['data']:
            raise ValueError('retention configuration read-back failed')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    report = reconcile(strict_json(Path(args.config).read_bytes()), args.evidence, args.apply)
    print(canonical({'declaration_digest': report['declaration_digest'], 'observations': report['observations'], 'changed_pods': len(report['applied'])}))
