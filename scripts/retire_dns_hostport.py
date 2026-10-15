import argparse
import copy
import json
from pathlib import Path
import re
import time

from scripts import reconcile_dns_transport as transport
from scripts import recover_dns_transport as recovery


RETIRED = 'transport.fugue.dev/retired-hostport'


def validate(config):
    fields = {'schema', 'generation', 'namespace', 'node', 'node_uid', 'workload', 'workload_uid', 'image', 'service', 'service_uid', 'candidate_uid', 'candidate_image', 'origin', 'api_address'}
    if not isinstance(config, dict) or set(config) != fields or config['schema'] != 'fugue.dns-hostport-retirement/v1':
        raise ValueError('complete explicit DNS port retirement required')
    if type(config['generation']) is not int or config['generation'] < 1:
        raise ValueError('positive retirement generation required')
    for field in ['namespace', 'node', 'workload', 'service']:
        if not isinstance(config[field], str) or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?', config[field]):
            raise ValueError('canonical retirement object identity required')
    for field in ['node_uid', 'workload_uid', 'service_uid', 'candidate_uid']:
        if not isinstance(config[field], str) or not re.fullmatch(r'[a-f0-9-]{36}', config[field]):
            raise ValueError('exact retirement Kubernetes UID required')
    for field in ['image', 'candidate_image']:
        if not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', config[field]):
            raise ValueError('immutable retirement images required')
    return config


def port_patch(workload, config):
    metadata, spec = workload['metadata'], workload['spec']
    if metadata.get('uid') != config['workload_uid'] or not metadata.get('resourceVersion') or metadata.get('deletionTimestamp'):
        raise ValueError('retirement workload identity changed')
    pod_spec = spec['template']['spec']
    if pod_spec.get('hostNetwork') or len(pod_spec['containers']) != 1 or spec.get('updateStrategy', {}).get('type') != 'RollingUpdate':
        raise ValueError('retirement requires one isolated rolling DNS container')
    container = pod_spec['containers'][0]
    if container.get('image') != config['image'] or container.get('name') != 'dns':
        raise ValueError('retirement container differs from declared predecessor')
    ports = container.get('ports', [])
    claimed = [port for port in ports if port.get('hostPort')]
    if not claimed:
        if metadata.get('annotations', {}).get(RETIRED) != str(config['generation']):
            raise ValueError('missing DNS claim has no retirement receipt')
        return []
    if len(claimed) != 2 or {(port.get('hostPort'), port.get('protocol', 'TCP')) for port in claimed} != {(53, 'UDP'), (53, 'TCP')}:
        raise ValueError('retirement cannot alter unrelated host ports')
    updated = copy.deepcopy(ports)
    for port in updated:
        if port.get('hostPort') == 53:
            port.pop('hostPort')
            port.pop('hostIP', None)
    annotations = dict(metadata.get('annotations', {}))
    if RETIRED in annotations and annotations[RETIRED] != str(config['generation']):
        raise ValueError('retirement generation changed')
    annotations[RETIRED] = str(config['generation'])
    return [{'op': 'test', 'path': '/metadata/uid', 'value': metadata['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': metadata['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': spec},
            {'op': 'add', 'path': '/metadata/annotations', 'value': annotations},
            {'op': 'replace', 'path': '/spec/template/spec/containers/0/ports', 'value': updated}]


def active_dns_connections(raw, ports=(53,)):
    active = 0
    for line in raw.splitlines():
        columns = line.split()
        if not columns or columns[0] == 'sl':
            continue
        if len(columns) < 4 or ':' not in columns[1]:
            raise ValueError('DNS connection observation is malformed')
        if int(columns[1].rsplit(':', 1)[1], 16) in ports and columns[3] not in {'0A', '06'}:
            active += 1
    return active


def inspect(config):
    namespace = config['namespace']
    workload = transport.read_json('get', 'daemonset', config['workload'], '-n', namespace, '-o', 'json')
    operations = port_patch(workload, config)
    node = transport.read_json('get', 'node', config['node'], '-o', 'json')
    if node['metadata']['uid'] != config['node_uid'] or not any(item.get('type') == 'Ready' and item.get('status') == 'True' for item in node.get('status', {}).get('conditions', [])):
        raise ValueError('retirement node identity or readiness changed')
    service = transport.read_json('get', 'service', config['service'], '-n', namespace, '-o', 'json')
    metadata, spec = service['metadata'], service['spec']
    if metadata.get('uid') != config['service_uid'] or metadata.get('labels', {}).get('app.kubernetes.io/managed-by') != transport.MANAGER or len(spec.get('externalIPs', [])) != 1:
        raise ValueError('retirement public Service authority changed')
    if {(port['port'], port.get('protocol', 'TCP'), port['targetPort']) for port in spec['ports']} != {(53, 'UDP', 53), (53, 'TCP', 53)}:
        raise ValueError('retirement public listener ports differ')
    declaration = {'namespace': namespace, 'generation': int(metadata.get('annotations', {}).get(transport.GENERATION, '0'))}
    listener = {'name': config['service'], 'address': spec['externalIPs'][0], 'port': 53, 'targetPort': 53, 'selector': spec['selector']}
    transport.validate_existing(service, transport.service(declaration, listener))
    candidate = transport.handoff_backend(namespace, spec['selector'], config['node'], 53)
    verifier_config = {'namespace': namespace, 'recovery': {'candidate_pod_uid': config['candidate_uid'], 'candidate_image': config['candidate_image'],
                                                          'origin': config['origin'], 'api_address': config['api_address']}}
    recovery.verify_candidate(candidate, verifier_config)
    recovery.validate_publication(candidate['snapshot'], recovery.current_publication(verifier_config))
    slices = transport.read_json('get', 'endpointslices', '-n', namespace, '-l', 'kubernetes.io/service-name=' + config['service'], '-o', 'json')['items']
    transport.validate_handoff_endpoints(service, candidate['pod'], slices, 53)
    selector = workload['spec']['selector']['matchLabels']
    if all(candidate['pod']['metadata']['labels'].get(key) == value for key, value in selector.items()):
        raise ValueError('retirement would replace the selected DNS candidate')
    query = ','.join(key + '=' + value for key, value in sorted(selector.items()))
    pods = transport.read_json('get', 'pods', '-n', namespace, '-l', query, '-o', 'json')['items']
    if len(pods) != 1 or pods[0].get('spec', {}).get('nodeName') != config['node']:
        raise ValueError('retirement requires exactly one declared old DNS Pod')
    old = pods[0]
    if old['metadata'].get('deletionTimestamp') or not any(owner.get('uid') == config['workload_uid'] and owner.get('controller') for owner in old['metadata'].get('ownerReferences', [])):
        raise ValueError('retirement Pod ownership changed')
    if operations:
        if transport.ready_pod(old):
            raise ValueError('healthy old DNS requires an explicit connection drain first')
        claimed = [port for container in old['spec']['containers'] for port in container.get('ports', []) if port.get('hostPort')]
        if len(claimed) != 2 or any(port.get('hostIP') not in spec['externalIPs'] for port in claimed):
            raise ValueError('old DNS host ports do not match the selected public address')
        raw = transport.kubectl('exec', '-n', namespace, old['metadata']['name'], '-c', 'dns', '--', 'cat', '/proc/net/tcp', '/proc/net/tcp6')
        if len(raw) > 2 << 20 or active_dns_connections(raw):
            raise ValueError('old DNS still has active TCP connections')
    return workload, operations, {'old_pod_uid': old['metadata']['uid'], 'candidate_uid': candidate['pod']['metadata']['uid'],
                                  'service_uid': metadata['uid'], 'service_resource_version': metadata['resourceVersion'],
                                  'assignment': candidate['snapshot']['assignment'], 'plan_digest': candidate['snapshot']['plan_digest']}


def reconcile(config, evidence, apply=False):
    validate(config)
    workload, operations, witness = inspect(config)
    report = {'schema': config['schema'], 'generation': config['generation'], 'workload_uid': config['workload_uid'],
              'witness': witness, 'patch': operations, 'applied': False, 'state_materials_preserved': True}
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.touch(mode=0o600, exist_ok=True)
    evidence.chmod(0o600)
    evidence.write_text(json.dumps(report, sort_keys=True) + '\n')
    if operations:
        command = ('patch', 'daemonset', config['workload'], '-n', config['namespace'], '--type=json', '--field-manager=fugue-dns-port-retirement', '--patch-file=/dev/stdin')
        transport.kubectl(*command, '--dry-run=server', body=json.dumps(operations))
        if apply:
            fresh_workload, fresh_patch, fresh = inspect(config)
            if witness != fresh or workload['metadata']['resourceVersion'] != fresh_workload['metadata']['resourceVersion']:
                raise ValueError('DNS retirement witness changed before CAS')
            transport.kubectl(*command, body=json.dumps(fresh_patch))
            report['applied'] = True
            evidence.write_text(json.dumps(report, sort_keys=True) + '\n')
            for attempt in range(12):
                pods = transport.read_json('get', 'pods', '-n', config['namespace'], '--field-selector', 'spec.nodeName=' + config['node'], '-o', 'json')['items']
                old = [pod for pod in pods if pod['metadata']['uid'] == witness['old_pod_uid']]
                if not old:
                    report['old_pod_retired'] = True
                    break
                if attempt < 11:
                    time.sleep(5)
            if not report.get('old_pod_retired'):
                raise ValueError('old DNS port release has not completed')
            evidence.write_text(json.dumps(report, sort_keys=True) + '\n')
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--evidence', required=True, type=Path)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    result = reconcile(json.loads(Path(args.config).read_text()), args.evidence, args.apply)
    print(json.dumps({'applied': result['applied'], 'state_materials_preserved': result['state_materials_preserved']}))
