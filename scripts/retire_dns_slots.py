#!/usr/bin/env python3
"""Declaratively stop obsolete DNS executors without changing serving artifacts."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import re
import time

from scripts import reconcile_dns_transport as transport
from scripts import recover_dns_transport as recovery
from scripts.retire_dns_hostport import active_dns_connections

RETIRED = 'retirement.fugue.dev/dns-slot'
DISABLED = 'retirement.fugue.dev/disabled'


def digest(value):
    return 'sha256:' + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def validate(config):
    fields = {'schema', 'generation', 'namespace', 'origin', 'api_address', 'replacements', 'workloads', 'quiescent_services', 'verifier_revision'}
    if set(config) != fields or config['schema'] != 'fugue.dns-slot-retirement/v1':
        raise ValueError('complete DNS slot retirement declaration required')
    if type(config['generation']) is not int or config['generation'] < 1:
        raise ValueError('positive retirement generation required')
    if not re.fullmatch(r'[a-f0-9]{40}', config['verifier_revision']):
        raise ValueError('verified reader source revision required')
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', config['namespace']):
        raise ValueError('canonical namespace required')
    if not isinstance(config['workloads'], list) or not 1 <= len(config['workloads']) <= 32:
        raise ValueError('bounded obsolete workload set required')
    seen = set()
    for item in config['workloads']:
        if set(item) != {'kind', 'name', 'uid', 'image', 'before_spec_digest', 'retired_spec_digest'} or item['kind'] not in {'DaemonSet', 'Deployment'}:
            raise ValueError('exact supported workload required')
        identity = (item['kind'], item['name'])
        if identity in seen or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,252}', item['name']):
            raise ValueError('duplicate or invalid workload')
        seen.add(identity)
        if not re.fullmatch(r'[a-f0-9-]{36}', item['uid']) or not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', item['image']):
            raise ValueError('immutable workload UID and image required')
        if any(not re.fullmatch(r'sha256:[a-f0-9]{64}', item[key]) for key in ['before_spec_digest', 'retired_spec_digest']):
            raise ValueError('exact before and retired spec digests required')
    if not isinstance(config['replacements'], list) or not 1 <= len(config['replacements']) <= 16:
        raise ValueError('bounded positive replacement set required')
    for replacement in config['replacements']:
        if set(replacement) != {'node', 'service', 'service_uid', 'active_uid', 'active_image', 'standby_selector', 'standby_uid', 'standby_image'}:
            raise ValueError('active and standby replacement identities required')
        if replacement['active_uid'] == replacement['standby_uid'] or not replacement['standby_selector']:
            raise ValueError('independent standby required')
        for key in ['service_uid', 'active_uid', 'standby_uid']:
            if not re.fullmatch(r'[a-f0-9-]{36}', replacement[key]):
                raise ValueError('exact replacement UID required')
        for key in ['active_image', 'standby_image']:
            if not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', replacement[key]):
                raise ValueError('immutable replacement image required')
    services = config['quiescent_services']
    if not isinstance(services, list) or len(services) > 32 or len({s['name'] for s in services}) != len(services):
        raise ValueError('bounded unique internal quiescent Services required')
    for service in services:
        if set(service) != {'name', 'uid', 'spec_digest'} or not re.fullmatch(r'[a-f0-9-]{36}', service['uid']) or not re.fullmatch(r'sha256:[a-f0-9]{64}', service['spec_digest']):
            raise ValueError('quiescent Service identity and exact spec required')
    return config


def retired_spec(workload):
    spec = copy.deepcopy(workload['spec'])
    if workload['kind'] == 'Deployment':
        spec['replicas'] = 0
    else:
        # The same key cannot simultaneously be in and not in this set. Unlike
        # a currently unmatched label, no future node can satisfy this selector.
        affinity = spec['template']['spec'].setdefault('affinity', {})
        node_affinity = affinity.setdefault('nodeAffinity', {})
        node_affinity['requiredDuringSchedulingIgnoredDuringExecution'] = {'nodeSelectorTerms': [
            {'matchExpressions': [{'key': DISABLED, 'operator': 'In', 'values': ['true']},
                                  {'key': DISABLED, 'operator': 'NotIn', 'values': ['true']}]}]}
    return spec


def retirement_patch(workload, item, config):
    meta, spec = workload['metadata'], workload['spec']
    if meta.get('uid') != item['uid'] or not meta.get('resourceVersion') or meta.get('deletionTimestamp') or workload.get('kind') != item['kind']:
        raise ValueError('retirement workload identity changed')
    pod_spec = spec['template']['spec']
    containers = pod_spec['containers']
    if len(containers) != 1 or containers[0].get('name') != 'dns' or containers[0].get('image') != item['image']:
        raise ValueError('retirement executor image changed')
    if pod_spec.get('hostNetwork') or any(port.get('hostPort') for c in containers for port in c.get('ports', [])):
        raise ValueError('release host listeners before retiring DNS slots')
    receipt = str(config['generation']) + ':' + digest(config)
    annotations = dict(meta.get('annotations', {}))
    if annotations.get(RETIRED) == receipt and digest(spec) == item['retired_spec_digest']:
        return []
    if RETIRED in annotations or digest(spec) != item['before_spec_digest']:
        raise ValueError('retirement predecessor spec or generation changed')
    desired = retired_spec(workload)
    if digest(desired) != item['retired_spec_digest']:
        raise ValueError('retirement declaration does not bind the exact stopped spec')
    annotations[RETIRED] = receipt
    return [{'op': 'test', 'path': '/metadata/uid', 'value': item['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': meta['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': spec},
            {'op': 'add', 'path': '/metadata/annotations', 'value': annotations},
            {'op': 'replace', 'path': '/spec', 'value': desired}]


def ensure_unselected(workload, services, quiescent=()):
    labels = workload['spec']['template']['metadata']['labels']
    declared = {entry['name']: entry for entry in quiescent}
    for service in services:
        spec = service.get('spec', {})
        selector = spec.get('selector')
        if selector and all(labels.get(key) == value for key, value in selector.items()):
            expected = declared.get(service['metadata']['name'])
            if expected and service['metadata'].get('uid') == expected['uid'] and digest(spec) == expected['spec_digest']:
                if spec.get('type') == 'ClusterIP' and not spec.get('externalIPs') and not any(port.get('nodePort') or port.get('port') == 53 for port in spec.get('ports', [])):
                    continue
            raise ValueError('obsolete DNS slot is still selected by Service ' + service['metadata']['name'])


def replacements(config):
    namespace = config['namespace']
    publication = recovery.current_publication({'recovery': config})
    witnesses = []
    for replacement in config['replacements']:
        service = transport.read_json('get', 'service', replacement['service'], '-n', namespace, '-o', 'json')
        if service['metadata'].get('uid') != replacement['service_uid'] or service['metadata'].get('deletionTimestamp'):
            raise ValueError('replacement public Service changed')
        spec = service['spec']
        if service['metadata'].get('labels', {}).get('app.kubernetes.io/managed-by') != transport.MANAGER or not spec.get('externalIPs'):
            raise ValueError('replacement is not a managed public listener')
        if {(p['port'], p.get('protocol', 'TCP'), p['targetPort']) for p in spec['ports']} != {(53, 'UDP', 53), (53, 'TCP', 53)}:
            raise ValueError('replacement must cover UDP and TCP DNS')
        active = transport.handoff_backend(namespace, spec['selector'], replacement['node'], 53)
        standby = transport.handoff_backend(namespace, replacement['standby_selector'], replacement['node'], 53)
        for role, backend in [('active', active), ('standby', standby)]:
            verification = {'namespace': namespace, 'recovery': {'candidate_pod_uid': replacement[role + '_uid'],
                            'candidate_image': replacement[role + '_image']}}
            recovery.verify_candidate(backend, verification)
            recovery.validate_publication(backend['snapshot'], publication)
        transport.validate_handoff_snapshots(active['snapshot'], standby['snapshot'], replacement['node'])
        slices = transport.read_json('get', 'endpointslices', '-n', namespace, '-l', 'kubernetes.io/service-name=' + replacement['service'], '-o', 'json')['items']
        transport.validate_handoff_endpoints(service, active['pod'], slices, 53)
        witnesses.append({'service_uid': service['metadata']['uid'], 'service_resource_version': service['metadata']['resourceVersion'],
                          'active_uid': active['pod']['metadata']['uid'], 'standby_uid': standby['pod']['metadata']['uid'],
                          'assignment': active['snapshot']['assignment'], 'plan_digest': active['snapshot']['plan_digest']})
    return witnesses


def inspect_workload(config, item):
    namespace = config['namespace']
    workload = transport.read_json('get', item['kind'], item['name'], '-n', namespace, '-o', 'json')
    operations = retirement_patch(workload, item, config)
    services = transport.read_json('get', 'services', '-n', namespace, '-o', 'json')['items']
    ensure_unselected(workload, services, config['quiescent_services'])
    selector = workload['spec']['selector']['matchLabels']
    query = ','.join(key + '=' + value for key, value in sorted(selector.items()))
    pods = transport.read_json('get', 'pods', '-n', namespace, '-l', query, '-o', 'json')['items']
    if len(pods) > 16:
        raise ValueError('obsolete workload Pod set exceeds bound')
    for pod in pods:
        owners = [owner for owner in pod['metadata'].get('ownerReferences', []) if owner.get('controller')]
        if len(owners) != 1:
            raise ValueError('obsolete Pod ownership ambiguous')
        owner = owners[0]
        if item['kind'] == 'Deployment' and owner['kind'] == 'ReplicaSet':
            rs = transport.read_json('get', 'replicaset', owner['name'], '-n', namespace, '-o', 'json')
            if rs['metadata']['uid'] != owner['uid']:
                raise ValueError('obsolete ReplicaSet identity changed')
            owners = [entry for entry in rs['metadata'].get('ownerReferences', []) if entry.get('controller')]
            owner = owners[0] if len(owners) == 1 else {}
        if owner.get('uid') != item['uid']:
            raise ValueError('obsolete selector includes foreign Pods')
        if operations and pod.get('status', {}).get('phase') == 'Running':
            raw = transport.kubectl('exec', '-n', namespace, pod['metadata']['name'], '-c', 'dns', '--', 'cat', '/proc/net/tcp', '/proc/net/tcp6')
            tcp_ports = {port['containerPort'] for container in pod['spec']['containers'] for port in container.get('ports', [])
                         if port.get('protocol', 'TCP') == 'TCP' and port.get('name') == 'dns-tcp'}
            if not tcp_ports or len(raw) > 2 << 20 or active_dns_connections(raw, tcp_ports):
                raise ValueError('obsolete DNS slot still has active TCP connections')
    return workload, operations, pods


def save(path, report):
    path.write_text(json.dumps(report, sort_keys=True, indent=2) + '\n')
    path.chmod(0o600)


def reconcile(config, path, apply=False):
    validate(config)
    report = {'schema': config['schema'], 'generation': config['generation'], 'declaration_digest': digest(config), 'workloads': []}
    report['replacements'] = replacements(config)
    save(path, report)
    for item in config['workloads']:
        workload, operations, pods = inspect_workload(config, item)
        record = {'kind': item['kind'], 'name': item['name'], 'uid': item['uid'], 'patch': operations, 'applied': False}
        report['workloads'].append(record)
        save(path, report)
        if operations:
            args = ('patch', item['kind'], item['name'], '-n', config['namespace'], '--type=json', '-p', json.dumps(operations))
            transport.kubectl(*args, '--dry-run=server')
            if apply:
                # Independent configuration lane shares the DNS transport lock.
                # Recheck replacements and selectors immediately before exact CAS.
                report['replacements'] = replacements(config)
                current, fresh, _ = inspect_workload(config, item)
                if fresh != operations:
                    raise ValueError('retirement workload changed before CAS')
                transport.kubectl(*args)
                record['applied'] = True
                save(path, report)
        if apply:
            deadline = time.monotonic() + 90
            while True:
                current, fresh, pods = inspect_workload(config, item)
                if fresh:
                    raise ValueError('retirement stopped spec did not persist')
                status = current.get('status', {})
                desired = status.get('desiredNumberScheduled', -1) if item['kind'] == 'DaemonSet' else current['spec']['replicas']
                if not pods and desired == 0 and status.get('observedGeneration', 0) >= current['metadata']['generation']:
                    break
                if time.monotonic() >= deadline:
                    raise ValueError('obsolete DNS Pods did not terminate')
                time.sleep(2)
            record['stopped'] = True
            save(path, report)
    if apply:
        report['replacements_after'] = replacements(config)
        save(path, report)
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config', type=Path)
    parser.add_argument('--evidence', required=True, type=Path)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--validate-only', action='store_true')
    args = parser.parse_args()
    config = validate(json.loads(args.config.read_text()))
    if not args.validate_only:
        reconcile(config, args.evidence, args.apply)


if __name__ == '__main__':
    main()
