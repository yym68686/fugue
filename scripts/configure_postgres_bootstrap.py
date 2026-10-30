#!/usr/bin/env python3
"""Apply an explicitly enrolled CNPG init CPU request and replace one Pod at a time.

Uses the existing CNPG podPatch contract. Database images, runtime resources,
memory, limits, storage and operator configuration are never mutation targets.
Run only through the declarative CI entrypoint for production.
"""
import argparse
import copy
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import time

ANNOTATION = 'cnpg.io/podPatch'
INIT = 'bootstrap-controller'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def cpu_milli(value):
    match = re.fullmatch(r'([0-9]+)(m?)', str(value))
    if not match:
        raise ValueError('CPU request must use integer cores or millicores')
    return int(match[1]) * (1 if match[2] else 1000)


def quantity(value):
    if value is None:
        return Decimal(0)
    match = re.fullmatch(r'([0-9]+(?:\.[0-9]+)?)([KMGTPE]i|[numkKMGTPE]|[eE][+-]?[0-9]+)?', str(value))
    if not match:
        raise ValueError('Unsupported resource quantity')
    number, suffix = Decimal(match[1]), match[2] or ''
    if suffix.endswith('i'):
        return number * (Decimal(1024) ** ('KMGTPE'.index(suffix[0]) + 1))
    if suffix.startswith(('e', 'E')) and len(suffix) > 1:
        return number * (Decimal(10) ** int(suffix[1:]))
    return number * {'': Decimal(1), 'n': Decimal('1e-9'), 'u': Decimal('1e-6'), 'm': Decimal('1e-3'),
                     **{s: Decimal(1000) ** (i + 1) for i, s in enumerate('kMGTPE')}, 'K': Decimal(1000)}[suffix]


def pod_request(pod, resource):
    # Conservatively include allocation still held by an in-progress resize.
    # Summing all init containers also covers native sidecars without
    # underestimating their overlap with later init containers.
    statuses = {c['name']: c for c in pod.get('status', {}).get('containerStatuses', [])}
    main = sum(max(quantity(c.get('resources', {}).get('requests', {}).get(resource)),
                   quantity(statuses.get(c['name'], {}).get('allocatedResources', {}).get(resource)),
                   quantity(statuses.get(c['name'], {}).get('resources', {}).get('requests', {}).get(resource)))
               for c in pod['spec'].get('containers', []))
    init = sum(quantity(c.get('resources', {}).get('requests', {}).get(resource)) for c in pod['spec'].get('initContainers', []))
    return max(main + sum(quantity(c.get('resources', {}).get('requests', {}).get(resource))
                          for c in pod['spec'].get('initContainers', []) if c.get('restartPolicy') == 'Always'), init,
               quantity(pod['spec'].get('resources', {}).get('requests', {}).get(resource))) + quantity(pod['spec'].get('overhead', {}).get(resource))


def capacity_witness(target, pod, node, pods):
    if (node['metadata']['name'] != target['node'] or node['spec'].get('unschedulable')
            or not any(c['type'] == 'Ready' and c['status'] == 'True' for c in node['status']['conditions'])):
        raise ValueError('Database node is not schedulable and Ready')
    witness = {}
    for resource in ('cpu', 'memory'):
        used = sum(pod_request(p, resource) for p in pods['items'] if p['spec'].get('nodeName') == target['node']
                   and p.get('status', {}).get('phase') not in ('Succeeded', 'Failed'))
        available = quantity(node['status']['allocatable'][resource]) - used
        required = pod_request(pod, resource)
        # Require a full replacement's spare capacity before removing the old
        # Pod. Its released allocation is deliberately not counted as headroom.
        if available < required:
            raise ValueError(f'Insufficient spare {resource} for a controlled database restart')
        witness[resource] = {'spare': str(available), 'required': str(required)}
    return witness


def validate_config(config):
    if set(config) != {'schema', 'apply', 'operator', 'targets'} or config['schema'] != 'fugue.postgres-bootstrap-cpu/v1' or type(config['apply']) is not bool:
        raise ValueError('Unsupported bootstrap CPU declaration')
    operator = config['operator']
    if set(operator) != {'namespace', 'deployment', 'uid', 'image'} or not re.fullmatch(r'[^\s@]+@sha256:[0-9a-f]{64}', operator['image']):
        raise ValueError('An exact existing operator is required')
    if not isinstance(config['targets'], list) or not 1 <= len(config['targets']) <= 16:
        raise ValueError('Explicit bounded database enrollment is required')
    seen = set()
    required = {'namespace', 'cluster', 'clusterUID', 'systemID', 'pod', 'originalPodUID', 'node', 'cpuRequest', 'expectedMainResources', 'expectedInitResources'}
    for target in config['targets']:
        if set(target) != required:
            raise ValueError('Unsupported target fields')
        for key in ('namespace', 'cluster', 'pod', 'node'):
            if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,252}', target[key]):
                raise ValueError('Invalid target identity')
        identity = (target['namespace'], target['cluster'])
        if identity in seen or not re.fullmatch(r'[0-9]+', target['systemID']):
            raise ValueError('Duplicate database or missing system ID')
        seen.add(identity)
        for key in ('clusterUID', 'originalPodUID'):
            if not re.fullmatch(r'[0-9a-f-]{36}', target[key]):
                raise ValueError('Exact resource UID required')
        old = cpu_milli(target['expectedInitResources']['requests']['cpu'])
        if not 1 <= cpu_milli(target['cpuRequest']) < old:
            raise ValueError('Only positive init CPU request reductions are allowed')
    return config


def ready(pod):
    return (not pod['metadata'].get('deletionTimestamp') and pod.get('status', {}).get('phase') == 'Running'
            and any(c['type'] == 'Ready' and c['status'] == 'True' for c in pod['status'].get('conditions', [])))


def bootstrap(pod):
    matches = [(i, c) for i, c in enumerate(pod['spec'].get('initContainers', [])) if c['name'] == INIT]
    if len(matches) != 1 or matches[0][1].get('restartPolicy') == 'Always':
        raise ValueError('Expected one ordinary bootstrap init container')
    return matches[0]


def desired_resources(target):
    resources = copy.deepcopy(target['expectedInitResources'])
    resources['requests']['cpu'] = target['cpuRequest']
    return resources


def annotation_value(target, pod):
    index, _ = bootstrap(pod)
    return canonical([
        {'op': 'test', 'path': f'/spec/initContainers/{index}/name', 'value': INIT},
        {'op': 'replace', 'path': f'/spec/initContainers/{index}/resources/requests/cpu', 'value': target['cpuRequest']},
    ])


def validate_target(target, cluster, pod, require_ready=True):
    meta, spec, status = cluster['metadata'], cluster['spec'], cluster.get('status', {})
    if (meta['name'], meta['namespace'], meta['uid']) != (target['cluster'], target['namespace'], target['clusterUID']) or meta.get('deletionTimestamp'):
        raise ValueError('Database identity changed')
    if str(status.get('systemID')) != target['systemID'] or spec.get('instances') != 1 or status.get('currentPrimary') != target['pod'] or status.get('targetPrimary') != target['pod']:
        raise ValueError('Database system ID, primary or replica policy changed')
    if meta.get('annotations', {}).get('cnpg.io/hibernation') == 'on':
        raise ValueError('Hibernating database cannot be restarted')
    pm = pod['metadata']
    if (pm['name'], pm['namespace']) != (target['pod'], target['namespace']) or pod['spec'].get('nodeName') != target['node']:
        raise ValueError('Pod identity or placement changed')
    if not any(o.get('uid') == target['clusterUID'] and o.get('kind') == 'Cluster' and o.get('controller') is True for o in pm.get('ownerReferences', [])):
        raise ValueError('Pod is not owned by the enrolled database')
    containers = pod['spec']['containers']
    if len(containers) != 1 or containers[0]['name'] != 'postgres' or containers[0]['resources'] != target['expectedMainResources']:
        raise ValueError('Database runtime resources changed')
    _, init = bootstrap(pod)
    if init['resources'] not in (target['expectedInitResources'], desired_resources(target)):
        raise ValueError('Bootstrap resources changed outside the enrolled CPU request')
    if require_ready and (not ready(pod) or status.get('readyInstances') != 1):
        raise ValueError('Database must be healthy before continuing')
    if pm['uid'] != target['originalPodUID'] and init['resources'] != desired_resources(target):
        raise ValueError('Unrecognized replacement Pod; refusing another restart')


def plan(target, cluster, pod):
    validate_target(target, cluster, pod)
    annotations = cluster['metadata'].get('annotations', {})
    value = annotation_value(target, pod)
    previous = annotations.get(ANNOTATION)
    if previous is not None and previous != value:
        raise ValueError('Existing Pod customization must not be overwritten')
    if previous == value:
        return [], value
    updated = {**annotations, ANNOTATION: value}
    return [
        {'op': 'test', 'path': '/metadata/uid', 'value': target['clusterUID']},
        {'op': 'test', 'path': '/metadata/resourceVersion', 'value': cluster['metadata']['resourceVersion']},
        {'op': 'test', 'path': '/spec', 'value': cluster['spec']},
        {'op': 'add', 'path': '/metadata/annotations', 'value': updated},
    ], value


def outside_snapshot(config, clusters, pods):
    enrolled = {(t['namespace'], t['cluster']) for t in config['targets']}
    return {
        'clusters': {c['metadata']['uid']: digest(c['spec']) for c in clusters['items'] if (c['metadata']['namespace'], c['metadata']['name']) not in enrolled},
        'pods': {p['metadata']['uid']: {
            'namespace': p['metadata']['namespace'], 'name': p['metadata']['name'], 'spec': digest(p['spec']),
            'restarts': {c['name']: c.get('restartCount', 0) for c in p.get('status', {}).get('containerStatuses', [])},
        } for p in pods['items'] if (p['metadata']['namespace'], p['metadata'].get('labels', {}).get('cnpg.io/cluster')) not in enrolled},
    }


class Runner:
    def __init__(self, config, evidence_path):
        self.config, self.path = validate_config(config), evidence_path
        self.command = shlex.split(os.environ.get('KUBECTL', 'kubectl'))
        self.evidence = {'configDigest': digest(config), 'targets': [], 'mode': 'preview'}

    def kube(self, *args, body=None, text=False):
        result = subprocess.run(self.command + ['--request-timeout=30s', *args], input=None if body is None else canonical(body),
                                text=True, capture_output=True, timeout=60, check=False)
        if result.returncode:
            raise RuntimeError(f'kubectl {args[0]} failed: {result.stderr[:1200]}')
        return result.stdout.strip() if text else json.loads(result.stdout)

    def get(self, kind, name=None, namespace=None, *extra):
        return self.kube(*(['-n', namespace] if namespace else []), 'get', kind, *([name] if name else []), *extra, '-o', 'json')

    def save(self):
        self.path.write_text(json.dumps(self.evidence, indent=2) + '\n')

    def operator(self):
        expected = self.config['operator']
        live = self.get('deployment', expected['deployment'], expected['namespace'])
        status = live.get('status', {})
        managers = [c for c in live['spec']['template']['spec']['containers'] if c['name'] == 'manager']
        if live['metadata']['uid'] != expected['uid'] or len(managers) != 1 or managers[0]['image'] != expected['image']:
            raise ValueError('Existing operator identity changed')
        if status.get('observedGeneration') != live['metadata']['generation'] or status.get('readyReplicas') != live['spec']['replicas'] or status.get('updatedReplicas') != live['spec']['replicas']:
            raise ValueError('Operator is not fully healthy')

    def target(self, target):
        return self.get('cluster.postgresql.cnpg.io', target['cluster'], target['namespace']), self.get('pod', target['pod'], target['namespace'])

    def outside(self):
        return outside_snapshot(self.config, self.get('clusters.postgresql.cnpg.io', None, None, '-A'), self.get('pods', None, None, '-A', '-l', 'cnpg.io/cluster'))

    def sql_identity(self, target):
        value = self.kube('-n', target['namespace'], 'exec', target['pod'], '-c', 'postgres', '--',
                          'psql', '-X', '-A', '-t', '-U', 'postgres', '-d', 'postgres', '-c',
                          'SELECT system_identifier::text FROM pg_control_system()', text=True)
        if value != target['systemID']:
            raise ValueError('SQL database identity probe failed')

    def claims(self, target):
        pvcs = self.get('pvc', None, target['namespace'], '-l', 'cnpg.io/cluster=' + target['cluster'])
        return {p['metadata']['uid']: {'name': p['metadata']['name'], 'spec': digest(p['spec']), 'phase': p.get('status', {}).get('phase')} for p in pvcs['items']}

    def capacity(self, target, pod):
        return capacity_witness(target, pod, self.get('node', target['node']),
                                self.get('pods', None, None, '-A', '--field-selector=spec.nodeName=' + target['node']))

    def patch(self, target, patch, preview=False):
        if patch:
            return self.kube('-n', target['namespace'], 'patch', 'cluster.postgresql.cnpg.io', target['cluster'], '--type=json', '--patch-file=/dev/stdin', '-o', 'json', *(['--dry-run=server'] if preview else []), body=patch)

    def prepare(self):
        self.operator()
        self.evidence['outsideBefore'] = self.outside()
        for target in self.config['targets']:
            cluster, pod = self.target(target)
            patch, value = plan(target, cluster, pod)
            self.sql_identity(target)
            claims = self.claims(target)
            if not claims or any(p['phase'] != 'Bound' for p in claims.values()):
                raise ValueError('Database claims are not bound')
            self.patch(target, patch, True)
            headroom = self.capacity(target, pod)
            self.evidence['targets'].append({'namespace': target['namespace'], 'cluster': target['cluster'], 'podUID': pod['metadata']['uid'],
                                              'annotation': value, 'claims': claims, 'headroom': headroom, 'clusterSpec': digest(cluster['spec']), 'status': 'prepared'})
        self.evidence['preparedAt'] = time.time()
        self.save()

    def replace(self, target, pod, preview=False):
        # Graceful deletion of exactly one observed Pod; retain the existing
        # termination grace period. UID/RV preconditions reject replacement races.
        options = {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'preconditions': {
            'uid': pod['metadata']['uid'], 'resourceVersion': pod['metadata']['resourceVersion']}}
        path = '/api/v1/namespaces/' + target['namespace'] + '/pods/' + target['pod']
        if preview:
            # The DELETE handler decodes options from the body when present;
            # URL query options are ignored in that case.
            options['dryRun'] = ['All']
        self.kube('delete', '--raw', path, '-f', '-', body=options)

    def wait(self, target, receipt):
        deadline, stable = time.monotonic() + 360, None
        while time.monotonic() < deadline:
            try:
                cluster, pod = self.target(target)
            except RuntimeError as error:
                if 'NotFound' not in str(error):
                    raise
                stable = None
                time.sleep(3)
                continue
            if pod['metadata']['uid'] != target['originalPodUID']:
                observed = receipt.setdefault('observedReplacementUID', pod['metadata']['uid'])
                if observed != pod['metadata']['uid']:
                    raise ValueError('Replacement Pod was replaced again during verification')
            if pod['metadata']['uid'] == target['originalPodUID'] or not ready(pod) or cluster.get('status', {}).get('readyInstances') != 1:
                stable = None
                time.sleep(3)
                continue
            validate_target(target, cluster, pod)
            if cluster['metadata'].get('annotations', {}).get(ANNOTATION) != receipt['annotation'] or digest(cluster['spec']) != receipt['clusterSpec'] or self.claims(target) != receipt['claims']:
                raise ValueError('Database configuration or storage changed during restart')
            if bootstrap(pod)[1]['resources'] != desired_resources(target):
                raise ValueError('Replacement Pod did not receive bootstrap CPU request')
            statuses = pod['status'].get('initContainerStatuses', [])
            init_status = next((s for s in statuses if s['name'] == INIT), {})
            terminated = init_status.get('state', {}).get('terminated', {})
            if terminated.get('exitCode') != 0 or init_status.get('restartCount', 0) or any(c.get('restartCount', 0) for c in pod['status'].get('containerStatuses', [])):
                raise ValueError('New Pod has failed or restarted containers')
            self.sql_identity(target)
            stable = stable or time.monotonic()
            if time.monotonic() - stable >= 30:
                receipt.update(status='verified', newPodUID=pod['metadata']['uid'], initResources=bootstrap(pod)[1]['resources'],
                               initStartedAt=terminated.get('startedAt'), initFinishedAt=terminated.get('finishedAt'), systemID=target['systemID'])
                self.save()
                return
            time.sleep(3)
        raise RuntimeError('Database replacement did not remain healthy within 360 seconds; stop before the next database')

    def execute(self, prepared):
        if not self.config['apply'] or prepared.get('configDigest') != digest(self.config) or not 0 <= time.time() - prepared.get('preparedAt', 0) < 600:
            raise ValueError('Apply requires a fresh matching prepared witness')
        self.evidence = copy.deepcopy(prepared)
        self.evidence['mode'] = 'apply'
        self.save()
        self.operator()
        for target, receipt in zip(self.config['targets'], self.evidence['targets'], strict=True):
            if self.outside() != prepared['outsideBefore']:
                raise ValueError('Other database inventory changed; stop without modifying it')
            cluster, pod = self.target(target)
            patch, _ = plan(target, cluster, pod)
            if digest(cluster['spec']) != receipt['clusterSpec'] or self.claims(target) != receipt['claims']:
                raise ValueError('Prepared database configuration or claims changed')
            self.sql_identity(target)
            self.patch(target, patch, True)
            if pod['metadata']['uid'] == target['originalPodUID']:
                self.capacity(target, pod)
                self.replace(target, pod, True)
            self.patch(target, patch)
            cluster, pod = self.target(target)
            validate_target(target, cluster, pod)
            if cluster['metadata'].get('annotations', {}).get(ANNOTATION) != receipt['annotation']:
                raise ValueError('Bootstrap annotation did not persist')
            if pod['metadata']['uid'] == target['originalPodUID']:
                self.capacity(target, pod)
                receipt['status'] = 'replacing'
                receipt['replacementRequestedAt'] = time.time()
                self.save()
                self.replace(target, pod)
            self.wait(target, receipt)
        after = self.outside()
        self.evidence['outsideAfter'] = after
        self.evidence['otherDatabasesUnchanged'] = after == prepared['outsideBefore']
        self.save()
        if not self.evidence['otherDatabasesUnchanged']:
            raise ValueError('Other database inventory changed during the test; inspect the evidence')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--prepared')
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    runner = Runner(json.loads(Path(args.config).read_text()), Path(args.evidence))
    try:
        if args.prepared:
            runner.execute(json.loads(Path(args.prepared).read_text()))
        else:
            runner.prepare()
    except Exception as error:
        runner.evidence['error'] = str(error)
        runner.save()
        raise
    print(canonical({'mode': runner.evidence['mode'], 'targets': runner.evidence['targets'], 'otherDatabasesUnchanged': runner.evidence.get('otherDatabasesUnchanged')}))


if __name__ == '__main__':
    main()
