#!/usr/bin/env python3
"""Declare one UID-bound code-release hold without changing serving workloads.

The configuration lane shares the release and authority leases with Guardian.
Removing a hold requires a separate, proved member recovery; this writer cannot
release a hold on the basis of Ready, an elapsed date, or one successful probe.
"""
import argparse
import json
import re
from pathlib import Path

from scripts.reconcile_edge_member import Recovery, canonical, now, timestamp

ANNOTATION = 'fugue.io/edge-release-hold'
CAPABILITY = 'fugue.pro/edge-release-hold'
SCHEMA = 'fugue.edge-release-hold/v1'


def validate(c):
    fields = {'schema', 'generation', 'namespace', 'component', 'group', 'node', 'node_uid', 'reason', 'readers'}
    if set(c) != fields or c['schema'] != 'fugue.edge-release-hold-declaration/v1' or type(c['generation']) is not int or c['generation'] < 1:
        raise ValueError('Exact versioned hold declaration required')
    for key in ('namespace', 'component', 'group', 'node'):
        if not isinstance(c[key], str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,200}', c[key]):
            raise ValueError('Canonical release identity required')
    if not c['component'].startswith('edge-worker-') or not isinstance(c['node_uid'], str) or not re.fullmatch(r'[a-zA-Z0-9-]{1,100}', c['node_uid']):
        raise ValueError('Worker component and exact Node UID required')
    if c['reason'] not in ('quota_exhausted', 'network_unavailable', 'maintenance'):
        raise ValueError('Unsupported hold reason')
    if not isinstance(c['readers'], list) or len(c['readers']) != 2:
        raise ValueError('Both guardian and canary reader identities required')
    roles = set()
    names = set()
    for reader in c['readers']:
        if set(reader) != {'role', 'deployment', 'uid', 'image', 'revision'} or reader['role'] not in ('guardian', 'canary-prober') or reader['role'] in roles or reader['deployment'] in names:
            raise ValueError('Unique exact reader identities required')
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,200}', reader['deployment']) or not re.fullmatch(r'[a-zA-Z0-9-]{1,100}', reader['uid']) or not re.fullmatch(r'[0-9a-f]{40}', reader['revision']) or not re.fullmatch(r'[a-z0-9./_-]+@sha256:[0-9a-f]{64}', reader['image']):
            raise ValueError('Immutable reader identity required')
        roles.add(reader['role'])
        names.add(reader['deployment'])
    return c


def hold(c):
    return {'schema': SCHEMA, 'node_uid': c['node_uid'], 'edge_group_id': c['group'], 'reason': c['reason']}


def validate_reader(reader, deployment, pods, group):
    m, spec, status = deployment['metadata'], deployment['spec'], deployment.get('status', {})
    template = spec['template']
    if m['name'] != reader['deployment'] or m['uid'] != reader['uid'] or m.get('deletionTimestamp') or m.get('annotations', {}).get(CAPABILITY) != 'v1' or template['metadata'].get('annotations', {}).get('fugue.pro/oci-revision') != reader['revision']:
        raise ValueError('Reader hold capability or immutable deployment identity differs')
    count = spec.get('replicas', 1)
    if count < 1 or status.get('observedGeneration') != m['generation'] or any(status.get(key) != count for key in ('replicas', 'updatedReplicas', 'readyReplicas', 'availableReplicas')):
        raise ValueError('Reader rollout is not completely ready')
    matching = [x for x in template['spec']['containers'] if x['image'] == reader['image']]
    if len(matching) != 1:
        raise ValueError('Reader executable identity differs')
    container = matching[0]
    env = {e['name']: e.get('value') for e in container.get('env', [])}
    if env.get('FUGUE_RELEASE_GUARDIAN_ROLE') != reader['role']:
        raise ValueError('Reader role differs')
    baseline = None
    if reader['role'] == 'guardian':
        entries = [v.split(',') for v in env.get('FUGUE_RELEASE_GUARDIAN_AUTHORITY_ACTIVATORS', '').split(';') if v.split(',')[0] == group]
        if len(entries) != 1 or len(entries[0]) != 10 or not entries[0][3].isdigit() or not 1 <= int(entries[0][3]) <= 100:
            raise ValueError('Reader does not declare this group baseline')
        baseline = int(entries[0][3])
    if pods.get('metadata', {}).get('continue') or len(pods['items']) != count:
        raise ValueError('Reader pod inventory is incomplete')
    for pod in pods['items']:
        meta = pod['metadata']
        if meta.get('deletionTimestamp') or not meta.get('uid') or meta.get('annotations', {}).get('fugue.pro/oci-revision') != reader['revision'] or pod.get('status', {}).get('phase') != 'Running' or not any(x['type'] == 'Ready' and x['status'] == 'True' for x in pod['status'].get('conditions', [])):
            raise ValueError('Reader pod has not converged')
        specs = [x for x in pod['spec']['containers'] if x['name'] == container['name']]
        statuses = [x for x in pod['status'].get('containerStatuses', []) if x['name'] == container['name']]
        if len(specs) != 1 or specs[0]['image'] != reader['image'] or len(statuses) != 1 or not statuses[0].get('ready') or 'running' not in statuses[0].get('state', {}) or statuses[0].get('restartCount', 0) != 0 or not statuses[0].get('imageID', '').endswith('@' + reader['image'].split('@')[1]):
            raise ValueError('Reader runtime executable differs or is unstable')
    return baseline


def validate_node(c, node, inventory, baseline):
    meta = node['metadata']
    labels = meta.get('labels', {})
    if meta['name'] != c['node'] or meta['uid'] != c['node_uid'] or not meta.get('resourceVersion') or meta.get('deletionTimestamp') or labels.get('fugue.io/edge-group-id') != c['group'] or labels.get('fugue.io/role.edge') != 'true' or labels.get('fugue.io/edge-enrollment-phase', '') not in ('', 'admitted'):
        raise ValueError('Hold does not bind a live enrolled node')
    previous = meta.get('annotations', {}).get(ANNOTATION)
    if previous is not None and json.loads(previous) != hold(c):
        raise ValueError('Another hold exists; it cannot be replaced implicitly')
    if inventory.get('metadata', {}).get('continue') or len(inventory['items']) > 1000:
        raise ValueError('Group inventory incomplete')
    fixed, eligible, identities, target_count = 0, 0, set(), 0
    for member in inventory['items']:
        m = member['metadata']; labels = m.get('labels', {})
        if labels.get('fugue.io/role.edge') != 'true':
            continue
        if m['name'] in identities or not m.get('uid') or not m.get('resourceVersion') or m.get('deletionTimestamp') or labels.get('fugue.io/edge-group-id') != c['group']:
            raise ValueError('Group membership is ambiguous')
        identities.add(m['name'])
        phase = labels.get('fugue.io/edge-enrollment-phase', '')
        if phase not in ('', 'preparing', 'admitted'):
            raise ValueError('Unknown enrollment phase')
        fixed += phase == ''
        if m['name'] == c['node']:
            if m != meta:
                raise ValueError('Node changed during inventory read')
            target_count += 1
        elif ANNOTATION not in m.get('annotations', {}) and labels.get('fugue.io/schedulable') != 'false' and phase != 'preparing':
            eligible += 1
    if target_count != 1 or fixed != baseline or eligible < 1:
        raise ValueError('Hold would hide missing baseline membership or remove every available member')
    return previous


def validate_retained_workload(workload, group):
    m, spec = workload['metadata'], workload['spec']
    if not m.get('uid') or m.get('deletionTimestamp') or spec.get('updateStrategy', {}).get('type') != 'OnDelete' or spec['template']['metadata'].get('labels', {}).get('fugue.io/edge-group-id') != group:
        raise ValueError('Held members require current group-owned OnDelete Front and Worker workloads')


class HoldWriter(Recovery):
    def validate_release_binding(self, deployment, baseline):
        c = self.config
        container = next(x for x in deployment['spec']['template']['spec']['containers'] if any(e.get('name') == 'FUGUE_RELEASE_GUARDIAN_ROLE' and e.get('value') == 'guardian' for e in x.get('env', [])))
        env = {e['name']: e.get('value') for e in container.get('env', [])}
        targets = [x.split(',') for x in env.get('FUGUE_RELEASE_GUARDIAN_TARGETS', '').split(';') if x.split(',')[0] == c['component']]
        if len(targets) != 1 or len(targets[0]) != 5 or targets[0][2] != c['namespace']:
            raise ValueError('Release component is not owned by the pinned Guardian')
        key = c['component'] + '-' + targets[0][1]
        selected = self.get('configmap', 'fugue-desired-release-'+key)
        desired = json.loads(selected['data']['desired.json'])
        digest = desired.get('recordDigest', '')
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest):
            raise ValueError('Release record binding unavailable')
        record = self.get('configmap', 'fugue-guardian-record-'+key+'-'+digest[7:23])
        if record.get('immutable') is not True or record['metadata'].get('labels', {}).get('fugue.pro/component') != c['component']:
            raise ValueError('Release plan is not immutable or belongs to another component')
        plan = json.loads(record['data']['release-plan.json'])
        members = [x for x in plan['releases'] if x['component'] == c['component']]
        if len(members) != 1:
            raise ValueError('Release plan component is ambiguous')
        member = members[0]
        transition = member.get('transition', {})
        group = transition.get('edgeGroupAB', {})
        if member.get('concurrency') != 'fugue-production-'+c['component'] or member.get('workload', {}).get('namespace') != c['namespace'] or transition.get('type') != 'edge-group-ab' or group.get('groupId') != c['group'] or group.get('expectedNodes') != baseline:
            raise ValueError('Component release lease does not bind this exact authority and baseline')
        names = [group.get(k) for k in ('frontName', 'workerAName', 'workerBName')]
        if len(set(names)) != 3 or any(not isinstance(n, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,200}', n) for n in names):
            raise ValueError('Release plan lacks exact Front and both Worker workloads')
        return names

    def preflight(self):
        c = self.config
        baseline = None
        readers = []
        for reader in c['readers']:
            deployment = self.get('deployment', reader['deployment'])
            selector = deployment['spec']['selector']
            if set(selector) != {'matchLabels'} or not selector['matchLabels']:
                raise ValueError('Reader selector is not exact')
            label_selector = ','.join(k+'='+v for k, v in sorted(selector['matchLabels'].items()))
            pods = json.loads(self.kube('-n', c['namespace'], 'get', 'pods', '-l', label_selector, '-o', 'json'))
            value = validate_reader(reader, deployment, pods, c['group'])
            baseline = value if value is not None else baseline
            if reader['role'] == 'guardian':
                for name in self.validate_release_binding(deployment, value):
                    validate_retained_workload(self.get('daemonset', name), c['group'])
            readers.append({'uid': deployment['metadata']['uid'], 'revision': reader['revision'], 'image': reader['image']})
        node = self.get('node', c['node'])
        inventory = json.loads(self.kube('get', 'nodes', '-l', 'fugue.io/edge-group-id='+c['group'], '-o', 'json'))
        previous = validate_node(c, node, inventory, baseline)
        journals = json.loads(self.kube('-n', c['namespace'], 'get', 'configmaps', '-l', 'fugue.pro/group='+c['group']+',fugue.pro/authority-kind=transition-journal', '-o', 'json'))
        if journals.get('metadata', {}).get('continue') or journals['items']:
            raise ValueError('An unfinished authority transition prevents a membership change')
        return node, previous, readers

    def apply(self):
        with self.leases() as leases:
            node, previous, readers = self.preflight()
            if previous is not None:
                return {'applied': False, 'idempotent': True, 'node_uid': node['metadata']['uid'], 'hold': hold(self.config), 'readers': readers}
            for lease in leases:
                current = self.get('lease', lease['metadata']['name'])
                if current['metadata']['uid'] != lease['metadata']['uid'] or current['metadata']['resourceVersion'] != lease['metadata']['resourceVersion'] or (timestamp(current['spec']['renewTime'])-now()).total_seconds() + current['spec']['leaseDurationSeconds'] < 15:
                    raise ValueError('Release lease changed or lacks mutation time budget')
            m = node['metadata']
            patch = [{'op': 'test', 'path': '/metadata/uid', 'value': m['uid']}, {'op': 'test', 'path': '/metadata/resourceVersion', 'value': m['resourceVersion']}]
            if 'annotations' not in m:
                patch.append({'op': 'add', 'path': '/metadata/annotations', 'value': {}})
            patch.append({'op': 'add', 'path': '/metadata/annotations/'+ANNOTATION.replace('/', '~1'), 'value': canonical(hold(self.config))})
            after = json.loads(self.kube('patch', 'node', self.config['node'], '--type=json', '-p', canonical(patch), '-o', 'json'))
            if after['metadata']['uid'] != m['uid'] or json.loads(after['metadata']['annotations'][ANNOTATION]) != hold(self.config):
                raise ValueError('Hold write readback differs')
            return {'applied': True, 'node_uid': m['uid'], 'resource_version': after['metadata']['resourceVersion'], 'hold': hold(self.config), 'readers': readers}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    c = validate(json.loads(Path(args.config).read_text()))
    writer = HoldWriter(c)
    node, previous, readers = writer.preflight()
    evidence = {'schema': 'fugue.edge-release-hold-evidence/v1', 'generation': c['generation'], 'node_uid': node['metadata']['uid'], 'hold': hold(c), 'readers': readers, 'applied': False}
    Path(args.evidence).write_text(json.dumps(evidence, indent=2)+'\n')
    if args.apply:
        evidence.update(writer.apply())
        Path(args.evidence).write_text(json.dumps(evidence, indent=2)+'\n')


if __name__ == '__main__':
    main()
