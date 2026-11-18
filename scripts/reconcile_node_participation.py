"""Reconcile explicitly authorized intermittent members through normal CI.

This controller never changes placement, DNS, replica counts or group authority.
Only an inactive Worker may be recreated by the existing recovery primitive.
"""
import argparse
import copy
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import urllib.request
import urllib.parse

from scripts.configure_edge_release_hold import ANNOTATION, hold, validate_node, validate_reader, validate_retained_workload
from scripts.reconcile_edge_member import Recovery, canonical, now, timestamp, reconcile, without_time

STATE = 'fugue.io/automatic-edge-participation'


def validate(policy):
    if set(policy) != {'schema', 'generation', 'namespace', 'origin', 'readers', 'members'} or policy['schema'] != 'fugue.automatic-node-participation/v1' or type(policy['generation']) is not int or policy['generation'] < 1:
        raise ValueError('Exact automatic participation policy required')
    origin = urllib.parse.urlsplit(policy['origin'])
    if origin.scheme != 'https' or not origin.netloc or origin.username or origin.password or origin.path or origin.query or origin.fragment:
        raise ValueError('Credential-free HTTPS origin required')
    if not isinstance(policy['members'], list) or not 1 <= len(policy['members']) <= 16 or set(policy['readers']) != {'guardian', 'canary-prober'}:
        raise ValueError('Bounded members and both code-release readers required')
    seen = set()
    for m in policy['members']:
        if set(m) != {'node', 'node_uid', 'group', 'component', 'front', 'workers', 'reason', 'adopt_hold', 'stable_seconds', 'probes'} or set(m['workers']) != {'a', 'b'}:
            raise ValueError('Exact member identity and both Worker slots required')
        for name in [policy['namespace'], *policy['readers'].values(), m['node'], m['group'], m['component'], m['front'], *m['workers'].values()]:
            if not isinstance(name, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,200}', name):
                raise ValueError('Canonical Kubernetes identity required')
        if not re.fullmatch(r'[a-zA-Z0-9-]{1,100}', m['node_uid']) or m['node'] in seen or m['reason'] not in ('quota_exhausted', 'network_unavailable') or type(m['adopt_hold']) is not bool or type(m['stable_seconds']) is not int or not 120 <= m['stable_seconds'] <= 3600:
            raise ValueError('UID-bound automatic recovery intent required')
        if not isinstance(m['probes'], list) or not 2 <= len(m['probes']) <= 8:
            raise ValueError('At least two route probes required')
        for p in m['probes']:
            if set(p) != {'host', 'path'} or not re.fullmatch(r'[a-z0-9.-]{1,253}', p['host']) or not p['path'].startswith('/') or any(c in p['path'] for c in '\r\n?#'):
                raise ValueError('Bounded route probe required')
        seen.add(m['node'])
    return policy


def node_ready(node):
    values = [c for c in node.get('status', {}).get('conditions', []) if c['type'] == 'Ready']
    if len(values) != 1:
        raise ValueError('Node readiness observation is missing or ambiguous')
    return values[0]['status'] == 'True'


def owned_pod(pods, workload, node):
    found = [p for p in pods if p['spec'].get('nodeName') == node and not p['metadata'].get('deletionTimestamp') and any(r.get('controller') and r.get('kind') == 'DaemonSet' and r.get('uid') == workload['metadata']['uid'] for r in p['metadata'].get('ownerReferences', []))]
    if len(found) != 1:
        raise ValueError('Physical workload Pod is missing or ambiguous')
    return found[0]


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Participant(Recovery):
    def __init__(self, policy, member):
        self.policy, self.member = policy, member
        super().__init__(dict(member, namespace=policy['namespace'], activationPath='/var/lib/fugue-edge-front/activation.json'))
        identity = {k: member[k] for k in ('node', 'node_uid', 'group', 'component')}
        self.owner = 'sha256:' + hashlib.sha256(canonical(identity).encode()).hexdigest()

    def node(self):
        value = self.get('node', self.member['node'])
        m, c = value['metadata'], self.member
        if m['name'] != c['node'] or m['uid'] != c['node_uid'] or not m.get('resourceVersion') or m.get('deletionTimestamp') or m.get('labels', {}).get('fugue.io/edge-group-id') != c['group'] or m.get('labels', {}).get('fugue.io/role.edge') != 'true':
            raise ValueError('Automatic member physical identity changed')
        return value

    def inventory(self):
        value = json.loads(self.kube('-n', self.policy['namespace'], 'get', 'pods', '-l', 'fugue.io/edge-group-id='+self.member['group'], '-o', 'json'))
        if value.get('metadata', {}).get('continue') or len(value['items']) > 400:
            raise ValueError('Member Pod inventory is incomplete')
        return value['items']

    def readers(self):
        baseline = None
        for role, name in self.policy['readers'].items():
            d = self.get('deployment', name)
            containers = [c for c in d['spec']['template']['spec']['containers'] if any(e.get('name') == 'FUGUE_RELEASE_GUARDIAN_ROLE' and e.get('value') == role for e in c.get('env', []))]
            if len(containers) != 1:
                raise ValueError('Code-release reader ownership differs')
            selector = ','.join(k+'='+v for k, v in sorted(d['spec']['selector']['matchLabels'].items()))
            pods = json.loads(self.kube('-n', self.policy['namespace'], 'get', 'pods', '-l', selector, '-o', 'json'))
            r = dict(role=role, deployment=name, uid=d['metadata']['uid'], image=containers[0]['image'], revision=d['spec']['template']['metadata']['annotations']['fugue.pro/oci-revision'])
            found = validate_reader(r, d, pods, self.member['group'])
            if found is not None: baseline = found
        if baseline is None: raise ValueError('No declared group baseline')
        return baseline

    def transition_node(self, node, phase, since=None, remove_hold=False):
        annotations = copy.deepcopy(node['metadata'].get('annotations', {}))
        desired = canonical(dict(schema='fugue.automatic-edge-participation/v1', owner=self.owner, phase=phase, since=since or now().isoformat()))
        if annotations.get(STATE) == desired and not remove_hold:
            return node
        annotations[STATE] = desired
        if remove_hold:
            annotations.pop(ANNOTATION, None)
        else:
            annotations[ANNOTATION] = canonical(hold(self.config))
        patch = [{'op': 'test', 'path': '/metadata/uid', 'value': node['metadata']['uid']},
                 {'op': 'test', 'path': '/metadata/resourceVersion', 'value': node['metadata']['resourceVersion']},
                 {'op': 'add', 'path': '/metadata/annotations', 'value': annotations}]
        return json.loads(self.kube('patch', 'node', self.member['node'], '--type=json', '-p', canonical(patch), '-o', 'json'))

    def approved_network_recovery(self):
        key = os.environ.get('FUGUE_API_KEY', '')
        if not key: raise ValueError('Recovery observation credential unavailable')
        req = urllib.request.Request(self.policy['origin']+'/v1/edge/nodes/'+urllib.parse.quote(self.member['node'], safe=''), headers={'Authorization': 'Bearer '+key})
        with urllib.request.build_opener(NoRedirect()).open(req, timeout=15) as response:
            raw = response.read(131073)
        if len(raw) > 131072: raise ValueError('Recovery observation exceeds bound')
        node = json.loads(raw)['node']
        if node['id'] != self.member['node'] or node['edge_group_id'] != self.member['group'] or node.get('draining'):
            return False
        o = node.get('observation', {})
        intent = o.get('dns_admission_intent', {})
        # The existing signed serving policy owns capacity/route admission.
        # This read cannot remove its suspension or change any DNS publication.
        return (o.get('heartbeat_fresh') is True and o.get('admission_allows_dns') is True and o.get('eligible') is True and
                intent.get('state') == 'allowed' and timestamp(o['fresh_until']) > now() and
                intent.get('recovery_last_success_at') is not None and
                dt.timedelta(0) <= now()-timestamp(intent['recovery_last_success_at']) <= dt.timedelta(minutes=10))

    def recovery_config(self):
        m = self.member
        authority_owner = self.get('configmap', 'fugue-current-authority-'+m['group'])
        authority = json.loads(authority_owner['data']['authority.json'])
        if authority_owner['metadata'].get('labels', {}).get('fugue.pro/authority-store') != 'true' or authority['groupId'] != m['group']:
            raise ValueError('Member authority ownership differs')
        pods = self.inventory()
        front_ds = self.get('daemonset', m['front'])
        worker_ds = self.get('daemonset', m['workers'][authority['currentWorkerSlot']])
        front = owned_pod(pods, front_ds, m['node'])
        worker = owned_pod(pods, worker_ds, m['node'])
        nodes = json.loads(self.kube('get', 'nodes', '-l', 'fugue.io/edge-group-id='+m['group'], '-o', 'json'))
        if nodes.get('metadata', {}).get('continue'): raise ValueError('Group Node inventory incomplete')
        references = sorted(n['metadata']['name'] for n in nodes['items'] if n['metadata']['name'] != m['node'] and node_ready(n) and ANNOTATION not in n['metadata'].get('annotations', {}))
        if not references: raise ValueError('No healthy unheld reference member')
        proven = []
        for name in references:
            try: reference = owned_pod(pods, worker_ds, name)
            except ValueError: continue
            if reference['metadata'].get('annotations', {}).get('fugue.pro/source-commit') == authority['currentWorkerSourceSha']:
                proven.append(name)
        if not proven: raise ValueError('No reference running the committed Worker code')
        ref_front, ref_worker = owned_pod(pods, front_ds, proven[0]), owned_pod(pods, worker_ds, proven[0])
        previous, _ = self.activation(front['metadata']['name'])
        bridge = previous['active_slot'] == authority['currentWorkerSlot'] and worker['metadata'].get('annotations', {}).get('fugue.pro/source-commit') != authority['currentWorkerSourceSha']
        slot = authority['currentWorkerSlot']
        if bridge:
            slot = authority.get('previousWorkerSlot')
            if slot not in ('a','b') or slot == previous['active_slot'] or previous['generation'] >= authority.get('previousFrontGeneration',0):
                raise ValueError('Same-slot catch-up lacks a retained inactive authority')
            worker_ds = self.get('daemonset', m['workers'][slot])
            worker, ref_worker = owned_pod(pods, worker_ds, m['node']), owned_pod(pods, worker_ds, proven[0])
        config = dict(schema='fugue.edge-member-recovery/v1', namespace=self.policy['namespace'], component=m['component'], group=m['group'], node=m['node'],
                      front=front['metadata']['name'], frontUID=front['metadata']['uid'], worker=worker['metadata']['name'], workerUID=worker['metadata']['uid'],
                      referenceFront=ref_front['metadata']['name'], referenceFrontUID=ref_front['metadata']['uid'], referenceWorker=ref_worker['metadata']['name'], referenceWorkerUID=ref_worker['metadata']['uid'],
                      authorityUID=authority_owner['metadata']['uid'], authorityRecord=authority['currentRecordDigest'], previousState=previous,
                      activationPath=self.config['activationPath'], workerPort={'a':18443, 'b':28443}[slot], probes=m['probes'])
        if bridge: config['retainedPrevious'] = True
        if previous['active_slot'] != slot:
            config['refreshInactiveWorker'] = dict(daemonset=worker_ds['metadata']['name'], daemonsetUID=worker_ds['metadata']['uid'], nodeUID=m['node_uid'])
        return config

    def run(self, apply=False):
        node = self.node()
        annotations = node['metadata'].get('annotations', {})
        existing = annotations.get(ANNOTATION)
        state = json.loads(annotations[STATE]) if STATE in annotations else None
        if state is not None and (state.get('schema') != 'fugue.automatic-edge-participation/v1' or state.get('owner') != self.owner):
            raise ValueError('Another controller owns automatic member participation')
        expected = hold(self.config)
        if existing is not None and (json.loads(existing) != expected or state is None and not self.member['adopt_hold']):
            raise ValueError('Manual release hold retained; automatic policy cannot replace it')
        ready = node_ready(node)
        if ready and existing is None:
            return {'node': self.member['node'], 'state': 'active', 'changed': False}
        baseline = self.readers()
        for workload in [self.member['front'], *self.member['workers'].values()]:
            validate_retained_workload(self.get('daemonset', workload), self.member['group'])
        inventory = json.loads(self.kube('get', 'nodes', '-l', 'fugue.io/edge-group-id='+self.member['group'], '-o', 'json'))
        validate_node(self.config, node, inventory, baseline)
        since = state.get('since') if state else None
        phase = 'recovering' if ready else 'offline'
        if state is None or state.get('phase') != phase:
            if apply:
                with self.leases():
                    current = self.node()
                    if current['metadata']['resourceVersion'] != node['metadata']['resourceVersion']: raise ValueError('Node changed before participation transition')
                    self.transition_node(current, phase)
            return {'node': self.member['node'], 'state': phase, 'changed': apply}
        if not ready or now()-timestamp(since) < dt.timedelta(seconds=self.member['stable_seconds']):
            return {'node': self.member['node'], 'state': phase, 'changed': False}
        condition = next(c for c in node['status']['conditions'] if c['type']=='Ready')
        if 'lastTransitionTime' in condition and now()-timestamp(condition['lastTransitionTime']) < dt.timedelta(seconds=self.member['stable_seconds']):
            return {'node': self.member['node'], 'state':'recovering', 'changed':False}
        config = self.recovery_config()
        evidence = reconcile(config, apply)
        if apply and config.get('retainedPrevious') and evidence.get('verified'):
            config = self.recovery_config()
            if config.get('retainedPrevious'): raise ValueError('Retained recovery did not advance to the current authority')
            evidence = reconcile(config, apply)
        if not apply or not evidence.get('verified') or not self.approved_network_recovery():
            return {'node': self.member['node'], 'state': 'recovering', 'code': evidence, 'changed': False}
        # Recheck exact authority, running code, physical Node and readers under
        # the same release/authority leases before removing our own hold.
        with self.leases():
            final = self.recovery_config()
            if final['authorityRecord'] != config['authorityRecord'] or final['authorityUID'] != config['authorityUID']:
                raise ValueError('Authority changed before readmission')
            owner, target, previous, raw, worker, reference = Recovery(final).snapshot()
            if without_time(previous) != without_time(target): raise ValueError('Code catch-up not complete')
            self.readers()
            if not self.approved_network_recovery(): raise ValueError('Network recovery proof expired')
            current = self.node()
            a = current['metadata'].get('annotations', {})
            if not node_ready(current) or a.get(STATE) != annotations.get(STATE) or a.get(ANNOTATION) != existing:
                raise ValueError('Participation ownership changed before readmission')
            self.transition_node(current, 'active', remove_hold=True)
        return {'node': self.member['node'], 'state': 'active', 'changed': True, 'code': evidence}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('policy')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    policy = validate(json.loads(Path(args.policy).read_text()))
    results = []
    for member in policy['members']:
        try:
            results.append(Participant(policy, member).run(args.apply))
        except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
            # Preserve one bounded actionable reason; the scheduled controller
            # retries later without restarting workloads or spamming alerts.
            results.append({'node': member['node'], 'state': 'waiting', 'reason': str(exc)[:512]})
    Path(args.evidence).write_text(json.dumps({'schema': policy['schema'], 'observed_at': now().isoformat(), 'members': results}, indent=2)+'\n')
    print(canonical({'members': [{k:v for k,v in r.items() if k != 'code'} for r in results]}))


if __name__ == '__main__':
    main()
