"""Reconcile explicitly authorized intermittent members through normal CI.

This controller never changes placement, DNS, replica counts or group authority.
Only an inactive Worker may be recreated by the existing recovery primitive.
"""
import argparse
import copy
import datetime as dt
import json
from pathlib import Path
import re
import subprocess
import urllib.parse

from scripts.configure_edge_release_hold import ANNOTATION, hold, validate_node, validate_reader, validate_retained_workload
from scripts.edge_node_participation import STATE, STATE_SCHEMA, node_ready, node_service, participation_owner, read_edge_node, serving_services, verified_dns_exclusion
from scripts.reconcile_edge_member import Recovery, canonical, now, timestamp, reconcile, without_time
from scripts import run_node_worker_handoff as worker_driver


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


def owned_pod(pods, workload, node):
    found = [p for p in pods if p['spec'].get('nodeName') == node and not p['metadata'].get('deletionTimestamp') and any(r.get('controller') and r.get('kind') == 'DaemonSet' and r.get('uid') == workload['metadata']['uid'] for r in p['metadata'].get('ownerReferences', []))]
    if len(found) != 1:
        raise ValueError('Physical workload Pod is missing or ambiguous')
    return found[0]


def recovery_since(node, since):
    ready = next(c for c in node['status']['conditions'] if c['type'] == 'Ready')
    return max(timestamp(since), timestamp(ready.get('lastTransitionTime', since)))


class Participant(Recovery):
    def __init__(self, policy, member):
        self.policy, self.member = policy, member
        super().__init__(dict(member, namespace=policy['namespace'], activationPath='/var/lib/fugue-edge-front/activation.json'))
        self.owner = participation_owner(member)

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
        desired = canonical(dict(schema=STATE_SCHEMA, owner=self.owner, phase=phase, since=since or now().isoformat()))
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

    def approved_network_recovery(self, not_before=None):
        node = read_edge_node(self.policy, self.member)
        if node.get('draining'):
            return False
        o = node.get('observation', {})
        intent = o.get('dns_admission_intent', {})
        # The existing signed serving policy owns capacity/route admission.
        # This read cannot remove its suspension or change any DNS publication.
        return (o.get('heartbeat_fresh') is True and o.get('admission_allows_dns') is True and o.get('eligible') is True and
                intent.get('state') == 'allowed' and timestamp(o['fresh_until']) > now() and
                intent.get('recovery_last_success_at') is not None and
                dt.timedelta(0) <= now()-timestamp(intent['recovery_last_success_at']) <= dt.timedelta(minutes=10) and
                (not_before is None or timestamp(intent['recovery_last_success_at']) >= not_before))

    def public_service(self):
        node = self.member['node']
        found = node_service(serving_services(self.policy['namespace']), node)
        if found is None:
            return None
        name, group = found['metadata']['name'], self.member['group']
        cluster = worker_driver.ActiveCluster(dict(namespace=self.policy['namespace'], node=node, group=group))
        address = found['spec']['externalIPs']
        if len(address) != 1:
            raise ValueError('Returned node has ambiguous public ingress')
        svc = cluster.read_service(dict(service=name, node=node, uid=found['metadata']['uid'],
                                        generation=int(found['metadata']['annotations']['transport.fugue.dev/generation']),
                                        address=address[0]))
        return cluster, svc

    def recovery_phase(self, node, public, existing):
        if not node_ready(node):
            # The Kubernetes control plane is sufficient to establish this
            # release hold. Do not contact a disconnected Front or Worker.
            return 'offline'
        if public is None:
            return 'recovering' if existing is not None else 'active'
        _, svc = public
        value = read_edge_node(self.policy, self.member)
        observation = value.get('observation', {})
        intent = observation.get('dns_admission_intent', {})
        excluded = verified_dns_exclusion(value, svc) and intent.get('reason') == self.member['reason']
        if existing is None and not excluded:
            return 'active'
        # A kubelet can remain Ready even when the Edge data plane cannot
        # operate. Do not attempt Pod exec while the online API has not
        # observed a fresh Edge heartbeat for this registered transport.
        fresh = observation.get('heartbeat_fresh') is True
        return 'recovering' if fresh else 'offline'

    def node_local_snapshot(self, public):
        if public is None:
            return None
        cluster, svc = public
        group, node = self.member['group'], self.member['node']
        selected, endpoint = cluster.service_front(svc, node)
        state, _ = self.activation(selected['metadata']['name'])
        # Group-mode recovery can still use the existing mutable-inactive-slot
        # path. A node-local binding must never be reset to a group slot.
        reason = state.get('reason', '')
        if not reason.startswith(('advance node Worker using journal ', 'advance fixed Worker using journal ',
                                  'restore node Worker using journal ', 'restore fixed Worker using journal ')):
            return None
        identity = worker_driver.peer_serving_identity(cluster.ns, selected)
        expected = {
            'active_slot': state['active_slot'], 'activation_generation': state['generation'],
            'bundle_generation': state['bundle_generation'], 'worker_source_commit': state['worker_source_commit'],
            'worker_image_digest': state['worker_image_digest'], 'route_authority': state['authority'],
        }
        if state.get('edge_group_id') != group or any(identity[k] != v for k, v in expected.items()):
            raise ValueError('Returned Front disagrees with the live activation')
        workers = [p for p in cluster.pods(node=node) if
                   p['metadata'].get('labels', {}).get('fugue.io/edge-group-id') == group and
                   {'edge', 'caddy'} <= {c['name'] for c in p['spec']['containers']}]
        uid = worker_driver.peer_selected_worker(cluster, workers, identity)
        worker = next(p for p in workers if p['metadata']['uid'] == uid)
        return dict(activation=state, front=worker_driver.process(selected, 'edge-front'), worker_uid=uid,
                    worker=worker_driver.worker_process(worker), endpoint=endpoint, address=svc['spec']['externalIPs'][0])

    def readmit_node_local(self, snapshot, annotations, existing, apply, since):
        node = self.member['node']
        result = {'node': node, 'state': 'recovering', 'changed': False}
        stamp = max(since, timestamp(snapshot['activation']['updated_at']))
        if not apply or not self.approved_network_recovery(stamp):
            return result
        with self.leases():
            self.require_settled_group()
            self.readers()
            for p in self.member['probes']:
                proof = worker_driver.front.proof(snapshot['address'], p['host'], p['path'])
                if proof['edge'] != node or proof['group'] != self.member['group']:
                    raise ValueError('Returned public Front did not prove the physical member')
            if self.node_local_snapshot(self.public_service()) != snapshot or not self.approved_network_recovery(stamp):
                raise ValueError('Node-local recovery evidence or serving process changed')
            current = self.node()
            a = current['metadata'].get('annotations', {})
            if (not node_ready(current) or a.get(STATE) != annotations.get(STATE) or a.get(ANNOTATION) != existing or
                    recovery_since(current, json.loads(a[STATE])['since']) != since):
                raise ValueError('Participation ownership changed before readmission')
            self.transition_node(current, 'active', remove_hold=True)
        return {'node': node, 'state': 'active', 'changed': True, 'code': {'verified': True, 'mode': 'node-local'}}

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
        if state is not None and (state.get('schema') != STATE_SCHEMA or state.get('owner') != self.owner):
            raise ValueError('Another controller owns automatic member participation')
        expected = hold(self.config)
        if existing is not None and (json.loads(existing) != expected or state is None and not self.member['adopt_hold']):
            raise ValueError('Manual release hold retained; automatic policy cannot replace it')
        public = self.public_service()
        phase = self.recovery_phase(node, public, existing)
        if phase == 'active':
            return {'node': self.member['node'], 'state': 'active', 'changed': False}
        baseline = self.readers()
        if public is None:
            for workload in [self.member['front'], *self.member['workers'].values()]:
                validate_retained_workload(self.get('daemonset', workload), self.member['group'])
        inventory = json.loads(self.kube('get', 'nodes', '-l', 'fugue.io/edge-group-id='+self.member['group'], '-o', 'json'))
        validate_node(self.config, node, inventory, baseline)
        since = state.get('since') if state else None
        if state is None or state.get('phase') != phase:
            if apply:
                with self.leases():
                    current = self.node()
                    if current['metadata']['resourceVersion'] != node['metadata']['resourceVersion']: raise ValueError('Node changed before participation transition')
                    current_public = self.public_service()
                    if ((public is None) != (current_public is None) or
                            (public and public[1] != current_public[1]) or
                            self.recovery_phase(current, current_public, existing) != phase):
                        raise ValueError('Public transport or recovery phase changed before participation transition')
                    self.transition_node(current, phase)
            return {'node': self.member['node'], 'state': phase, 'changed': apply}
        if phase == 'offline':
            return {'node': self.member['node'], 'state': phase, 'changed': False}
        since = recovery_since(node, since)
        if now()-since < dt.timedelta(seconds=self.member['stable_seconds']):
            return {'node': self.member['node'], 'state':'recovering', 'changed':False}
        local = self.node_local_snapshot(public)
        if local is not None:
            return self.readmit_node_local(local, annotations, existing, apply, since)
        config = self.recovery_config()
        evidence = reconcile(config, apply)
        if apply and config.get('retainedPrevious') and evidence.get('verified'):
            config = self.recovery_config()
            if config.get('retainedPrevious'): raise ValueError('Retained recovery did not advance to the current authority')
            evidence = reconcile(config, apply)
        if not apply or not evidence.get('verified') or not self.approved_network_recovery(since):
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
            if not self.approved_network_recovery(max(since, timestamp(previous['updated_at']))):
                raise ValueError('Network recovery proof predates the current activation or expired')
            current = self.node()
            a = current['metadata'].get('annotations', {})
            if (not node_ready(current) or a.get(STATE) != annotations.get(STATE) or a.get(ANNOTATION) != existing or
                    recovery_since(current, json.loads(a[STATE])['since']) != since):
                raise ValueError('Participation ownership changed before readmission')
            self.transition_node(current, 'active', remove_hold=True)
        return {'node': self.member['node'], 'state': 'active', 'changed': True, 'code': evidence}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('policy')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence')
    parser.add_argument('--member-index', type=int)
    parser.add_argument('--local-origin')
    args = parser.parse_args()
    policy = validate(json.loads(Path(args.policy).read_text()))
    members = policy['members']
    if args.member_index is not None:
        if not 0 <= args.member_index < len(members):
            parser.error('invalid participation policy member index')
        members = [members[args.member_index]]
    if args.local_origin:
        local = urllib.parse.urlsplit(args.local_origin)
        if not re.fullmatch(r'http://(?:127\.0\.0\.1|\[::1\]):[0-9]{1,5}', args.local_origin) or not 0 < local.port <= 65535:
            parser.error('online observation origin must be literal loopback')
        policy = dict(policy, _local_origin=args.local_origin)
    elif args.evidence is None:
        parser.error('scheduled reconciliation requires an evidence destination')
    results = []
    for member in members:
        try:
            results.append(Participant(policy, member).run(args.apply))
        except (ValueError, KeyError, OSError, subprocess.SubprocessError) as exc:
            # Preserve one bounded actionable reason; the scheduled controller
            # retries later without restarting workloads or spamming alerts.
            results.append({'node': member['node'], 'state': 'waiting', 'reason': str(exc)[:512]})
    if args.evidence:
        Path(args.evidence).write_text(json.dumps({'schema': policy['schema'], 'observed_at': now().isoformat(), 'members': results}, indent=2)+'\n')
    print(canonical({'members': [{k:v for k,v in r.items() if k != 'code'} for r in results]}))


if __name__ == '__main__':
    main()
