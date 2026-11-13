#!/usr/bin/env python3
"""Catch up one returned Front to the already committed group authority.

This is a separately declared configuration recovery, not a new code release.
The selected worker must first prove the current signed publication. A returned
node may already run current code in its active slot after Pod recreation; in
that case only the stale activation record is reconciled, without a slot switch.
"""
import argparse
import base64
import contextlib
import datetime as dt
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import socket
import ssl
import subprocess
import time

from scripts.observe_front_candidate import parse_proof


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def digest(raw):
    return hashlib.sha256(raw.encode()).hexdigest()


def now():
    return dt.datetime.now(dt.timezone.utc)


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00'))


def validate(config, authority, source, previous):
    fields = {'schema', 'namespace', 'component', 'group', 'node', 'front', 'frontUID',
              'worker', 'workerUID', 'referenceFront', 'referenceFrontUID', 'referenceWorker',
              'referenceWorkerUID', 'authorityUID', 'authorityRecord', 'previousState',
              'activationPath', 'workerPort', 'probes'}
    if set(config) not in (fields, fields | {'refreshInactiveWorker'}) or config['schema'] != 'fugue.edge-member-recovery/v1':
        raise ValueError('Unsupported member recovery declaration')
    refresh = config.get('refreshInactiveWorker')
    if refresh is not None and (set(refresh) != {'daemonset', 'daemonsetUID', 'nodeUID'} or
            any(not isinstance(v, str) or not re.fullmatch(r'[a-zA-Z0-9-]{1,200}', v) for v in refresh.values())):
        raise ValueError('Inactive refresh requires exact workload and physical node identities')
    if config['activationPath'] != '/var/lib/fugue-edge-front/activation.json' or config['workerPort'] not in (18443, 28443):
        raise ValueError('Recovery must use the existing activation and worker slots')
    for key in ['namespace', 'component', 'group', 'node', 'front', 'worker', 'referenceFront', 'referenceWorker']:
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,200}', config[key]):
            raise ValueError('Recovery identity is not canonical')
    if not isinstance(config['probes'], list) or not 2 <= len(config['probes']) <= 8:
        raise ValueError('At least two bounded route proofs are required')
    for probe in config['probes']:
        if set(probe) != {'host', 'path'} or not re.fullmatch(r'[a-z0-9.-]+', probe['host']) or not probe['path'].startswith('/') or any(c in probe['path'] for c in '\r\n?#'):
            raise ValueError('Invalid route proof target')
    if previous != config['previousState']:
        # Idempotent retry is admitted only when it is already the exact current
        # group state, ignoring the local execution timestamp.
        if without_time(previous) != without_time(source):
            raise ValueError('Member activation changed outside this recovery')
    mapping = {'edge_group_id': 'groupId', 'generation': 'currentFrontGeneration', 'active_slot': 'currentWorkerSlot',
               'worker_source_commit': 'currentWorkerSourceSha', 'worker_image_digest': 'currentWorkerImageDigest',
               'bundle_generation': 'currentBundleGeneration'}
    if authority.get('currentRecordDigest') != config['authorityRecord'] or authority.get('groupId') != config['group'] or any(source.get(k) != authority.get(v) for k, v in mapping.items()):
        raise ValueError('Reference Front is not the declared committed authority')
    if source.get('schema') != 'edge-front-group-activation/v1' or source.get('authority') != 'edge-control' or source.get('operation') not in ('promote', 'rollback'):
        raise ValueError('Only the exact committed activation can recover a member')
    if previous.get('edge_group_id') != config['group'] or previous.get('schema') != source['schema'] or previous.get('authority') != source['authority']:
        raise ValueError('Member belongs to another activation authority')
    if without_time(previous) != without_time(source) and previous['generation'] >= source['generation']:
        raise ValueError('Recovery requires a strictly older member')
    if config['workerPort'] != {'a': 18443, 'b': 28443}[source['active_slot']]:
        raise ValueError('Worker listener does not match committed slot')


def without_time(value):
    return {k: v for k, v in value.items() if k != 'updated_at'}


def validate_worker(pod, config, target, reference=False):
    prefix = 'referenceWorker' if reference else 'worker'
    meta = pod['metadata']
    if meta['name'] != config[prefix] or meta['uid'] != config[prefix + 'UID'] or meta.get('deletionTimestamp') or meta['namespace'] != config['namespace']:
        raise ValueError('Worker identity changed')
    if not reference and pod['spec']['nodeName'] != config['node']:
        raise ValueError('Worker moved to another node')
    if meta.get('labels', {}).get('fugue.io/edge-group-id') != config['group'] or meta.get('labels', {}).get('fugue.io/edge-slot') != target['active_slot']:
        raise ValueError('Worker group or slot differs from committed authority')
    containers = [c for c in pod['spec']['containers'] if c['name'] == 'edge']
    statuses = [c for c in pod['status'].get('containerStatuses', []) if c['name'] == 'edge']
    if len(containers) != 1 or len(statuses) != 1 or not statuses[0].get('ready') or pod['status'].get('phase') != 'Running':
        raise ValueError('Worker process is not ready')
    if meta.get('annotations', {}).get('fugue.pro/source-commit') != target['worker_source_commit'] or not containers[0]['image'].endswith('@' + target['worker_image_digest']) or not statuses[0]['imageID'].endswith('@' + target['worker_image_digest']):
        raise ValueError('Worker executable differs from committed authority')
    for name in ['edge', 'caddy']:
        declared = [c for c in pod['spec']['containers'] if c['name'] == name]
        running = [c for c in pod['status'].get('containerStatuses', []) if c['name'] == name]
        if len(declared) != 1 or len(running) != 1 or not re.fullmatch(r'.+@sha256:[a-f0-9]{64}', declared[0]['image']) or not running[0].get('ready') or not running[0].get('state', {}).get('running') or running[0]['imageID'].removeprefix('docker-pullable://') != declared[0]['image']:
            raise ValueError('Worker runtime image is not the declared pinned executable')
    ports = [p for c in pod['spec']['containers'] if c['name'] == 'caddy' for p in c.get('ports', [])
             if p.get('name') == 'https-slot' and p.get('protocol') == 'TCP' and p.get('containerPort') == config['workerPort']]
    if len(ports) != 1:
        raise ValueError('Worker HTTPS socket differs from committed slot')
    return pod['status']['podIP']


def verified_worker_health(health, config):
    return (health.get('healthy') is True and health.get('status') == 'ok' and not health.get('candidate_bundle_loaded') and
            not health.get('stale_cache') and not health.get('max_stale_exceeded') and not health.get('caddy_last_error') and
            health.get('bundle_version') == health.get('caddy_applied_version') and
            health.get('edge_group_id') == config['group'] and timestamp(health['bundle_valid_until']) > now() + dt.timedelta(seconds=30))


def validate_inactive_refresh(config, daemonset, node, source, previous, worker, reference):
    """Permit only normal replacement of one declared inactive OnDelete Pod.

    The shared template must already be the committed code running on the
    reference member. This cannot edit an image, a workload or an admission hold.
    """
    refresh = config['refreshInactiveWorker']
    meta, spec = daemonset['metadata'], daemonset['spec']
    template = spec['template']
    if (meta['name'] != refresh['daemonset'] or meta['uid'] != refresh['daemonsetUID'] or
            meta['namespace'] != config['namespace'] or meta.get('deletionTimestamp') or
            spec.get('updateStrategy', {}).get('type') != 'OnDelete' or
            node['metadata']['name'] != config['node'] or node['metadata']['uid'] != refresh['nodeUID'] or
            node['metadata'].get('deletionTimestamp') or node['spec'].get('unschedulable') or
            not spec['template']['spec'].get('nodeSelector') or
            any(node['metadata'].get('labels', {}).get(k) != v for k, v in spec['template']['spec']['nodeSelector'].items())):
        raise ValueError('Inactive refresh workload or schedulable node binding changed')
    if previous['active_slot'] == source['active_slot']:
        raise ValueError('An active Worker must never be replaced by member recovery')
    for pod in [worker, reference]:
        owners = [r for r in pod['metadata'].get('ownerReferences', []) if r.get('controller')]
        if len(owners) != 1 or owners[0].get('kind') != 'DaemonSet' or owners[0]['name'] != meta['name'] or owners[0]['uid'] != meta['uid']:
            raise ValueError('Worker does not belong to the declared shared workload')
    labels = template['metadata'].get('labels', {})
    if (labels.get('fugue.io/edge-slot') != source['active_slot'] or labels.get('fugue.io/edge-group-id') != config['group'] or
            template['metadata'].get('annotations', {}).get('fugue.pro/source-commit') != source['worker_source_commit']):
        raise ValueError('Inactive template does not bind the committed authority')
    for name in ['edge', 'caddy']:
        expected = [x['image'] for x in template['spec']['containers'] if x['name'] == name]
        proven = [x['image'] for x in reference['spec']['containers'] if x['name'] == name]
        if len(expected) != 1 or expected != proven or not re.fullmatch(r'.+@sha256:[a-f0-9]{64}', expected[0]):
            raise ValueError('Inactive template differs from the proven reference executable')
    validate_worker(reference, config, source, True)
    # Verify the old process too. A source mismatch alone is permitted here;
    # foreign ownership, an unready process or an unpinned image is not.
    edge = next(x for x in worker['spec']['containers'] if x['name'] == 'edge')
    old = dict(source, worker_source_commit=worker['metadata'].get('annotations', {}).get('fugue.pro/source-commit'),
               worker_image_digest=edge['image'].split('@')[-1])
    validate_worker(worker, config, old)
    return old['worker_source_commit'] != source['worker_source_commit'] or old['worker_image_digest'] != source['worker_image_digest']


# flock is the same inode/lock protocol used by the existing Go activation CAS.
# A backup and receipt are retained; rename never interrupts established TCP
# streams. The caller holds both release and authority Kubernetes leases.
ACTIVATION_CAS = r'''
set -eu
path=$1
expected=$2
prepared=$3
target=$4
exec 9>"$path.lock"
flock -n 9
test "$(sha256sum "$path" | cut -d ' ' -f 1)" = "$expected"
if [ -n "$prepared" ]; then
  test "$(sha256sum "$path.prepared" | cut -d ' ' -f 1)" = "$prepared"
fi
backup="$path.recovery-before-$expected"
if [ ! -e "$backup" ]; then cp -p "$path" "$backup"; fi
temporary=$(mktemp "$path.recovery.XXXXXX")
trap 'rm -f "$temporary"' EXIT
printf '%s' "$target" | base64 -d > "$temporary"
chmod 600 "$temporary"
sync "$temporary"
mv "$temporary" "$path"
sync "$path"
cat "$path"
'''


class Recovery:
    def __init__(self, config):
        self.config = config
        self.command = shlex.split(os.environ.get('KUBECTL', 'kubectl'))

    def kube(self, *args, body=None):
        return subprocess.check_output(self.command + list(args), input=body, text=True, timeout=20)

    def get(self, kind, name):
        return json.loads(self.kube('-n', self.config['namespace'], 'get', kind, name, '-o', 'json'))

    def activation(self, pod):
        raw = self.kube('-n', self.config['namespace'], 'exec', pod, '-c', 'edge-front', '--', 'cat', self.config['activationPath'])
        return json.loads(raw), raw

    def health(self, pod, port=7832):
        return json.loads(self.kube('get', '--raw', '/api/v1/namespaces/' + self.config['namespace'] + '/pods/' + pod + ':' + str(port) + '/proxy/healthz'))

    def current_worker(self, target):
        c = self.config
        if not c.get('refreshInactiveWorker'):
            return self.get('pod', c['worker'])
        inventory = json.loads(self.kube('-n', c['namespace'], 'get', 'pods', '--field-selector', 'spec.nodeName=' + c['node'], '-o', 'json'))
        if inventory.get('metadata', {}).get('continue'):
            raise ValueError('Replacement inventory is incomplete')
        candidates = [p for p in inventory['items'] if not p['metadata'].get('deletionTimestamp') and
                      any(r.get('controller') and r.get('kind') == 'DaemonSet' and r.get('name') == c['refreshInactiveWorker']['daemonset'] and
                          r.get('uid') == c['refreshInactiveWorker']['daemonsetUID'] for r in p['metadata'].get('ownerReferences', []))]
        if len(candidates) != 1:
            raise ValueError('Declared inactive Worker is missing or ambiguous')
        worker = candidates[0]
        if worker['metadata']['uid'] != c['workerUID']:
            binding = dict(c, worker=worker['metadata']['name'], workerUID=worker['metadata']['uid'])
            validate_worker(worker, binding, target)
            # A retry may observe the exact committed replacement from a
            # previous attempt. It may never delete that new process again.
            c.update(worker=binding['worker'], workerUID=binding['workerUID'])
        return worker

    def snapshot(self, allow_refresh=False):
        c = self.config
        owner = self.get('configmap', 'fugue-current-authority-' + c['group'])
        if owner['metadata']['uid'] != c['authorityUID'] or owner['metadata'].get('labels', {}).get('fugue.pro/authority-store') != 'true':
            raise ValueError('Group authority owner changed')
        authority = json.loads(owner['data']['authority.json'])
        source, _ = self.activation(c['referenceFront'])
        previous, raw = self.activation(c['front'])
        for key in ['front', 'referenceFront']:
            pod = self.get('pod', c[key])
            if pod['metadata']['uid'] != c[key + 'UID'] or pod['metadata'].get('deletionTimestamp') or (key == 'front' and pod['spec']['nodeName'] != c['node']):
                raise ValueError('Front identity changed')
        validate(c, authority, source, previous)
        worker, reference = self.current_worker(source), self.get('pod', c['referenceWorker'])
        validate_worker(reference, c, source, True)
        try:
            validate_worker(worker, c, source)
        except ValueError:
            if not allow_refresh or not c.get('refreshInactiveWorker'):
                raise
            validate_inactive_refresh(c, self.get('daemonset', c['refreshInactiveWorker']['daemonset']), self.get('node', c['node']), source, previous, worker, reference)
        journals = json.loads(self.kube('-n', c['namespace'], 'get', 'configmaps', '-l', 'fugue.pro/group=' + c['group'] + ',fugue.pro/authority-kind=transition-journal', '-o', 'json'))
        if journals['items']:
            raise ValueError('Group authority has an unfinished transition')
        return owner, source, previous, raw, worker, reference

    def refresh_inactive_worker(self, held, owner, target, previous, raw, worker, reference):
        c = self.config
        if not c.get('refreshInactiveWorker'):
            return
        try:
            validate_worker(worker, c, target)
            return
        except ValueError:
            pass
        daemonset = self.get('daemonset', c['refreshInactiveWorker']['daemonset'])
        if not validate_inactive_refresh(c, daemonset, self.get('node', c['node']), target, previous, worker, reference):
            return
        # Recheck authority, both physical identities and lease time before the
        # only destructive step. UID+resourceVersion preconditions prevent a
        # late retry from deleting a replacement Pod.
        observed = self.snapshot(allow_refresh=True)
        if observed[0]['metadata']['resourceVersion'] != owner['metadata']['resourceVersion'] or observed[1] != target or observed[3] != raw or observed[4]['metadata']['uid'] != worker['metadata']['uid']:
            raise ValueError('Authority changed before inactive replacement')
        deadline = min(timestamp(x['spec']['renewTime']) + dt.timedelta(seconds=x['spec']['leaseDurationSeconds']) for x in held)
        if (deadline - now()).total_seconds() < 100:
            raise ValueError('Recovery leases lack the bounded inactive replacement budget')
        for lease in held:
            current = self.get('lease', lease['metadata']['name'])
            if current['metadata']['uid'] != lease['metadata']['uid'] or current['metadata']['resourceVersion'] != lease['metadata']['resourceVersion']:
                raise ValueError('Recovery lease changed before inactive replacement')
        meta = worker['metadata']
        self.kube('delete', '--raw', '/api/v1/namespaces/' + c['namespace'] + '/pods/' + meta['name'], '-f', '-',
                  body=canonical({'apiVersion': 'v1', 'kind': 'DeleteOptions', 'preconditions': {'uid': meta['uid'], 'resourceVersion': meta['resourceVersion']}}))
        stop = time.monotonic() + 75
        while time.monotonic() < stop:
            inventory = json.loads(self.kube('-n', c['namespace'], 'get', 'pods', '--field-selector', 'spec.nodeName=' + c['node'], '-o', 'json'))
            if inventory.get('metadata', {}).get('continue'):
                raise ValueError('Replacement inventory is incomplete')
            candidates = [p for p in inventory['items'] if not p['metadata'].get('deletionTimestamp') and
                          any(r.get('controller') and r.get('uid') == daemonset['metadata']['uid'] for r in p['metadata'].get('ownerReferences', []))]
            if len(candidates) > 1:
                raise ValueError('Inactive replacement is ambiguous')
            if candidates and candidates[0]['metadata']['uid'] != meta['uid']:
                candidate = candidates[0]
                binding = dict(c, worker=candidate['metadata']['name'], workerUID=candidate['metadata']['uid'])
                try:
                    validate_worker(candidate, binding, target)
                except ValueError:
                    time.sleep(2)
                    continue
                c.update(worker=binding['worker'], workerUID=binding['workerUID'])
                self.snapshot()  # Still the exact authority and predecessor.
                self.renew_leases(held)
                return
            time.sleep(2)
        raise ValueError('Inactive replacement did not converge; serving Front remains unchanged')

    def renew_leases(self, held):
        for i, lease in enumerate(held):
            current = self.get('lease', lease['metadata']['name'])
            if (current['metadata']['uid'] != lease['metadata']['uid'] or current['metadata']['resourceVersion'] != lease['metadata']['resourceVersion'] or
                    timestamp(current['spec']['renewTime']) + dt.timedelta(seconds=current['spec']['leaseDurationSeconds']) <= now()):
                raise ValueError('Recovery lease expired or changed during inactive replacement')
            patch = [{'op': 'test', 'path': '/metadata/uid', 'value': lease['metadata']['uid']},
                     {'op': 'test', 'path': '/metadata/resourceVersion', 'value': lease['metadata']['resourceVersion']},
                     {'op': 'replace', 'path': '/spec/renewTime', 'value': now().isoformat().replace('+00:00', 'Z')}]
            held[i] = json.loads(self.kube('-n', self.config['namespace'], 'patch', 'lease', lease['metadata']['name'], '--type=json', '-p', canonical(patch), '-o', 'json'))

    @contextlib.contextmanager
    def leases(self):
        held = []
        c = self.config
        holder = 'member-recovery:' + os.environ.get('GITHUB_RUN_ID', 'preview') + ':' + secrets.token_hex(8)
        try:
            for name in ['fugue-production-' + c['component'], 'fugue-authority-' + c['group']]:
                obj = self.get('lease', name)
                spec = obj['spec']
                if spec.get('holderIdentity') and timestamp(spec['renewTime']) + dt.timedelta(seconds=spec['leaseDurationSeconds']) > now():
                    raise ValueError('Another release owns the recovery lease')
                at = now().isoformat().replace('+00:00', 'Z')
                desired = dict(spec, holderIdentity=holder, acquireTime=at, renewTime=at)
                patch = [{'op': 'test', 'path': '/metadata/uid', 'value': obj['metadata']['uid']},
                         {'op': 'test', 'path': '/metadata/resourceVersion', 'value': obj['metadata']['resourceVersion']},
                         {'op': 'replace', 'path': '/spec', 'value': desired}]
                after = json.loads(self.kube('-n', c['namespace'], 'patch', 'lease', name, '--type=json', '-p', canonical(patch), '-o', 'json'))
                held.append(after)
            yield held
        finally:
            for obj in reversed(held):
                patch = [{'op': 'test', 'path': '/metadata/uid', 'value': obj['metadata']['uid']},
                         {'op': 'test', 'path': '/metadata/resourceVersion', 'value': obj['metadata']['resourceVersion']},
                         {'op': 'test', 'path': '/spec/holderIdentity', 'value': holder},
                         {'op': 'replace', 'path': '/spec/holderIdentity', 'value': ''}]
                self.kube('-n', c['namespace'], 'patch', 'lease', obj['metadata']['name'], '--type=json', '-p', canonical(patch))

    def prepare(self, previous, target):
        c = self.config
        return json.loads(self.kube('-n', c['namespace'], 'exec', c['front'], '-c', 'edge-front', '--',
            '/usr/local/bin/fugue-edge-front-cas', '--state-file', c['activationPath'], '--group', c['group'],
            '--expected-generation', str(previous['generation']), '--expected-slot', previous['active_slot'], '--target-slot', target['active_slot'],
            '--bundle-generation', target['bundle_generation'], '--worker-source-commit', target['worker_source_commit'],
            '--worker-image-digest', target['worker_image_digest'], '--operation', 'promote', '--reason', 'Recover returned member to committed authority', '--prepare-publication'))


def proof(ip, port, probe, send_proxy=True):
    context = ssl.create_default_context()
    raw = socket.create_connection((ip, port), timeout=4)
    conn = http.client.HTTPSConnection(probe['host'], context=context, timeout=4)
    try:
        src, dst = raw.getsockname(), raw.getpeername()
        if send_proxy:
            raw.sendall(('PROXY TCP4 %s %s %d %d\r\n' % (src[0], dst[0], src[1], dst[1])).encode())
        conn.sock = context.wrap_socket(raw, server_hostname=probe['host'])
        expiry = dt.datetime.fromtimestamp(ssl.cert_time_to_seconds(conn.sock.getpeercert()['notAfter']), dt.timezone.utc)
        nonce = secrets.token_hex(16)
        conn.request('HEAD', probe['path'], headers={'Host': probe['host'], 'X-Fugue-Route-Probe': '1', 'X-Fugue-Route-Probe-Nonce': nonce})
        return parse_proof(conn.getresponse(), nonce, expiry)
    finally:
        conn.close()
        raw.close()


def verify_pair(config, worker, reference):
    evidence = []
    for p in config['probes']:
        a = proof(reference['status']['podIP'], config['workerPort'], p)
        b = proof(worker['status']['podIP'], config['workerPort'], p)
        again = proof(reference['status']['podIP'], config['workerPort'], p)
        if a != again or a['edge'] != reference['spec']['nodeName'] or b['edge'] != config['node'] or a['group'] != config['group'] or b['group'] != config['group']:
            raise ValueError('Fresh worker proof identity/publication is unstable')
        if {k: v for k, v in a.items() if k != 'edge'} != {k: v for k, v in b.items() if k != 'edge'}:
            raise ValueError('Returned worker does not prove the same serving routes')
        evidence.append({'host': p['host'], 'proof': b})
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    recovery = Recovery(config)
    owner, target, previous, raw, worker, reference = recovery.snapshot(allow_refresh=bool(config.get('refreshInactiveWorker')))
    evidence = {'mode': 'apply' if args.apply else 'preview', 'node': config['node'], 'previous': previous, 'target': target,
                'authority_uid': owner['metadata']['uid'], 'authority_record': config['authorityRecord'], 'activated': False}
    save = lambda: Path(args.evidence).write_text(json.dumps(evidence, indent=2) + '\n')
    save()
    if args.apply and without_time(previous) == without_time(target):
        if not verified_worker_health(recovery.health(config['worker']), config):
            raise ValueError('Already recovered worker has not retained healthy authority')
        evidence['proofs'] = verify_pair(config, worker, reference)
        evidence.update(activated=True, verified=True, already_applied=True)
        save()
    if args.apply and without_time(previous) != without_time(target):
        with recovery.leases() as held:
            owner, target, previous, raw, worker, reference = recovery.snapshot(allow_refresh=bool(config.get('refreshInactiveWorker')))
            recovery.refresh_inactive_worker(held, owner, target, previous, raw, worker, reference)
            owner, target, previous, raw, worker, reference = recovery.snapshot()
            # An active worker already obtains the current signed publication.
            # Preparation is exclusively an inactive-slot capability: never
            # fabricate an intermediate switch just to repair stale metadata.
            prepared = None
            if previous['active_slot'] != target['active_slot']:
                prepared = recovery.prepare(previous, target)
            deadline = time.monotonic() + 55
            if prepared:
                deadline = min(deadline, time.monotonic() + (timestamp(prepared['expires_at']) - now()).total_seconds() - 20)
            while True:
                health = recovery.health(config['worker'])
                if verified_worker_health(health, config):
                    try:
                        evidence['proofs'] = verify_pair(config, worker, reference)
                        break
                    except ValueError:
                        if time.monotonic() >= deadline:
                            raise
                if time.monotonic() >= deadline:
                    raise ValueError('Prepared worker did not obtain fresh current authority; Front remains unchanged')
                time.sleep(2)
            current_owner, current_target, current_previous, current_raw, _, _ = recovery.snapshot()
            if current_owner['metadata']['resourceVersion'] != owner['metadata']['resourceVersion'] or current_target != target or current_raw != raw:
                raise ValueError('Authority changed before activation')
            prep_raw = None
            if prepared:
                prep_raw = recovery.kube('-n', config['namespace'], 'exec', config['front'], '-c', 'edge-front', '--', 'cat', config['activationPath'] + '.prepared')
                if json.loads(prep_raw) != prepared or timestamp(prepared['expires_at']) <= now() + dt.timedelta(seconds=15):
                    raise ValueError('Publication preparation changed or expired')
            for lease in held:
                current = recovery.get('lease', lease['metadata']['name'])
                if current['metadata']['uid'] != lease['metadata']['uid'] or current['metadata']['resourceVersion'] != lease['metadata']['resourceVersion'] or timestamp(current['spec']['renewTime']) + dt.timedelta(seconds=current['spec']['leaseDurationSeconds']) <= now() + dt.timedelta(seconds=15):
                    raise ValueError('Recovery lease changed or lacks activation time budget')
            target = dict(target, updated_at=now().isoformat().replace('+00:00', 'Z'))
            evidence['target'] = target
            save()
            after = json.loads(recovery.kube('-n', config['namespace'], 'exec', config['front'], '-c', 'edge-front', '--', 'sh', '-ceu',
                ACTIVATION_CAS, 'sh', config['activationPath'], digest(raw), digest(prep_raw) if prep_raw is not None else '', base64.b64encode((canonical(target) + '\n').encode()).decode()))
            if after != target:
                raise ValueError('Activation result is indeterminate')
            evidence['activated'] = True
            save()
            health = recovery.health(config['front'], 7831)
            if health.get('active_slot') != target['active_slot'] or health.get('activation_generation') != target['generation'] or health.get('status') != 'ok':
                raise ValueError('Recovered Front health is not confirmed; retain current authority and evidence')
            front = recovery.get('pod', config['front'])
            if front['metadata']['uid'] != config['frontUID']:
                raise ValueError('Front process changed during recovery')
            for item in config['probes']:
                observed = proof(front['status']['podIP'], 443, item, False)
                if observed['edge'] != config['node'] or observed['group'] != config['group']:
                    raise ValueError('Recovered Front routes to another physical worker')
            evidence['verified'] = True
            save()
    print(canonical(evidence))


if __name__ == '__main__':
    main()
