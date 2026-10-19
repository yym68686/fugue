#!/usr/bin/env python3
"""Catch up one returned Front to the already committed group authority.

This is a separately declared configuration recovery, not a new code release.
The inactive worker must first load and prove the current signed publication.
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
    if set(config) != fields or config['schema'] != 'fugue.edge-member-recovery/v1':
        raise ValueError('Unsupported member recovery declaration')
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
    if source.get('schema') != 'edge-front-group-activation/v1' or source.get('authority') != 'edge-control' or source.get('operation') != 'promote':
        raise ValueError('Only a committed positive promotion can recover a member')
    if previous.get('edge_group_id') != config['group'] or previous.get('schema') != source['schema'] or previous.get('authority') != source['authority']:
        raise ValueError('Member belongs to another activation authority')
    if without_time(previous) != without_time(source) and (previous['generation'] >= source['generation'] or previous['active_slot'] == source['active_slot']):
        raise ValueError('Recovery requires an older member on the other slot')
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
    containers = [c for c in pod['spec']['containers'] if c['name'] == 'edge']
    statuses = [c for c in pod['status'].get('containerStatuses', []) if c['name'] == 'edge']
    if len(containers) != 1 or len(statuses) != 1 or not statuses[0].get('ready') or pod['status'].get('phase') != 'Running':
        raise ValueError('Worker process is not ready')
    if meta.get('annotations', {}).get('fugue.pro/source-commit') != target['worker_source_commit'] or not containers[0]['image'].endswith('@' + target['worker_image_digest']) or not statuses[0]['imageID'].endswith('@' + target['worker_image_digest']):
        raise ValueError('Worker executable differs from committed authority')
    return pod['status']['podIP']


def verified_worker_health(health, config):
    return (health.get('healthy') is True and health.get('status') == 'ok' and not health.get('candidate_bundle_loaded') and
            not health.get('stale_cache') and not health.get('max_stale_exceeded') and not health.get('caddy_last_error') and
            health.get('bundle_version') == health.get('caddy_applied_version') and
            health.get('edge_group_id') == config['group'] and timestamp(health['bundle_valid_until']) > now() + dt.timedelta(seconds=30))


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
test "$(sha256sum "$path.prepared" | cut -d ' ' -f 1)" = "$prepared"
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

    def snapshot(self):
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
        worker, reference = self.get('pod', c['worker']), self.get('pod', c['referenceWorker'])
        validate_worker(worker, c, source)
        validate_worker(reference, c, source, True)
        journals = json.loads(self.kube('-n', c['namespace'], 'get', 'configmaps', '-l', 'fugue.pro/group=' + c['group'] + ',fugue.pro/authority-kind=transition-journal', '-o', 'json'))
        if journals['items']:
            raise ValueError('Group authority has an unfinished transition')
        return owner, source, previous, raw, worker, reference

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
    owner, target, previous, raw, worker, reference = recovery.snapshot()
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
            owner, target, previous, raw, worker, reference = recovery.snapshot()
            prepared = recovery.prepare(previous, target)
            deadline = min(time.monotonic() + 55, time.monotonic() + (timestamp(prepared['expires_at']) - now()).total_seconds() - 20)
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
                ACTIVATION_CAS, 'sh', config['activationPath'], digest(raw), digest(prep_raw), base64.b64encode((canonical(target) + '\n').encode()).decode()))
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
