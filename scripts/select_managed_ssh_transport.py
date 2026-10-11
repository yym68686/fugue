import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import select as readiness
import subprocess
import tempfile
import time
import urllib.parse
import uuid

from scripts import observe_managed_ssh_front as observation
from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json
from scripts.reconcile_managed_ssh_runtime import digest, name


MANAGER = 'fugue-managed-ssh-transport'
GENERATION = 'ssh.fugue.dev/transport-generation'
INTENT = 'ssh.fugue.dev/transport-intent'
PHASE = 'ssh.fugue.dev/transport-phase'
SELECTION = 'ssh.fugue.dev/transport-selection'


class HeldSSH:
    def __init__(self, address, binding, private_key):
        self.directory = tempfile.TemporaryDirectory(prefix='fugue-ssh-witness-')
        self.process = None
        try:
            known = Path(self.directory.name)/'known_hosts'
            known.write_text('['+address+']:'+str(binding['public_port'])+' '+binding['public_key'].strip()+'\n')
            known.chmod(0o600)
            command = ['ssh', '-F', '/dev/null', '-T', '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes', '-o', 'StrictHostKeyChecking=yes', '-o', 'GlobalKnownHostsFile=/dev/null', '-o', 'UserKnownHostsFile='+str(known), '-o', 'ConnectTimeout=10', '-o', 'ServerAliveInterval=5', '-o', 'ServerAliveCountMax=2', '-i', private_key, '-p', str(binding['public_port']), binding['route']['user']+'@'+address, 'while IFS= read -r value; do printf "%s\\n" "$value"; done']
            self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1)
            self.exchange()
        except Exception:
            self.close()
            raise

    def exchange(self):
        if self.process.poll() is not None:
            raise ValueError('authenticated SSH witness exited')
        marker = uuid.uuid4().hex
        self.process.stdin.write(marker+'\n')
        self.process.stdin.flush()
        ready, _, _ = readiness.select([self.process.stdout], [], [], 15)
        if not ready or self.process.stdout.readline().rstrip('\n') != marker:
            raise ValueError('authenticated held SSH session was interrupted')

    def close(self):
        if self.process is not None:
            if not self.process.stdin.closed:
                try:
                    self.process.stdin.close()
                except BrokenPipeError:
                    pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
            self.process.stdout.close()
        self.directory.cleanup()


def validate(config, stage):
    fields = {'schema', 'namespace', 'service', 'node', 'stageDigest', 'applications', 'stagedGeneration', 'servingGeneration', 'compensationGeneration'}
    restored = config.get('schema') == 'fugue.ssh-transport-selection/v2'
    if restored:
        fields |= {'previousServiceUID', 'previousIntentDigest'}
    if set(config) != fields or config['schema'] not in ('fugue.ssh-transport-selection/v1', 'fugue.ssh-transport-selection/v2'):
        raise ValueError('complete independent SSH transport intent required')
    for field in ('namespace', 'service', 'node'):
        name(config[field])
    if config['namespace'] != stage['namespace'] or config['stageDigest'] != digest(stage):
        raise ValueError('transport differs from the exact front stage')
    generations = [config[field] for field in ('stagedGeneration', 'servingGeneration', 'compensationGeneration')]
    if any(type(value) is not int for value in generations) or not 1 <= generations[0] < generations[2] <= 2**53-1 or generations != [generations[0], generations[0]+1, generations[0]+2] or not restored and generations != [1, 2, 3]:
        raise ValueError('explicit consecutive transport generations required')
    if restored and (generations[0]<3 or not re.fullmatch(r'[A-Za-z0-9-]{1,128}',config['previousServiceUID']) or not re.fullmatch(r'sha256:[a-f0-9]{64}',config['previousIntentDigest'])):
        raise ValueError('reactivation requires the exact compensated Service and previous intent')
    if not isinstance(config['applications'], list) or not 1 <= len(config['applications']) <= 100 or len(set(config['applications'])) != len(config['applications']):
        raise ValueError('explicit bounded unique application scope required')
    nodes = [node for node in stage['nodes'] if node['id'] == config['node']]
    if len(nodes) != 1 or not ipaddress.ip_address(nodes[0]['publicAddress']).is_global:
        raise ValueError('transport must bind a declared public edge address')
    return nodes[0]


def spec_for(config, evidence, address, public):
    ports = sorted(binding['public_port'] for binding in evidence['bindings'])
    if not ports or len(set(ports)) != len(ports):
        raise ValueError('managed port inventory is empty or ambiguous')
    spec = {'type': 'ClusterIP', 'selector': evidence['selector'], 'ports': [{'name': 'ssh-'+str(port), 'protocol': 'TCP', 'port': port, 'targetPort': port} for port in ports], 'publishNotReadyAddresses': False, 'internalTrafficPolicy': 'Local'}
    if public:
        spec.update(externalIPs=[address], externalTrafficPolicy='Local')
    return spec


def projection(spec):
    return {'type': spec.get('type'), 'selector': spec.get('selector'), 'ports': [{key: item.get(key) for key in ('name', 'protocol', 'port', 'targetPort')} for item in spec.get('ports', [])], 'publishNotReadyAddresses': bool(spec.get('publishNotReadyAddresses')), 'internalTrafficPolicy': spec.get('internalTrafficPolicy'), 'externalIPs': spec.get('externalIPs') or [], 'externalTrafficPolicy': spec.get('externalTrafficPolicy') or ''}


def validate_service(current, config, expected_spec, generation, phase, intent_digest=None):
    meta = current['metadata']
    annotations = meta.get('annotations', {})
    if meta.get('name') != config['service'] or meta.get('namespace') != config['namespace'] or meta.get('labels', {}).get('app.kubernetes.io/managed-by') != MANAGER or meta.get('deletionTimestamp') or not meta.get('uid') or not meta.get('resourceVersion'):
        raise ValueError('SSH transport is foreign or lacks stable ownership')
    if config.get('previousServiceUID') and meta['uid'] != config['previousServiceUID']:
        raise ValueError('reactivation cannot adopt a replacement Service')
    if annotations.get(GENERATION) != str(generation) or annotations.get(INTENT) != (intent_digest or digest(config)) or annotations.get(PHASE) != phase or annotations.get(SELECTION) != digest(projection(expected_spec)) or projection(current['spec']) != projection(expected_spec):
        raise ValueError('SSH transport generation or declared selection changed')
    if current['spec'].get('loadBalancerIP') or current['spec'].get('externalName') or current.get('status', {}).get('loadBalancer', {}).get('ingress'):
        raise ValueError('SSH transport has undeclared external authority')


def patch_selection(current, specification, annotations):
    metadata = current['metadata']
    patch = [{'op': 'test', 'path': '/metadata/uid', 'value': metadata['uid']}, {'op': 'test', 'path': '/metadata/resourceVersion', 'value': metadata['resourceVersion']}, {'op': 'test', 'path': '/metadata/annotations', 'value': metadata['annotations']}, {'op': 'test', 'path': '/metadata/labels', 'value': metadata['labels']}, {'op': 'test', 'path': '/spec', 'value': current['spec']}]
    for field in ('externalIPs', 'externalTrafficPolicy'):
        if field in specification:
            patch.append({'op': 'add', 'path': '/spec/'+field, 'value': specification[field]})
        elif field in current['spec']:
            patch.append({'op': 'remove', 'path': '/spec/'+field})
    patch.append({'op': 'replace', 'path': '/metadata/annotations', 'value': annotations})
    return patch


def select(config, stage, action, observe, witness=None, kube=kubectl, sleep=time.sleep):
    if action not in ('stage', 'activate', 'compensate'):
        raise ValueError('explicit transport operation required')
    node = validate(config, stage)
    evidence = observe(False)
    private_spec = spec_for(config, evidence, node['publicAddress'], False)
    public_spec = spec_for(config, evidence, node['publicAddress'], True)
    namespace = config['namespace']
    staged_generation, serving_generation, compensation_generation = (config[field] for field in ('stagedGeneration','servingGeneration','compensationGeneration'))
    baseline_phase = 'compensated' if config.get('previousServiceUID') else 'staged'
    baseline_intent = config.get('previousIntentDigest')
    def read():
        raw = kube(['get', 'service', config['service'], '-n', namespace, '--ignore-not-found', '-o', 'json'])
        return strict_json(raw) if raw.strip() else None
    def annotations(generation, phase, spec):
        return {GENERATION: str(generation), INTENT: digest(config), PHASE: phase, SELECTION: digest(projection(spec))}
    current = read()
    if action == 'stage':
        if current is not None:
            validate_service(current, config, private_spec, staged_generation, baseline_phase, baseline_intent)
        else:
            if config.get('previousServiceUID'):
                raise ValueError('missing compensated Service requires explicit recovery')
            kube(['create', '-f', '-'], {'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': config['service'], 'namespace': namespace, 'labels': {'app.kubernetes.io/managed-by': MANAGER}, 'annotations': annotations(staged_generation, baseline_phase, private_spec)}, 'spec': private_spec})
            current = read()
            validate_service(current, config, private_spec, staged_generation, baseline_phase)
        return {'verified': True, 'phase': baseline_phase, 'public_changed': False, 'service_uid': current['metadata']['uid']}
    if current is None:
        raise ValueError('missing internal transport stage')
    if witness is None:
        raise ValueError('public selection requires an authenticated held-session witness')
    desired_spec, desired_generation, desired_phase = (public_spec, serving_generation, 'serving') if action == 'activate' else (private_spec, compensation_generation, 'compensated')
    already_selected = current['metadata'].get('annotations',{}).get(GENERATION) == str(desired_generation)
    if already_selected:
        validate_service(current, config, desired_spec, desired_generation, desired_phase)
    else:
        before_spec, before_generation, before_phase = (private_spec, staged_generation, baseline_phase) if action == 'activate' else (public_spec, serving_generation, 'serving')
        validate_service(current, config, before_spec, before_generation, before_phase, baseline_intent if action == 'activate' else None)
    services = strict_json(kube(['get', 'services', '-A', '-o', 'json']))
    if services.get('metadata', {}).get('continue'):
        raise ValueError('public transport ownership inventory incomplete')
    ports = {binding['public_port'] for binding in evidence['bindings']}
    for service in services['items']:
        if service['metadata'].get('uid') == current['metadata']['uid']:
            continue
        addresses = set(service['spec'].get('externalIPs', [])) | {item.get('ip') for item in service.get('status', {}).get('loadBalancer', {}).get('ingress', [])}
        if node['publicAddress'] in addresses and any(port.get('protocol', 'TCP') == 'TCP' and port['port'] in ports for port in service['spec'].get('ports', [])):
            raise ValueError('another Service already owns this public SSH port')
    slices = strict_json(kube(['get', 'endpointslices', '-n', namespace, '-l', 'kubernetes.io/service-name='+config['service'], '-o', 'json']))
    observation.endpoint_witness(current, evidence['pod'], slices, ports)
    latest = observe(True)
    if latest['pod']['metadata']['uid'] != evidence['pod']['metadata']['uid'] or latest['route_digest'] != evidence['route_digest'] or [binding['route'] for binding in latest['bindings']] != [binding['route'] for binding in evidence['bindings']]:
        raise ValueError('front or route ownership changed before selection')
    if already_selected:
        connection = witness(node['publicAddress'], evidence['bindings'][0])
        try:
            connection.exchange()
            validate_service(read(), config, desired_spec, desired_generation, desired_phase)
            return {'verified': True, 'phase': desired_phase, 'public_changed': False, 'service_uid': current['metadata']['uid'], 'already_selected': True}
        finally:
            connection.close()
    patch = patch_selection(current, desired_spec, annotations(desired_generation, desired_phase, desired_spec))
    held = [witness(node['publicAddress'], evidence['bindings'][0])]
    try:
        held[0].exchange()
        try:
            kube(['patch', 'service', config['service'], '-n', namespace, '--type=json', '-p', canonical(patch)])
            selected = read()
            validate_service(selected, config, desired_spec, desired_generation, desired_phase)
            if selected['metadata']['uid'] != current['metadata']['uid']:
                raise ValueError('transport identity changed during selection')
            held.append(witness(node['publicAddress'], evidence['bindings'][0]))
            checks = []
            for index in range(3):
                if index:
                    sleep(5)
                checked = observe(True)
                if checked['pod']['metadata']['uid'] != evidence['pod']['metadata']['uid']:
                    raise ValueError('front instance changed after selection')
                validate_service(read(), config, desired_spec, desired_generation, desired_phase)
                for connection in held:
                    connection.exchange()
                checks.append({'observed_at': checked['observed_at'], 'route_digest': checked['route_digest'], 'bindings': checked['bindings']})
            return {'verified': True, 'phase': desired_phase, 'public_changed': True, 'service_uid': selected['metadata']['uid'], 'observations': checks, 'previous_executors_retained': True, 'held_sessions_preserved': True}
        except Exception as failure:
            if action != 'activate':
                raise
            latest = read()
            if latest is not None and latest['metadata'].get('uid') == current['metadata']['uid']:
                try:
                    validate_service(latest, config, private_spec, staged_generation, baseline_phase, baseline_intent)
                except ValueError:
                    validate_service(latest, config, public_spec, serving_generation, 'serving')
                    rollback = patch_selection(latest, private_spec, annotations(compensation_generation, 'compensated', private_spec))
                    kube(['patch', 'service', config['service'], '-n', namespace, '--type=json', '-p', canonical(rollback)])
                    validate_service(read(), config, private_spec, compensation_generation, 'compensated')
                    for connection in held:
                        connection.exchange()
                    raise ValueError('SSH public verification failed; exact selection compensated and held sessions preserved') from failure
            raise
    finally:
        for connection in reversed(held):
            connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('intent')
    parser.add_argument('stage')
    parser.add_argument('action', choices=['stage', 'activate', 'compensate'])
    parser.add_argument('--api-url', required=True)
    parser.add_argument('--probe-binary', required=True)
    parser.add_argument('--witness-key')
    args = parser.parse_args()
    origin = urllib.parse.urlparse(args.api_url)
    if origin.scheme not in ('https', 'http') or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment or origin.path not in ('', '/'):
        raise ValueError('credential-free API origin required')
    token = os.environ.get('FUGUE_API_KEY')
    if not token:
        raise ValueError('administrator API credential required')
    stage = strict_json(Path(args.stage).read_bytes())
    config = strict_json(Path(args.intent).read_bytes())
    api = lambda path: observation.read_json(args.api_url.rstrip('/')+path, token)
    probe = lambda target, identity: observation.probe_identity(args.probe_binary, target, identity)
    observe = lambda public: observation.observe(stage, config['node'], config['applications'], api, probe, public=public)
    witness = (lambda address, binding: HeldSSH(address, binding, args.witness_key)) if args.witness_key else None
    print(canonical(select(config, stage, args.action, observe, witness)))
