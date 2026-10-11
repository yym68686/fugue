import argparse
import ipaddress
import json
import os
from pathlib import Path
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
    if set(config) != fields or config['schema'] != 'fugue.ssh-transport-selection/v1':
        raise ValueError('complete independent SSH transport intent required')
    for field in ('namespace', 'service', 'node'):
        name(config[field])
    if config['namespace'] != stage['namespace'] or config['stageDigest'] != digest(stage):
        raise ValueError('transport differs from the exact front stage')
    generations = [config[field] for field in ('stagedGeneration', 'servingGeneration', 'compensationGeneration')]
    if any(type(value) is not int for value in generations) or generations != [1, 2, 3]:
        raise ValueError('initial transport must declare stage, selection, and compensation generations')
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


def validate_service(current, config, expected_spec, generation, phase):
    meta = current['metadata']
    annotations = meta.get('annotations', {})
    if meta.get('name') != config['service'] or meta.get('namespace') != config['namespace'] or meta.get('labels', {}).get('app.kubernetes.io/managed-by') != MANAGER or meta.get('deletionTimestamp') or not meta.get('uid') or not meta.get('resourceVersion'):
        raise ValueError('SSH transport is foreign or lacks stable ownership')
    if annotations.get(GENERATION) != str(generation) or annotations.get(INTENT) != digest(config) or annotations.get(PHASE) != phase or annotations.get(SELECTION) != digest(projection(expected_spec)) or projection(current['spec']) != projection(expected_spec):
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
    def read():
        raw = kube(['get', 'service', config['service'], '-n', namespace, '--ignore-not-found', '-o', 'json'])
        return strict_json(raw) if raw.strip() else None
    def annotations(generation, phase, spec):
        return {GENERATION: str(generation), INTENT: digest(config), PHASE: phase, SELECTION: digest(projection(spec))}
    current = read()
    if action == 'stage':
        if current is not None:
            validate_service(current, config, private_spec, 1, 'staged')
        else:
            kube(['create', '-f', '-'], {'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': config['service'], 'namespace': namespace, 'labels': {'app.kubernetes.io/managed-by': MANAGER}, 'annotations': annotations(1, 'staged', private_spec)}, 'spec': private_spec})
            current = read()
            validate_service(current, config, private_spec, 1, 'staged')
        return {'verified': True, 'phase': 'staged', 'public_changed': False, 'service_uid': current['metadata']['uid']}
    if current is None:
        raise ValueError('missing internal transport stage')
    if witness is None:
        raise ValueError('public selection requires an authenticated held-session witness')
    before_spec, before_generation, before_phase = (private_spec, 1, 'staged') if action == 'activate' else (public_spec, 2, 'serving')
    validate_service(current, config, before_spec, before_generation, before_phase)
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
    desired_spec, desired_generation, desired_phase = (public_spec, 2, 'serving') if action == 'activate' else (private_spec, 3, 'compensated')
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
                    validate_service(latest, config, private_spec, 1, 'staged')
                except ValueError:
                    validate_service(latest, config, public_spec, 2, 'serving')
                    rollback = patch_selection(latest, private_spec, annotations(3, 'compensated', private_spec))
                    kube(['patch', 'service', config['service'], '-n', namespace, '--type=json', '-p', canonical(rollback)])
                    validate_service(read(), config, private_spec, 3, 'compensated')
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
