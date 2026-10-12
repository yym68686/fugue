import base64
import ipaddress
import json
import os
from pathlib import PurePosixPath
import re
import subprocess
import urllib.parse

from scripts import reconcile_dns_transport as transport


def validate(config):
    recovery = config.get('recovery')
    fields = {'listener', 'node_uid', 'service_uid', 'old_pod_uid', 'candidate_pod_uid', 'candidate_image', 'origin', 'api_address'}
    if not isinstance(recovery, dict) or set(recovery) != fields:
        raise ValueError('complete explicit DNS recovery declaration required')
    matching = [listener for listener in config['listeners'] if listener['name'] == recovery['listener']]
    if len(matching) != 1 or matching[0]['port'] != 53:
        raise ValueError('DNS recovery must select one declared public listener')
    for field in ['node_uid', 'service_uid', 'old_pod_uid', 'candidate_pod_uid']:
        if not isinstance(recovery[field], str) or not re.fullmatch(r'[a-f0-9-]{36}', recovery[field]):
            raise ValueError('DNS recovery requires exact Kubernetes identities')
    if recovery['old_pod_uid'] == recovery['candidate_pod_uid']:
        raise ValueError('DNS recovery requires distinct instances')
    if not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', recovery['candidate_image']):
        raise ValueError('DNS recovery requires an immutable candidate image')
    origin = urllib.parse.urlsplit(recovery['origin'])
    if origin.scheme != 'https' or recovery['origin'] != 'https://' + str(origin.hostname) or not re.fullmatch(r'[a-z0-9][a-z0-9.-]*', str(origin.hostname)):
        raise ValueError('DNS recovery requires an authenticated HTTPS API origin')
    address = ipaddress.ip_address(recovery['api_address'])
    if not address.is_global or str(address) != recovery['api_address']:
        raise ValueError('DNS recovery requires a canonical public API address')


def current_publication(config):
    recovery = config['recovery']
    token = os.environ.get('FUGUE_API_KEY', '')
    if not token or any(character in token for character in '\r\n'):
        raise ValueError('DNS recovery API credential unavailable')
    host = urllib.parse.urlsplit(recovery['origin']).hostname
    headers = 'header = ' + json.dumps('Authorization: Bearer ' + token) + '\n'
    result = subprocess.run(['curl', '--silent', '--show-error', '--fail', '--noproxy', '*', '--max-time', '15',
                             '--resolve', host + ':443:' + recovery['api_address'], '--config', '-',
                             recovery['origin'] + '/v1/platform-state/artifacts/release_set?scope_key=global&channel=full'],
                            input=headers, text=True, capture_output=True, timeout=20)
    if result.returncode or len(result.stdout) > 16 << 20:
        raise ValueError('DNS recovery current publication unavailable')
    return json.loads(result.stdout)


def validate_publication(snapshot, publication):
    artifact, release = publication.get('artifact', {}), publication.get('release', {})
    assignment = snapshot['assignment']
    if (artifact.get('artifact_kind') != 'release_set' or artifact.get('scope_key') != 'global' or artifact.get('status') != 'validated' or
            artifact.get('id') != assignment['release_set_id'] or artifact.get('content_hash') != snapshot['parent_digest'] or
            release.get('artifact_id') != artifact.get('id') or release.get('artifact_kind') != 'release_set' or release.get('scope_key') != 'global' or
            release.get('id') != assignment['artifact_release_id'] or release.get('fencing_token') != assignment['fencing_token'] or
            release.get('status') != 'active' or release.get('release_channel') != 'full' or
            release.get('verification_state') != 'verified' or release.get('verified_lkg_generation') != artifact.get('generation')):
        raise ValueError('DNS recovery candidate does not match current verified full authority')


def verify_candidate(candidate, config):
    pod, snapshot = candidate['pod'], candidate['snapshot']
    containers = [container for container in pod['spec']['containers'] if container.get('image') == config['recovery']['candidate_image']]
    if len(containers) != 1 or pod['metadata']['uid'] != config['recovery']['candidate_pod_uid']:
        raise ValueError('DNS recovery candidate identity or image changed')
    container = containers[0]
    environment = {entry['name']: entry for entry in container.get('env', [])}
    if len(environment) != len(container.get('env', [])) or container.get('envFrom'):
        raise ValueError('DNS recovery candidate environment is ambiguous')
    def literal(name, default=''):
        entry = environment.get(name, {})
        if entry.get('valueFrom'):
            raise ValueError('DNS recovery requires explicit verification settings')
        return entry.get('value', default)
    def secret(name):
        entry = environment.get(name, {})
        reference = entry.get('valueFrom', {}).get('secretKeyRef')
        if not reference:
            if entry.get('value'):
                raise ValueError('DNS recovery refuses inline signing material')
            return ''
        observed = transport.read_json('get', 'secret', reference['name'], '-n', config['namespace'], '-o', 'json')
        encoded = observed.get('data', {}).get(reference['key'])
        if not encoded and reference.get('optional'):
            return ''
        if not encoded:
            raise ValueError('DNS recovery verification material unavailable')
        return base64.b64decode(encoded, validate=True).decode()
    cache = literal('FUGUE_DNS_CACHE_PATH')
    parsed = PurePosixPath(cache)
    if not parsed.is_absolute() or '..' in parsed.parts or str(parsed) != cache:
        raise ValueError('DNS recovery checkpoint path is not canonical')
    request = {'node_id': pod['spec']['nodeName'], 'group_id': literal('FUGUE_EDGE_GROUP_ID'), 'zone': literal('FUGUE_DNS_ZONE'),
               'scope': literal('FUGUE_PLATFORM_ARTIFACT_SCOPE', 'global'),
               'signing_key': secret('FUGUE_BUNDLE_SIGNING_KEY'), 'signing_key_id': literal('FUGUE_BUNDLE_SIGNING_KEY_ID'),
               'previous_key': secret('FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY'), 'previous_key_id': literal('FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID'),
               'revoked_key_ids': [value.strip() for value in literal('FUGUE_BUNDLE_REVOKED_KEY_IDS').split(',') if value.strip()]}
    request['checkpoint'] = transport.read_json('exec', '-n', config['namespace'], pod['metadata']['name'], '-c', container['name'], '--', 'cat', cache + '.platform-serving.json')
    candidate['snapshot'] = transport.read_json('get', '--raw', candidate['snapshot_path'])
    request['snapshot'] = candidate['snapshot']
    verifier = os.environ.get('FUGUE_DNS_OBSERVATION_VERIFIER', '')
    if not verifier or not os.path.isabs(verifier):
        raise ValueError('DNS recovery observation verifier unavailable')
    result = subprocess.run([verifier], input=json.dumps(request), text=True, capture_output=True, timeout=30)
    if result.returncode or result.stdout.strip() != '{"verified":true}':
        categories = {'invalid bounded DNS verification input': 'input',
                      'DNS observation checkpoint authentication failed': 'checkpoint_authentication',
                      'DNS observation artifact authentication failed': 'artifact_authentication',
                      'DNS observation authority or freshness mismatch': 'authority_or_freshness',
                      'DNS observation proof membership mismatch': 'proof_membership',
                      'DNS observation contains an invalid positive proof': 'positive_proof',
                      'DNS observation lacks complete record readiness': 'record_readiness',
                      'DNS observation cannot materialize all declared answers': 'wire_materialization'}
        category = categories.get(result.stderr.strip(), 'verifier_failed')
        raise ValueError('DNS recovery observation failed cryptographic or readiness verification: ' + category)


def witness(config, listener, current, node):
    recovery = config['recovery']
    if node['metadata']['uid'] != recovery['node_uid'] or current['metadata']['uid'] != recovery['service_uid']:
        raise ValueError('DNS recovery node or Service identity changed')
    selector = ','.join(key + '=' + value for key, value in sorted(current['spec']['selector'].items()))
    pods = transport.read_json('get', 'pods', '-n', config['namespace'], '-l', selector, '-o', 'json')['items']
    previous = [pod for pod in pods if pod.get('spec', {}).get('nodeName') == node['metadata']['name']]
    if len(previous) != 1 or previous[0]['metadata']['uid'] != recovery['old_pod_uid'] or previous[0]['metadata'].get('deletionTimestamp'):
        raise ValueError('DNS recovery predecessor identity changed')
    if transport.ready_pod(previous[0]):
        raise ValueError('healthy DNS predecessor requires normal handoff')
    candidate = transport.handoff_backend(config['namespace'], listener['selector'], node['metadata']['name'], listener['targetPort'])
    verify_candidate(candidate, config)
    validate_publication(candidate['snapshot'], current_publication(config))
    current_pod = transport.read_json('get', 'pod', previous[0]['metadata']['name'], '-n', config['namespace'], '-o', 'json')
    next_pod = transport.read_json('get', 'pod', candidate['pod']['metadata']['name'], '-n', config['namespace'], '-o', 'json')
    for before, after in [(previous[0], current_pod), (candidate['pod'], next_pod)]:
        if before['metadata']['uid'] != after['metadata']['uid'] or before['metadata']['resourceVersion'] != after['metadata']['resourceVersion']:
            raise ValueError('DNS recovery observation changed during validation')
    snapshot = candidate['snapshot']
    return {'node_uid': node['metadata']['uid'], 'old_uid': previous[0]['metadata']['uid'], 'candidate_uid': candidate['pod']['metadata']['uid'],
            'assignment': snapshot['assignment'], 'parent_digest': snapshot['parent_digest'], 'plan_digest': snapshot['plan_digest'], 'recovery': True}
