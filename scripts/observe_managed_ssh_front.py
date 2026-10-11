import argparse
import datetime
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import urllib.parse
import urllib.request

from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json
from scripts.reconcile_managed_ssh_runtime import digest, name
from scripts.stage_managed_ssh_fronts import DIGEST, GENERATION, MANAGER, slot_name


class EvidencePending(ValueError):
    pass


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def timestamp(value):
    parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timestamp must have an explicit timezone')
    return parsed


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file, code, message, headers, new_url):
        raise ValueError('observation redirects are not allowed')


def read_json(url, token=None):
    headers = {'Authorization': 'Bearer '+token} if token else {}
    query = urllib.request.Request(url, headers=headers)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(query, timeout=10) as response:
            raw = response.read(8*1024*1024+1)
    except Exception:
        raise ValueError('SSH observation request failed') from None
    if len(raw)>8*1024*1024:
        raise ValueError('SSH observation exceeded response bound')
    return strict_json(raw)


def validate_ack(value, node, pod, health, evaluated):
    ack = value['acknowledgement']
    if value.get('fresh') is not True or value.get('current_routes_match') is not True:
        raise ValueError('persisted acknowledgement is stale or differs from authorized routes')
    if ack.get('edge_id') != node['id'] or ack.get('edge_group_id') != node['group'] or ack.get('instance_uid') != pod['metadata']['uid'] or ack.get('persisted') is not True:
        raise ValueError('acknowledgement belongs to another node or Pod')
    if not isinstance(ack.get('routes'), list) or not isinstance(ack.get('connections'), list):
        raise ValueError('complete route and connection inventory required')
    for field in ('loaded_at', 'observed_at'):
        observed = timestamp(ack[field])
        if observed > evaluated+datetime.timedelta(seconds=5) or observed < evaluated-datetime.timedelta(seconds=120):
            raise ValueError('acknowledgement clock or freshness differs')
    if timestamp(ack['expires_at']) <= evaluated+datetime.timedelta(seconds=10) or timestamp(ack['expires_at']) > timestamp(ack['observed_at'])+datetime.timedelta(seconds=120):
        raise ValueError('acknowledgement expires before verification can finish')
    if health.get('status') != 'ok' or health.get('draining') is not False or health.get('instance_uid') != pod['metadata']['uid'] or health.get('route_digest') != ack.get('route_digest') or health.get('bundle_version') != ack.get('bundle_version') or health.get('last_ack_error') or health.get('last_error'):
        raise ValueError('live front differs from its persisted acknowledgement')
    if timestamp(health['last_ack_at']) < evaluated-datetime.timedelta(seconds=45) or timestamp(health['last_ack_at']) > evaluated+datetime.timedelta(seconds=5):
        raise ValueError('front has not acknowledged recently')
    return ack


def selected_binding(app_id, status, identity, ack, node):
    intent, binding, control = status['intent'], status['binding'], status['control']
    active = control.get('active')
    if not active or not intent['spec']['enabled'] or intent['app_id'] != app_id or binding['app_id'] != app_id or identity['app_id'] != app_id:
        raise ValueError('application has no committed enabled managed SSH binding')
    if not binding['tenant_id'] == intent['tenant_id'] == identity['tenant_id'] or not binding['identity_id'] == intent['spec']['identity_id'] == control['identity_id'] == identity['id']:
        raise ValueError('application identity ownership differs')
    if binding.get('edge_group_id') and binding['edge_group_id'] != node['group']:
        raise ValueError('binding is outside this edge group')
    routes = [route for route in ack['routes'] if route['public_port'] == binding['public_port']]
    if len(routes) != 1:
        raise ValueError('acknowledgement omits or duplicates selected public binding')
    route = routes[0]
    if route['app_id'] != app_id or route['tenant_id'] != binding['tenant_id'] or route['target_host'] != active['id']+'.'+active['namespace']+'.svc.cluster.local' or route['target_port'] != 2222 or route['status'] not in ('ready', 'pending'):
        raise ValueError('front route differs from the committed managed target')
    evidence = active.get('evidence')
    if evidence and (evidence.get('fingerprint') != identity['fingerprint'] or evidence.get('identity_id') != identity['id']):
        raise ValueError('active managed target identity differs from its public inventory')
    if not active.get('verified') or not evidence or timestamp(evidence['expires_at']) <= now()+datetime.timedelta(seconds=10):
        raise EvidencePending('active managed target is awaiting fresh identity evidence: '+app_id)
    return {'app_id': app_id, 'tenant_id': identity['tenant_id'], 'identity_id': identity['id'], 'binding_id': binding['id'], 'public_port': binding['public_port'], 'public_key': identity['public_key'], 'fingerprint': identity['fingerprint'], 'route': route, 'slot_id': active['id'], 'route_generation': control['route_generation']}


def probe_identity(binary, target, identity):
    request = {key: identity[key] for key in ('public_key', 'fingerprint')}
    request['target'] = target
    result = subprocess.run([binary], input=canonical(request), capture_output=True, text=True, timeout=8)
    if result.returncode:
        raise ValueError('SSH key-exchange verification failed for '+target)
    observed = strict_json(result.stdout)
    if observed.get('key_exchange_verified') is not True or observed.get('identity_matched') is not True or observed.get('fingerprint') != identity['fingerprint']:
        raise ValueError('SSH probe did not verify the declared identity')
    return observed


def endpoint_witness(service, pod, slices, ports):
    metadata, specification = service['metadata'], service['spec']
    if not metadata.get('uid') or metadata.get('deletionTimestamp') or specification.get('publishNotReadyAddresses'):
        raise ValueError('transport Service has no stable ready-only identity')
    expected_ports = {('ssh-'+str(port), 'TCP', port) for port in ports}
    endpoints = []
    if slices.get('metadata', {}).get('continue'):
        raise ValueError('incomplete EndpointSlice inventory')
    for item in slices['items']:
        owners = item['metadata'].get('ownerReferences', [])
        if item['metadata'].get('deletionTimestamp') or not any(owner.get('kind') == 'Service' and owner.get('uid') == metadata['uid'] and owner.get('controller') is True for owner in owners):
            raise ValueError('EndpointSlice belongs to another Service')
        if {(entry['name'], entry.get('protocol', 'TCP'), entry['port']) for entry in item.get('ports', [])} != expected_ports:
            raise ValueError('EndpointSlice listener set differs')
        endpoints.extend(item.get('endpoints', []))
    if len(endpoints) != 1:
        raise ValueError('exactly one transport endpoint required')
    endpoint = endpoints[0]
    target = endpoint.get('targetRef', {})
    conditions = endpoint.get('conditions', {})
    if endpoint.get('nodeName') != pod['spec']['nodeName'] or endpoint.get('addresses') != [pod['status']['podIP']] or target.get('uid') != pod['metadata']['uid'] or target.get('kind') != 'Pod' or target.get('name') != pod['metadata']['name'] or target.get('namespace') != pod['metadata']['namespace'] or conditions.get('ready') is not True or conditions.get('terminating') is True:
        raise ValueError('transport endpoint is not the exact ready candidate')
    return {'service_uid': metadata['uid'], 'pod_uid': target['uid'], 'ports': sorted(ports), 'address': pod['status']['podIP']}


def observe(stage, node_id, app_ids, api, probe, kube=kubectl, health_read=read_json, public=False):
    name(node_id)
    if not isinstance(app_ids, list) or not 1 <= len(app_ids) <= 100 or len(set(app_ids)) != len(app_ids) or any(not isinstance(app, str) or not app.startswith('app_') or any(character not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_' for character in app) for app in app_ids):
        raise ValueError('bounded unique application identities required')
    nodes = [node for node in stage['nodes'] if node['id'] == node_id]
    if len(nodes) != 1:
        raise ValueError('node is absent from the exact stage')
    node = nodes[0]
    namespace = stage['namespace']
    state = strict_json(kube(['get', 'configmap', stage['stateConfigMap'], '-n', namespace, '-o', 'json']))
    if state['metadata'].get('labels', {}).get('app.kubernetes.io/managed-by') != MANAGER or state['metadata'].get('deletionTimestamp') or state['metadata'].get('annotations', {}).get(DIGEST) != digest(stage) or state['metadata']['annotations'].get(GENERATION) != str(stage['generation']) or strict_json(state['data']['declaration.json']) != stage:
        raise ValueError('stage declaration is not the exact persisted generation')
    slot = slot_name(stage, node)
    deployment = strict_json(kube(['get', 'deployment', slot, '-n', namespace, '-o', 'json']))
    selector = {'fugue.pro/ssh-front-slot': slot}
    template = deployment['spec']['template']['spec']
    if deployment['metadata'].get('annotations', {}).get(DIGEST) != digest(stage) or deployment['metadata'].get('labels', {}).get('app.kubernetes.io/managed-by') != MANAGER or deployment['metadata'].get('deletionTimestamp') or deployment['spec'].get('replicas') != 1 or deployment['spec']['selector']['matchLabels'] != selector or template.get('nodeName') != node_id or template.get('hostNetwork') is True or len(template['containers']) != 1 or template['containers'][0]['image'] != stage['image']:
        raise ValueError('front executor differs from its stage')
    pods = strict_json(kube(['get', 'pods', '-n', namespace, '-l', 'fugue.pro/ssh-front-slot='+slot, '-o', 'json']))
    if pods.get('metadata', {}).get('continue') or len(pods['items']) != 1:
        raise ValueError('front Pod selection is missing or ambiguous')
    pod = pods['items'][0]
    containers = pod['status'].get('containerStatuses', [])
    if pod['metadata'].get('deletionTimestamp') or not pod['metadata'].get('uid') or pod['spec']['nodeName'] != node_id or pod['spec'].get('hostNetwork') is True or len(containers) != 1 or containers[0].get('ready') is not True or containers[0].get('restartCount') != 0 or containers[0].get('imageID', '').removeprefix('docker-pullable://') != stage['image'] or pod['spec']['containers'][0]['image'] != stage['image']:
        raise ValueError('front Pod is not the exact healthy immutable executor')
    replicas = [owner for owner in pod['metadata'].get('ownerReferences', []) if owner.get('kind') == 'ReplicaSet' and owner.get('controller') is True]
    if len(replicas) != 1:
        raise ValueError('candidate has no unique ReplicaSet owner')
    replica = strict_json(kube(['get', 'replicaset', replicas[0]['name'], '-n', namespace, '-o', 'json']))
    if replica['metadata']['uid'] != replicas[0]['uid'] or not any(owner.get('kind') == 'Deployment' and owner.get('uid') == deployment['metadata']['uid'] and owner.get('controller') is True for owner in replica['metadata'].get('ownerReferences', [])):
        raise ValueError('candidate ownership does not lead to its declared Deployment')
    observed_node = strict_json(kube(['get', 'node', node_id, '-o', 'json']))
    if node['publicAddress'] not in {address['address'] for address in observed_node['status']['addresses']} or not any(condition['type'] == 'Ready' and condition['status'] == 'True' for condition in observed_node['status']['conditions']):
        raise ValueError('node is not ready or no longer owns the public address')
    address = str(ipaddress.ip_address(pod['status']['podIP']))
    host = '['+address+']' if ':' in address else address
    health = health_read('http://'+host+':7836/healthz')
    ack_path = '/v1/admin/ssh/front-acknowledgements/'+node_id+'/'+pod['metadata']['uid']
    acknowledgement = api(ack_path)
    ack = validate_ack(acknowledgement, node, pod, health, now())
    bindings = []
    for app_id in app_ids:
        status = api('/v1/apps/'+app_id+'/ssh/managed/status')
        identity = api('/v1/apps/'+app_id+'/ssh/identity')['identity']
        binding = selected_binding(app_id, status, identity, ack, node)
        binding['candidate_probe'] = probe(host+':'+str(binding['public_port']), identity)
        if public:
            public_host = '['+node['publicAddress']+']' if ':' in node['publicAddress'] else node['publicAddress']
            binding['public_probe'] = probe(public_host+':'+str(binding['public_port']), identity)
        bindings.append(binding)
    latest = api(ack_path)
    if latest['acknowledgement']['route_digest'] != ack['route_digest']:
        raise ValueError('route set changed during observation')
    validate_ack(latest, node, pod, health_read('http://'+host+':7836/healthz'), now())
    for binding in bindings:
        app_id = binding['app_id']
        current = selected_binding(app_id, api('/v1/apps/'+app_id+'/ssh/managed/status'), api('/v1/apps/'+app_id+'/ssh/identity')['identity'], latest['acknowledgement'], node)
        if current != {key: value for key, value in binding.items() if key not in ('candidate_probe', 'public_probe')}:
            raise ValueError('managed binding changed during observation')
    current_pod = strict_json(kube(['get', 'pod', pod['metadata']['name'], '-n', namespace, '-o', 'json']))
    current_deployment = strict_json(kube(['get', 'deployment', slot, '-n', namespace, '-o', 'json']))
    current_stage = strict_json(kube(['get', 'configmap', stage['stateConfigMap'], '-n', namespace, '-o', 'json']))
    if current_pod['metadata']['uid'] != pod['metadata']['uid'] or current_pod.get('spec') != pod.get('spec') or current_pod['metadata'].get('deletionTimestamp') or current_deployment['metadata']['uid'] != deployment['metadata']['uid'] or current_deployment.get('spec') != deployment.get('spec') or current_deployment['metadata'].get('deletionTimestamp') or current_stage.get('data') != state.get('data') or current_stage['metadata']['uid'] != state['metadata']['uid']:
        raise ValueError('candidate or stage changed during observation')
    public_pod = {'metadata': {key: pod['metadata'][key] for key in ('name', 'namespace', 'uid')}, 'spec': {'nodeName': node_id}, 'status': {'podIP': address}}
    return {'schema': 'fugue.ssh-front-observation/v1', 'verified': True, 'authorizes_traffic': False, 'observed_at': now().isoformat(), 'stage_digest': digest(stage), 'node': node_id, 'pod': public_pod, 'selector': selector, 'image': stage['image'], 'route_digest': ack['route_digest'], 'bindings': bindings}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stage')
    parser.add_argument('node')
    parser.add_argument('--app', action='append', required=True)
    parser.add_argument('--probe-binary', required=True)
    parser.add_argument('--api-url', required=True)
    parser.add_argument('--public', action='store_true')
    args = parser.parse_args()
    origin = urllib.parse.urlparse(args.api_url)
    if origin.scheme not in ('https', 'http') or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment or origin.path not in ('', '/'):
        raise ValueError('credential-free API origin required')
    token = os.environ.get('FUGUE_API_KEY')
    if not token:
        raise ValueError('administrator API credential required')
    stage = strict_json(Path(args.stage).read_bytes())
    api = lambda path: read_json(args.api_url.rstrip('/')+path, token)
    probe = lambda target, identity: probe_identity(args.probe_binary, target, identity)
    print(canonical(observe(stage, args.node, args.app, api, probe, public=args.public)))
