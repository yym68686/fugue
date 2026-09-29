#!/usr/bin/env python3
"""Read-only ingress acceptance from existing restricted application networks."""
import argparse
import ast
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import re
import subprocess

try:
    from . import observe_front_candidate as front
    from . import observe_front_node_conntrack as node_observer
    from . import reconcile_front_probe_transport as transport
except ImportError:
    import observe_front_candidate as front
    import observe_front_node_conntrack as node_observer
    import reconcile_front_probe_transport as transport


def validate(config):
    if set(config) != {'schema', 'namespacePrefixes', 'maxConsumers', 'crictl'} or config['schema'] != 'fugue.front-restricted-egress-observation/v1':
        raise ValueError('explicit restricted egress observation declaration required')
    if not isinstance(config['namespacePrefixes'], list) or not 1 <= len(config['namespacePrefixes']) <= 8 or any(not isinstance(p, str) or not re.fullmatch(r'[a-z][a-z0-9-]{1,62}', p) for p in config['namespacePrefixes']):
        raise ValueError('bounded explicit application namespace prefixes required')
    if type(config['maxConsumers']) is not int or not 1 <= config['maxConsumers'] <= 128 or not re.fullmatch(r'/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+', config['crictl']):
        raise ValueError('bounded consumer count and explicit host runtime reader required')
    return config


def consumers(config, managed):
    if managed.get('metadata', {}).get('continue') or not isinstance(managed.get('items'), list):
        raise ValueError('application intent observation is incomplete')
    selected = []
    for item in managed['items']:
        meta, spec = item.get('metadata', {}), item.get('spec', {})
        namespace = meta.get('namespace', '')
        if meta.get('deletionTimestamp') or not any(namespace.startswith(p) for p in config['namespacePrefixes']):
            continue
        egress = (spec.get('appSpec', {}).get('network_policy') or {}).get('egress') or {}
        if egress.get('mode') != 'restricted' or egress.get('allow_public_internet') is not True or spec.get('appSpec', {}).get('replicas', 0) < 1:
            continue
        if not meta.get('uid') or not meta.get('resourceVersion') or not spec.get('appID'):
            raise ValueError('restricted application has no immutable identity')
        selected.append(item)
    if not 1 <= len(selected) <= config['maxConsumers']:
        raise ValueError('bounded nonempty restricted public-internet application set required')
    return sorted(selected, key=lambda x: (x['metadata']['namespace'], x['metadata']['name']))


def source_pod(managed):
    meta, spec = managed['metadata'], managed['spec']
    result = front.read('get', 'pods', '-n', meta['namespace'], '-l', 'fugue.pro/app-id='+spec['appID'], '-o', 'json')
    if result.get('metadata', {}).get('continue'):
        raise ValueError('restricted source Pod observation is incomplete')
    eligible = []
    for pod in result['items']:
        m, p, state = pod['metadata'], pod['spec'], pod['status']
        if m.get('deletionTimestamp') or p.get('hostNetwork') or state.get('phase') != 'Running' or not any(c.get('type') == 'Ready' and c.get('status') == 'True' for c in state.get('conditions', [])):
            continue
        if m.get('namespace') != meta['namespace'] or m.get('labels', {}).get('fugue.pro/app-id') != spec['appID'] or not m.get('uid') or not state.get('hostIP'):
            raise ValueError('restricted source Pod identity differs from the application')
        containers = [c for c in state.get('containerStatuses', []) if c.get('ready') and re.fullmatch(r'containerd://[0-9a-f]{64}', c.get('containerID', ''))]
        if containers:
            eligible.append((pod, sorted(containers, key=lambda x: x['name'])[0]))
    if not eligible or len(eligible) > 16:
        raise ValueError('restricted application has no bounded Ready source Pod')
    return sorted(eligible, key=lambda x: x[0]['metadata']['name'])[0]


def remote_program():
    # Both programs are fixed repository code. Only addresses and observed
    # runtime IDs are data. No filesystem or namespace configuration is changed.
    source = Path(front.__file__).read_text()
    required = {'now','canonical','timestamp','proof','parse_proof','stable_proof_pair'}
    functions = [ast.get_source_segment(source, item) for item in ast.parse(source).body if isinstance(item, ast.FunctionDef) and item.name in required]
    if len(functions) != len(required):
        raise ValueError('fixed TLS proof reader is incomplete')
    proof_source = 'import base64,datetime,hashlib,http.client,json,re,secrets,socket,ssl\n'+'\n\n'.join(functions)+'\n'
    inner = proof_source + '''
params = json.loads(sys.argv[1])
proofs = []
for route in params['routes']:
    observed = stable_proof_pair(params['address'],params['probe_port'],route['host'],route['path'],params['node'],params['group'])
    proofs.append({'host':route['host'],'path':route['path'],'proof_digest':'sha256:'+hashlib.sha256(canonical(observed['proof']).encode()).hexdigest(),'publication_retries':observed['publication_retries']})
try:
    connection = socket.create_connection((params['private_address'],7831),timeout=2)
except OSError:
    private_blocked = True
else:
    connection.close()
    raise ValueError('restricted egress unexpectedly reached private Front management')
print(canonical({'proofs':proofs,'private_management_blocked':private_blocked}))
'''
    # observe_front_candidate imports sys only in the entrypoint on some
    # revisions; keep the child program's argument reader explicit.
    inner = 'import sys\n'+inner
    return '''import json,subprocess,sys,socket
params=json.loads(sys.argv[1])
raw=subprocess.check_output([params['crictl'],'inspect',params['container_id']],stderr=subprocess.DEVNULL,timeout=10)
if len(raw)>4<<20:raise ValueError('runtime observation exceeds bound')
observed=json.loads(raw)
labels=observed['status'].get('labels',{})
if labels.get('io.kubernetes.pod.uid')!=params['pod_uid'] or labels.get('io.kubernetes.pod.name')!=params['pod_name'] or labels.get('io.kubernetes.pod.namespace')!=params['namespace']:
    raise ValueError('source container identity changed')
pid=observed['info']['pid']
if type(pid) is not int or pid<2:raise ValueError('source container PID invalid')
# A positive host-side control distinguishes policy denial from a dead port.
with socket.create_connection((params['private_address'],7831),timeout=3):pass
result=subprocess.run(['nsenter','-t',str(pid),'-n','--',params['python'],'-I','-B','-c',CHILD,json.dumps(params)],capture_output=True,text=True,timeout=45)
if result.returncode or len(result.stdout)>32768:
    raise ValueError('restricted application ingress observation failed: '+result.stderr[-1200:])
print(result.stdout)
'''.replace('CHILD', repr(inner))


def execute_observation(command):
    for attempt in range(3):
        execution = subprocess.run(command, capture_output=True, text=True, timeout=60)
        if execution.returncode == 0:
            if len(execution.stdout) > 32768:
                raise ValueError('restricted egress observation exceeds bound')
            result = json.loads(execution.stdout)
            result['observer_transport_retries'] = attempt
            return result
        # Only retry the Kubernetes exec transport, not a failed network test
        # or a remote process that returned an error. Every retry rechecks CRI
        # identity inside the fixed observer before entering the source netns.
        failure = execution.stderr.strip()
        transient = failure.endswith(': EOF') or 'unexpected EOF' in failure or 'TLS handshake timeout' in failure
        if not transient or 'Traceback' in failure or 'command terminated with exit code' in failure or attempt == 2:
            raise ValueError('restricted egress network observation failed: '+failure[-1200:])


def observe(config, listener):
    profile = front.validate(json.loads(Path(listener['observation']).read_text()))
    candidate = front.pod(profile, profile['candidateSelector'], True)
    observer_config = node_observer.validate(json.loads(node_observer.CONFIG.read_text()))
    selected = consumers(config, front.read('get', 'managedapps.fugue.pro', '-A', '-o', 'json'))
    def observe_consumer(managed):
        pod, container = source_pod(managed)
        node, observer = node_observer.observer(observer_config, pod['status']['hostIP'])
        if node['metadata']['name'] != pod['spec']['nodeName']:
            raise ValueError('source Pod and host observer disagree on node identity')
        params = {'crictl':config['crictl'],'python':observer_config['python'],'container_id':container['containerID'].split('://')[1], 'pod_uid':pod['metadata']['uid'],'pod_name':pod['metadata']['name'],'namespace':pod['metadata']['namespace'], 'address':listener['address'],'probe_port':listener['port'],'node':profile['node'],'group':profile['group'],'routes':profile['probes'],'private_address':candidate['status']['podIP']}
        command = ['kubectl','-n',observer_config['namespace'],'exec',observer['metadata']['name'],'-c',observer_config['container'],'--','nsenter','-t','1','-n','--','chroot',observer_config['hostRoot'],observer_config['python'],'-I','-B','-c',remote_program(),json.dumps(params)]
        result = execute_observation(command)
        if result.get('private_management_blocked') is not True or len(result.get('proofs',[])) != len(profile['probes']):
            raise ValueError('restricted egress observation is incomplete')
        latest = front.read('get','managedapp',managed['metadata']['name'],'-n',managed['metadata']['namespace'],'-o','json')
        fresh_pod, fresh_container = source_pod(managed)
        fresh_node, fresh_observer = node_observer.observer(observer_config, pod['status']['hostIP'])
        if fresh_node['metadata']['uid'] != node['metadata']['uid'] or fresh_observer['metadata']['uid'] != observer['metadata']['uid'] or fresh_observer['spec'] != observer['spec']:
            raise ValueError('host observer identity changed during network observation')
        if latest['metadata']['uid'] != managed['metadata']['uid'] or latest['spec'] != managed['spec'] or fresh_pod['metadata']['uid'] != pod['metadata']['uid'] or fresh_container['containerID'] != container['containerID']:
            raise ValueError('application intent or source executor changed during observation')
        return {'at':front.now().isoformat(),'app_id':managed['spec']['appID'],'managed_uid':managed['metadata']['uid'],'pod_uid':pod['metadata']['uid'],'node_uid':node['metadata']['uid'],'observer_uid':observer['metadata']['uid'],'listener':listener['name'],**result}
    with ThreadPoolExecutor(max_workers=4) as workers:
        results = list(workers.map(observe_consumer, selected))
    if front.pod(profile, profile['candidateSelector'], True)['metadata']['uid'] != candidate['metadata']['uid']:
        raise ValueError('candidate Front identity changed during restricted egress observation')
    return {'schema':'fugue.front-restricted-egress-evidence/v1','authorizes_traffic':False,'declaration_digest':transport.digest(config),'listener_digest':transport.digest(listener),'observations':results}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--listener',required=True)
    parser.add_argument('--evidence',required=True)
    args=parser.parse_args()
    config=validate(json.loads(Path(args.config).read_text()))
    declared=transport.validate(json.loads(Path('deploy/environments/production/front-probe-transport/listeners.json').read_text()))
    listener=transport.select_named(declared,'listeners',args.listener)['listeners'][0]
    try:
        result=observe(config,listener)
    except Exception as error:
        failure={'schema':'fugue.front-restricted-egress-failure/v1','authorizes_traffic':False,'at':front.now().isoformat(),'listener':listener['name'],'declaration_digest':transport.digest(config),'error_type':type(error).__name__,'error':str(error)[-2048:]}
        Path(args.evidence).write_text(front.canonical(failure)+'\n')
        raise
    Path(args.evidence).write_text(front.canonical(result)+'\n')
    print(front.canonical({'restricted_app_egress_verified':True,'consumers':len(result['observations']),'listener':listener['name']}))

if __name__=='__main__':main()
