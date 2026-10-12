import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

from scripts import retire_dns_hostport as retirement


def run(arguments, timeout=120, body=None, env=None):
    result = subprocess.run(arguments, input=body, capture_output=True, timeout=timeout, env=env)
    if result.returncode:
        raise RuntimeError('isolated DNS test command failed: ' + ' '.join(arguments[:3]) + '\n' + result.stderr.decode()[-4000:])
    return result.stdout


def verify():
    root = Path(__file__).resolve().parents[1]
    cluster = 'fugue-dns-isolation-' + uuid.uuid4().hex[:10]
    kind = [shutil.which('kind')] if shutil.which('kind') else ['go', 'run', 'sigs.k8s.io/kind@v0.30.0']
    image = 'fugue-dns-transport-fixture:' + cluster
    architecture = run(['docker', 'version', '--format', '{{.Server.Arch}}']).decode().strip()
    if architecture not in {'amd64', 'arm64'}:
        raise ValueError('native Docker architecture required')
    if cluster in run(kind + ['get', 'clusters']).decode().split():
        raise ValueError('isolated cluster name already exists')
    started = time.monotonic()
    observations = []
    created = False
    with tempfile.TemporaryDirectory(prefix=cluster + '-') as directory:
        work = Path(directory)
        kube = ['kubectl', '--kubeconfig', str(work / 'kubeconfig')]
        try:
            environment = dict(os.environ, GOOS='linux', GOARCH=architecture, CGO_ENABLED='0')
            run(['go', 'build', '-trimpath', '-o', str(work / 'fixture'), str(root / 'scripts/testdata/dns_transport_fixture.go')], env=environment)
            (work / 'Dockerfile').write_text('FROM scratch\nCOPY fixture /fixture\nENTRYPOINT ["/fixture"]\n')
            run(['docker', 'build', '-t', image, str(work)])
            created = True
            run(kind + ['create', 'cluster', '--name', cluster, '--image', 'kindest/node:v1.34.0', '--kubeconfig', str(work / 'kubeconfig')], timeout=240)
            run(kind + ['load', 'docker-image', '--name', cluster, image])
            nodes = json.loads(run(kube + ['get', 'nodes', '-o', 'json']))['items']
            if len(nodes) != 1 or nodes[0]['metadata']['name'] != cluster + '-control-plane':
                raise ValueError('test reached an unexpected cluster')
            node = nodes[0]
            address = next(item['address'] for item in node['status']['addresses'] if item['type'] == 'InternalIP')
            namespace = 'dns-isolation'

            def spec(identity, host_port=False):
                ports = [{'containerPort': 53, 'protocol': protocol} for protocol in ['UDP', 'TCP']]
                if host_port:
                    for port in ports:
                        port.update(hostPort=53, hostIP=address)
                return {'nodeName': node['metadata']['name'], 'serviceAccountName': 'fixture', 'automountServiceAccountToken': False, 'terminationGracePeriodSeconds': 1,
                        'volumes': [{'name': 'scratch', 'emptyDir': {}}],
                        'containers': [{'name': 'dns', 'image': image, 'imagePullPolicy': 'Never', 'args': ['serve', identity], 'ports': ports,
                                        'volumeMounts': [{'name': 'scratch', 'mountPath': '/tmp'}],
                                        'readinessProbe': {'exec': {'command': ['/fixture', 'ready']}, 'periodSeconds': 1, 'failureThreshold': 1},
                                        'resources': {'requests': {'cpu': '5m', 'memory': '8Mi'}, 'limits': {'memory': '32Mi'}}}]}

            resources = [{'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace}},
                         {'apiVersion': 'v1', 'kind': 'ServiceAccount', 'metadata': {'name': 'fixture', 'namespace': namespace}, 'automountServiceAccountToken': False},
                         {'apiVersion': 'apps/v1', 'kind': 'DaemonSet', 'metadata': {'name': 'previous', 'namespace': namespace},
                          'spec': {'selector': {'matchLabels': {'fixture': 'previous'}}, 'updateStrategy': {'type': 'RollingUpdate'},
                                   'template': {'metadata': {'labels': {'fixture': 'previous'}}, 'spec': spec('previous', True)}}},
                         {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'candidate', 'namespace': namespace, 'labels': {'fixture': 'candidate'}}, 'spec': spec('candidate')},
                         {'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'entry', 'namespace': namespace},
                          'spec': {'selector': {'fixture': 'candidate'}, 'externalTrafficPolicy': 'Local', 'publishNotReadyAddresses': False, 'externalIPs': [address],
                                   'ports': [{'port': 53, 'targetPort': 53, 'protocol': protocol, 'name': protocol.lower()} for protocol in ['UDP', 'TCP']]}}]
            run(kube + ['create', '-f', '-'], body=json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': resources}).encode())
            run(kube + ['wait', '-n', namespace, '--for=condition=Ready', 'pod', '--all', '--timeout=90s'])

            def query(destination, expected, stage, within_cluster=False):
                for protocol in ['udp', 'tcp']:
                    client = kube + ['exec', '-n', namespace, 'candidate', '--', '/fixture'] if within_cluster else ['docker', 'run', '--rm', '--network', 'kind', image]
                    result = json.loads(run(client + ['query', destination, protocol], timeout=15))
                    observations.append(dict(result, stage=stage))
                    if result.get('identity') != expected or result['reachable'] != (expected is not None):
                        raise ValueError('isolated DNS forwarding mismatch: ' + json.dumps(observations[-1]))

            query(address, 'candidate', 'before-retirement')
            daemonset = json.loads(run(kube + ['get', 'daemonset', 'previous', '-n', namespace, '-o', 'json']))
            operations = retirement.port_patch(daemonset, {'workload_uid': daemonset['metadata']['uid'], 'image': image, 'generation': 1})
            run(kube + ['patch', 'daemonset', 'previous', '-n', namespace, '--type=json', '-p', json.dumps(operations)])
            run(kube + ['rollout', 'status', 'daemonset/previous', '-n', namespace, '--timeout=90s'])
            previous = json.loads(run(kube + ['get', 'pods', '-n', namespace, '-l', 'fixture=previous', '-o', 'json']))['items']
            if len(previous) != 1:
                raise ValueError('retired predecessor must remain exactly once')
            previous = previous[0]
            if previous['spec'].get('hostNetwork') or any(port.get('hostPort') for container in previous['spec']['containers'] for port in container.get('ports', [])):
                raise ValueError('retired predecessor still claims public ports')
            previous_name, previous_ip = previous['metadata']['name'], previous['status']['podIP']
            query(previous_ip, 'previous', 'retained-predecessor', within_cluster=True)
            query(address, 'candidate', 'after-retirement')
            baseline_count = run(kube + ['exec', '-n', namespace, previous_name, '--', '/fixture', 'count'])
            run(kube + ['exec', '-n', namespace, 'candidate', '--', '/fixture', 'unready'])
            deadline = time.monotonic() + 30
            while True:
                slices = json.loads(run(kube + ['get', 'endpointslice', '-n', namespace, '-l', 'kubernetes.io/service-name=entry', '-o', 'json']))['items']
                if slices and all(endpoint.get('conditions', {}).get('ready') is False for item in slices for endpoint in item.get('endpoints', [])):
                    break
                if time.monotonic() >= deadline:
                    raise ValueError('isolated Service never withdrew its candidate')
                time.sleep(1)
            time.sleep(3)
            for sample in range(3):
                query(address, None, 'empty-ready-endpoints-' + str(sample))
            if run(kube + ['exec', '-n', namespace, previous_name, '--', '/fixture', 'count']) != baseline_count:
                raise ValueError('empty Service sent traffic to the retired predecessor')
            query(previous_ip, 'previous', 'predecessor-still-alive', within_cluster=True)
            run(kube + ['exec', '-n', namespace, 'candidate', '--', '/fixture', 'restore'])
            run(kube + ['wait', '-n', namespace, '--for=condition=Ready', 'pod/candidate', '--timeout=30s'])
            time.sleep(3)
            query(address, 'candidate', 'candidate-recovered')
            pods = json.loads(run(kube + ['get', 'pods', '-n', namespace, '-o', 'json']))['items']
            if any(status.get('restartCount') != 0 for pod in pods for status in pod['status']['containerStatuses']):
                raise ValueError('isolated executors restarted unexpectedly')
            return {'schema': 'fugue.dns-transport-isolation/v1', 'verified': True, 'cluster': cluster,
                    'checks': ['udp-tcp-exact-executor', 'real-retirement-cas-patch', 'retained-predecessor-alive', 'empty-ready-endpoints-no-fallback', 'predecessor-receive-counter-unchanged', 'candidate-recovery'],
                    'observations': observations, 'elapsed_seconds': round(time.monotonic() - started, 2)}
        except Exception:
            if created:
                for arguments in [['get', 'pods', '-A', '-o', 'wide'], ['get', 'events', '-n', 'dns-isolation', '--sort-by=.lastTimestamp'], ['logs', '-n', 'dns-isolation', 'candidate', '--tail=20']]:
                    diagnostic = subprocess.run(kube + arguments, capture_output=True, timeout=15)
                    sys.stderr.buffer.write(diagnostic.stdout[-8000:] + diagnostic.stderr[-2000:])
            raise
        finally:
            if created:
                run(kind + ['delete', 'cluster', '--name', cluster], timeout=90)
            run(['docker', 'image', 'rm', image], timeout=30)


if __name__ == '__main__':
    print(json.dumps(verify(), sort_keys=True))
