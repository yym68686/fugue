import json
from pathlib import Path
import select
import subprocess
import tempfile
import time
import uuid

from scripts.verify_managed_ssh_image import run


class Connection:
    def __init__(self, cluster, address, port):
        self.name = cluster + '-client-' + uuid.uuid4().hex[:8]
        self.process = subprocess.Popen(['docker', 'run', '--rm', '-i', '--name', self.name, '--label', 'fugue.test.ssh-transport='+cluster, '--network', 'kind', 'alpine:3.22', 'nc', address, str(port)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        try:
            self.identity = self.read()
        except Exception:
            self.close()
            raise

    def read(self):
        ready, _, _ = select.select([self.process.stdout], [], [], 10)
        if not ready:
            raise ValueError('isolated TCP witness did not respond')
        value = self.process.stdout.readline().strip()
        if not value:
            raise ValueError('isolated TCP witness disconnected')
        return value

    def exchange(self, value, identity):
        self.process.stdin.write(value+'\n')
        self.process.stdin.flush()
        if self.read() != identity+':'+value:
            raise ValueError('established TCP stream changed executor or payload')

    def close(self):
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            run(['docker', 'rm', '-f', self.name], timeout=10)
            self.process.wait(timeout=5)
        self.process.stdout.close()
        self.process.stderr.close()


def echo_pod(name, namespace, node, host_network, port):
    script = '#!/bin/sh\nprintf "'+name+'-ready\\n"\nwhile IFS= read -r value; do printf "'+name+':%s\\n" "$value"; done\n'
    return {'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':namespace,'labels':{'fixture':name}},'spec':{'nodeName':node,'hostNetwork':host_network,'automountServiceAccountToken':False,'terminationGracePeriodSeconds':1,'containers':[{'name':'tcp','image':'alpine:3.22','imagePullPolicy':'IfNotPresent','command':['/bin/sh','-ec','printf %s "$1" >/tmp/fixture; chmod 700 /tmp/fixture; exec nc -lk -p '+str(port)+' -e /tmp/fixture','fixture',script],'resources':{'requests':{'cpu':'5m','memory':'8Mi'},'limits':{'memory':'32Mi'}}}]}}


def fixture_resources(namespace, node, port):
    account = 'transport-witness'
    pods = [echo_pod('previous', namespace, node, True, port), echo_pod('candidate', namespace, node, False, port)]
    for pod in pods:
        pod['spec']['serviceAccountName'] = account
    return [
        {'apiVersion': 'v1', 'kind': 'Namespace', 'metadata': {'name': namespace}},
        {'apiVersion': 'v1', 'kind': 'ServiceAccount', 'metadata': {'name': account, 'namespace': namespace}, 'automountServiceAccountToken': False},
        *pods,
        {'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': 'entry', 'namespace': namespace}, 'spec': {'type': 'ClusterIP', 'selector': {'fixture': 'candidate'}, 'ports': [{'name': 'ssh', 'port': port, 'targetPort': port, 'protocol': 'TCP'}]}},
    ]


def verify():
    cluster = 'fugue-ssh-transport-'+uuid.uuid4().hex[:10]
    kind = ['go','run','sigs.k8s.io/kind@v0.30.0']
    if cluster in run(kind+['get','clusters']).decode().split():
        raise ValueError('isolated cluster name already exists')
    connections = []
    created = False
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=cluster+'-') as directory:
        work = Path(directory)
        kubeconfig = work/'kubeconfig'
        try:
            created = True
            run(kind+['create','cluster','--name',cluster,'--image','kindest/node:v1.34.0','--kubeconfig',str(kubeconfig)],timeout=180)
            run(['docker','pull','alpine:3.22'],timeout=180)
            run(kind+['load','docker-image','--name',cluster,'alpine:3.22'],timeout=180)
            kube = ['kubectl','--kubeconfig',str(kubeconfig)]
            nodes = json.loads(run(kube+['get','nodes','-o','json']))['items']
            if len(nodes)!=1 or nodes[0]['metadata']['name']!=cluster+'-control-plane':
                raise ValueError('transport test reached an unexpected cluster')
            node = nodes[0]
            address = next(value['address'] for value in node['status']['addresses'] if value['type']=='InternalIP')
            namespace = 'transport-fixture'
            port = 29444
            resources = fixture_resources(namespace, node['metadata']['name'], port)
            manifest = work/'resources.json'
            manifest.write_text(json.dumps({'apiVersion':'v1','kind':'List','items':resources}))
            run(kube+['create','-f',str(manifest)])
            run(kube+['wait','-n',namespace,'--for=condition=Ready','pod','--all','--timeout=90s'],timeout=100)

            def connect():
                connection = Connection(cluster,address,port)
                connections.append(connection)
                return connection

            def select_path(identity):
                deadline = time.monotonic()+30
                while True:
                    connection = connect()
                    if connection.identity==identity+'-ready':
                        return connection
                    connection.close()
                    connections.remove(connection)
                    if time.monotonic()>deadline:
                        raise ValueError('public transport selection did not converge')
                    time.sleep(0.25)

            previous = select_path('previous')
            previous.exchange('before','previous')
            baseline = json.loads(run(kube+['get','service','entry','-n',namespace,'-o','json']))

            def replace(spec):
                observed = json.loads(run(kube+['get','service','entry','-n',namespace,'-o','json']))
                if observed['metadata']['uid']!=baseline['metadata']['uid']:
                    raise ValueError('transport Service was replaced')
                desired = dict(observed)
                desired['spec'] = spec
                replacement = work/'replacement.json'
                replacement.write_text(json.dumps(desired))
                run(kube+['replace','-f',str(replacement)])

            selected = dict(baseline['spec'],externalIPs=[address],externalTrafficPolicy='Local')
            replace(selected)
            candidate = select_path('candidate')
            previous.exchange('after-selection','previous')
            candidate.exchange('after-selection','candidate')
            replace(baseline['spec'])
            restored = select_path('previous')
            previous.exchange('after-compensation','previous')
            candidate.exchange('after-compensation','candidate')
            restored.exchange('after-compensation','previous')
            pods = json.loads(run(kube+['get','pods','-n',namespace,'-o','json']))['items']
            if any(status.get('restartCount')!=0 for pod in pods for status in pod['status']['containerStatuses']):
                raise ValueError('transport test restarted an executor')
            return {'schema':'fugue.ssh-front-transport-receipt/v1','verified':True,'checks':['host-network-predecessor','pod-network-candidate','external-address-local-endpoint','new-connection-selection','established-connection-preservation','compensation-preserves-both-streams','zero-executor-restarts'],'elapsed_seconds':round(time.monotonic()-started,2)}
        finally:
            for connection in connections:
                connection.close()
            if created:
                run(kind+['delete','cluster','--name',cluster],timeout=90)


if __name__=='__main__':
    print(json.dumps(verify(),sort_keys=True))
