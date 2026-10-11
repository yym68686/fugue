import argparse
import ipaddress
import os
from pathlib import Path
import tempfile
import urllib.parse

from scripts import observe_managed_ssh_front as observation
from scripts import select_managed_ssh_transport as transport
from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json


def validate_operation(operation):
    if set(operation) != {'schema', 'stage', 'selection', 'action'} or operation['schema'] != 'fugue.ssh-transport-operation/v1' or operation['action'] not in ('stage', 'activate', 'compensate'):
        raise ValueError('explicit transport operation required')
    root = Path('deploy/environments/production/managed-ssh')
    if Path(operation['stage']) != root/'front-stage.json' or Path(operation['selection']).parent != root/'transport' or Path(operation['selection']).suffix != '.json' or '..' in Path(operation['selection']).parts:
        raise ValueError('transport operation must name declared configuration files')
    return operation


def api_origin(stage, kube=kubectl):
    origin = urllib.parse.urlparse(stage['apiURL'])
    if origin.scheme not in ('https', 'http') or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment or origin.path not in ('', '/'):
        raise ValueError('credential-free declared API origin required')
    suffix = '.'+stage['namespace']+'.svc.cluster.local'
    if origin.scheme == 'http' and origin.hostname.endswith(suffix):
        service_name = origin.hostname[:-len(suffix)]
        transport.name(service_name)
        service = strict_json(kube(['get', 'service', service_name, '-n', stage['namespace'], '-o', 'json']))
        port = origin.port or 80
        if service['metadata'].get('deletionTimestamp') or not service['metadata'].get('uid') or service['spec'].get('type') != 'ClusterIP' or not any(item['port'] == port and item.get('protocol', 'TCP') == 'TCP' for item in service['spec']['ports']):
            raise ValueError('declared API Service is not available at its selected port')
        address = ipaddress.ip_address(service['spec']['clusterIP'])
        host = '['+str(address)+']' if address.version == 6 else str(address)
        return 'http://'+host+':'+str(port)
    return stage['apiURL'].rstrip('/')


def run(operation, probe_binary):
    validate_operation(operation)
    stage = strict_json(Path(operation['stage']).read_bytes())
    origin = api_origin(stage)
    config = strict_json(Path(operation['selection']).read_bytes())
    transport.validate(config, stage)
    token = os.environ.get('FUGUE_API_KEY')
    if not token:
        raise ValueError('administrator observation credential required')
    api = lambda path: observation.read_json(origin+path, token)
    probe = lambda target, identity: observation.probe_identity(probe_binary, target, identity)
    observe = lambda public: observation.observe(stage, config['node'], config['applications'], api, probe, public=public)
    with tempfile.TemporaryDirectory(prefix='fugue-ssh-transport-witness-') as directory:
        witness = None
        if operation['action'] != 'stage':
            private_key = os.environ.get('FUGUE_SSH_WITNESS_KEY')
            if not private_key:
                raise ValueError('authenticated migration witness key required before public selection')
            path = Path(directory)/'witness'
            descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, 'w') as output:
                output.write(private_key.rstrip()+'\n')
            witness = lambda address, binding: transport.HeldSSH(address, binding, str(path))
        return transport.select(config, stage, operation['action'], observe, witness)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('operation')
    parser.add_argument('--probe-binary', required=True)
    args = parser.parse_args()
    print(canonical(run(strict_json(Path(args.operation).read_bytes()), args.probe_binary)))
