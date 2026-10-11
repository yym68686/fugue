import argparse
import base64
import json
import ipaddress
import os
from pathlib import Path
import re
import subprocess
import tempfile
from urllib.parse import urlsplit

from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json
from scripts.reconcile_managed_ssh_runtime import name
from scripts import reconcile_managed_ssh_keys as keys


def validate(config):
    if set(config)!={'schema','operation','planID','expectedPlanDigest','namespace','keyringDeclaration','databaseSecret','storageDirectory'} or config['schema']!='fugue.ssh-identity-maintenance-operation/v1' or config['operation'] not in ('plan','apply'):
        raise ValueError('explicit SSH identity maintenance operation required')
    for field in ('namespace','databaseSecret','planID'):
        name(config[field])
    if config['keyringDeclaration']!='deploy/environments/production/managed-ssh/keys.json':
        raise ValueError('independently declared keyring authority required')
    storage=Path(config['storageDirectory'])
    if not storage.is_absolute() or '..' in storage.parts or str(storage)!=config['storageDirectory'] or not str(storage).endswith('/fugue/ssh-identity-maintenance'):
        raise ValueError('dedicated absolute protected maintenance directory required')
    if config['operation']=='plan' and config['expectedPlanDigest']!='' or config['operation']=='apply' and not re.fullmatch(r'sha256:[a-f0-9]{64}',config['expectedPlanDigest']):
        raise ValueError('apply must reference an exact previously reviewed plan digest')
    return config


def material(config,declaration,kube=kubectl):
    keys.validate(declaration)
    if config['namespace']!=declaration['namespace']:
        raise ValueError('maintenance cannot transfer keyring namespace ownership')
    selected=declaration['identity']
    secret=strict_json(kube(['get','secret',selected['secretName'],'-n',config['namespace'],'-o','json']))
    metadata=secret['metadata']
    annotations=metadata.get('annotations',{})
    if metadata.get('deletionTimestamp') or metadata.get('labels',{}).get('app.kubernetes.io/managed-by')!=keys.MANAGER or metadata.get('labels',{}).get('ssh.fugue.dev/keyring-purpose')!='identity' or annotations.get(keys.GENERATION)!=str(declaration['generation']) or annotations.get(keys.CONTENT_DIGEST)!=selected['contentDigest']:
        raise ValueError('maintenance keyring is not the independently committed generation')
    ring=strict_json(base64.b64decode(secret['data']['keyring.json'],validate=True))
    if keys.content_digest(ring)!=selected['contentDigest']:
        raise ValueError('maintenance keyring content differs from its declaration')
    database=strict_json(kube(['get','secret',config['databaseSecret'],'-n',config['namespace'],'-o','json']))
    if database['metadata'].get('deletionTimestamp') or not database['metadata'].get('uid'):
        raise ValueError('declared database credential is unavailable')
    dsn=base64.b64decode(database['data']['FUGUE_DATABASE_URL'],validate=True).decode()
    if not dsn or '\x00' in dsn:
        raise ValueError('declared database credential is invalid')
    return canonical(ring),dsn


def database_route(dsn,namespace,kube=kubectl):
    try:
        address=urlsplit(dsn)
        suffix='.'+namespace+'.svc.cluster.local'
        if address.scheme not in ('postgres','postgresql') or not address.hostname or not address.hostname.endswith(suffix) or address.port is None:
            raise ValueError('explicit database Service authority required')
        service_name=address.hostname[:-len(suffix)]
        name(service_name)
        service=strict_json(kube(['get','service',service_name,'-n',namespace,'-o','json']))
        spec=service['spec']
        selected=ipaddress.ip_address(spec['clusterIP'])
        if service['metadata'].get('deletionTimestamp') or not service['metadata'].get('uid') or spec.get('type','ClusterIP')!='ClusterIP' or not spec.get('selector') or not selected.is_private or selected.is_unspecified or selected.is_multicast or not any(port.get('port')==address.port and port.get('protocol','TCP')=='TCP' for port in spec.get('ports',[])):
            raise ValueError('database Service route is unavailable')
        return str(selected)
    except (ValueError,KeyError,TypeError):
        raise ValueError('database Service route could not be verified') from None


def protected_directory(path):
    path.mkdir(mode=0o700,parents=True,exist_ok=True)
    metadata=path.lstat()
    if not path.is_dir() or path.is_symlink() or metadata.st_mode&0o077 or metadata.st_uid!=os.geteuid():
        raise ValueError('maintenance output directory must remain private and owned by its executor')


def run(config,binary,actor,kube=kubectl):
    validate(config)
    declaration=strict_json(Path(config['keyringDeclaration']).read_bytes())
    ring,dsn=material(config,declaration,kube)
    database_ip=database_route(dsn,config['namespace'],kube)
    storage=Path(config['storageDirectory'])
    protected_directory(storage)
    directory=storage/config['planID']
    if config['operation']=='apply' and not directory.is_dir():
        raise ValueError('reviewed maintenance plan and backup are missing')
    protected_directory(directory)
    backup,plan=directory/'identities.json',directory/'plan.json'
    with tempfile.TemporaryDirectory(prefix='fugue-ssh-keyring-') as temporary:
        keyring=Path(temporary)/'keyring.json'
        descriptor=os.open(keyring,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(descriptor,'w') as output:output.write(ring)
        environment=dict(os.environ,FUGUE_DATABASE_URL=dsn,FUGUE_SSH_MAINTENANCE_DB_IP=database_ip)
        def command(action):
            arguments=[binary,action,'--keyring-file',str(keyring),'--backup-file',str(backup)]
            if action!='snapshot':arguments.extend(['--plan-file',str(plan)])
            if action=='apply':arguments.extend(['--actor',actor])
            result=subprocess.run(arguments,env=environment,capture_output=True,text=True,timeout=50)
            if result.returncode:
                raise ValueError('SSH identity maintenance '+action+' failed; protected data was not emitted')
            return strict_json(result.stdout)
        if config['operation']=='plan':
            if not backup.exists():command('snapshot')
            if not plan.exists():command('plan')
            result=command('check')
            reviewed=strict_json(plan.read_bytes())
            return dict(result,plan_id=config['planID'],rewrap_count=sum(entry['rewrap'] for entry in reviewed['entries']),encryption_key_transitions=sorted({entry['from_key_id']+' -> '+entry['to_key_id'] for entry in reviewed['entries']}),retention=reviewed['retention'],historical_backup_keys_must_be_retained=True)
        reviewed=strict_json(plan.read_bytes())
        if reviewed.get('digest')!=config['expectedPlanDigest']:
            raise ValueError('reviewed plan digest differs from the apply declaration')
        command('check')
        return command('apply')


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('declaration')
    parser.add_argument('--binary',required=True)
    parser.add_argument('--actor',required=True)
    args=parser.parse_args()
    print(canonical(run(strict_json(Path(args.declaration).read_bytes()),args.binary,args.actor)))
