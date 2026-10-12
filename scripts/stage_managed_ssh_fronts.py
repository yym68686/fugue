import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import urllib.parse

from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json
from scripts.reconcile_managed_ssh_runtime import digest, exact_address, name, verify_artifact_run
from scripts.reconcile_fixed_node_retention import retain_tolerations


MANAGER='fugue-ssh-front-stage'
GENERATION='ssh.fugue.dev/front-stage-generation'
DIGEST='ssh.fugue.dev/front-stage-digest'


def validate(config, receipt):
    fields={'schema','generation','expectedPreviousGeneration','expectedPreviousDigest','namespace','stateConfigMap','image','artifactReceiptDigest','artifactSourceCommit','artifactRunID','apiURL','signingSecret','signingKeyID','signingPreviousKeyID','revokedSigningKeyIDs','portStart','portEnd','cacheHostRoot','healthSourceCIDRs','backendNamespace','nodes'}
    if set(config)!=fields or config['schema']!='fugue.ssh-front-stage/v1':
        raise ValueError('complete independent SSH front stage required')
    generation,previous=config['generation'],config['expectedPreviousGeneration']
    if type(generation) is not int or type(previous) is not int or not 0<=previous<generation<=2**53-1:
        raise ValueError('explicit increasing front stage generation required')
    if (previous==0 and config['expectedPreviousDigest']!='') or (previous>0 and not re.fullmatch(r'sha256:[a-f0-9]{64}',config['expectedPreviousDigest'])):
        raise ValueError('exact previous stage digest required')
    for field in ['namespace','stateConfigMap','signingSecret','backendNamespace']:
        name(config[field])
    if config['namespace']==config['backendNamespace']:
        raise ValueError('front and managed backend ownership must be distinct')
    if not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}',config['signingKeyID']) or (config['signingPreviousKeyID'] and not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}',config['signingPreviousKeyID'])) or not isinstance(config['revokedSigningKeyIDs'],list) or len(config['revokedSigningKeyIDs'])>16 or any(not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_.-]{1,128}',value) for value in config['revokedSigningKeyIDs']):
        raise ValueError('explicit bounded signing-key identities required')
    required={'actual-front-image','signed-route-load','persisted-exact-instance-ack','preserved-target-streams','invalid-configuration-keeps-lkg','credentials-excluded-from-urls','service-public-handoff','compensation-preserves-established-streams'}
    if receipt.get('schema')!='fugue.ssh-front-image-receipt/v1' or receipt.get('verified') is not True or not required.issubset(receipt.get('checks',[])) or receipt.get('transport',{}).get('verified') is not True:
        raise ValueError('actual front image and transport acceptance required')
    if config['artifactReceiptDigest']!=digest(receipt) or config['artifactSourceCommit']!=receipt.get('source_commit') or config['image']!=receipt.get('image') or not re.fullmatch(r'[^\s]+@sha256:[a-f0-9]{64}',config['image']) or not re.fullmatch(r'[a-f0-9]{40}',config['artifactSourceCommit']) or type(config['artifactRunID']) is not int or config['artifactRunID']<1:
        raise ValueError('front artifact provenance differs from intent')
    address=urllib.parse.urlparse(config['apiURL'])
    if address.scheme not in {'http','https'} or not address.hostname or address.username or address.password or address.query or address.fragment or address.path not in {'','/'}:
        raise ValueError('explicit credential-free API origin required')
    if type(config['portStart']) is not int or type(config['portEnd']) is not int or not 1024<=config['portStart']<=config['portEnd']<=65535 or config['portStart']<=7836<=config['portEnd']:
        raise ValueError('bounded public port range excluding health required')
    root=Path(config['cacheHostRoot'])
    if not root.is_absolute() or '..' in root.parts or str(root)!=config['cacheHostRoot'] or not str(root).startswith('/var/lib/fugue/ssh-front/'):
        raise ValueError('dedicated front cache root required')
    if not isinstance(config['healthSourceCIDRs'],list) or not 1<=len(config['healthSourceCIDRs'])<=16:
        raise ValueError('explicit bounded health observation peers required')
    for cidr in config['healthSourceCIDRs']:
        exact_address(cidr)
    nodes=config['nodes']
    if not isinstance(nodes,list) or not 1<=len(nodes)<=64:
        raise ValueError('bounded explicit node enrollment required')
    seen=set()
    for node in nodes:
        if set(node)!={'id','group','publicAddress','credential'} or node['id'] in seen:
            raise ValueError('exact unique node identity required')
        name(node['id'])
        name(node['group'])
        seen.add(node['id'])
        public=ipaddress.ip_address(node['publicAddress'])
        if not public.is_global or str(public)!=node['publicAddress']:
            raise ValueError('canonical public node address required')
        credential=node['credential']
        if credential.get('kind')=='secret' and set(credential)=={'kind','name'}:
            name(credential['name'])
        elif credential.get('kind')=='node-file' and set(credential)=={'kind','path'} and credential['path']=='/etc/fugue/edge-node.env':
            pass
        else:
            raise ValueError('reviewed node credential source required')
    return config


def slot_name(config,node):
    identity=hashlib.sha256(canonical({'stage':digest(config),'node':node['id']}).encode()).hexdigest()[:24]
    return 'ssh-front-'+identity


def objects(config,node):
    slot=slot_name(config,node)
    labels={'app.kubernetes.io/component':'fugue-ssh-front','app.kubernetes.io/managed-by':MANAGER,'fugue.pro/ssh-front-slot':slot,'fugue.pro/ssh-front-node':node['id']}
    metadata={'name':slot,'namespace':config['namespace'],'labels':labels,'annotations':{DIGEST:digest(config)}}
    env=[{'name':'FUGUE_API_URL','value':config['apiURL']},{'name':'FUGUE_SSH_FRONT_EDGE_ID','valueFrom':{'fieldRef':{'fieldPath':'spec.nodeName'}}},{'name':'FUGUE_SSH_FRONT_INSTANCE_UID','valueFrom':{'fieldRef':{'fieldPath':'metadata.uid'}}},{'name':'FUGUE_SSH_FRONT_EDGE_GROUP_ID','value':node['group']},{'name':'FUGUE_SSH_FRONT_ROUTES_CACHE_PATH','value':'/state/routes.json'},{'name':'FUGUE_SSH_FRONT_HEALTH_LISTEN_ADDR','value':':7836'},{'name':'FUGUE_SSH_PUBLIC_PORT_START','value':str(config['portStart'])},{'name':'FUGUE_SSH_PUBLIC_PORT_END','value':str(config['portEnd'])},{'name':'FUGUE_BUNDLE_SIGNING_KEY','valueFrom':{'secretKeyRef':{'name':config['signingSecret'],'key':'FUGUE_BUNDLE_SIGNING_KEY'}}},{'name':'FUGUE_BUNDLE_SIGNING_KEY_ID','value':'control-plane'},{'name':'FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY','valueFrom':{'secretKeyRef':{'name':config['signingSecret'],'key':'FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY','optional':True}}}]
    volumes=[{'name':'state','hostPath':{'path':config['cacheHostRoot']+'/'+slot,'type':'DirectoryOrCreate'}}]
    for variable in env:
        if variable['name']=='FUGUE_BUNDLE_SIGNING_KEY_ID':
            variable['value']=config['signingKeyID']
    env.extend([{'name':'FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID','value':config['signingPreviousKeyID']},{'name':'FUGUE_BUNDLE_REVOKED_KEY_IDS','value':','.join(config['revokedSigningKeyIDs'])}])
    mounts=[{'name':'state','mountPath':'/state'}]
    credential=node['credential']
    if credential['kind']=='secret':
        env.extend([{'name':'FUGUE_EDGE_TOKEN','valueFrom':{'secretKeyRef':{'name':credential['name'],'key':'FUGUE_EDGE_TOKEN'}}},{'name':'FUGUE_EDGE_NODE_ENV_FILE','value':''}])
    else:
        volumes.append({'name':'node-credential','hostPath':{'path':credential['path'],'type':'File'}})
        mounts.append({'name':'node-credential','mountPath':'/run/fugue/node.env','readOnly':True})
        env.append({'name':'FUGUE_EDGE_NODE_ENV_FILE','value':'/run/fugue/node.env'})
    deployment={'apiVersion':'apps/v1','kind':'Deployment','metadata':metadata,'spec':{'replicas':1,'revisionHistoryLimit':1,'selector':{'matchLabels':{'fugue.pro/ssh-front-slot':slot}},'strategy':{'type':'Recreate'},'template':{'metadata':{'labels':labels},'spec':{'nodeName':node['id'],'automountServiceAccountToken':False,'hostNetwork':False,'terminationGracePeriodSeconds':60,'securityContext':{'seccompProfile':{'type':'RuntimeDefault'}},'tolerations':[{'operator':'Exists','effect':'NoSchedule'}],'containers':[{'name':'ssh-front','image':config['image'],'imagePullPolicy':'IfNotPresent','env':env,'ports':[{'name':'health','containerPort':7836}],'readinessProbe':{'httpGet':{'path':'/readyz','port':'health'},'periodSeconds':5,'timeoutSeconds':2},'resources':{'requests':{'cpu':'10m','memory':'32Mi'},'limits':{'memory':'128Mi'}},'securityContext':{'runAsUser':0,'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},'volumeMounts':mounts}],'volumes':volumes}}}}
    deployment['spec']['template']['spec']['tolerations'] = retain_tolerations(deployment['spec']['template']['spec']['tolerations'])
    def selector(namespace,labels):
        return {'namespaceSelector':{'matchLabels':{'kubernetes.io/metadata.name':namespace}},'podSelector':{'matchLabels':labels}}
    policy={'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':metadata,'spec':{'podSelector':{'matchLabels':{'fugue.pro/ssh-front-slot':slot}},'policyTypes':['Ingress','Egress'],'ingress':[{'ports':[{'protocol':'TCP','port':config['portStart'],'endPort':config['portEnd']}]},{'from':[{'ipBlock':{'cidr':cidr}} for cidr in config['healthSourceCIDRs']],'ports':[{'protocol':'TCP','port':7836}]}],'egress':[{'to':[selector('kube-system',{'k8s-app':'kube-dns'})],'ports':[{'protocol':'UDP','port':53},{'protocol':'TCP','port':53}]},{'to':[selector(config['namespace'],{'app.kubernetes.io/component':'api','app.kubernetes.io/name':'fugue'})],'ports':[{'protocol':'TCP','port':8080}]},{'to':[selector(config['backendNamespace'],{'app.kubernetes.io/component':'managed-ssh'})],'ports':[{'protocol':'TCP','port':2222}]}]}}
    return [policy,deployment]


def reconcile(config,receipt,kube=kubectl):
    validate(config,receipt)
    namespace=config['namespace']
    raw=kube(['get','configmap',config['stateConfigMap'],'-n',namespace,'--ignore-not-found','-o','json'])
    current=strict_json(raw) if raw.strip() else None
    if current:
        metadata=current['metadata']
        previous=strict_json(current.get('data',{}).get('declaration.json',''))
        annotations=metadata.get('annotations',{})
        if metadata.get('labels',{}).get('app.kubernetes.io/managed-by')!=MANAGER or metadata.get('deletionTimestamp') or digest(previous)!=annotations.get(DIGEST):
            raise ValueError('previous front stage ownership or integrity differs')
        if previous['namespace']!=namespace or previous['cacheHostRoot']!=config['cacheHostRoot']:
            raise ValueError('stage cannot transfer namespace or cache ownership')
        if annotations.get(GENERATION)==str(config['generation']):
            if annotations.get(DIGEST)!=digest(config):
                raise ValueError('same-generation front stage substitution')
        elif annotations.get(GENERATION)!=str(config['expectedPreviousGeneration']) or annotations.get(DIGEST)!=config['expectedPreviousDigest']:
            raise ValueError('front stage predecessor changed')
    elif config['expectedPreviousGeneration']!=0:
        raise ValueError('missing front stage requires explicit recovery')
    signing=strict_json(kube(['get','secret',config['signingSecret'],'-n',namespace,'-o','json']))
    if not signing.get('data',{}).get('FUGUE_BUNDLE_SIGNING_KEY'):
        raise ValueError('route verification material unavailable')
    planned=[]
    for node in config['nodes']:
        observed=strict_json(kube(['get','node',node['id'],'-o','json']))
        if observed.get('spec',{}).get('unschedulable') or observed.get('metadata',{}).get('labels',{}).get('fugue.io/role.edge')!='true' or observed.get('status',{}).get('nodeInfo',{}).get('operatingSystem')!='linux' or not any(condition.get('type')=='Ready' and condition.get('status')=='True' for condition in observed.get('status',{}).get('conditions',[])):
            raise ValueError('front node is not an eligible ready Linux edge')
        if observed.get('metadata',{}).get('deletionTimestamp') or observed.get('status',{}).get('nodeInfo',{}).get('architecture')!=receipt['architecture'] or node['publicAddress'] not in {item['address'] for item in observed.get('status',{}).get('addresses',[])}:
            raise ValueError('front node address or architecture differs from declaration')
        if node['credential']['kind']=='secret':
            secret=strict_json(kube(['get','secret',node['credential']['name'],'-n',namespace,'-o','json']))
            if not secret.get('data',{}).get('FUGUE_EDGE_TOKEN'):
                raise ValueError('reviewed node credential unavailable')
        planned.extend(objects(config,node))
    for desired in planned:
        metadata=desired['metadata']
        existing=kube(['get',desired['kind'],metadata['name'],'-n',namespace,'--ignore-not-found','-o','json'])
        if existing.strip():
            observed=strict_json(existing)
            old=observed['metadata']
            if old.get('labels',{}).get('app.kubernetes.io/managed-by')!=MANAGER or old.get('annotations',{}).get(DIGEST)!=digest(config) or old.get('deletionTimestamp'):
                raise ValueError('foreign or changed front candidate')
            if desired['kind']=='Deployment' and (observed.get('spec',{}).get('template',{}).get('spec',{}).get('containers',[{}])[0].get('image')!=config['image'] or observed['spec']['template']['spec'].get('nodeName')!=desired['spec']['template']['spec']['nodeName']):
                raise ValueError('immutable front executor differs')
            if desired['kind']=='Deployment':
                desired['spec']['template']['spec']['tolerations'] = observed['spec']['template']['spec'].get('tolerations', [])
            desired['metadata']=dict(metadata,resourceVersion=old['resourceVersion'])
    for desired in planned:
        kube(['apply','--server-side','--field-manager='+MANAGER,'-f','-'],desired)
    desired={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':config['stateConfigMap'],'namespace':namespace,'labels':{'app.kubernetes.io/managed-by':MANAGER},'annotations':{GENERATION:str(config['generation']),DIGEST:digest(config)}},'data':{'declaration.json':canonical(config),'slots.json':canonical([{'node':node['id'],'name':slot_name(config,node)} for node in config['nodes']])}}
    if current:
        desired['metadata'].update(resourceVersion=current['metadata']['resourceVersion'],uid=current['metadata']['uid'])
    kube(['replace' if current else 'create','-f','-'],desired)
    verified=strict_json(kube(['get','configmap',config['stateConfigMap'],'-n',namespace,'-o','json']))
    if verified.get('data')!=desired['data'] or verified.get('metadata',{}).get('annotations',{}).get(DIGEST)!=digest(config):
        raise ValueError('front stage persistence did not verify')
    return {'verified':True,'generation':config['generation'],'digest':digest(config),'slots':strict_json(desired['data']['slots.json']),'public_transport_changed':False}


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('declaration')
    selected=parser.add_mutually_exclusive_group(required=True)
    selected.add_argument('--receipt')
    selected.add_argument('--verify-artifact-run',action='store_true')
    parser.add_argument('--validate-only',action='store_true')
    args=parser.parse_args()
    config=strict_json(Path(args.declaration).read_bytes())
    if args.verify_artifact_run:
        print(canonical(verify_artifact_run(config,os.environ.get('GITHUB_REPOSITORY'),os.environ.get('GH_TOKEN'),job_name='managed_ssh_front_artifact')))
    else:
        receipt=strict_json(Path(args.receipt).read_bytes())
        validate(config,receipt)
        print(canonical({'valid':True,'generation':config['generation']} if args.validate_only else reconcile(config,receipt)))
