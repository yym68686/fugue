#!/usr/bin/env python3
"""Install declared cell keyrings from encrypted CI configuration, never generate keys."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import time

try:
    from .reconcile_agent_edge_trust import canonical, digest, strict_json, kubectl
except ImportError:
    from reconcile_agent_edge_trust import canonical, digest, strict_json, kubectl

MANAGER = 'fugue-edge-cell-trust'
GENERATION = 'cell-trust.fugue.dev/generation'
DECLARATION = 'cell-trust.fugue.dev/declaration-digest'
CONTENT = 'cell-trust.fugue.dev/content-digest'


def validate(config):
    fields = {'schema', 'namespace', 'cell', 'generation', 'previousGeneration', 'previousDigest', 'edgeIds', 'secrets'}
    append = config.get('schema') == 'fugue.cell-trust/v2'
    if append:
        fields.add('operation')
    if set(config)-{'materialSecret'} != fields or config['schema'] not in ['fugue.cell-trust/v1', 'fugue.cell-trust/v2'] or append and config['operation'] != 'add-reader':
        raise ValueError('explicit cell trust declaration required')
    if not re.fullmatch(r'cell-[a-z0-9]+(?:-[a-z0-9]+)*', config['cell']) or len(config['cell']) > 63 or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', config['namespace']):
        raise ValueError('canonical cell and namespace required')
    if 'materialSecret' in config and config['materialSecret'] != 'FUGUE_EDGE_CELL_TRUST_'+config['cell'].upper().replace('-', '_'):
        raise ValueError('dedicated encrypted package must bind exactly this cell')
    generation, previous = config['generation'], config['previousGeneration']
    if type(generation) is not int or type(previous) is not int or not 1 <= generation <= 2**64 - 1 or (not append and (generation != 1 or previous != 0)) or (append and (previous < 1 or generation != previous + 1)):
        raise ValueError('initial trust or one explicit successor reader declaration required')
    if (previous == 0 and config['previousDigest'] != '') or (previous > 0 and not re.fullmatch(r'sha256:[0-9a-f]{64}', config['previousDigest'])):
        raise ValueError('exact predecessor declaration digest required')
    edges = config['edgeIds']
    if not isinstance(edges, list) or not 1 <= len(edges) <= 32 or edges != sorted(set(edges)) or any(not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,127}', e) for e in edges):
        raise ValueError('explicit stable Edge membership required')
    secrets = config['secrets']
    expected = ['bundle-signing', 'bundle-readers', 'inventory-writer', 'recovery'] + ['reader-token/'+edge for edge in edges]
    if not isinstance(secrets, list) or len(secrets) != len(expected) or {s.get('purpose') for s in secrets} != set(expected):
        raise ValueError('complete bounded keyring set required')
    names = set()
    for secret in secrets:
        if set(secret) != {'name', 'purpose', 'digest'} or not re.fullmatch(r'sha256:[0-9a-f]{64}', secret['digest']) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', secret['name']) or not secret['name'].startswith('fugue-'+config['cell']+'-') or secret['name'] in names:
            raise ValueError('private material or invalid Secret identity in declaration')
        names.add(secret['name'])
    return config


def secret_bytes(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_-]{43}', value):
        raise ValueError('canonical 256-bit key required')
    raw = base64.urlsafe_b64decode(value+'=')
    if len(raw) != 32 or base64.urlsafe_b64encode(raw).decode().rstrip('=') != value:
        raise ValueError('noncanonical key material')
    return raw


def validate_material(config, material):
    if set(material) != {s['name'] for s in config['secrets']}:
        raise ValueError('encrypted material set differs from declaration')
    purposes = {s['purpose']: s for s in config['secrets']}
    key_material = set()
    def distinct(value):
        raw = secret_bytes(value)
        if raw in key_material: raise ValueError('cell purposes require independent key material')
        key_material.add(raw)
    for item in config['secrets']:
        data = material[item['name']]
        if digest(data) != item['digest']:
            raise ValueError('encrypted material digest mismatch')
        purpose = item['purpose']
        if purpose.startswith('reader-token/'):
            if set(data) != {'token'}: raise ValueError('invalid reader token')
            distinct(data['token'])
            continue
        if set(data) != {'keyring.json'} or not isinstance(data['keyring.json'], str):
            raise ValueError('exact keyring file required')
        ring = strict_json(data['keyring.json'])
        ring_generation = ring.get('generation')
        generation_valid = type(ring_generation) is int and ring_generation == config['generation']
        if config['schema'] == 'fugue.cell-trust/v2' and purpose != 'bundle-readers':
            generation_valid = type(ring_generation) is int and 1 <= ring_generation < config['generation']
        if canonical(ring) != data['keyring.json'] or not generation_valid:
            raise ValueError('canonical generation-bound keyring required')
        if purpose == 'bundle-signing':
            if set(ring) != {'schema','generation','group'} or ring['schema'] != 'edge-control-group-bundle-signing-keyring/v1': raise ValueError('invalid signing keyring')
            group = ring['group']
            if set(group) != {'edge_group_id','primary_key_id','primary_key'} or group['edge_group_id'] != config['cell'] or not re.fullmatch(r'[a-z0-9][a-z0-9._-]{2,63}',group['primary_key_id']): raise ValueError('signing scope mismatch')
            distinct(group['primary_key'])
        elif purpose == 'inventory-writer':
            if set(ring) != {'schema','generation','edge_group_id','active_key_id','active_key'} or ring['schema'] != 'edge-inventory-platform-identity-keyring/v1' or ring['edge_group_id'] != config['cell'] or not re.fullmatch(r'[a-z0-9][a-z0-9._-]{2,63}',ring['active_key_id']): raise ValueError('inventory scope mismatch')
            distinct(ring['active_key'])
        elif purpose == 'recovery':
            if set(ring) != {'schema','generation','edge_group_id','keys'} or ring['schema'] != 'edge-control-group-recovery-keyring/v1' or ring['edge_group_id'] != config['cell'] or len(ring['keys']) != 1: raise ValueError('recovery scope mismatch')
            key = ring['keys'][0]
            if set(key) != {'key_id','secret','not_before_unix','not_after_unix','revoked'} or not re.fullmatch(r'[a-z0-9][a-z0-9._-]{2,63}',key['key_id']): raise ValueError('recovery identity mismatch')
            lifetime(key)
            distinct(key['secret'])
        elif purpose == 'bundle-readers':
            if set(ring) != {'schema','generation','edge_group_id','credentials'} or ring['schema'] != 'edge-control-group-bundle-reader-keyring/v1' or ring['edge_group_id'] != config['cell']: raise ValueError('reader scope mismatch')
            if [c.get('edge_id') for c in ring['credentials']] != config['edgeIds']: raise ValueError('reader membership mismatch')
            ids = set()
            for credential in ring['credentials']:
                if set(credential) != {'credential_id','edge_id','token_digest','not_before_unix','not_after_unix','revoked'} or not re.fullmatch(r'[a-z0-9][a-z0-9._-]{2,63}',credential['credential_id']) or credential['credential_id'] in ids: raise ValueError('reader identity invalid')
                ids.add(credential['credential_id']); lifetime(credential)
                token = material[purposes['reader-token/'+credential['edge_id']]['name']]['token']
                if credential['token_digest'] != 'sha256:'+hashlib.sha256(token.encode()).hexdigest(): raise ValueError('reader token is not bound')
    return material


def lifetime(key):
    start, end = key['not_before_unix'], key['not_after_unix']
    if type(start) is not int or type(end) is not int or not 0 < start < end or end-start > 370*86400 or type(key['revoked']) is not bool:
        raise ValueError('bounded absolute credential lifetime required')


def validate_addition(config, material, previous):
    """Prove all predecessor bytes survive before reading or writing Secrets."""
    validate(config); validate_material(config, material)
    if config['schema'] == 'fugue.cell-trust/v1':
        if previous is not None:
            raise ValueError('initial trust cannot adopt a predecessor')
        return
    if previous is None:
        raise ValueError('exact preceding declaration required for reader addition')
    validate(previous)
    if config['previousGeneration'] != previous['generation'] or config['previousDigest'] != digest(previous) or any(config[k] != previous[k] for k in ['namespace', 'cell']) or config.get('materialSecret') != previous.get('materialSecret'):
        raise ValueError('reader addition differs from exact predecessor')
    old_edges, new_edges = set(previous['edgeIds']), set(config['edgeIds'])
    if not old_edges < new_edges or len(new_edges - old_edges) != 1:
        raise ValueError('exactly one new reader may be added')
    added = next(iter(new_edges - old_edges))
    old = {s['purpose']: s for s in previous['secrets']}
    new = {s['purpose']: s for s in config['secrets']}
    if set(new) != set(old) | {'reader-token/' + added}:
        raise ValueError('reader addition changed unrelated purposes')
    for purpose, item in old.items():
        if new[purpose]['name'] != item['name']:
            raise ValueError('reader addition cannot rename existing Secrets')
        if purpose != 'bundle-readers' and new[purpose] != item:
            raise ValueError('existing keys and reader tokens must remain identical')
    reader = new['bundle-readers']
    ring = strict_json(material[reader['name']]['keyring.json'])
    added_credentials = [c for c in ring['credentials'] if c['edge_id'] == added]
    if len(added_credentials) != 1 or added_credentials[0]['revoked']:
        raise ValueError('one non-revoked new credential required')
    predecessor_ring = dict(ring)
    predecessor_ring['generation'] = previous['generation']
    predecessor_ring['credentials'] = [c for c in ring['credentials'] if c['edge_id'] != added]
    if digest({'keyring.json': canonical(predecessor_ring)}) != old['bundle-readers']['digest']:
        raise ValueError('existing reader credential identity or lifetime changed')


def validate_new_reader_time(config, material, previous):
    added = next(iter(set(config['edgeIds']) - set(previous['edgeIds'])))
    name = next(s['name'] for s in config['secrets'] if s['purpose'] == 'bundle-readers')
    ring = strict_json(material[name]['keyring.json'])
    credential = next(c for c in ring['credentials'] if c['edge_id'] == added)
    if not credential['not_before_unix'] <= time.time() < credential['not_after_unix']:
        raise ValueError('new reader must be within its declared lifetime before granting access')


def resources(config, material, previous=None):
    validate_addition(config, material, previous)
    return [{'apiVersion':'v1','kind':'Secret','type':'Opaque','metadata':{'namespace':config['namespace'],'name':s['name'],'labels':{'app.kubernetes.io/managed-by':MANAGER,'fugue.io/authority-cell-id':config['cell']},'annotations':{GENERATION:str(config['generation']),DECLARATION:digest(config),CONTENT:s['digest']}},'data':{k:base64.b64encode(v.encode()).decode() for k,v in material[s['name']].items()}} for s in config['secrets']]


def inspect(current, desired, config, previous=None):
    if current is None:
        if config['previousGeneration'] != 0 and (previous is None or desired['metadata']['name'] in {s['name'] for s in previous['secrets']}): raise ValueError('previous cell trust Secret is absent')
        return
    meta = current.get('metadata',{}); annotations = meta.get('annotations',{})
    if current.get('kind') != 'Secret' or current.get('apiVersion') != 'v1' or current.get('type') != 'Opaque' or current.get('immutable') or meta.get('ownerReferences') or meta.get('deletionTimestamp') or not meta.get('uid') or not meta.get('resourceVersion') or meta.get('name') != desired['metadata']['name'] or meta.get('namespace') != config['namespace'] or any(meta.get('labels',{}).get(k) != v for k,v in desired['metadata']['labels'].items()):
        raise ValueError('refusing foreign or unbound cell trust Secret')
    data = {k:base64.b64decode(v,validate=True).decode() for k,v in current.get('data',{}).items()}
    if digest(data) != annotations.get(CONTENT): raise ValueError('cell trust content drift')
    generation = annotations.get(GENERATION)
    if generation == str(config['generation']):
        if annotations.get(DECLARATION) != digest(config) or current['data'] != desired['data']: raise ValueError('same-generation cell trust changed')
    elif previous is not None and generation == str(previous['generation']):
        prior = next((s for s in previous['secrets'] if s['name'] == meta['name']), None)
        if prior is None or annotations.get(DECLARATION) != digest(previous) or annotations.get(CONTENT) != prior['digest']:
            raise ValueError('reader addition predecessor identity changed')
    else:
        raise ValueError('initial cell trust cannot overwrite another generation')


def read(resource):
    m=resource['metadata']; raw=kubectl(['get','secret',m['name'],'-n',m['namespace'],'--ignore-not-found','-o','json'])
    return strict_json(raw) if raw.strip() else None


def reconcile(config, material, check=False, previous=None):
    desired=resources(config,material,previous); observed=[read(r) for r in desired]; operations=[]
    for old,new in zip(observed,desired):
        inspect(old,new,config,previous)
        if old is not None and old['data']==new['data'] and all(old['metadata']['annotations'].get(k)==v for k,v in new['metadata']['annotations'].items()): continue
        if check:raise ValueError('cell trust not converged')
        if old is None:operations.append((['create','-f','-'],new,previous is not None));continue
        if previous is None:raise ValueError('initial trust cannot modify an existing Secret')
        # JSON Patch tests preserve UID and resourceVersion. Never replace a
        # Secret recreated or edited after observation, even on a retry.
        patch=[{'op':'test','path':'/metadata/uid','value':old['metadata']['uid']},
               {'op':'test','path':'/metadata/resourceVersion','value':old['metadata']['resourceVersion']}]
        changes_access = old['data'] != new['data']
        if changes_access:
            patch.append({'op':'replace','path':'/data','value':new['data']})
        for key,value in new['metadata']['annotations'].items():
            patch.append({'op':'replace','path':'/metadata/annotations/'+key.replace('~','~0').replace('/','~1'),'value':value})
        operations.append((['patch','secret',new['metadata']['name'],'-n',config['namespace'],'--type=json','--patch-file=/dev/stdin'],patch,changes_access))
    # Provision the new token before granting its digest reader access. No
    # workload is enrolled or restarted by either operation.
    operations.sort(key=lambda op: op[0][0] != 'create')
    if any(changes_access for _,_,changes_access in operations):
        validate_new_reader_time(config,material,previous)
    for args,body,_ in operations:kubectl([*args,'--dry-run=server'],body)
    for args,body,changes_access in operations:
        if changes_access:
            validate_new_reader_time(config,material,previous)
        kubectl(args,body)
    for new in desired:
        old=read(new);inspect(old,new,config,previous)
        if old is None or old['data']!=new['data'] or any(old['metadata']['annotations'].get(k)!=v for k,v in new['metadata']['annotations'].items()):raise ValueError('cell trust verification incomplete')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('config');parser.add_argument('--previous');parser.add_argument('--check',action='store_true');args=parser.parse_args()
    raw=Path(args.config).read_bytes()
    if len(raw)>65536:raise ValueError('declaration too large')
    config=validate(strict_json(raw))
    previous=None
    if args.previous:
        prior_raw=Path(args.previous).read_bytes()
        if len(prior_raw)>65536:raise ValueError('preceding declaration too large')
        previous=validate(strict_json(prior_raw))
    secret=os.environ.pop('FUGUE_EDGE_CELL_TRUST','')
    if not secret or len(secret)>262144:raise ValueError('encrypted configuration missing')
    packages=strict_json(secret);material=packages[config['cell']]
    reconcile(config,material,args.check,previous)
    print(canonical({'verified':True,'cell':config['cell'],'generation':config['generation'],'declaration_digest':digest(config),'authorizes_traffic':False}))


if __name__=='__main__':
    try:main()
    except Exception:raise SystemExit('cell trust reconciliation failed; private diagnostics suppressed') from None
