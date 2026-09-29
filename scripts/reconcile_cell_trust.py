#!/usr/bin/env python3
"""Install declared cell keyrings from encrypted CI configuration, never generate keys."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re

try:
    from .reconcile_agent_edge_trust import canonical, digest, strict_json, kubectl
except ImportError:
    from reconcile_agent_edge_trust import canonical, digest, strict_json, kubectl

MANAGER = 'fugue-edge-cell-trust'
GENERATION = 'cell-trust.fugue.dev/generation'
DECLARATION = 'cell-trust.fugue.dev/declaration-digest'
CONTENT = 'cell-trust.fugue.dev/content-digest'


def validate(config):
    if set(config)-{'materialSecret'} != {'schema', 'namespace', 'cell', 'generation', 'previousGeneration', 'previousDigest', 'edgeIds', 'secrets'} or config['schema'] != 'fugue.cell-trust/v1':
        raise ValueError('explicit cell trust declaration required')
    if not re.fullmatch(r'cell-[a-z0-9]+(?:-[a-z0-9]+)*', config['cell']) or len(config['cell']) > 63 or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', config['namespace']):
        raise ValueError('canonical cell and namespace required')
    if 'materialSecret' in config and config['materialSecret'] != 'FUGUE_EDGE_CELL_TRUST_'+config['cell'].upper().replace('-', '_'):
        raise ValueError('dedicated encrypted package must bind exactly this cell')
    generation, previous = config['generation'], config['previousGeneration']
    if type(generation) is not int or type(previous) is not int or generation != 1 or previous != 0:
        raise ValueError('this declaration provisions initial cell trust only; existing keys cannot rotate implicitly')
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
        if canonical(ring) != data['keyring.json'] or ring.get('generation') != config['generation']:
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


def resources(config, material):
    validate(config); validate_material(config, material)
    return [{'apiVersion':'v1','kind':'Secret','type':'Opaque','metadata':{'namespace':config['namespace'],'name':s['name'],'labels':{'app.kubernetes.io/managed-by':MANAGER,'fugue.io/authority-cell-id':config['cell']},'annotations':{GENERATION:str(config['generation']),DECLARATION:digest(config),CONTENT:s['digest']}},'data':{k:base64.b64encode(v.encode()).decode() for k,v in material[s['name']].items()}} for s in config['secrets']]


def inspect(current, desired, config):
    if current is None:
        if config['previousGeneration'] != 0: raise ValueError('previous cell trust Secret is absent')
        return
    meta = current.get('metadata',{}); annotations = meta.get('annotations',{})
    if current.get('kind') != 'Secret' or current.get('apiVersion') != 'v1' or current.get('type') != 'Opaque' or current.get('immutable') or meta.get('ownerReferences') or meta.get('deletionTimestamp') or not meta.get('uid') or not meta.get('resourceVersion') or meta.get('name') != desired['metadata']['name'] or meta.get('namespace') != config['namespace'] or any(meta.get('labels',{}).get(k) != v for k,v in desired['metadata']['labels'].items()):
        raise ValueError('refusing foreign or unbound cell trust Secret')
    data = {k:base64.b64decode(v,validate=True).decode() for k,v in current.get('data',{}).items()}
    if digest(data) != annotations.get(CONTENT): raise ValueError('cell trust content drift')
    generation = annotations.get(GENERATION)
    if generation == str(config['generation']):
        if annotations.get(DECLARATION) != digest(config) or current['data'] != desired['data']: raise ValueError('same-generation cell trust changed')
    else:
        raise ValueError('initial cell trust cannot overwrite another generation')


def read(resource):
    m=resource['metadata']; raw=kubectl(['get','secret',m['name'],'-n',m['namespace'],'--ignore-not-found','-o','json'])
    return strict_json(raw) if raw.strip() else None


def reconcile(config, material, check=False):
    desired=resources(config,material); observed=[read(r) for r in desired]; operations=[]
    for old,new in zip(observed,desired):
        inspect(old,new,config)
        if old is not None and old['data']==new['data'] and all(old['metadata']['annotations'].get(k)==v for k,v in new['metadata']['annotations'].items()): continue
        if check:raise ValueError('cell trust not converged')
        if old is None:operations.append((['create','-f','-'],new));continue
        raise ValueError('initial trust cannot modify an existing Secret')
    for args,body in operations:kubectl([*args,'--dry-run=server'],body)
    for args,body in operations:kubectl(args,body)
    for new in desired:
        old=read(new);inspect(old,new,config)
        if old is None or old['data']!=new['data']:raise ValueError('cell trust verification incomplete')


def main():
    parser=argparse.ArgumentParser();parser.add_argument('config');parser.add_argument('--check',action='store_true');args=parser.parse_args()
    raw=Path(args.config).read_bytes()
    if len(raw)>65536:raise ValueError('declaration too large')
    config=validate(strict_json(raw))
    secret=os.environ.pop('FUGUE_EDGE_CELL_TRUST','')
    if not secret or len(secret)>262144:raise ValueError('encrypted configuration missing')
    packages=strict_json(secret);material=packages[config['cell']]
    reconcile(config,material,args.check)
    print(canonical({'verified':True,'cell':config['cell'],'generation':config['generation'],'declaration_digest':digest(config),'authorizes_traffic':False}))


if __name__=='__main__':
    try:main()
    except Exception:raise SystemExit('cell trust reconciliation failed; private diagnostics suppressed') from None
