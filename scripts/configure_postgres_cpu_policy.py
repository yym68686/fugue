#!/usr/bin/env python3
"""Apply the two Postgres CPU increase policy gates without replacing code.

Runs only in the declarative CI entrypoint. Uses CAS and preserves every other
Deployment field, the pinned image and the current positive serving artifacts.
It never patches a database Pod, resource quota, PVC or resize operation.
"""
import argparse
import copy
import hashlib
import json
import os
import re
import shlex
import subprocess
from pathlib import Path

FIELDS = {
    'cpuRequestUpscale': 'FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_REQUEST_UPSCALE_ENABLED',
    'cpuLimitUpscale': 'FUGUE_MANAGED_POSTGRES_IN_PLACE_CPU_LIMIT_UPSCALE_ENABLED',
}
ANNOTATION = 'fugue.pro/postgres-cpu-policy-sha256'


def plan(config, live):
    required = {'schema', 'namespace', 'deployment', 'container', 'expectedImage', 'expectedConfigSha', *FIELDS}
    if set(config) != required or config['schema'] != 'fugue.managed-postgres-cpu-policy/v1':
        raise ValueError('Unsupported CPU policy schema or fields')
    if any(type(config[k]) is not bool for k in FIELDS):
        raise ValueError('CPU policy values must be booleans')
    if not re.fullmatch(r'.+@sha256:[a-f0-9]{64}', config['expectedImage']):
        raise ValueError('Expected image must be immutable')
    if not re.fullmatch(r'[a-f0-9]{40}', config['expectedConfigSha']):
        raise ValueError('Expected configuration revision is required')
    meta, spec, status = live['metadata'], live['spec'], live.get('status', {})
    if meta['name'] != config['deployment'] or meta['namespace'] != config['namespace']:
        raise ValueError('Deployment identity mismatch')
    if meta.get('deletionTimestamp') or not meta.get('uid') or not meta.get('resourceVersion'):
        raise ValueError('Deployment cannot be fenced')
    replicas = spec.get('replicas', 1)
    strategy = spec.get('strategy', {})
    rolling = strategy.get('rollingUpdate', {})
    if replicas < 2 or strategy.get('type') != 'RollingUpdate' or str(rolling.get('maxUnavailable')) not in ('0', '0%') or str(rolling.get('maxSurge', 0)) in ('0', '0%'):
        raise ValueError('At least two replicas and zero unavailable rolling policy required')
    if status.get('observedGeneration', 0) < meta.get('generation', 0) or status.get('updatedReplicas', 0) != replicas or status.get('availableReplicas', 0) < replicas:
        raise ValueError('A healthy converged controller is required')
    containers = spec['template']['spec']['containers']
    matches = [(i,c) for i,c in enumerate(containers) if c['name'] == config['container']]
    if len(matches) != 1:
        raise ValueError('Controller container is ambiguous')
    index, container = matches[0]
    if container['image'] != config['expectedImage'] or meta.get('annotations', {}).get('fugue.pro/production-config-sha') != config['expectedConfigSha']:
        raise ValueError('Pinned code/config changed; re-review the policy precondition')
    prefix = f'/spec/template/spec/containers/{index}'
    patch = [{'op':'test','path':'/metadata/uid','value':meta['uid']},
             {'op':'test','path':'/metadata/resourceVersion','value':meta['resourceVersion']},
             {'op':'test','path':prefix+'/image','value':container['image']}]
    changes = {}
    for key, name in FIELDS.items():
        entries = [(i,e) for i,e in enumerate(container.get('env',[])) if e['name']==name]
        if len(entries)!=1 or entries[0][1].get('value') not in ('true','false') or 'valueFrom' in entries[0][1]:
            raise ValueError('Expected explicit CPU direction gate is missing')
        env_index, entry = entries[0]
        desired = str(config[key]).lower()
        if entry['value'] != desired:
            patch.append({'op':'replace','path':prefix+f'/env/{env_index}/value','value':desired})
            changes[name] = {'before':entry['value'],'after':desired}
    digest = hashlib.sha256(json.dumps(config,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    if not changes:
        return [], {'policySHA256':digest,'image':container['image'],'changes':{},'alreadyApplied':True}
    annotations = copy.deepcopy(meta.get('annotations',{}))
    annotations[ANNOTATION] = digest
    patch.append({'op':'add','path':'/metadata/annotations','value':annotations})
    return patch, {'policySHA256':digest,'image':container['image'],'changes':changes,'deploymentUID':meta['uid'],'previousResourceVersion':meta['resourceVersion']}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--evidence', required=True)
    args=parser.parse_args()
    config=json.loads(Path(args.config).read_text())
    command=shlex.split(os.environ.get('KUBECTL','kubectl'))
    def kube(*argv):
        return subprocess.check_output(command+list(argv),text=True,timeout=330)
    ns, name=config['namespace'],config['deployment']
    live=json.loads(kube('-n',ns,'get','deployment',name,'-o','json'))
    patch,evidence=plan(config,live)
    evidence['mode']='apply' if args.apply else 'preview'
    evidence['patch']=patch
    Path(args.evidence).write_text(json.dumps(evidence,indent=2)+'\n')
    if args.apply and patch:
        # JSON Patch tests fence competing deployments; failures leave live state intact.
        kube('-n',ns,'patch','deployment',name,'--type=json','-p',json.dumps(patch))
        kube('-n',ns,'rollout','status','deployment/'+name,'--timeout=300s')
        after=json.loads(kube('-n',ns,'get','deployment',name,'-o','json'))
        remaining,result=plan(config,after)
        if remaining:
            raise RuntimeError('CPU policy did not converge')
        evidence['verified']=result
        Path(args.evidence).write_text(json.dumps(evidence,indent=2)+'\n')
    print(json.dumps({k:v for k,v in evidence.items() if k!='patch'},sort_keys=True))


if __name__=='__main__':
    main()
