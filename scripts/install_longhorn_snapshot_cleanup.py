"""Publish a pinned host bundle via Fugue's verified node-janitor installer."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

from scripts.install_local_node_resource_policy import verified_executor
from scripts.plan_longhorn_snapshot_recovery import NAME, require
from scripts.recover_longhorn_snapshot import canonical
from scripts.reconcile_longhorn_snapshot_cleanup import validate_policy

ROOT = Path(__file__).resolve().parents[1]
CLEANUP = 'deploy/environments/production/local-snapshot-cleanup'
HOST_BINDING = 'fugue-longhorn-cleanup-executor-host'
RUNTIME = ('run_longhorn_snapshot_cleanup', 'reconcile_longhorn_snapshot_cleanup',
           'recover_longhorn_snapshot', 'plan_longhorn_snapshot_recovery', 'longhorn_snapshot_map')
FILES = {**{f'scripts/{name}.py': f'scripts/{name}.py' for name in RUNTIME},
         'policy.json': CLEANUP + '/policy.json', 'executor.service': CLEANUP + '/executor.service'}
RELEASE_PATHS = tuple(FILES.values()) + (CLEANUP + '/executor.json',
                                          'scripts/install_longhorn_snapshot_cleanup.py',
                                          'scripts/install_local_node_resource_policy.py')

HOST_INSTALL = r'''
import datetime, hashlib, json, os, re, socket, subprocess, sys, tempfile, time
from pathlib import Path

os.environ['PATH']='/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'
p=json.load(sys.stdin)
if not (os.geteuid()==0 and socket.gethostname()==p['node']):
    raise SystemExit('executor install host differs')
unit='fugue-longhorn-local-cleanup.service'
root=Path('/usr/local/lib/fugue/longhorn-local-cleanup')
status=Path('/var/lib/fugue-longhorn-local-cleanup/status.json')
def command(*args):
    return subprocess.run(args, check=True, timeout=45, stdout=subprocess.DEVNULL)
def read_status():
    try:
        return json.loads(status.read_text())
    except (FileNotFoundError, ValueError):
        return {}
if p['status']:
    command('systemctl', 'is-active', unit)
    print(json.dumps(read_status()),flush=True)
    sys.exit(0)
sha,mode=p['revision'],p['mode']
if not (re.fullmatch('[0-9a-f]{40}',sha) and mode in ('observe','active')):
    raise SystemExit('invalid host release')
release=root/'releases'/sha
expected={'policy.json','executor.service',*(f'scripts/{x}.py' for x in (
    'run_longhorn_snapshot_cleanup','reconcile_longhorn_snapshot_cleanup','recover_longhorn_snapshot',
    'plan_longhorn_snapshot_recovery','longhorn_snapshot_map'))}
if set(p['files']) != expected:
    raise SystemExit('invalid runtime bundle inventory')
def digest(obj):
    return hashlib.sha256(json.dumps(obj,sort_keys=True,separators=(',',':')).encode()).hexdigest()
if digest(p['files']) != p['bundle_sha256']:
    raise SystemExit('runtime bundle digest mismatch')
policy_sha=digest(json.loads(p['files']['policy.json']))
pointer=root/'current'
previous=os.readlink(pointer) if pointer.is_symlink() else None
if mode=='active':
    old=read_status()
    try:
        age=(datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(old['observed_at'])).total_seconds()
    except (KeyError,ValueError):
        age=float('inf')
    # Initial cutover requires a current, actual online canary for the same
    # policy. Subsequent updates can retain the active authority.
    previous_revision=Path(previous).name if previous else None
    if not (0<=age<900 and old.get('revision')==previous_revision and old.get('watch_bootstrapped') and old.get('inventory_verified') and
            (old.get('mode')=='active' or (old.get('mode')=='observe' and old.get('policy_sha256')==policy_sha))):
        raise SystemExit('online read-only canary is not current; no active cutover')
def atomic(path,text):
    with tempfile.NamedTemporaryFile('w',dir=path.parent,delete=False) as f:
        f.write(text); f.flush(); os.fsync(f.fileno())
        tmp=Path(f.name)
    tmp.chmod(0o644)
    tmp.replace(path)
for name,src in p['files'].items():
    path=release/name
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists() and path.read_text()!=src:
        raise SystemExit('immutable version content differs')
    if not path.exists():
        atomic(path,src)
env=f'FUGUE_EXECUTOR_REVISION={sha}\nFUGUE_EXECUTOR_MODE={mode}\n'
if (release/'release.env').exists() and (release/'release.env').read_text()!=env:
    raise SystemExit('immutable version mode differs')
atomic(release/'release.env',env)
unit_path=Path('/etc/systemd/system')/unit
previous_unit=unit_path.read_text() if unit_path.is_file() else None
def point(destination):
    tmp=root/'.next'
    tmp.unlink(missing_ok=True)
    tmp.symlink_to(destination)
    tmp.replace(pointer)
try:
    point(release)
    atomic(unit_path,p['files']['executor.service'])
    command('systemd-analyze','verify',str(unit_path))
    command('systemctl','daemon-reload')
    command('systemctl','enable',unit)
    command('systemctl','restart',unit)
    for _ in range(72):
        live=read_status()
        if (live.get('revision')==sha and live.get('mode')==mode and live.get('inventory_verified')
                and live.get('watch_bootstrapped') and live.get('policy_sha256')==policy_sha):
            command('systemctl','is-active',unit)
            print(json.dumps(dict(bundle_sha256=p['bundle_sha256'],executor=live)),flush=True)
            sys.exit(0)
        time.sleep(5)
    raise RuntimeError('no live inventory/ledger/watch receipt for the exact release')
except Exception:
    if previous and previous_unit is not None:
        point(previous)
        atomic(unit_path,previous_unit)
        command('systemctl','daemon-reload')
        command('systemctl','restart',unit)
    else:
        command('systemctl','disable','--now',unit)
    raise
'''


def packet(revision, status=False):
    config = json.loads((ROOT / CLEANUP / 'executor.json').read_text())
    if (set(config) != {'schema','mode','local_node','local_executor'} or
            config['schema'] != 'fugue.longhorn-cleanup-executor/v1' or config['mode'] not in ('observe','active') or
            not re.fullmatch(r'[0-9a-f]{40}', revision)):
        raise ValueError('invalid executor release declaration')
    require(NAME.fullmatch(config['local_node']), 'invalid executor host name')
    executor = config['local_executor']
    require(set(executor) == {'namespace','selector','daemonset','container','image'}
            and all(NAME.fullmatch(executor[k]) for k in ('namespace','daemonset','container'))
            and re.fullmatch(r'[^@\s]+@sha256:[0-9a-f]{64}', executor['image'])
            and isinstance(executor['selector'], dict) and executor['selector'], 'invalid verified host executor')
    files = {name: (ROOT / path).read_text() for name, path in FILES.items()}
    validate_policy(json.loads(files['policy.json']))
    return config, dict(node=config['local_node'], revision=revision, mode=config['mode'], status=status,
                        bundle_sha256=hashlib.sha256(canonical(files).encode()).hexdigest(), files=files)


def host_binding(target, release, kube):
    # A new node cannot silently coexist with an old enabled unit after a
    # config edit. Host replacement requires explicitly fencing the old host.
    namespace = json.loads(release['files']['policy.json'])['namespace']
    node = target['local_node']
    node_uid = kube(['get','node',node,'-o','json'])['metadata']['uid']
    require(node_uid, 'executor node UID missing')
    data = {'node': node, 'nodeUID': node_uid}
    scope = ['-n', namespace]
    old = kube(scope + ['get','configmap',HOST_BINDING,'--ignore-not-found','-o','json'])
    if old is None and not release['status']:
        declaration = dict(apiVersion='v1', kind='ConfigMap', immutable=True, data=data,
                           metadata=dict(name=HOST_BINDING, namespace=namespace))
        old = kube(scope + ['create','-f','-','-o','json'], declaration)
    require(old and old.get('immutable') is True and old['data'] == data, 'executor is pinned to a different host identity')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('revision', nargs='?')
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--paths', action='store_true')
    args = parser.parse_args()
    if args.paths:
        print('\n'.join(RELEASE_PATHS))
        return
    target, release = packet(args.revision, args.status)
    e = target['local_executor']
    selector = ','.join(k + '=' + v for k, v in sorted(e['selector'].items()))
    kubectl = ['kubectl','-n',e['namespace']]
    def kube(args, data=None):
        proc = subprocess.run(['kubectl', *args], input=canonical(data) if data else None,
                              check=True, text=True, stdout=subprocess.PIPE, timeout=30)
        return json.loads(proc.stdout) if proc.stdout.strip() else None
    pods = kube(['-n',e['namespace'],'get','pods','-l',selector,'--field-selector',
                 'spec.nodeName='+target['local_node'],'-o','json'])
    name = verified_executor(target,pods)
    host_binding(target, release, kube)
    subprocess.run(kubectl+['exec','-i',name,'-c',e['container'],'--','nsenter','-t','1','-n','-u','--',
                            'chroot','/host','/usr/bin/python3','-IB','-c',HOST_INSTALL],
                   input=canonical(release),text=True,check=True,timeout=460)


if __name__ == '__main__':
    main()
