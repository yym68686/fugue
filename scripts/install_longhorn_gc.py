"""Install a verified backup utility through the normal CI lane.

No volume engine upgrades, process kills, lock breaking or finalizer changes.
"""
import argparse
import hashlib
import json
import re
import subprocess
import time
from pathlib import Path


def kubectl(*args, body=None):
    p = subprocess.run(['kubectl', *args], input=json.dumps(body) if body is not None else None,
                       text=True, capture_output=True, timeout=45)
    if p.returncode:
        raise RuntimeError('kubectl failed: ' + p.stderr[-4000:])
    return json.loads(p.stdout) if p.stdout.strip() else None


def volumes(ns):
    return {v['metadata']['uid']: v['spec'].get('image', v['spec'].get('engineImage', ''))
            for v in kubectl('-n', ns, 'get', 'volumes.longhorn.io', '-o', 'json')['items']}


def quiescent(ns):
    for b in kubectl('-n', ns, 'get', 'backups.longhorn.io', '-o', 'json')['items']:
        if b['metadata'].get('deletionTimestamp') or b.get('status', {}).get('state') not in ('Completed', 'Error'):
            return False
    for v in kubectl('-n', ns, 'get', 'volumes.longhorn.io', '-o', 'json')['items']:
        if v.get('status', {}).get('restoreRequired') or v.get('status', {}).get('restoreInitiated'):
            return False
    return True


def manager_engine_patch(manager, container_name, previous_image, image):
    for index, container in enumerate(manager['spec']['template']['spec']['containers']):
        if container['name'] != container_name:
            continue
        command = container.get('command', [])
        if command.count('--engine-image') != 1:
            raise ValueError('manager must declare exactly one --engine-image flag')
        position = command.index('--engine-image') + 1
        if position >= len(command) or command[position] not in (previous_image, image):
            raise ValueError('manager engine image differs from the reviewed predecessor')
        if command[position] == image:
            return []
        root = f'/spec/template/spec/containers/{index}'
        return [
            {'op': 'test', 'path': '/metadata/uid', 'value': manager['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': manager['metadata']['resourceVersion']},
            {'op': 'test', 'path': root + '/name', 'value': container_name},
            {'op': 'test', 'path': root + '/command', 'value': command},
            {'op': 'replace', 'path': root + f'/command/{position}', 'value': image},
        ]
    raise ValueError('configured manager container is absent')


def main():
    p = argparse.ArgumentParser()
    p.add_argument('config')
    p.add_argument('--image', required=True)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    config = json.loads(Path(args.config).read_text())
    ns = config['namespace']
    if not args.image.startswith(config['repository'] + '@sha256:') or not re.fullmatch('[0-9a-f]{64}', args.image.rsplit(':', 1)[1]):
        raise SystemExit('requires digest-addressed image from the configured repository')
    setting = kubectl('-n', ns, 'get', 'settings.longhorn.io', 'default-engine-image', '-o', 'json')
    if setting['value'] not in (config['expectedPreviousImage'], args.image):
        raise SystemExit('default engine changed; review the declaration before retrying')
    upgrades = kubectl('-n', ns, 'get', 'settings.longhorn.io',
                       'concurrent-automatic-engine-upgrade-per-node-limit', '-o', 'json')
    if upgrades['value'] != '0':
        raise SystemExit('automatic volume engine upgrades must already be disabled')
    manager = kubectl('-n', ns, 'get', 'daemonset', config['managerDaemonSet'], '-o', 'json')
    manager_engine_patch(manager, config['managerContainer'], config['expectedPreviousImage'], args.image)
    original = volumes(ns)
    previous_name = 'ei-' + hashlib.sha512(setting['value'].strip().encode()).hexdigest()[:8]
    previous = kubectl('-n', ns, 'get', 'engineimages.longhorn.io', previous_name, '-o', 'json')
    print(json.dumps({'previous': setting['value'], 'candidate': args.image, 'volumes': original, 'apply': args.apply}))
    if not args.apply:
        return
    images = kubectl('-n', ns, 'get', 'engineimages.longhorn.io', '-o', 'json')['items']
    candidate = next((v for v in images if v['spec']['image'] == args.image), None)
    if candidate is None:
        candidate = kubectl('create', '-f', '-', '-o', 'json', body={
            'apiVersion': 'longhorn.io/v1beta2', 'kind': 'EngineImage',
            'metadata': {'name': 'ei-' + hashlib.sha512(args.image.strip().encode()).hexdigest()[:8], 'namespace': ns},
            'spec': {'image': args.image},
        })
    deadline = time.monotonic() + 480
    while time.monotonic() < deadline:
        live = kubectl('-n', ns, 'get', 'engineimages.longhorn.io', candidate['metadata']['name'], '-o', 'json')
        if live.get('status', {}).get('state') == 'deployed':
            version_fields = ('cliAPIVersion', 'cliAPIMinVersion', 'controllerAPIVersion',
                              'controllerAPIMinVersion', 'dataFormatVersion', 'dataFormatMinVersion')
            if any(field not in previous.get('status', {}) or live.get('status', {}).get(field) != previous['status'][field]
                   for field in version_fields):
                raise SystemExit('candidate engine protocol or data format changed; no default change')
            if quiescent(ns):
                break
        time.sleep(10)
    else:
        raise SystemExit('candidate not ready or backup activity still running; no default change')
    setting = kubectl('-n', ns, 'get', 'settings.longhorn.io', 'default-engine-image', '-o', 'json')
    if setting['value'] not in (config['expectedPreviousImage'], args.image):
        raise SystemExit('default engine changed while waiting')
    # This setting is read-only. The manager's supported startup flag owns it;
    # changing the utility must survive manager restarts without spoofing an
    # internal Longhorn annotation or changing existing volume engines.
    manager = kubectl('-n', ns, 'get', 'daemonset', config['managerDaemonSet'], '-o', 'json')
    patches = manager_engine_patch(manager, config['managerContainer'], config['expectedPreviousImage'], args.image)
    if patches:
        manager = kubectl('-n', ns, 'patch', 'daemonset', config['managerDaemonSet'],
                          '--type=json', '-p', json.dumps(patches), '-o', 'json')
    deadline = time.monotonic() + 480
    while time.monotonic() < deadline:
        live = kubectl('-n', ns, 'get', 'daemonset', config['managerDaemonSet'], '-o', 'json')
        status = live.get('status', {})
        desired = status.get('desiredNumberScheduled', 0)
        if (status.get('observedGeneration', 0) >= live['metadata']['generation']
                and desired > 0 and status.get('updatedNumberScheduled', 0) == desired
                and status.get('numberReady', 0) == desired
                and status.get('numberUnavailable', 0) == 0):
            setting = kubectl('-n', ns, 'get', 'settings.longhorn.io', 'default-engine-image', '-o', 'json')
            if setting['value'] == args.image:
                break
        time.sleep(10)
    else:
        raise SystemExit('manager rollout or default engine did not converge')
    if volumes(ns) != original:
        raise SystemExit('volume engine identities changed during installation; investigate')
    print(json.dumps({'installed': args.image, 'existing_volume_engines_unchanged': True}))


if __name__ == '__main__':
    main()
