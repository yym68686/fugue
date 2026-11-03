"""Install a verified backup utility through the normal CI lane.

No volume engine upgrades, process kills, lock breaking or finalizer changes.
"""
import argparse
import hashlib
import json
import subprocess
import time
from pathlib import Path


def kubectl(*args, body=None):
    p = subprocess.run(['kubectl', *args], input=json.dumps(body) if body is not None else None,
                       text=True, check=True, capture_output=True)
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


def main():
    p = argparse.ArgumentParser()
    p.add_argument('config')
    p.add_argument('--image', required=True)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    config = json.loads(Path(args.config).read_text())
    ns = config['namespace']
    if not args.image.startswith(config['repository'] + '@sha256:') or len(args.image.rsplit(':', 1)[1]) != 64:
        raise SystemExit('requires digest-addressed image from the configured repository')
    setting = kubectl('-n', ns, 'get', 'settings.longhorn.io', 'default-engine-image', '-o', 'json')
    if setting['value'] not in (config['expectedPreviousImage'], args.image):
        raise SystemExit('default engine changed; review the declaration before retrying')
    original = volumes(ns)
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
        if live.get('status', {}).get('state') == 'deployed' and quiescent(ns):
            break
        time.sleep(10)
    else:
        raise SystemExit('candidate not ready or backup activity still running; no default change')
    setting = kubectl('-n', ns, 'get', 'settings.longhorn.io', 'default-engine-image', '-o', 'json')
    if setting['value'] not in (config['expectedPreviousImage'], args.image):
        raise SystemExit('default engine changed while waiting')
    if setting['value'] != args.image:
        kubectl('-n', ns, 'patch', 'settings.longhorn.io', 'default-engine-image', '--type=json',
                '-p', json.dumps([
                    {'op': 'test', 'path': '/metadata/resourceVersion', 'value': setting['metadata']['resourceVersion']},
                    {'op': 'test', 'path': '/value', 'value': config['expectedPreviousImage']},
                    {'op': 'replace', 'path': '/value', 'value': args.image},
                ]), '-o', 'json')
    if volumes(ns) != original:
        raise SystemExit('volume engine identities changed during installation; investigate')
    print(json.dumps({'installed': args.image, 'existing_volume_engines_unchanged': True}))


if __name__ == '__main__':
    main()
