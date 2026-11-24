"""Select configuration-driven production writers before entering their queues."""
import json
from pathlib import PurePosixPath
import re
import subprocess
import sys

from scripts.recover_longhorn_snapshot import Recovery

ROOT = 'deploy/environments/production'
REVIEW = ROOT + '/local-snapshot-review'
REQUESTS = REVIEW + '/requests/'
OBSERVER = ROOT + '/local-snapshot-cleanup/'
DECLARATIONS = {
    'node_policy': ROOT + '/node-participation/policy.json',
    'expansion': ROOT + '/postgres-volume-expansion/recovery.json',
    'protection': ROOT + '/control-plane-postgres/protection.json',
    'capacity_access': ROOT + '/postgres-capacity-observation/rbac.json',
}


def git(*args):
    return subprocess.check_output(['git', *args], text=True)


def read(sha, path):
    return json.loads(git('show', sha + ':' + path))


def select(before, after):
    for sha in (before, after):
        if not re.fullmatch('[0-9a-f]{40}', sha) or sha == '0' * 40:
            raise ValueError('production maintenance requires exact push commit identities')
    git('merge-base', '--is-ancestor', before, after)
    diff = git('diff', '--name-status', '--no-renames', '-z', before, after).split('\0')
    changes = dict(zip(diff[1::2], diff[::2]))
    selected = {name: changes.get(path) == 'A' or
                (changes.get(path) == 'M' and read(before, path) != read(after, path))
                for name, path in DECLARATIONS.items()}
    selected['cleanup_observer'] = any(changes.get(OBSERVER + path) in {'A', 'M'}
                                      for path in ('kustomization.yaml', 'observe.py', 'observer.yaml', 'policy.json'))
    requests = {path: status for path, status in changes.items() if path.startswith(REQUESTS)}
    if not requests:
        return selected
    parents = {str(PurePosixPath(path).parent) for path in requests}
    if len(parents) != 1:
        raise ValueError('add exactly one immutable snapshot request per push')
    request = parents.pop()
    if not re.fullmatch(re.escape(REQUESTS) + '[0-9a-f]{64}', request):
        raise ValueError('snapshot request directory must be the full declaration digest')
    review, approval = (request + '/' + file for file in ('review.json', 'approval.json'))
    if requests != {review: 'A', approval: 'A'} or git('log', '-1', '--full-history', '--format=%H', before, '--', request).strip():
        raise ValueError('snapshot requests are append-only and cannot replay a previously used digest')
    recovery = Recovery(None, read(after, review), read(after, approval), '/dev/null')
    if recovery.digest != PurePosixPath(request).name:
        raise ValueError('snapshot request directory does not bind the approved declaration')
    # Legacy approvals are historical evidence, not new execution requests.
    legacy = REVIEW + '/approval.json'
    for sha in git('log', '--full-history', '--diff-filter=AM', '--format=%H', before, '--', legacy).splitlines():
        if recovery.config['snapshotUID'] == read(sha, legacy)['snapshotUID']:
            raise ValueError('a legacy snapshot authorization cannot be replayed as a new request')
    return {**selected, 'snapshot_request': request}


if __name__ == '__main__':
    print(json.dumps(select(*sys.argv[1:])))
