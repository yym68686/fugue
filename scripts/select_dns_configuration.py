"""Select DNS mutations before acquiring their shared transport concurrency fence."""
import json
import subprocess
import sys


def select(paths):
    paths = set(paths)
    return {
        'transport': bool(paths & {'deploy/environments/production/dns-transport/transport.json',
                                  'scripts/reconcile_dns_transport.py', 'scripts/recover_dns_transport.py'}),
        'ports': any(path.startswith('deploy/environments/production/dns-port-retirement/') for path in paths),
        'slots': 'deploy/environments/production/dns-slot-retirement/intent.json' in paths,
    }


if __name__ == '__main__':
    paths = subprocess.check_output(['git', 'diff', '--name-only', sys.argv[1], sys.argv[2], '--'], text=True).splitlines()
    print(json.dumps(select(paths)))
