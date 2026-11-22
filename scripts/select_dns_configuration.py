"""Select DNS mutations before acquiring their shared transport concurrency fence."""
import json
import subprocess
import sys

TRANSPORT_DECLARATION = 'deploy/environments/production/dns-transport/transport.json'
LEDGER_INSPECTION = 'deploy/environments/production/dns-transport/ledger-inspection.json'
TRANSPORT_EXECUTORS = {
    'scripts/reconcile_dns_transport.py',
    'scripts/recover_dns_transport.py',
    'scripts/inspect_dns_predecessor.py',
    'scripts/dns_transport_failed_predecessor.py',
    'scripts/dns_transport_rollback_authority.py',
    'scripts/capture_dns_observation_policy.py',
}


def select(paths):
    paths = set(paths)
    return {
        'transport': bool(paths & (TRANSPORT_EXECUTORS | {TRANSPORT_DECLARATION, LEDGER_INSPECTION})),
        'ports': any(path.startswith('deploy/environments/production/dns-port-retirement/') for path in paths),
        'slots': 'deploy/environments/production/dns-slot-retirement/intent.json' in paths,
    }


if __name__ == '__main__':
    paths = subprocess.check_output(['git', 'diff', '--name-only', sys.argv[1], sys.argv[2], '--'], text=True).splitlines()
    print(json.dumps(select(paths)))
