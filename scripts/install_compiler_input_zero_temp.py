#!/usr/bin/env python3
"""Install the fixed transaction-local zero-temp guard in the normal CI lane.

No caller-supplied SQL, settings or roles; no general SET privilege for apps.
Existing CNPG administration installs only the checked-in no-argument body.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess

from scripts.compiler_input_storage import get

SQL_PATH = Path(__file__).resolve().parent.parent / 'internal/schemamigrate/compiler_input_zero_temp.sql'
FIELDS = {'version', 'namespace', 'cluster', 'clusterUID', 'systemID', 'primary', 'primaryUID', 'database', 'container'}


def validate(c):
    if (type(c) is not dict or set(c) != FIELDS or type(c['version']) is not int or c['version'] != 1
            or not all(type(c[k]) is str and re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', c[k])
                       for k in ('namespace', 'cluster', 'primary', 'database', 'container'))
            or not all(type(c[k]) is str and re.fullmatch(r'[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}', c[k])
                       for k in ('clusterUID', 'primaryUID'))
            or type(c['systemID']) is not str or not re.fullmatch(r'[1-9][0-9]{0,19}', c['systemID'])
            or int(c['systemID']) >= 1 << 64):
        raise ValueError('invalid fixed zero-temp installation identity')
    return c


def ready(c):
    cluster = get(c['namespace'], 'clusters.postgresql.cnpg.io', c['cluster'])
    primary = get(c['namespace'], 'pod', c['primary'])
    m, s, p = cluster['metadata'], cluster.get('status', {}), primary['metadata']
    statuses = [x for x in primary.get('status', {}).get('containerStatuses', []) if x['name'] == c['container']]
    if (m.get('uid') != c['clusterUID'] or m.get('deletionTimestamp')
            or s.get('systemID') != c['systemID'] or s.get('currentPrimary') != c['primary']
            or s.get('targetPrimary') != c['primary'] or s.get('phase') != 'Cluster in healthy state'
            or s.get('readyInstances') != cluster['spec'].get('instances') or s.get('readyInstances', 0) < 1
            or p.get('uid') != c['primaryUID'] or p.get('deletionTimestamp')
            or p.get('labels', {}).get('cnpg.io/cluster') != c['cluster']
            or len(statuses) != 1 or statuses[0].get('ready') is not True
            or not any(x.get('type') == 'Ready' and x.get('status') == 'True'
                       for x in primary.get('status', {}).get('conditions', []))):
        raise ValueError('fixed zero-temp database identity or health changed')


def sql(c, apply):
    validate(c)
    body = SQL_PATH.read_text()
    if not body.endswith('COMMIT;\n') or body.count('COMMIT;') != 1:
        raise ValueError('invalid fixed zero-temp installer transaction')
    # Decimal-only identity cannot supply SQL or target another system.
    bound = "SELECT 1/CASE WHEN system_identifier::text='" + c['systemID'] + "' THEN 1 ELSE 0 END FROM pg_control_system();\n"
    body = body.replace('SELECT pg_advisory_xact_lock', bound + 'SELECT pg_advisory_xact_lock', 1)
    return body if apply else body[:-len('COMMIT;\n')] + 'ROLLBACK;\n'


def install(c, apply=False):
    validate(c)
    ready(c)
    result = subprocess.run(['kubectl', '-n', c['namespace'], 'exec', '-i', c['primary'], '-c', c['container'], '--',
                             'psql', '-X', '-U', 'postgres', '-d', c['database'], '-qAt', '-v', 'ON_ERROR_STOP=1'],
                            input=sql(c, apply), capture_output=True, text=True, timeout=30)
    if result.returncode or len(result.stdout) > 4096:
        raise ValueError('fixed zero-temp installation not confirmed; inspect before retry')
    ready(c)
    return {'schema': 'fugue.compiler-input-zero-temp.installation.v1', 'applied': apply,
            'cluster_uid': c['clusterUID'], 'primary_uid': c['primaryUID'], 'system_id': c['systemID'],
            'general_set_privilege_granted': False, 'data_or_retention_changed': False}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(install(json.loads(Path(args.config).read_text()), args.apply), sort_keys=True))


if __name__ == '__main__':
    main()
