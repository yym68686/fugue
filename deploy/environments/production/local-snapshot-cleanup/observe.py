"""Metadata-only Longhorn cleanup observation; cannot authorize a native fold."""
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import ssl
import sys
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener


SA = Path('/var/run/secrets/kubernetes.io/serviceaccount')
API = 'https://kubernetes.default.svc'
LIMIT = 4 << 20


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Incomplete(ValueError):
    pass


def read_only_client():
    token = (SA / 'token').read_text().strip()
    ca = ssl.create_default_context(cafile=str(SA / 'ca.crt'))
    opener = build_opener(ProxyHandler({}), NoRedirect(), HTTPSHandler(context=ca))

    def get(path, **selectors):
        request = Request(API + path + ('?' + urlencode(selectors) if selectors else ''),
                          method='GET', headers={'Authorization': 'Bearer ' + token})
        with opener.open(request, timeout=6) as response:
            raw = response.read(LIMIT + 1)
        if len(raw) > LIMIT:
            raise Incomplete('response_limit')
        return json.loads(raw)

    return get


def name(value):
    if not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,251}[a-z0-9]|[a-z0-9]', value or ''):
        raise Incomplete('invalid_resource_name')
    return value


def live_uid(obj):
    metadata = obj.get('metadata', {})
    if metadata.get('deletionTimestamp') or not metadata.get('uid'):
        raise Incomplete('resource_identity_unavailable')
    return metadata['uid']


def observe(get, policy):
    ns = name(policy['namespace'])
    cluster_name = name(policy['cluster'])
    storage = name(policy['longhornNamespace'])
    lease_name = name(policy['coordinationLease'])
    cluster_path = f'/apis/postgresql.cnpg.io/v1/namespaces/{ns}/clusters/{cluster_name}'
    lease_path = f'/apis/coordination.k8s.io/v1/namespaces/{ns}/leases/{lease_name}'
    core = f'/api/v1/namespaces/{ns}'
    longhorn = f'/apis/longhorn.io/v1beta2/namespaces/{storage}'

    cluster = get(cluster_path)
    status = cluster.get('status', {})
    primary = name(status.get('currentPrimary'))
    if primary != status.get('targetPrimary'):
        raise Incomplete('primary_transition')
    pod = get(f'{core}/pods/{primary}')
    claim = get(f'{core}/persistentvolumeclaims/{primary}')
    pv = get(f"/api/v1/persistentvolumes/{name(claim.get('spec', {}).get('volumeName'))}")
    spec = pv.get('spec', {})
    csi = spec.get('csi', {})
    ref = spec.get('claimRef', {})
    if (ref.get('uid'), ref.get('namespace'), ref.get('name'), csi.get('driver')) != (
            live_uid(claim), ns, primary, 'driver.longhorn.io'):
        raise Incomplete('claim_binding_changed')
    handle = name(csi.get('volumeHandle'))
    volume_path = f'{longhorn}/volumes/{handle}'
    volume = get(volume_path)
    lease = get(lease_path)
    owner = lease.get('spec', {}).get('holderIdentity') or ''
    snapshots = get(f'{longhorn}/snapshots', limit=500, labelSelector='fugue.pro/backup-run')
    items = snapshots.get('items')
    if not isinstance(items, list) or len(items) > 500 or snapshots.get('metadata', {}).get('continue'):
        raise Incomplete('snapshot_inventory_incomplete')
    for initial, path in ((cluster, cluster_path), (pod, f'{core}/pods/{primary}'),
                          (claim, f'{core}/persistentvolumeclaims/{primary}'),
                          (volume, volume_path), (lease, lease_path)):
        fresh = get(path)
        # Lease renewals and routine Pod status updates don't invalidate the
        # sample, but changing the primary, claim, volume or owner does.
        if live_uid(initial) != live_uid(fresh):
            raise Incomplete('identity_changed_during_inventory')
        if path == lease_path and owner != (fresh.get('spec', {}).get('holderIdentity') or ''):
            raise Incomplete('lease_owner_changed_during_inventory')
        if path == cluster_path and any(status.get(k) != fresh.get('status', {}).get(k)
                                        for k in ('currentPrimary', 'targetPrimary')):
            raise Incomplete('primary_transition')
        if path.endswith(f'/persistentvolumeclaims/{primary}') and claim.get('spec', {}).get('volumeName') != fresh.get('spec', {}).get('volumeName'):
            raise Incomplete('claim_binding_changed')
    return {
        'phase': 'inventory_observed',
        'policy_enabled': policy['enabled'] is True,
        'coordination_occupied': bool(owner),
        'labelled_snapshots_on_platform_volume': sum(
            i.get('spec', {}).get('volume') == handle and not i.get('metadata', {}).get('deletionTimestamp')
            for i in items),
    }


def main():
    policy = json.loads(Path(sys.argv[1]).read_text())
    digest = hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    result = dict(schema='fugue.longhorn-cleanup-observation/v1', mode='metadata-only',
                  policy_sha256=digest, authorizes_deletion=False,
                  observed_at=dt.datetime.now(dt.timezone.utc).isoformat())
    try:
        result.update(observe(read_only_client(), policy))
    except Incomplete as exc:
        result.update(phase='blocked', reason=str(exc))
    except HTTPError as exc:
        result.update(phase='failed', reason='kubernetes_read_failed', http_status=exc.code)
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        result.update(phase='failed', reason='invalid_or_unavailable_observation')
    print(json.dumps(result, sort_keys=True), flush=True)
    return 1 if result['phase'] == 'failed' else 0


if __name__ == '__main__':
    sys.exit(main())
