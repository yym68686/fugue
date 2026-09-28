#!/usr/bin/env python3
"""Read-only proof that public ingress moved and the retained Front is empty."""
import argparse
import json
from pathlib import Path
import time

try:
    from . import handoff_front_serving_transport as handoff
    from . import front_connection_witness as connection
except ImportError:
    import handoff_front_serving_transport as handoff
    import front_connection_witness as connection

front, stage, transport = handoff.front, handoff.stage, handoff.probe


def serving(config, staging, service, profile):
    live = handoff.current(config)
    desired = handoff.selected_spec(config, stage.desired(staging, service, profile))
    projected = {key: live['spec'].get(key) for key in desired}
    projected['publishNotReadyAddresses'] = bool(projected['publishNotReadyAddresses'])
    meta = live['metadata']
    annotations = meta.get('annotations', {})
    if meta.get('deletionTimestamp') or not meta.get('uid') or not meta.get('resourceVersion') or meta.get('labels', {}).get('app.kubernetes.io/managed-by') != stage.MANAGER or projected != desired or annotations.get(stage.GENERATION) != str(config['generation']) or annotations.get(stage.DIGEST) != transport.digest(desired) or annotations.get('transport.fugue.dev/phase') != 'serving':
        raise ValueError('public Front handoff is not the exact declared serving generation')
    return live


def connections(profile, pod):
    value = front.read('get', '--raw', '/api/v1/namespaces/'+profile['namespace']+'/pods/'+pod['metadata']['name']+':7831/proxy/edge/tcp-connections')
    count, active = value.get('count'), value.get('active')
    if type(count) is not int or not isinstance(active, list) or count != len(active) or not 0 <= count <= 16384 or any(not item.get('id') for item in active) or len({item['id'] for item in active}) != count:
        raise ValueError('retained Front connection inventory is incomplete')
    return {'count': count, 'connection_ids': sorted(item['id'] for item in active)}


def observe(config, samples=3, interval=10):
    if type(samples) is not int or type(interval) is not int or not 3 <= samples <= 12 or not 10 <= interval <= 30:
        raise ValueError('bounded complete drain observation window required')
    staging, service, profile = handoff.load_stage(config)
    records = []
    identities = None
    for index in range(samples):
        selected = serving(config, staging, service, profile)
        old = front.pod(profile, profile['legacySelector'], False)
        candidate = front.pod(profile, profile['candidateSelector'], True)
        endpoint = stage.selected_endpoint_witness(staging, service, profile, candidate, selected)
        current_ids = (selected['metadata']['uid'], old['metadata']['uid'], candidate['metadata']['uid'])
        if old['metadata']['uid'] == candidate['metadata']['uid'] or identities is not None and current_ids != identities:
            raise ValueError('Front identities changed during drain observation')
        identities = current_ids
        route = profile['probes'][0]
        held = connection.HeldTLS(config['address'], route['host'])
        try:
            proof = held.proof(route['path'])
            if proof['edge'] != profile['node'] or proof['group'] != profile['group']:
                raise ValueError('new public connections no longer reach the declared authority')
            fact = connection.fact(profile, candidate, held)
            inventory = connections(profile, old)
            if serving(config, staging, service, profile) != selected:
                raise ValueError('public transport changed during drain observation')
            for observed, selector, is_candidate in [(old, profile['legacySelector'], False), (candidate, profile['candidateSelector'], True)]:
                latest = front.pod(profile, selector, is_candidate)
                if latest['metadata']['uid'] != observed['metadata']['uid'] or latest['spec'] != observed['spec']:
                    raise ValueError('Front executor changed during drain observation')
            records.append({'at': front.now().isoformat(), 'old_front_uid': old['metadata']['uid'], 'old_connections': inventory, 'public_connection': fact, 'endpoint': endpoint, 'service_uid': selected['metadata']['uid'], 'service_version': selected['metadata']['resourceVersion']})
        finally:
            held.close()
        if index+1 < samples:
            time.sleep(interval)
    return {'schema': 'fugue.front-drain-observation/v1', 'authorizes_traffic': False, 'declaration_digest': transport.digest(config), 'drained': all(r['old_connections']['count'] == 0 for r in records), 'observations': records}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('--evidence', required=True)
    parser.add_argument('--require-drained', action='store_true')
    args = parser.parse_args()
    config = handoff.validate(json.loads(Path(args.config).read_text()))
    evidence = observe(config)
    Path(args.evidence).write_text(front.canonical(evidence)+'\n')
    print(front.canonical({'front_drained': evidence['drained'], 'samples': len(evidence['observations']), 'authorizes_traffic': False}))
    if args.require_drained and not evidence['drained']:
        raise SystemExit('existing Front connections remain; keep the executor unchanged')

if __name__ == '__main__':
    main()
