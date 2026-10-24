import argparse
import copy
import datetime
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import urllib.parse

from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.configure_dynamic_quality import wait_verified_baseline
from scripts.observe_front_candidate import now
from scripts.publish_agent_edge_shadow import API, canonical
from scripts.reconcile_agent_edge_trust import strict_json
from scripts.reconfigure_cell_producer import DIGEST, IDENTITY, validate_ref
from scripts.reconfigure_physical_dns import SCOPE, baseline, bounded_json


def holds(policy):
    result = policy.get('offline_edges', [])
    if not isinstance(result, list) or len(result) > 64:
        raise ValueError('bounded offline intent required')
    previous = ''
    for hold in result:
        required = {'edge_id', 'edge_group_id', 'public_ipv4', 'reason'}
        if not isinstance(hold, dict) or not required <= set(hold) or set(hold) - required - {'expected_recovery_date', 'recovery'}:
            raise ValueError('complete offline endpoint identity required')
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', hold['edge_id']) or hold['edge_id'] <= previous or not re.fullmatch(r'edge-group-[a-z0-9]+(?:-[a-z0-9]+)*', hold['edge_group_id']):
            raise ValueError('ordered unique offline endpoint identities required')
        address = ipaddress.IPv4Address(hold['public_ipv4'])
        if not address.is_global or str(address) != hold['public_ipv4'] or hold['reason'] not in {'quota_exhausted', 'maintenance', 'network_unavailable'}:
            raise ValueError('canonical public endpoint and typed reason required')
        if hold.get('expected_recovery_date'):
            datetime.date.fromisoformat(hold['expected_recovery_date'])
        if 'recovery' in hold:
            p = hold['recovery']
            numeric = {'probe_interval_seconds', 'probe_timeout_seconds', 'success_threshold', 'failure_threshold', 'freshness_seconds'}
            if not isinstance(p, dict) or not numeric | {'hostname', 'path'} <= set(p) or set(p) - numeric - {'hostname', 'path', 'capacity'} or any(type(p[k]) is not int for k in numeric):
                raise ValueError('complete bounded recovery probe policy required')
            if not isinstance(p['hostname'], str) or len(p['hostname']) > 253 or '.' not in p['hostname'] or any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label) for label in p['hostname'].split('.')):
                raise ValueError('canonical recovery probe hostname required')
            if not isinstance(p['path'], str) or not p['path'].startswith('/') or p['path'].startswith('//') or len(p['path']) > 1024 or any(c in p['path'] for c in ['?', '#', '\n', '\r']):
                raise ValueError('canonical recovery probe path required')
            if not (300 <= p['probe_interval_seconds'] <= 3600 and 1 <= p['probe_timeout_seconds'] <= 5 and 2 <= p['success_threshold'] <= 5 and 1 <= p['failure_threshold'] <= 3 and max(600, 2*p['probe_interval_seconds']+p['probe_timeout_seconds']) <= p['freshness_seconds'] <= 10800):
                raise ValueError('recovery probe bounds invalid')
            if 'capacity' in p:
                validate_capacity_policy(p['capacity'], hold['edge_id'], p['freshness_seconds'])
        previous = hold['edge_id']
    return result


def validate_capacity_policy(p, edge_id, freshness):
    required = {'version', 'traffic_class', 'reference_edge_ids', 'minimum_bytes_per_second', 'max_elapsed_milliseconds', 'minimum_stable_seconds'}
    if not isinstance(p, dict) or set(p) != required or p['version'] != 'edge-capacity-recovery-v1' or p['traffic_class'] != 'streaming':
        raise ValueError('explicit capacity recovery contract required')
    ids = p['reference_edge_ids']
    if not isinstance(ids, list) or not 1 <= len(ids) <= 3 or any(not isinstance(i, str) or not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,127}', i) or i == edge_id for i in ids) or len(set(ids)) != len(ids):
        raise ValueError('unique other physical capacity references required')
    for key, low, high in [('minimum_bytes_per_second', 65536, 16777216), ('max_elapsed_milliseconds', 1000, 15000), ('minimum_stable_seconds', 300, min(7200, freshness))]:
        if type(p[key]) is not int or not low <= p[key] <= high:
            raise ValueError('capacity recovery policy exceeds bound')


def capacity_observation_ready(hold, observation, success):
    policy = hold['recovery'].get('capacity')
    if policy is None:
        return True
    try:
        e = observation['capacity']
        code = e['code']
        if observation.get('last_result') != 'capacity_verified' or observation['successes'] != hold['recovery']['success_threshold'] or observation['failures'] != 0:
            return False
        first = datetime.datetime.fromisoformat(observation['first_success'].replace('Z', '+00:00'))
        checked = datetime.datetime.fromisoformat(e['checked_at'].replace('Z', '+00:00'))
        if checked != success or first.tzinfo is None or first > success or (success-first).total_seconds() < policy['minimum_stable_seconds']:
            return False
        if not DIGEST.fullmatch(e['report_digest']) or not DIGEST.fullmatch(e['route_digest']) or e['reference_edge_id'] not in policy['reference_edge_ids'] or e['reference_edge_id'] == hold['edge_id'] or e['bytes'] != 1 << 20:
            return False
        elapsed, rate = e['elapsed_milliseconds'], e['bytes_per_second']
        if type(elapsed) not in (int, float) or type(rate) not in (int, float) or not math.isfinite(elapsed) or not math.isfinite(rate) or not 0 < elapsed <= policy['max_elapsed_milliseconds'] or rate < policy['minimum_bytes_per_second']:
            return False
        expected = e['bytes'] * 1000 / elapsed
        if abs(expected-rate) > expected * 0.000001:
            return False
        if any(not isinstance(code[k], str) or not re.fullmatch(r'[^ \t\r\n\x00]{1,128}', code[k]) for k in ['node_uid', 'front_pod_uid', 'worker_pod_uid', 'service_uid', 'authority_uid']):
            return False
        if any(not DIGEST.fullmatch(code[k]) for k in ['authority_record_digest', 'worker_image_digest', 'front_image_digest', 'proxy_image_digest']):
            return False
        return bool(re.fullmatch(r'[a-f0-9]{40}', code['worker_source_sha'])) and code['slot'] in ['a', 'b'] and type(code['fence']) is int and 0 < code['fence'] < 2**64-1 and bool(re.fullmatch(r'[^ \t\r\n\x00]{1,512}', code['bundle']))
    except (KeyError, TypeError, ValueError):
        return False


def validate(config):
    required = {'schema', 'generation', 'origin', 'producer_generation', 'precondition', 'projection_policy', 'baseline_mode'}
    if set(config) != required or not bounded_json(config) or config['schema'] != 'fugue.edge-offline-reconfiguration/v1' or type(config['generation']) is not int or not 1 <= config['generation'] <= 2**53-1 or config['baseline_mode'] not in {'declared', 'latest_verified_same_policy'}:
        raise ValueError('explicit bounded offline configuration required')
    origin = urllib.parse.urlsplit(config['origin'])
    if origin.scheme != 'https' or config['origin'] != 'https://' + str(origin.hostname) or not re.fullmatch(r'[a-z0-9][a-z0-9.-]+[a-z0-9]', str(origin.hostname)):
        raise ValueError('canonical TLS API origin required')
    precondition = config['precondition']
    if set(precondition) != {'operation', 'previous_policy', 'serving_full', 'verification_evidence_hash'} or precondition['operation'] != 'edge_offline' or not DIGEST.fullmatch(precondition['verification_evidence_hash']):
        raise ValueError('fenced offline configuration precondition required')
    for field in ['previous_policy', 'serving_full']:
        validate_ref(precondition[field])
    policy = config['projection_policy']
    if policy.get('schema_version') != 'fugue.platform.config/v1' or policy.get('scope') != 'global' or policy.get('publication_role') or policy.get('dns_placement_mode') != 'consumer_readiness' or not IDENTITY.fullmatch(policy.get('generation', '')) or not IDENTITY.fullmatch(config['producer_generation']):
        raise ValueError('global versioned consumer-readiness policy required')
    holds(policy)
    return config


def check_delta(previous, successor):
    before = {hold['edge_id']: hold for hold in holds(previous)}
    after = {hold['edge_id']: hold for hold in holds(successor)}
    changed = [edge for edge in before.keys() | after.keys() if before.get(edge) != after.get(edge)]
    old, new = copy.deepcopy(previous), copy.deepcopy(successor)
    if old.pop('generation') == new.pop('generation'):
        raise ValueError('offline input generation must advance')
    old.pop('offline_edges', None)
    new.pop('offline_edges', None)
    if len(changed) != 1 or old != new:
        raise ValueError('only one offline hold may change; all other constraints must remain identical')
    return changed[0]


def recovery_declaration_digest(hold):
    # The runtime binds the typed OfflineEdge JSON, whose field order differs
    # from artifact map canonicalization. Keep the wire digest identical to Go,
    # including encoding/json's HTML and JavaScript separator escaping.
    value = {key: hold[key] for key in ('edge_id', 'edge_group_id', 'public_ipv4', 'reason')}
    if hold.get('expected_recovery_date'):
        value['expected_recovery_date'] = hold['expected_recovery_date']
    if hold.get('recovery'):
        value['recovery'] = {key: hold['recovery'][key] for key in ('hostname', 'path', 'probe_interval_seconds', 'probe_timeout_seconds', 'success_threshold', 'failure_threshold', 'freshness_seconds')}
        if 'capacity' in hold['recovery']:
            value['recovery']['capacity'] = {key: hold['recovery']['capacity'][key] for key in ('version', 'traffic_class', 'reference_edge_ids', 'minimum_bytes_per_second', 'max_elapsed_milliseconds', 'minimum_stable_seconds')}
    raw = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    for character, escape in [('<', r'\u003c'), ('>', r'\u003e'), ('&', r'\u0026'), ('\u2028', r'\u2028'), ('\u2029', r'\u2029')]:
        raw = raw.replace(character, escape)
    return 'sha256:' + hashlib.sha256(raw.encode()).hexdigest()


def validate_preview(previous, current, policy):
    if not previous.get('business_snapshot_revision') or not current.get('business_snapshot_revision') or not previous.get('intent') or previous['intent'] != current.get('intent'):
        raise ValueError('offline preview changed business intent or unrelated compiled policy')
    old_policy, new_policy = copy.deepcopy(previous.get('policy', {})), copy.deepcopy(current.get('policy', {}))
    for compiled in [old_policy, new_policy]:
        compiled.pop('generation', None)
        for rule in compiled.get('dns_answer_rules', []):
            rule.pop('selection_mode', None)
            rule.pop('physical_order', None)
    if not old_policy or old_policy != new_policy:
        raise ValueError('offline preview changed unrelated constraints or DNS rule ownership')
    if any(issue not in previous.get('issues', []) for issue in current.get('issues', [])):
        raise ValueError('offline preview introduced a configuration issue')
    endpoints = current.get('runtime_snapshot', {}).get('dns_edge_endpoints', [])
    suspended = {hold['edge_id'] for hold in holds(policy)}
    fact = current.get('runtime_snapshot', {}).get('facts', {}).get('offline_edge_recovery', {})
    recovered = fact.get('observations', [])
    for hold in holds(policy):
        p = hold.get('recovery')
        if not p:
            continue
        for o in recovered:
            if o.get('edge_id') != hold['edge_id'] or o.get('declaration_digest') != recovery_declaration_digest(hold) or o.get('admitted') is not True:
                continue
            at = datetime.datetime.fromisoformat(fact['captured_at'].replace('Z', '+00:00'))
            attempt = datetime.datetime.fromisoformat(o['last_attempt'].replace('Z', '+00:00'))
            success = datetime.datetime.fromisoformat(o['last_success'].replace('Z', '+00:00'))
            expiry = datetime.datetime.fromisoformat(o['proof_valid_until'].replace('Z', '+00:00'))
            if at.tzinfo is not None and success <= attempt <= at < expiry and (at-success).total_seconds() <= p['freshness_seconds'] and 0 <= o.get('failures', -1) < p['failure_threshold'] and 0 <= o.get('successes', -1) <= p['success_threshold'] and capacity_observation_ready(hold, o, success):
                suspended.remove(hold['edge_id'])
                break
    if not endpoints or len(endpoints) > 4096 or any(endpoint['edge_id'] in suspended for endpoint in endpoints):
        raise ValueError('offline preview failed to remove held candidates')
    return sorted(endpoint['edge_id'] for endpoint in endpoints)


def preview_evidence(previous, current):
    paths = []
    omitted = 0
    def compare(before, after, path):
        nonlocal omitted
        if before == after:
            return
        if isinstance(before, dict) and isinstance(after, dict):
            for key in sorted(before.keys() | after.keys()):
                compare(before.get(key), after.get(key), path + '/' + key)
        elif isinstance(before, list) and isinstance(after, list) and len(before) == len(after):
            for index, pair in enumerate(zip(before, after)):
                compare(*pair, path + '/' + str(index))
        elif len(paths) < 256:
            paths.append(path[:512])
        else:
            omitted += 1
    for field in ['intent', 'policy', 'issues']:
        compare(previous.get(field), current.get(field), '/' + field)
    return {'before_revision': previous.get('business_snapshot_revision'), 'after_revision': current.get('business_snapshot_revision'),
            'before_intent_digest': digest(previous.get('intent')), 'after_intent_digest': digest(current.get('intent')),
            'changed_paths': paths, 'changed_paths_omitted': omitted}


def publish(config, api, save):
    validate(config)
    declaration = digest(config)
    config = wait_verified_baseline(config, api)
    baseline(config, api)
    precondition = config['precondition']
    old = api('GET', '/v1/admin/artifacts/' + precondition['previous_policy']['artifact_id'])['artifact']
    previous = old['content']
    if old.get('content_hash') != precondition['previous_policy']['content_hash'] or not exact(old, 'policy_snapshot', SCOPE, previous) or previous.get('mode') != 'serving' or previous.get('target_scope') != 'global' or previous.get('publication_role') or previous.get('route_placement_transition'):
        raise ValueError('exact continuous global predecessor required')
    source = api('GET', '/v1/admin/artifacts/' + previous['dns_policy_artifact_id'])['artifact']
    if source.get('content_hash') != previous['dns_policy_digest'] or not exact(source, 'policy_snapshot', 'global', source['content']):
        raise ValueError('pinned source integrity mismatch')
    changed = check_delta(source['content'], config['projection_policy'])
    current = selected(api, SCOPE, 'shadow')
    if authority_identity(current) != precondition['previous_policy'] and current.get('artifact', {}).get('generation') != config['producer_generation']:
        raise ValueError('producer changed')
    source = ensure_input(api, 'policy_snapshot', 'global', config['projection_policy'])
    successor = copy.deepcopy(previous)
    successor.update(generation=config['producer_generation'], dns_policy_artifact_id=source['id'], dns_policy_digest=source['content_hash'])
    if authority_identity(current) != precondition['previous_policy'] and not exact(current.get('artifact', {}), 'policy_snapshot', SCOPE, successor):
        raise ValueError('successor differs')
    artifact = ensure_input(api, 'policy_snapshot', SCOPE, successor)
    evidence = {'schema': config['schema'], 'declaration_digest': declaration, 'resolved_precondition': precondition, 'artifact_id': artifact['id'], 'changed_edge': changed,
                'offline_edges': holds(config['projection_policy']), 'producer_activated': False, 'routing_acceptance_complete': False, 'automatic_readmission': any(hold.get('recovery') for hold in holds(config['projection_policy'])), 'phase': 'reading_previews'}
    save(evidence)
    before = api('GET', '/v1/admin/platform-config/routes/project?producer_policy_artifact_id=' + old['id'] + "&view=runtime")
    preview = api('GET', '/v1/admin/platform-config/routes/project?producer_policy_artifact_id=' + artifact['id'] + "&view=runtime")
    evidence.update(phase='validating_previews', preview=preview_evidence(before, preview))
    save(evidence)
    candidates = validate_preview(before, preview, config['projection_policy'])
    config = wait_verified_baseline(config, api)
    precondition = config['precondition']
    evidence.update(phase='publishing_producer', resolved_precondition=precondition, candidate_edge_ids=candidates)
    save(evidence)
    key = 'producer-reconfiguration/' + digest({'artifact_id': artifact['id'], 'content_hash': artifact['content_hash'], 'precondition': precondition})
    api('POST', '/v1/admin/artifacts/' + artifact['id'] + '/release', {'release_channel': 'shadow', 'producer_reconfiguration': precondition, 'idempotency_key': key, 'reason': 'Declarative endpoint maintenance with unchanged routes, proof requirements and positive LKG'})
    current = selected(api, SCOPE, 'shadow')
    if not exact(current.get('artifact', {}), 'policy_snapshot', SCOPE, successor):
        raise ValueError('producer activation not confirmed')
    evidence.update(phase='producer_published', producer_activated=True, authority=authority_identity(current), completed_at=now().isoformat())
    save(evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('declaration')
    parser.add_argument('--validate-only', action='store_true')
    parser.add_argument('--evidence', type=Path)
    args = parser.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 1 << 20:
        raise ValueError('offline declaration exceeds bound')
    config = validate(strict_json(raw))
    if args.validate_only:
        print(canonical({'valid': True, 'declaration_digest': digest(config)}))
        return
    token = os.environ.pop('FUGUE_API_KEY', '')
    if not token or not args.evidence:
        raise ValueError('configuration credential and evidence path required')
    def save(value):
        args.evidence.touch(mode=0o600, exist_ok=True)
        args.evidence.chmod(0o600)
        args.evidence.write_text(canonical(value) + '\n')
    print(canonical(publish(config, API(config['origin'], token, response_limit=128 << 20, timeout=120), save)))


if __name__ == '__main__':
    main()
