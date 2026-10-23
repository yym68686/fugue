"""Narrow configuration recovery from an exact, previously verified rollback.

This validates the declaration and read-only ledger. The API transaction verifies
signatures, original verified release, source bindings and fences under one lock.
No traffic publication, consumer ACK or LKG verification is performed here.
"""
import copy
import re

from scripts.bootstrap_cell_producer import authority_identity, selected, digest
from scripts.observe_front_candidate import now, timestamp

FOLLOW_SAME_ARTIFACT = 'latest_positive_same_artifact'


def resolve_baseline(config, api):
    if config.get('baseline_mode') != FOLLOW_SAME_ARTIFACT:
        return config
    precondition = config['precondition']
    if authority_identity(selected(api, 'platform-config-producer', 'shadow')) != precondition['previous_policy']:
        raise ValueError('recovery baseline producer authority changed')
    full = selected(api, 'global', 'full', 'release_set')
    reference = authority_identity(full)
    declared = precondition['serving_full']
    if any(reference[k] != declared[k] for k in ['artifact_id', 'content_hash']) or reference['fencing_token'] < declared['fencing_token'] or reference['fencing_token'] == declared['fencing_token'] and reference['release_id'] != declared['release_id']:
        raise ValueError('recovery renewal changed immutable content or regressed authority')
    if reference['fencing_token'] > declared['fencing_token'] and reference['release_id'] == declared['release_id']:
        raise ValueError('recovery renewal reused a release with another fence')
    lkg = api('GET', '/v1/admin/artifacts/' + reference['artifact_id'] + '/lkg').get('lkg', {})
    evidence = lkg.get('verification_evidence_hash', '')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', evidence):
        raise ValueError('recovery renewal lacks positive LKG evidence')
    resolved = copy.deepcopy(config)
    resolved['precondition'].update(serving_full=reference, verification_evidence_hash=evidence)
    # Same-content renewal never relaxes historical verification, exact
    # rollback-ledger, producer ownership, expiry or live transaction checks.
    baseline(resolved, api)
    return resolved


def baseline(config, api):
    precondition = config['precondition']
    if precondition['operation'] != 'dns_availability_recovery' or config['baseline_mode'] not in ['declared', FOLLOW_SAME_ARTIFACT]:
        raise ValueError('availability recovery requires an exact declared baseline')
    full = selected(api, 'global', 'full', 'release_set')
    artifact, release = full.get('artifact', {}), full.get('release', {})
    generation = artifact.get('generation')
    if authority_identity(full) != precondition['serving_full'] or not generation or artifact.get('content', {}).get('publication_role') or artifact.get('metadata', {}).get('producer_policy_release_id') != precondition['previous_policy']['release_id']:
        raise ValueError('availability recovery serving baseline changed')
    if release.get('released_by_type') != 'bootstrap' or release.get('released_by_id') != 'platform-config-producer' or release.get('override_mode') or release.get('bypassed_invariants'):
        raise ValueError('availability recovery requires normal producer authority')
    lkg = api('GET', '/v1/admin/artifacts/' + artifact['id'] + '/lkg').get('lkg', {})
    if lkg.get('artifact_id') != artifact['id'] or lkg.get('content_hash') != artifact['content_hash'] or lkg.get('generation') != generation or lkg.get('artifact_kind') != 'release_set' or lkg.get('scope_key') != 'global' or lkg.get('verification_evidence_hash') != precondition['verification_evidence_hash'] or timestamp(lkg['expires_at']) <= now():
        raise ValueError('availability recovery requires exact unexpired positive LKG')
    if release.get('verification_state') == 'verified' and release.get('verified_lkg_generation') == generation and lkg.get('verified_by_release_id') == release['id']:
        return full
    if release.get('verification_state') != 'serving_unverified' or any(release.get(field) != generation for field in ['pinned_rollback_generation', 'rollback_target_generation', 'verified_lkg_generation']) or not lkg.get('verified_by_release_id') or lkg['verified_by_release_id'] == release['id'] or timestamp(release['released_at']) > now():
        raise ValueError('availability recovery baseline is not a verified-artifact rollback')
    if not any(message.get('message_type') == 'rollback' and message.get('release_id') == release['id'] and message.get('artifact_id') == artifact['id'] and message.get('artifact_kind') == 'release_set' and message.get('scope_key') == 'global' and message.get('release_channel') == 'full' and message.get('generation') == generation and timestamp(message['created_at']) == timestamp(release['released_at']) for message in full.get('messages', [])):
        raise ValueError('availability recovery lacks the exact rollback ledger message')
    return full


def check_delta(previous, successor):
    # Lazy imports avoid the existing edge-offline publisher's dependency cycle.
    from scripts.configure_edge_offline import holds
    from scripts.configure_dynamic_quality import check_availability_delta, validate_availability_routes
    old, new = copy.deepcopy(previous), copy.deepcopy(successor)
    before = {hold['edge_id']: hold for hold in holds(old)}
    after = {hold['edge_id']: hold for hold in holds(new)}
    old.pop('offline_edges', None)
    new.pop('offline_edges', None)
    hostname = check_availability_delta(old, new)
    routes = validate_availability_routes(new['dns_query_policy'].get('availability_routes', []))
    if hostname not in routes:
        raise ValueError('availability recovery must explicitly enable or revise availability')
    if any(after.get(edge) != hold for edge, hold in before.items()):
        raise ValueError('availability recovery cannot remove or revise existing holds')
    added = after.keys() - before.keys()
    if len(added) > 1:
        raise ValueError('availability recovery may add at most one explicit hold')
    for edge in added:
        hold = after[edge]
        if edge not in routes[hostname]['policy']['ordered_edge_ids'] or hold['reason'] not in ['quota_exhausted', 'network_unavailable']:
            raise ValueError('new quota or network hold must be inside the availability pool')
    return hostname


def validate_preview(before, after, runtime, policy, hostname):
    from scripts.configure_dynamic_quality import validate_availability_preview
    if before.get('preview_mode') != 'configuration' or after.get('preview_mode') != 'configuration' or not before.get('business_snapshot_revision') or not after.get('business_snapshot_revision') or not runtime.get('business_snapshot_revision') or not before.get('intent') or before['intent'] != after.get('intent') or before['intent'] != runtime.get('intent'):
        raise ValueError('availability recovery changed frozen business or static intent')
    # Each HTTP preview has a distinct transaction revision. Exact intent
    # equality, not equality of unrelated MVCC counters, binds business content.
    if before.get('issues', []) != after.get('issues', []):
        raise ValueError('availability recovery introduced a configuration issue')
    # Live evidence is required for the successor, never synthesized from a
    # configuration-only draft. Its normal compiler/admission gates still apply.
    coverage = runtime.get('runtime_snapshot', {}).get('facts', {}).get('dynamic_quality_coverage', {})
    if coverage.get('mode') != 'all_dynamic' or not coverage.get('hostnames') or type(coverage.get('query_count')) is not int or coverage['query_count'] < 1:
        raise ValueError('availability recovery lacks current coverage accounting')
    return validate_availability_preview(runtime, policy, hostname)


def preview_evidence(before, after, runtime):
    """Bounded structural diagnostics; never retain raw tenant values or secrets."""
    views = {}
    for name, view in [('before', before), ('after', after), ('runtime', runtime)]:
        views[name] = {'preview_mode': view.get('preview_mode'), 'business_snapshot_revision': view.get('business_snapshot_revision'),
                       'intent_present': bool(view.get('intent')), 'intent_digest': digest(view.get('intent')), 'issues_digest': digest(view.get('issues', []))}
    comparisons = {}
    for name, left, right in [('before_after', before, after), ('after_runtime', after, runtime)]:
        paths, omitted = [], 0
        def compare(a, b, path):
            nonlocal omitted
            if a == b:
                return
            if isinstance(a, dict) and isinstance(b, dict):
                for key in sorted(a.keys() | b.keys()):
                    compare(a.get(key), b.get(key), path+'/'+str(key).replace('~', '~0').replace('/', '~1'))
            elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
                for index, (x, y) in enumerate(zip(a, b)):
                    compare(x, y, path+'/'+str(index))
            elif len(paths) < 128:
                paths.append(path[:512])
            else:
                omitted += 1
        compare(left.get('intent'), right.get('intent'), '/intent')
        comparisons[name] = {'equal': left.get('intent') == right.get('intent'), 'changed_paths': paths, 'omitted_paths': omitted}
    return {'schema': 'fugue.availability-preview-comparison/v1', 'views': views, 'comparisons': comparisons}
