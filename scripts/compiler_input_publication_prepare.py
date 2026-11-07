#!/usr/bin/env python3
"""Prepare a release/rollback/LKG closure; never activate it.

Producer method arguments are bound in full and derived by the trusted server;
raw producer guards and task impersonation are not accepted.
"""
import json
import re
import sys

from scripts import compiler_input_prepare as shared
from scripts.compiler_input_prepare import impact, index, r, os, time, subprocess
from scripts.compiler_input_storage import command, get, kube

PURPOSE = 'compiler-input-publication-prepare'
SUBJECT = 'publication'
NAME_PREFIX = 'fugue-input-publication-'
NATIVE_PREFIX = 'publication-input'
VERIFICATION_ENV = 'FUGUE_PUBLICATION_INPUT_PREPARATION_VERIFICATION_KEY'
ACTIVATED_FIELD = 'publication_activated'
SUCCESS = 'publication-inputs-prepared'
CONTRACT_LIMIT = 128 << 10
BOUNDS = shared.BOUNDS
require = shared.require


def strings(value, fields):
    require(type(value) is dict and all(type(value.get(k, '')) is str for k in fields),
            'publication request string field differs')
    return {k: value.get(k, '') for k in fields}


def precondition(value):
    fields = {'artifact_id', 'content_hash', 'release_id', 'fencing_token'}
    require(type(value) is dict and set(value) <= fields, 'publication precondition differs')
    result = strings(value, fields-{'fencing_token'})
    token = value.get('fencing_token', 0)
    require(type(token) is int and -(1 << 63) <= token < 1 << 63, 'publication fencing token differs')
    return {**result, 'fencing_token': token}

def go_json(value):
    out = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
    for char, escaped in [('<', '\\u003c'), ('>', '\\u003e'), ('&', '\\u0026'), ('\u2028', '\\u2028'), ('\u2029', '\\u2029')]:
        out = out.replace(char, escaped)
    return out


def release_request(value):
    # Match the native typed contract's JSON defaults/omitempty. The trusted
    # native decoder remains authoritative for business semantics and guards.
    text = {'release_channel', 'canary_rule_ref', 'reason', 'idempotency_key'}
    flags = {'soft_override', 'force_publish'}
    objects = {'dns_publication_precondition', 'producer_reconfiguration', 'kernel_break_glass'}
    require(type(value) is dict and set(value) <= text | flags | objects,
            'ordinary publication request fields differ')
    result = {k: v for k, v in strings(value, text).items() if k == 'release_channel' or v}
    for k in flags:
        require(type(value.get(k, False)) is bool, 'publication request boolean differs')
        if value.get(k):
            result[k] = True
    if value.get('producer_reconfiguration') is not None:
        v = value['producer_reconfiguration']
        fields = {'operation', 'previous_policy', 'serving_full', 'verification_evidence_hash'}
        require(type(v) is dict and set(v) <= fields, 'publication producer precondition differs')
        out = strings(v, {'operation', 'verification_evidence_hash'})
        if not out['operation']:
            del out['operation']
        out.update({k: precondition(v.get(k, {})) for k in ('previous_policy', 'serving_full')})
        result['producer_reconfiguration'] = out
    if value.get('dns_publication_precondition') is not None:
        v = value['dns_publication_precondition']
        require(type(v) is dict and set(v) <= {'lanes', 'lkg_id', 'lkg_publication', 'verification_evidence_hash'},
                'publication DNS precondition differs')
        out = strings(v, {'lkg_id', 'verification_evidence_hash'})
        lanes = v.get('lanes')
        require(lanes is None or type(lanes) is dict and all(type(k) is str for k in lanes),
                'publication DNS lane preconditions differ')
        out.update(lkg_publication=precondition(v.get('lkg_publication', {})),
                   lanes=None if lanes is None else {k: None if p is None else precondition(p) for k, p in lanes.items()})
        result['dns_publication_precondition'] = out
    if value.get('kernel_break_glass') is not None:
        v = value['kernel_break_glass']
        require(type(v) is dict and set(v) <= {'expires_at', 'confirmation', 'target_confirmation'},
                'publication kernel precondition differs')
        out = strings(v, {'confirmation', 'target_confirmation'})
        at = v.get('expires_at', '0001-01-01T00:00:00Z')
        shared.instant(at)
        match = re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?(Z|[+-]\d\d:\d\d)', at)
        require(match is not None, 'publication kernel expiry differs')
        body, fraction, zone = match.groups()
        fraction = (fraction or '').rstrip('0')
        out['expires_at'] = body+('.'+fraction if fraction else '')+('Z' if zone == '+00:00' else zone)
        result['kernel_break_glass'] = out
    require(len(go_json(result).encode()) <= 65536,
            'publication request exceeds native budget')
    return result


def producer_intent(p):
    value = p['producer']
    text = {'action', 'policy_release_id', 'previous_release_id', 'previous_full_release_id',
            'baseline_artifact_id', 'failed_release_id', 'canary_rule_ref'}
    require(type(value) is dict and set(value) <= text | {'rollback', 'verification'},
            'producer method arguments differ')
    args = strings(value, text)
    require(args['policy_release_id'] and all(len(v.encode()) <= 512 for v in args.values()),
            'producer method identity exceeds bound')
    out = {k: v for k, v in args.items() if v or k in ('action', 'policy_release_id')}
    action, policy, artifact = args['action'], args['policy_release_id'], p['artifact_id']
    absent = lambda *names: all(not args[name] for name in names)
    request = {'release_channel': ''}
    if action in ('shadow', 'gray', 'full'):
        require(p['mode'] == 'release' and value.get('rollback') is None
                and value.get('verification') is None and not args['failed_release_id'],
                'producer release purpose differs')
        if action == 'shadow':
            require(absent('previous_full_release_id', 'baseline_artifact_id', 'canary_rule_ref'),
                    'producer shadow method arguments differ')
            request = dict(release_channel='shadow', reason='versioned platform configuration producer',
                           idempotency_key='producer/'+policy+'/'+artifact)
        else:
            require(args['previous_full_release_id'] and args['baseline_artifact_id'],
                    'producer serving baseline differs')
            request = dict(release_channel=action, reason='versioned platform configuration serving producer',
                           idempotency_key='producer/'+policy+'/'+action+'/'+artifact)
            if args['canary_rule_ref']:
                request['canary_rule_ref'] = args['canary_rule_ref']
    elif action == 'rollback':
        require(p['mode'] == 'rollback' and absent('previous_release_id', 'canary_rule_ref')
                and args['previous_full_release_id'] and args['baseline_artifact_id']
                and args['failed_release_id'] and value.get('verification') is None,
                'producer recovery method arguments differ')
        v = value.get('rollback')
        fields = {'release_channel', 'to_generation', 'reason', 'canary_rule_ref',
                  'soft_override', 'force_publish', 'kernel_break_glass'}
        require(type(v) is dict and set(v) <= fields, 'producer rollback request differs')
        roll = strings(v, {'release_channel', 'to_generation', 'reason', 'canary_rule_ref'})
        require(roll['release_channel'] == 'full' and roll['to_generation'] == p['rollback_generation']
                and all(type(v.get(k, False)) is bool and not v.get(k, False) for k in ('soft_override', 'force_publish'))
                and v.get('kernel_break_glass') is None, 'producer rollback override refused')
        out['rollback'] = {k: v for k, v in roll.items() if v or k in ('to_generation', 'reason')}
    elif action in ('verify', 'verify-recovery'):
        require(p['mode'] == 'lkg' and absent('previous_release_id', 'previous_full_release_id',
                'failed_release_id', 'canary_rule_ref') and args['baseline_artifact_id']
                and value.get('rollback') is None, 'producer verification method arguments differ')
        v = value.get('verification')
        require(type(v) is dict and set(v) <= {'fencing_token', 'reason', 'allow_initial_lkg', 'evidence'},
                'producer verification request differs')
        token = v.get('fencing_token', 0)
        require(type(token) is int and -(1 << 63) <= token < 1 << 63
                and type(v.get('reason', '')) is str
                and type(v.get('allow_initial_lkg', False)) is bool and not v.get('allow_initial_lkg', False),
                'producer verification type or initial baseline differs')
        flags = {'consumer_convergence', 'local_probe', 'platform_evidence', 'watch_window',
                 'baseline_monotonic', 'database_rollback_compatible'}
        e = v.get('evidence', {})
        require(type(e) is dict and set(e) <= flags | {'expected_consumer_set_id', 'evidence_refs'}
                and all(type(e.get(k, False)) is bool for k in flags), 'producer verification evidence differs')
        evidence = {k: e.get(k, False) for k in flags}
        require(type(e.get('expected_consumer_set_id', '')) is str, 'producer expected consumer identity differs')
        if e.get('expected_consumer_set_id'):
            evidence['expected_consumer_set_id'] = e['expected_consumer_set_id']
        refs = e.get('evidence_refs')
        require(refs is None or type(refs) is list and all(type(x) is str for x in refs),
                'producer verification references differ')
        if refs:
            evidence['evidence_refs'] = refs
        out['verification'] = dict(fencing_token=token, reason=v.get('reason', ''), evidence=evidence)
    else:
        raise shared.r.Refusal('unknown producer workflow')
    require(shared.exact_json(release_request(p['request']), request),
            'producer request differs from original workflow')
    require(len(go_json(out).encode()) <= 65536, 'producer method arguments exceed native budget')
    return out


def validate(c):
    shared.validate_common(c, SUBJECT)
    p = c[SUBJECT]
    required = set(BOUNDS) | {
        'mode', 'artifact_id', 'rollback_generation', 'release_id', 'request', 'operation_key'}
    require(type(p) is dict and (set(p) == required or set(p) == required | {'producer'}),
            'publication preparation fields differ')
    require(all(type(p[k]) is int and 1 <= p[k] <= high for k, high in BOUNDS.items()),
            'publication preparation budget exceeds original envelope')
    require(all(type(p[k]) is str and len(p[k].encode()) <= 512 for k in ('artifact_id', 'rollback_generation', 'release_id'))
            and type(p['operation_key']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:/-]{0,47}', p['operation_key']),
            'publication preparation identity differs')
    request = release_request(p['request'])
    if p['mode'] == 'release':
        require(p['artifact_id'] and not p['rollback_generation'] and not p['release_id'], 'release identity differs')
    elif p['mode'] == 'rollback':
        require(p['artifact_id'] and p['rollback_generation'] and not p['release_id']
                and request == {'release_channel': ''}, 'rollback preparation is not exact')
    elif p['mode'] == 'lkg':
        require(p['release_id'] and not p['artifact_id'] and not p['rollback_generation']
                and request == {'release_channel': ''}, 'LKG preparation is not exact')
    else:
        raise shared.r.Refusal('unknown publication preparation purpose')
    if 'producer' in p:
        producer_intent(p)
    return c


def contract(c):
    result = {'version': 1, **c[SUBJECT], 'request': release_request(c[SUBJECT]['request']), 'expected_plan_sha256': ''}
    if 'producer' in c[SUBJECT]:
        result['producer'] = producer_intent(c[SUBJECT])
    return result


def physical_owner(c, v):
    if v['table'] == 'fugue_platform_lkg_snapshots':
        key = v['key']
        parts = shared.strict_json(key)
        require(type(parts) is list and len(parts) == 2
                and all(type(p) is str and p for p in parts)
                and len(parts[0].encode()) <= 128 and len(parts[1].encode()) <= 512,
                'publication LKG selector differs')
        require(go_json(parts) == key, 'publication LKG selector is not canonical')
    p = c[SUBJECT]
    table, key = ('fugue_platform_artifact_releases', p['release_id']) if p['mode'] == 'lkg' else ('fugue_platform_artifacts', p['artifact_id'])
    return v['table'] == table and v['key'] == key


def receipt_identity(c):
    p = c[SUBJECT]
    return {'mode': p['mode'], 'artifact_id': p['artifact_id'], 'release_id': p['release_id']}


def native(c, executor, mode, payload):
    return shared.native(c, executor, mode, payload, sys.modules[__name__])


def plan_contract(value, c):
    return shared.plan_contract(value, c, sys.modules[__name__])


def metadata(c, reader=False):
    return shared.metadata(c, sys.modules[__name__], reader)


def session(c, cluster, primary, deployment, executor):
    return shared.session(c, cluster, primary, deployment, executor, sys.modules[__name__])


def job_manifest(c, name, secret_name):
    return shared.job_manifest(c, name, secret_name, sys.modules[__name__])


def result_contract(value, c, plan, confirmed=False):
    return shared.result_contract(value, c, plan, sys.modules[__name__], confirmed)


def maintenance_gate(c, own_uid=''):
    return shared.maintenance_gate(c, own_uid, kube)


def execute(c, evidence, apply=False):
    return shared.execute(c, evidence, apply, sys.modules[__name__])


def terminal_receipt(c, report):
    return shared.terminal_receipt(c, report, sys.modules[__name__])


def main():
    shared.main(sys.modules[__name__])


if __name__ == '__main__':
    main()
