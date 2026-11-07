#!/usr/bin/env python3
"""Receipted consumer input preparation, never registration/heartbeat acceptance.

Registration carries the complete canonical server-produced expected set. The
native typed decoder/normalizer is authoritative; exact ticket comparison also
rejects omitted/defaulted or silently dropped fields before any resource write.
"""
import copy
import json
import re
import sys

from scripts import compiler_input_prepare as shared
from scripts.compiler_input_prepare import impact, index, r, os, time, subprocess
from scripts.compiler_input_storage import command, get, kube

PURPOSE = 'compiler-input-consumer-prepare'
SUBJECT = 'consumer'
NAME_PREFIX = 'fugue-input-consumer-'
NATIVE_PREFIX = 'consumer-input'
VERIFICATION_ENV = 'FUGUE_CONSUMER_INPUT_PREPARATION_VERIFICATION_KEY'
ACTIVATED_FIELD = 'consumer_activated'
SUCCESS = 'consumer-inputs-prepared'
CONTRACT_LIMIT = 128 << 10
BOUNDS = shared.BOUNDS
require = shared.require


def heartbeat(value):
    text = {'consumer_id', 'component', 'node_id', 'artifact_kind', 'scope_key', 'release_set_id',
            'expected_consumer_set_id', 'protocol_version', 'schema_version', 'nonce',
            'desired_generation', 'actual_generation', 'lkg_generation', 'apply_status', 'probe_status', 'evidence_hash'}
    optional = {'candidate_generation', 'last_error'}
    flags = {'serving_lkg', 'lkg_expired'}
    numbers = {'fencing_token', 'sequence', 'generation_sequence'}
    require(type(value) is dict and set(value) <= text | optional | flags | numbers | {'issued_at', 'compatibility_capabilities'},
            'consumer heartbeat fields differ')
    require(all(type(value.get(k, '')) is str for k in text | optional)
            and all(type(value.get(k, False)) is bool for k in flags)
            and all(type(value.get(k, 0)) is int and -(1 << 63) <= value.get(k, 0) < 1 << 63 for k in numbers),
            'consumer heartbeat types differ')
    out = {k: value.get(k, '') for k in text}
    out.update({k: value.get(k, 0) for k in numbers})
    out.update({k: value[k] for k in optional | flags if value.get(k)})
    at = value.get('issued_at', '0001-01-01T00:00:00Z')
    shared.instant(at)
    require(re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?Z', at) is not None,
            'consumer heartbeat time must be canonical UTC')
    if '.' in at:
        head, fraction = at[:-1].split('.')
        at = head+('.'+fraction.rstrip('0') if fraction.rstrip('0') else '')+'Z'
    out['issued_at'] = at
    caps = value.get('compatibility_capabilities')
    require(caps is None or type(caps) is list and all(type(x) is str for x in caps),
            'consumer capabilities differ')
    if caps:
        out['compatibility_capabilities'] = caps
    require(len(json.dumps(out).encode()) <= 65536, 'consumer heartbeat exceeds bound')
    return out


def validate(c):
    shared.validate_common(c, SUBJECT)
    p = c[SUBJECT]
    require(type(p) is dict, 'consumer preparation fields differ')
    required = set(BOUNDS) | {'mode', 'expected_set_id', 'operation_key'}
    field = 'registration' if p.get('mode') == 'register' else 'heartbeat'
    require(type(p) is dict and set(p) == required | {field}
            and p['mode'] in ('register', 'trusted', 'shadow'), 'consumer preparation purpose differs')
    require(all(type(p[k]) is int and 1 <= p[k] <= high for k, high in BOUNDS.items()), 'consumer budget exceeds original envelope')
    require(type(p['expected_set_id']) is str and p['expected_set_id'].strip() == p['expected_set_id']
            and 0 < len(p['expected_set_id'].encode()) <= 512
            and type(p['operation_key']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._:/-]{0,47}', p['operation_key']),
            'consumer preparation identity differs')
    if field == 'registration':
        s = p[field]
        require(type(s) is dict and s.get('id') == p['expected_set_id']
                and all(type(s.get(k)) is str and 0 < len(s[k].encode()) <= 512
                        for k in ('release_set_id', 'artifact_release_id', 'expected_generation')),
                'consumer registration identity differs')
    else:
        h = heartbeat(p[field])
        require(h['expected_consumer_set_id'] in ('', p['expected_set_id'])
                and (p['mode'] != 'shadow' or h['expected_consumer_set_id']), 'consumer expected set differs')
    require(len(json.dumps(contract(c), separators=(',', ':')).encode()) <= CONTRACT_LIMIT, 'consumer contract exceeds bound')
    return c


def contract(c):
    p = c[SUBJECT]
    result = {'version': 1, **copy.deepcopy(p), 'expected_plan_sha256': ''}
    if 'heartbeat' in p:
        result['heartbeat'] = heartbeat(p['heartbeat'])
    return result


def physical_owner(c, v):
    if v['table'] == 'fugue_platform_lkg_snapshots':
        parts = shared.strict_json(v['key'])
        require(type(parts) is list and len(parts) == 2
                and all(type(x) is str and x for x in parts)
                and len(parts[0].encode()) <= 128 and len(parts[1].encode()) <= 512,
                'consumer LKG selector differs')
        canonical = json.dumps(parts, ensure_ascii=False, separators=(',', ':'))
        for char, escaped in [('<', '\\u003c'), ('>', '\\u003e'), ('&', '\\u0026'), ('\u2028', '\\u2028'), ('\u2029', '\\u2029')]:
            canonical = canonical.replace(char, escaped)
        require(canonical == v['key'], 'consumer LKG selector is not canonical')
    p = c[SUBJECT]
    table, key = ('fugue_platform_artifacts', p['registration']['release_set_id']) if p['mode'] == 'register' else ('fugue_platform_expected_consumer_sets', p['expected_set_id'])
    return v['table'] == table and v['key'] == key


def receipt_identity(c):
    return {k: c[SUBJECT][k] for k in ('mode', 'expected_set_id')}


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
