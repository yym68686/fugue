"""Capture authenticated public-backend membership; never reconcile or write.

Only the reduced expectation receipt leaves the production runner. Signing keys,
checkpoints and proof payloads are kept in memory and are never artifact output.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

from scripts import reconcile_dns_transport as transport
from scripts.recover_dns_transport import candidate_request


# Only fixed verifier messages may become diagnostic categories. Unknown stderr
# can include sensitive subprocess details and must never enter CI output.
POLICY_VERIFIER_REJECTIONS = {
    'invalid bounded DNS verification input': 'input',
    'DNS observation policy checkpoint authentication failed': 'checkpoint_authentication',
    'DNS observation policy authority or freshness mismatch': 'authority_or_freshness',
    'bounded unique address observation queries required': 'query_identity',
    'ambiguous or unsupported DNS observation policy': 'unsupported_policy',
    'invalid DNS observation physical order': 'physical_order',
    'invalid DNS observation physical selection': 'physical_selection',
    'unsupported DNS observation candidate policy': 'candidate_policy',
    'invalid DNS observation availability policy': 'availability_policy',
    'invalid DNS observation address family': 'address_family',
}


POLICY_VERIFIER_REJECTIONS.update({
    'DNS observation policy rejected: ' + reason: reason
    for reason in (
        'plan_encoding', 'query_count', 'checkpoint_future', 'checkpoint_expired',
        'snapshot_schema', 'snapshot_node', 'snapshot_group', 'snapshot_parent', 'snapshot_plan',
        'assignment_expected_set', 'assignment_release_set', 'assignment_release', 'assignment_kind',
        'assignment_scope', 'assignment_generation', 'assignment_revision', 'assignment_artifact',
        'assignment_content', 'assignment_sequence', 'assignment_channel', 'assignment_fence',
        'assignment_protocol', 'assignment_schema', 'assignment_capabilities',
        'assignment_heartbeat_deadline', 'assignment_convergence_deadline', 'assignment_other',
        'checkpoint_deadline', 'evaluation_missing', 'evaluation_future', 'evaluation_stale',
    )
})


def verified_policy(request, verifier):
    try:
        checked = subprocess.run([verifier, '--export-policy'], input=json.dumps(request),
                                 text=True, capture_output=True, timeout=30)
    except subprocess.TimeoutExpired:
        raise ValueError('DNS observation policy authentication unavailable: verifier_timeout') from None
    if checked.returncode:
        category = POLICY_VERIFIER_REJECTIONS.get(checked.stderr.strip(), 'verifier_failed')
        raise ValueError('DNS observation policy authentication unavailable: ' + category)
    if len(checked.stdout) > 256 << 10:
        raise ValueError('DNS observation policy authentication unavailable: output_size')
    try:
        policy = json.loads(checked.stdout)
    except ValueError:
        raise ValueError('DNS observation policy verifier output invalid: output_encoding') from None
    if not isinstance(policy, dict) or policy.get('schema') != 'fugue.dns-observation-policy/v1':
        raise ValueError('DNS observation policy verifier output invalid: output_schema')
    return policy


def capture(declaration, declaration_bytes, config, verifier):
    if not os.path.isabs(verifier):
        raise ValueError('absolute DNS observation verifier required')
    queries = [{'hostname': q['hostname'], 'type': q['type']} for q in declaration['queries']
               if q.get('answer_source') == 'verified_serving_policy']
    if declaration.get('schema') != 'fugue.dns-public-observation/v2' or not 1 <= len(queries) <= 32:
        raise ValueError('explicit bounded serving-policy observation queries required')
    servers = declaration['servers']
    if not 1 <= len(servers) <= 8 or len(set(servers)) != len(servers):
        raise ValueError('bounded unique authoritative servers required')
    nodes = transport.read_json('get', 'nodes', '-o', 'json')['items']
    result = {'schema': 'fugue.dns-observation-expectations/v1',
              'declaration_digest': 'sha256:' + hashlib.sha256(declaration_bytes).hexdigest(), 'servers': []}
    for address in servers:
        listeners = [x for x in config['listeners'] if x['address'] == address and x['port'] == 53]
        owners = [n for n in nodes if address in [a['address'] for a in n.get('status', {}).get('addresses', [])]]
        if len(listeners) != 1 or len(owners) != 1 or owners[0]['metadata'].get('deletionTimestamp'):
            raise ValueError('public DNS observation listener or node is ambiguous')
        listener = listeners[0]
        current = transport.read_json('get', 'service', listener['name'], '-n', config['namespace'], '-o', 'json')
        transport.validate_existing(current, transport.service(config, listener))
        if not current['metadata'].get('uid') or not current['metadata'].get('resourceVersion'):
            raise ValueError('public DNS observation Service identity is incomplete')
        candidate = transport.handoff_backend(config['namespace'], current['spec']['selector'],
                                              owners[0]['metadata']['name'], listener['targetPort'])
        pod = candidate['pod']
        slices = transport.read_json('get', 'endpointslices', '-n', config['namespace'], '-l',
                                     'kubernetes.io/service-name=' + listener['name'], '-o', 'json')['items']
        transport.validate_handoff_endpoints(current, pod, slices, listener['targetPort'])
        images = [c['image'] for c in pod['spec']['containers']
                  if any(p.get('containerPort') == listener['targetPort'] for p in c.get('ports', []))]
        if len(images) != 1 or not re.fullmatch(r'[^\s@]+@sha256:[a-f0-9]{64}', images[0]):
            raise ValueError('immutable DNS backend image required')
        binding = dict(config, recovery={'candidate_image': images[0], 'candidate_pod_uid': pod['metadata']['uid']})
        request = candidate_request(candidate, binding)
        request['observation_queries'] = queries
        policy = verified_policy(request, verifier)
        for kind, before in [('service', current), ('pod', pod)]:
            after = transport.read_json('get', kind, before['metadata']['name'], '-n', config['namespace'], '-o', 'json')
            if any(before['metadata'][key] != after['metadata'].get(key) for key in ['uid', 'resourceVersion']):
                raise ValueError('public DNS backend changed during policy capture')
        result['servers'].append({'server': address, 'service_uid': current['metadata']['uid'],
                                  'pod_uid': pod['metadata']['uid'], 'policy': policy})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('declaration')
    parser.add_argument('transport')
    parser.add_argument('--verifier', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 64 << 10:
        raise ValueError('DNS observation declaration exceeds bound')
    result = capture(json.loads(raw), raw, transport.load_config(args.transport), args.verifier)
    Path(args.output).write_text(json.dumps(result, sort_keys=True) + '\n')


if __name__ == '__main__':
    main()
