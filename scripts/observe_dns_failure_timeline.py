import argparse
import datetime
import json
from pathlib import Path
import re
import time

from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json


def failure_count(metrics):
    values = re.findall(r'^fugue_dns_query_total\{[^\n}]*rcode="SERVFAIL"[^\n}]*\} ([0-9]+)$', metrics, re.MULTILINE)
    return sum(int(value) for value in values)


def sample(namespace, services, state, kube=kubectl):
    observations = []
    for service_name in services:
        service = strict_json(kube(['get', 'service', service_name, '-n', namespace, '-o', 'json']))
        selector = service.get('spec', {}).get('selector', {})
        if not selector or len(selector) > 16 or any(not re.fullmatch(r'[A-Za-z0-9./_-]+', key) or not re.fullmatch(r'[A-Za-z0-9._-]+', value) for key, value in selector.items()):
            raise ValueError('explicit bounded DNS selector required')
        query = ','.join(key + '=' + value for key, value in sorted(selector.items()))
        pods = strict_json(kube(['get', 'pods', '-n', namespace, '-l', query, '-o', 'json']))['items']
        active = [pod for pod in pods if not pod['metadata'].get('deletionTimestamp')]
        if len(active) != 1:
            observations.append({'service': service_name, 'reason': 'selected_instance_ambiguous', 'instances': len(active)})
            continue
        pod = active[0]
        candidates = [container for container in pod['spec']['containers'] if container.get('readinessProbe', {}).get('httpGet', {}).get('path') == '/readyz']
        if len(candidates) != 1:
            raise ValueError('DNS observation port ambiguous')
        container = candidates[0]
        port = container['readinessProbe']['httpGet']['port']
        if isinstance(port, str):
            ports = [value['containerPort'] for value in container.get('ports', []) if value.get('name') == port]
            port = ports[0] if len(ports) == 1 else None
        if type(port) is not int or not 1 <= port <= 65535:
            raise ValueError('DNS observation port invalid')
        prefix = '/api/v1/namespaces/' + namespace + '/pods/' + pod['metadata']['name'] + ':' + str(port) + '/proxy/'
        metrics = kube(['get', '--raw', prefix + 'metrics'])
        if len(metrics) > 1 << 20:
            raise ValueError('DNS metrics exceed bound')
        count = failure_count(metrics)
        identity = (pod['metadata']['uid'], tuple(item.get('containerID') for item in pod['status'].get('containerStatuses', [])))
        previous = state.get(service_name)
        delta = count - previous['count'] if previous and previous['identity'] == identity and count >= previous['count'] else 0
        observed = {'service': service_name, 'pod': pod['metadata']['name'], 'pod_uid': pod['metadata']['uid'], 'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'servfail_total': count, 'servfail_delta': delta, 'baseline_reset': previous is None or previous['identity'] != identity or count < previous['count'], 'new_failures': []}
        seen = set(previous['seen']) if previous and previous['identity'] == identity else set()
        if delta:
            raw = kube(['get', '--raw', prefix + 'decisions?limit=20'])
            if len(raw) > 32 << 20:
                raise ValueError('DNS decision sample exceeds bound')
            decisions = strict_json(raw)
            receipts = decisions.get('receipts', [])
            if not isinstance(receipts, list) or len(receipts) > 20 or any(len(canonical(receipt).encode()) > 256 << 10 for receipt in receipts):
                raise ValueError('DNS receipt count or size exceeds bound')
            for receipt in receipts:
                if receipt.get('rcode') != 2 or receipt.get('decision_id') in seen:
                    continue
                observed['new_failures'].append(receipt)
                seen.add(receipt['decision_id'])
            observed['journal_dropped'] = decisions.get('dropped')
            observed['journal_evicted'] = decisions.get('evicted')
            observed['unattributed_increment'] = max(0, delta - len(observed['new_failures']))
        state[service_name] = {'identity': identity, 'count': count, 'seen': sorted(seen)[-128:]}
        observations.append(observed)
    return observations


def observe(namespace, services, duration, interval, evidence, kube=kubectl):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', namespace) or not 1 <= len(services) <= 4 or len(set(services)) != len(services) or any(not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', service) for service in services):
        raise ValueError('bounded explicit namespace and Services required')
    if not 1 <= duration <= 900 or not 5 <= interval <= 60:
        raise ValueError('observation duration or interval outside bound')
    state, samples, failures = {}, [], []
    report = {'schema': 'fugue.dns-failure-timeline/v1', 'authorizes_traffic': False, 'coverage': 'sampled_counter_increments_and_retained_receipts', 'samples': samples, 'failures': failures, 'evicted_failure_receipts': 0, 'evicted_samples': 0}
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.touch(mode=0o600, exist_ok=True)
    evidence.chmod(0o600)
    started = time.monotonic()
    while True:
        try:
            for observed in sample(namespace, services, state, kube):
                for receipt in observed.pop('new_failures', []):
                    failures.append({'service': observed['service'], 'pod_uid': observed['pod_uid'], 'receipt': receipt})
                if len(failures) > 32:
                    report['evicted_failure_receipts'] += len(failures) - 32
                    del failures[:-32]
                samples.append(observed)
        except Exception as error:
            samples.append({'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'reason': type(error).__name__, 'observation_failed': True})
        if len(samples) > 512:
            report['evicted_samples'] += len(samples) - 512
            del samples[:-512]
        report['elapsed_seconds'] = round(time.monotonic() - started, 3)
        evidence.write_text(canonical(report) + '\n')
        if time.monotonic() - started >= duration:
            return report
        time.sleep(min(interval, duration - (time.monotonic() - started)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--service', action='append', required=True)
    parser.add_argument('--duration', type=int, default=300)
    parser.add_argument('--interval', type=int, default=5)
    parser.add_argument('--evidence', type=Path, required=True)
    args = parser.parse_args()
    report = observe(args.namespace, args.service, args.duration, args.interval, args.evidence)
    print(canonical({'samples': len(report['samples']), 'failures': len(report['failures']), 'unattributed_increment': sum(sample.get('unattributed_increment', 0) for sample in report['samples']), 'observation_errors': sum(sample.get('observation_failed', False) for sample in report['samples'])}))


if __name__ == '__main__':
    main()
