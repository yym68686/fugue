import copy
import json
from pathlib import Path
import textwrap
import unittest

SOURCE = Path(__file__).with_name('node_updater.go').read_text()
SELECTOR = SOURCE[SOURCE.index('service_cache = {}\ndef first_same_namespace_service'):SOURCE.index('\ndef pod_context_evidence')]
PROBE = textwrap.dedent(SOURCE[SOURCE.index('    if same_service:\n'):SOURCE.index('\ntry:\n    socket.getaddrinfo("cloudflare.com"')])


def service(name, ports=None):
    return {'metadata': {'name': name}, 'spec': {'clusterIP': '10.43.1.2', 'ports': ports or [{'name': 'http', 'port': 8080}]}}


def endpoints(name, **conditions):
    return {'metadata': {'labels': {'kubernetes.io/service-name': name}}, 'addressType': 'IPv4', 'ports': [{'name': 'http', 'port': 8080}], 'endpoints': [{'addresses': ['10.42.1.9'], 'conditions': {'ready': True, **conditions}}]}


class ServiceProbeTest(unittest.TestCase):
    def select(self, services, slices, observed=True):
        calls = []
        def kubectl(args, timeout):
            calls.append(args)
            if args[-1] == 'svc':
                return True, json.dumps({'items': services})
            return (True, json.dumps({'items': slices})) if observed else (False, 'forbidden')
        env = {'json': json, 'kubectl_json': kubectl}
        exec(SELECTOR, env)
        result = env['first_same_namespace_service']({'namespace': 'tenant-example'})
        self.assertEqual(result, env['first_same_namespace_service']({'namespace': 'tenant-example'}))
        self.assertEqual(2, len(calls))
        return result[0]

    def probe(self, selected, tcp_result=(False, 'connection refused')):
        checks, connections = [], []
        def tcp(*args):
            connections.append(args)
            return tcp_result
        def skipped(name, category, observed, expected, evidence):
            checks.append({'name': name, 'status': 'warning', 'observation_state': 'not_observed'})
        def check(name, category, status, observed, expected, *args, **kwargs):
            return {'name': name, 'status': status}
        env = {'same_service': selected, 'same_service_reason': '', 'pod_ctx': {}, 'kube_dns_service_ip': '10.43.0.10', 'checks': checks, 'check': check, 'pod_context_evidence': lambda ctx, extra=None: extra or {}, 'pod_dns_query': lambda *args: (True, 'answer matches'), 'pod_tcp_connect': tcp, 'optional_probe_skipped': skipped}
        exec(PROBE, env)
        return checks, connections

    def test_disabled_service_is_not_a_network_failure(self):
        selected = self.select([service('paused')], [])
        checks, connections = self.probe(selected)
        self.assertEqual([], connections)
        self.assertEqual([{'name': 'same_namespace_service_dns', 'status': 'pass'}, {'name': 'same_namespace_service_tcp', 'status': 'warning', 'observation_state': 'not_observed'}], checks)

    def test_ready_service_is_preferred_and_real_connection_failure_is_retained(self):
        selected = self.select([service('paused'), service('active')], [endpoints('active')])
        self.assertEqual('active', selected['name'])
        checks, connections = self.probe(selected)
        self.assertEqual(1, len(connections))
        self.assertEqual('fail', checks[-1]['status'])
        checks, _ = self.probe(selected, (True, 'connected'))
        self.assertEqual('pass', checks[-1]['status'])

    def test_unavailable_observation_never_implies_healthy_tcp(self):
        selected = self.select([service('active')], [], observed=False)
        checks, connections = self.probe(selected)
        self.assertEqual([], connections)
        self.assertEqual('not_observed', checks[-1]['observation_state'])

    def test_readiness_port_and_address_family_must_match(self):
        variants = [endpoints('app', ready=False), endpoints('app', ready=None), endpoints('app', terminating=True), endpoints('app', serving=False)]
        for mutate in [lambda x: x.update(addressType='IPv6'), lambda x: x['ports'][0].update(name='other'), lambda x: x['ports'][0].update(protocol='UDP'), lambda x: x['ports'][0].update(port=None), lambda x: x['endpoints'][0].update(addresses=[])]:
            v = endpoints('app'); mutate(v); variants.append(v)
        for v in variants:
            with self.subTest(endpoint=v):
                self.assertFalse(self.select([service('app')], [v])['tcp_backend_observed'])

    def test_second_port_can_be_ready(self):
        selected = self.select([service('app', [{'name': 'unserved', 'port': 81}, {'name': 'http', 'port': 8080}])], [endpoints('app')])
        self.assertTrue(selected['tcp_backend_observed'])
        self.assertEqual(8080, selected['port'])


if __name__ == '__main__':
    unittest.main()
