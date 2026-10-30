import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import configure_postgres_bootstrap as policy


def fixture():
    resources = {'requests': {'cpu': '100m', 'memory': '256Mi'}, 'limits': {'cpu': '250m', 'memory': '384Mi'}}
    target = {'namespace': 'tenant', 'cluster': 'sample-db', 'clusterUID': '11111111-1111-1111-1111-111111111111',
              'systemID': '123456789', 'pod': 'sample-db-1', 'originalPodUID': '22222222-2222-2222-2222-222222222222',
              'node': 'worker-a', 'cpuRequest': '1m', 'expectedMainResources': copy.deepcopy(resources), 'expectedInitResources': copy.deepcopy(resources)}
    config = {'schema': 'fugue.postgres-bootstrap-cpu/v1', 'apply': True, 'operator': {
        'namespace': 'system', 'deployment': 'cnpg', 'uid': 'operator-uid', 'image': 'registry/controller@sha256:' + 'a' * 64}, 'targets': [target]}
    cluster = {'metadata': {'name': target['cluster'], 'namespace': target['namespace'], 'uid': target['clusterUID'], 'resourceVersion': '20',
                            'annotations': {'existing': 'retained'}},
               'spec': {'instances': 1, 'resources': copy.deepcopy(resources), 'storage': {'size': '1Gi'}},
               'status': {'systemID': target['systemID'], 'currentPrimary': target['pod'], 'targetPrimary': target['pod'], 'readyInstances': 1}}
    pod = {'metadata': {'name': target['pod'], 'namespace': target['namespace'], 'uid': target['originalPodUID'], 'resourceVersion': '30',
                        'labels': {'cnpg.io/cluster': target['cluster']}, 'ownerReferences': [{'uid': target['clusterUID'], 'kind': 'Cluster', 'controller': True}]},
           'spec': {'nodeName': target['node'], 'containers': [{'name': 'postgres', 'resources': copy.deepcopy(resources)}],
                    'initContainers': [{'name': policy.INIT, 'resources': copy.deepcopy(resources)}]},
           'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}], 'containerStatuses': [{'name': 'postgres', 'restartCount': 0}]}}
    return config, cluster, pod


class BootstrapPolicyTests(unittest.TestCase):
    def test_only_enrolled_init_cpu_is_changed_and_main_limits_memory_retained(self):
        config, cluster, pod = fixture()
        before = copy.deepcopy((config, cluster, pod))
        policy.validate_config(config)
        operations, value = policy.plan(config['targets'][0], cluster, pod)
        self.assertEqual((config, cluster, pod), before)
        writes = [p for p in operations if p['op'] != 'test']
        self.assertEqual(len(writes), 1)
        self.assertEqual(writes[0]['path'], '/metadata/annotations')
        self.assertEqual(writes[0]['value']['existing'], 'retained')
        self.assertEqual(json.loads(value), [
            {'op': 'test', 'path': '/spec/initContainers/0/name', 'value': policy.INIT},
            {'op': 'replace', 'path': '/spec/initContainers/0/resources/requests/cpu', 'value': '1m'},
        ])
        self.assertEqual({p['path'] for p in operations if p['op'] == 'test'}, {'/metadata/uid', '/metadata/resourceVersion', '/spec'})
        expected = copy.deepcopy(pod['spec']['initContainers'][0]['resources'])
        expected['requests']['cpu'] = '1m'
        self.assertEqual(policy.desired_resources(config['targets'][0]), expected)

    def test_rejects_wrong_identity_unhealthy_database_and_customization_conflicts(self):
        mutations = [
            lambda c, p: c['metadata'].update(uid='different'),
            lambda c, p: c['status'].update(systemID='different'),
            lambda c, p: c['status'].update(targetPrimary='different'),
            lambda c, p: c['status'].update(readyInstances=0),
            lambda c, p: c['spec'].update(instances=2),
            lambda c, p: c['metadata']['annotations'].update({'cnpg.io/hibernation': 'on'}),
            lambda c, p: c['metadata']['annotations'].update({policy.ANNOTATION: '[]'}),
            lambda c, p: p['metadata'].update(ownerReferences=[]),
            lambda c, p: p['metadata'].update(uid='replacement'),
            lambda c, p: p['spec'].update(nodeName='different'),
            lambda c, p: p['spec']['containers'][0]['resources']['requests'].update(cpu='200m'),
            lambda c, p: p['spec']['initContainers'][0]['resources']['limits'].update(cpu='500m'),
            lambda c, p: p['spec']['initContainers'][0].update(restartPolicy='Always'),
            lambda c, p: p['metadata'].update(deletionTimestamp='now'),
        ]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                config, cluster, pod = fixture()
                mutate(cluster, pod)
                with self.assertRaises(ValueError):
                    policy.plan(config['targets'][0], cluster, pod)

    def test_retry_does_not_replace_an_already_optimized_pod(self):
        config, cluster, pod = fixture()
        target = config['targets'][0]
        cluster['metadata']['annotations'][policy.ANNOTATION] = policy.annotation_value(target, pod)
        pod['metadata']['uid'] = '33333333-3333-3333-3333-333333333333'
        pod['spec']['initContainers'][0]['resources'] = policy.desired_resources(target)
        self.assertEqual(policy.plan(target, cluster, pod)[0], [])

    def test_no_annotation_map_supported(self):
        config, cluster, pod = fixture()
        del cluster['metadata']['annotations']
        self.assertEqual(set(policy.plan(config['targets'][0], cluster, pod)[0][-1]['value']), {policy.ANNOTATION})

    def test_only_positive_reductions_with_explicit_unique_enrollment(self):
        for value in ('0m', '-1m', '100m', '1', '0.001'):
            config, _, _ = fixture()
            config['targets'][0]['cpuRequest'] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                policy.validate_config(config)
        config, _, _ = fixture()
        config['targets'] *= 2
        with self.assertRaises(ValueError):
            policy.validate_config(config)

    def test_outside_witness_excludes_enrollment_and_detects_other_pod_mutation(self):
        config, cluster, pod = fixture()
        other_cluster, other_pod = copy.deepcopy(cluster), copy.deepcopy(pod)
        other_cluster['metadata'].update(name='other-db', uid='other-cluster')
        other_pod['metadata'].update(name='other-db-1', uid='other-pod', labels={'cnpg.io/cluster': 'other-db'})
        args = (config, {'items': [cluster, other_cluster]}, {'items': [pod, other_pod]})
        before = policy.outside_snapshot(*args)
        self.assertEqual(set(before['clusters']), {'other-cluster'})
        self.assertEqual(set(before['pods']), {'other-pod'})
        other_pod['status']['containerStatuses'][0]['restartCount'] = 1
        self.assertNotEqual(policy.outside_snapshot(*args), before)

    def test_delete_uses_server_side_uid_and_rv_preconditions_and_no_force(self):
        config, _, pod = fixture()
        with tempfile.TemporaryDirectory() as tmp:
            runner = policy.Runner(config, Path(tmp) / 'evidence.json')
            with patch.object(runner, 'kube', return_value={}) as kube:
                runner.replace(config['targets'][0], pod, preview=True)
                args, kwargs = kube.call_args
                self.assertEqual(args[:2], ('delete', '--raw'))
                self.assertNotIn('?', args[2])
                self.assertEqual(kwargs['body']['dryRun'], ['All'])
                self.assertEqual(kwargs['body']['preconditions'], {'uid': pod['metadata']['uid'], 'resourceVersion': '30'})
                self.assertNotIn('gracePeriodSeconds', kwargs['body'])
                runner.replace(config['targets'][0], pod)
                self.assertNotIn('dryRun', kube.call_args.kwargs['body'])

    def test_failed_first_canary_stops_before_touching_second(self):
        config, cluster, pod = fixture()
        second = copy.deepcopy(config['targets'][0])
        second.update(cluster='second-db', clusterUID='33333333-3333-3333-3333-333333333333')
        config['targets'].append(second)
        receipt = {'annotation': policy.annotation_value(config['targets'][0], pod), 'clusterSpec': policy.digest(cluster['spec']), 'claims': {'claim': {}}}
        prepared = {'configDigest': policy.digest(config), 'preparedAt': policy.time.time(), 'outsideBefore': {}, 'targets': [receipt, receipt]}
        with tempfile.TemporaryDirectory() as tmp:
            runner = policy.Runner(config, Path(tmp) / 'evidence.json')
            with patch.object(runner, 'operator'), patch.object(runner, 'outside', return_value={}), \
                    patch.object(runner, 'target', return_value=(cluster, pod)) as get_target, \
                    patch.object(runner, 'claims', return_value={'claim': {}}), patch.object(runner, 'sql_identity'), \
                    patch.object(runner, 'patch', side_effect=RuntimeError('server validation failed')):
                with self.assertRaisesRegex(RuntimeError, 'server validation failed'):
                    runner.execute(prepared)
                self.assertEqual(get_target.call_count, 1)
                self.assertEqual(get_target.call_args.args[0]['cluster'], 'sample-db')

    def test_restart_requires_spare_capacity_without_counting_original_pod_release(self):
        config, _, pod = fixture()
        target = config['targets'][0]
        node = {'metadata': {'name': target['node']}, 'spec': {}, 'status': {
            'allocatable': {'cpu': '200m', 'memory': '512Mi'}, 'conditions': [{'type': 'Ready', 'status': 'True'}]}}
        pods = {'items': [pod]}
        self.assertEqual(policy.capacity_witness(target, pod, node, pods)['cpu']['spare'], '0.100')
        for resource, value in (('cpu', '199m'), ('memory', '511Mi')):
            full = copy.deepcopy(node)
            full['status']['allocatable'][resource] = value
            with self.subTest(resource=resource), self.assertRaisesRegex(ValueError, 'Insufficient spare'):
                policy.capacity_witness(target, pod, full, pods)
        pod['status']['containerStatuses'][0]['allocatedResources'] = {'cpu': '150m'}
        with self.assertRaisesRegex(ValueError, 'Insufficient spare cpu'):
            policy.capacity_witness(target, pod, node, pods)
        self.assertEqual(policy.quantity('0.5'), policy.quantity('500m'))
        self.assertEqual(policy.quantity('1Gi'), policy.quantity('1024Mi'))

    def test_wait_tolerates_delayed_cluster_readiness_and_requires_one_stable_replacement(self):
        config, cluster, pod = fixture()
        target = config['targets'][0]
        value = policy.annotation_value(target, pod)
        cluster['metadata']['annotations'][policy.ANNOTATION] = value
        pod['metadata']['uid'] = '33333333-3333-3333-3333-333333333333'
        pod['spec']['initContainers'][0]['resources'] = policy.desired_resources(target)
        pod['status']['initContainerStatuses'] = [{'name': policy.INIT, 'restartCount': 0, 'state': {'terminated': {'exitCode': 0}}}]
        delayed = copy.deepcopy(cluster)
        delayed['status']['readyInstances'] = 0
        receipt = {'annotation': value, 'clusterSpec': policy.digest(cluster['spec']), 'claims': {'claim': {}}}
        with tempfile.TemporaryDirectory() as tmp:
            runner = policy.Runner(config, Path(tmp) / 'evidence.json')
            with patch.object(runner, 'target', side_effect=[(delayed, pod), (cluster, pod), (cluster, pod)]), \
                    patch.object(runner, 'claims', return_value={'claim': {}}), patch.object(runner, 'sql_identity'), \
                    patch.object(policy.time, 'sleep'), patch.object(policy.time, 'monotonic', side_effect=[1, 2, 3, 4, 5, 40, 41]):
                runner.wait(target, receipt)
        self.assertEqual(receipt['status'], 'verified')
        self.assertEqual(receipt['newPodUID'], pod['metadata']['uid'])

    def test_wait_rejects_an_unexpected_second_replacement(self):
        config, cluster, pod = fixture()
        pod['metadata']['uid'] = '33333333-3333-3333-3333-333333333333'
        pod['status']['phase'] = 'Pending'
        successor = copy.deepcopy(pod)
        successor['metadata']['uid'] = '44444444-4444-4444-4444-444444444444'
        with tempfile.TemporaryDirectory() as tmp:
            runner = policy.Runner(config, Path(tmp) / 'evidence.json')
            with patch.object(runner, 'target', side_effect=[(cluster, pod), (cluster, successor)]), patch.object(policy.time, 'sleep'):
                with self.assertRaisesRegex(ValueError, 'replaced again'):
                    runner.wait(config['targets'][0], {})


if __name__ == '__main__':
    unittest.main()
