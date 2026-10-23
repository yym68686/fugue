import contextlib
import copy
import json
import unittest
from unittest.mock import patch

from scripts import configure_edge_release_hold as p


def declaration():
    return {'schema': 'fugue.edge-release-hold-declaration/v1', 'generation': 1, 'namespace': 'system', 'component': 'edge-worker-a', 'group': 'edge-pool-a', 'node': 'limited', 'node_uid': 'limited-uid', 'reason': 'quota_exhausted',
            'readers': [{'role': role, 'deployment': role, 'uid': role+'-uid', 'image': 'ghcr.io/example/guardian@sha256:'+'a'*64, 'revision': 'b'*40} for role in ['guardian', 'canary-prober']]}


def reader_fixture(reader):
    env = [{'name': 'FUGUE_RELEASE_GUARDIAN_ROLE', 'value': reader['role']}]
    if reader['role'] == 'guardian':
        env.append({'name': 'FUGUE_RELEASE_GUARDIAN_AUTHORITY_ACTIVATORS', 'value': 'edge-pool-a,http://control,identity,2,address,host,/,digest,slot-a,slot-b'})
    spec = {'containers': [{'name': 'process', 'image': reader['image'], 'env': env}]}
    meta = {'annotations': {'fugue.pro/oci-revision': reader['revision']}, 'labels': {'app': reader['role']}}
    deployment = {'metadata': {'name': reader['deployment'], 'uid': reader['uid'], 'generation': 3, 'annotations': {p.CAPABILITY: 'v1'}},
                  'spec': {'replicas': 1, 'template': {'metadata': meta, 'spec': spec}},
                  'status': dict(observedGeneration=3, replicas=1, updatedReplicas=1, readyReplicas=1, availableReplicas=1)}
    pod = {'metadata': dict(meta, uid='pod-uid'), 'spec': spec, 'status': {'phase': 'Running', 'conditions': [{'type': 'Ready', 'status': 'True'}], 'containerStatuses': [{'name': 'process', 'imageID': reader['image'], 'ready': True, 'state': {'running': {}}}]}}
    return deployment, {'items': [pod]}


def node_fixture():
    node = {'metadata': {'name': 'limited', 'uid': 'limited-uid', 'resourceVersion': '20', 'labels': {'fugue.io/edge-group-id': 'edge-pool-a', 'fugue.io/role.edge': 'true', 'fugue.io/schedulable': 'true'}, 'annotations': {'unrelated': 'preserved'}}}
    other = copy.deepcopy(node)
    other['metadata'].update(name='healthy', uid='healthy-uid')
    return node, {'items': [node, other]}


class ReleaseHoldTests(unittest.TestCase):
    def test_declaration_is_exact_and_has_both_readers(self):
        p.validate(declaration())
        for mutation in [lambda c: c.update(generation=True), lambda c: c.update(reason='healthy'), lambda c: c.update(node_uid=''), lambda c: c['readers'].pop(), lambda c: c.update(force=True), lambda c: c['readers'][1].update(role='guardian')]:
            c = declaration(); mutation(c)
            with self.assertRaises(ValueError): p.validate(c)

    def test_reader_requires_capability_and_current_physical_executable(self):
        r = declaration()['readers'][0]
        d, pods = reader_fixture(r)
        self.assertEqual(p.validate_reader(r, d, pods, 'edge-pool-a'), 2)
        mutations = [lambda d, x: d['metadata']['annotations'].clear(), lambda d, x: d['status'].update(updatedReplicas=0), lambda d, x: d['metadata'].update(uid='replacement'), lambda d, x: x['items'][0]['status']['containerStatuses'][0].update(imageID='wrong'), lambda d, x: x['items'][0]['status']['containerStatuses'][0].update(restartCount=1), lambda d, x: x['items'][0]['metadata']['annotations'].update({'fugue.pro/oci-revision': 'old'})]
        for change in mutations:
            d, pods = reader_fixture(r); change(d, pods)
            with self.assertRaises(ValueError): p.validate_reader(r, d, pods, 'edge-pool-a')

    def test_ready_throttled_node_can_be_held_without_touching_placement(self):
        c = declaration(); node, inventory = node_fixture(); before = copy.deepcopy(node)
        self.assertIsNone(p.validate_node(c, node, inventory, 2))
        self.assertEqual(node, before)
        node['metadata']['annotations'][p.ANNOTATION] = p.canonical(p.hold(c))
        self.assertIsNotNone(p.validate_node(c, node, inventory, 2))

    def test_no_implicit_identity_rebind_or_baseline_shrink(self):
        for scenario in ['uid', 'missing', 'duplicate', 'last', 'group', 'other-hold', 'node-changing']:
            c = declaration(); node, inventory = node_fixture()
            if scenario == 'uid': node['metadata']['uid'] = 'replaced'
            if scenario == 'group': node['metadata']['labels']['fugue.io/edge-group-id'] = 'foreign'
            if scenario == 'missing': inventory['items'].pop()
            if scenario == 'duplicate': inventory['items'].append(node)
            if scenario == 'last': inventory['items'][1]['metadata']['labels']['fugue.io/schedulable'] = 'false'
            if scenario == 'other-hold': node['metadata']['annotations'][p.ANNOTATION] = '{}'
            if scenario == 'node-changing': inventory['items'][0] = copy.deepcopy(node); inventory['items'][0]['metadata']['resourceVersion'] = '21'
            with self.subTest(scenario=scenario), self.assertRaises(ValueError): p.validate_node(c, node, inventory, 2)

    def test_apply_only_patches_the_hold_with_uid_rv_cas(self):
        c = declaration(); node, _ = node_fixture(); writer = p.HoldWriter(c)
        lease = {'metadata': {'name': 'lease', 'uid': 'lease-uid', 'resourceVersion': '8'}, 'spec': {'renewTime': p.now().isoformat(), 'leaseDurationSeconds': 120}}
        calls = []
        def kube(*args, **kwargs):
            calls.append(args)
            self.assertEqual(args[:3], ('patch', 'node', 'limited'))
            ops = json.loads(args[args.index('-p')+1])
            self.assertEqual(ops[:2], [{'op': 'test', 'path': '/metadata/uid', 'value': 'limited-uid'}, {'op': 'test', 'path': '/metadata/resourceVersion', 'value': '20'}])
            self.assertEqual(ops[-1]['path'], '/metadata/annotations/fugue.io~1edge-release-hold')
            self.assertEqual(len(ops), 3)
            result = copy.deepcopy(node); result['metadata']['resourceVersion'] = '21'; result['metadata']['annotations'][p.ANNOTATION] = ops[-1]['value']
            return json.dumps(result)
        with patch.object(writer, 'leases', return_value=contextlib.nullcontext([lease])), patch.object(writer, 'preflight', return_value=(node, None, [])), patch.object(writer, 'get', return_value=lease), patch.object(writer, 'kube', side_effect=kube):
            self.assertTrue(writer.apply()['applied'])
        self.assertEqual(len(calls), 1)

    def test_changed_or_expired_lease_prevents_node_write(self):
        for changed in [True, False]:
            writer = p.HoldWriter(declaration()); node, _ = node_fixture()
            lease = {'metadata': {'name': 'lease', 'uid': 'uid', 'resourceVersion': '8'}, 'spec': {'renewTime': p.now().isoformat(), 'leaseDurationSeconds': 1 if not changed else 120}}
            current = copy.deepcopy(lease)
            if changed: current['metadata']['resourceVersion'] = '9'
            with patch.object(writer, 'leases', return_value=contextlib.nullcontext([lease])), patch.object(writer, 'preflight', return_value=(node, None, [])), patch.object(writer, 'get', return_value=current), patch.object(writer, 'kube') as write:
                with self.assertRaises(ValueError): writer.apply()
                write.assert_not_called()

    def test_release_lease_must_belong_to_the_observed_authority(self):
        c = declaration(); writer = p.HoldWriter(c)
        d, _ = reader_fixture(c['readers'][0])
        d['spec']['template']['spec']['containers'][0]['env'].append({'name': 'FUGUE_RELEASE_GUARDIAN_TARGETS', 'value': 'edge-worker-a,a,system,worker,control'})
        selected = {'data': {'desired.json': json.dumps({'recordDigest': 'sha256:'+'f'*64})}}
        plan = {'releases': [{'component': c['component'], 'concurrency': 'fugue-production-'+c['component'], 'workload': {'namespace': c['namespace']}, 'transition': {'type': 'edge-group-ab', 'edgeGroupAB': {'groupId': c['group'], 'expectedNodes': 2, 'frontName': 'front', 'workerAName': 'worker-a', 'workerBName': 'worker-b'}}}]}
        record = {'immutable': True, 'metadata': {'labels': {'fugue.pro/component': c['component']}}, 'data': {'release-plan.json': json.dumps(plan)}}
        with patch.object(writer, 'get', side_effect=[selected, record]): writer.validate_release_binding(d, 2)
        for change in ['group', 'lease', 'baseline', 'mutable']:
            bad = copy.deepcopy(record); q = copy.deepcopy(plan)
            if change == 'group': q['releases'][0]['transition']['edgeGroupAB']['groupId'] = 'other'
            if change == 'lease': q['releases'][0]['concurrency'] = 'other'
            if change == 'baseline': q['releases'][0]['transition']['edgeGroupAB']['expectedNodes'] = 1
            if change == 'mutable': bad['immutable'] = False
            bad['data']['release-plan.json'] = json.dumps(q)
            with patch.object(writer, 'get', side_effect=[selected, bad]), self.assertRaises(ValueError): writer.validate_release_binding(d, 2)

    def test_idempotent_hold_does_not_patch_again(self):
        c = declaration(); node, _ = node_fixture(); writer = p.HoldWriter(c)
        with patch.object(writer, 'leases', return_value=contextlib.nullcontext([])), patch.object(writer, 'preflight', return_value=(node, p.canonical(p.hold(c)), [])), patch.object(writer, 'kube') as write:
            self.assertTrue(writer.apply()['idempotent'])
            write.assert_not_called()

    def test_rolling_controller_cannot_restart_a_retained_member(self):
        w = {'metadata': {'uid': 'workload-uid'}, 'spec': {'updateStrategy': {'type': 'OnDelete'}, 'template': {'metadata': {'labels': {'fugue.io/edge-group-id': 'edge-pool-a'}}}}}
        p.validate_retained_workload(w, 'edge-pool-a')
        w['spec']['updateStrategy']['type'] = 'RollingUpdate'
        with self.assertRaises(ValueError): p.validate_retained_workload(w, 'edge-pool-a')


if __name__ == '__main__':
    unittest.main()
