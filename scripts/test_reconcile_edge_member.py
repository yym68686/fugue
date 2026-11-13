import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from scripts.reconcile_edge_member import ACTIVATION_CAS, Recovery, canonical, digest, now, validate, validate_inactive_refresh
import base64


def fixture():
    state = {'schema': 'edge-front-group-activation/v1', 'edge_group_id': 'group-a', 'generation': 10,
             'active_slot': 'b', 'previous_slot': 'a', 'bundle_generation': 'bundle.p10.r1',
             'worker_source_commit': 'a' * 40, 'worker_image_digest': 'sha256:' + 'b' * 64,
             'authority': 'edge-control', 'operation': 'promote', 'reason': 'Committed group promotion', 'updated_at': '2026-01-01T00:00:00Z'}
    previous = dict(state, generation=3, active_slot='a', previous_slot='b')
    authority = dict(groupId='group-a', currentFrontGeneration=10, currentWorkerSlot='b', currentWorkerSourceSha='a'*40,
                     currentWorkerImageDigest='sha256:'+'b'*64, currentBundleGeneration='bundle.p10.r1', currentRecordDigest='sha256:'+'c'*64)
    config = dict(schema='fugue.edge-member-recovery/v1', namespace='system', component='edge-worker-a', group='group-a', node='node-returned',
                  front='front-returned', frontUID='front-uid', worker='worker-returned', workerUID='worker-uid', referenceFront='front-reference', referenceFrontUID='reference-front-uid',
                  referenceWorker='worker-reference', referenceWorkerUID='reference-worker-uid', authorityUID='authority-uid', authorityRecord=authority['currentRecordDigest'],
                  previousState=copy.deepcopy(previous), activationPath='/var/lib/fugue-edge-front/activation.json', workerPort=28443,
                  probes=[{'host':'app.example.test','path':'/'},{'host':'api.example.test','path':'/'}])
    return config, authority, state, previous


def inactive_fixture():
    c, authority, target, previous = fixture()
    c['refreshInactiveWorker'] = dict(daemonset='workers-b', daemonsetUID='workload-uid', nodeUID='node-uid')
    images = dict(edge='registry.example.test/worker@' + target['worker_image_digest'], caddy='registry.example.test/caddy@sha256:' + 'd' * 64)
    def worker(name, uid, node):
        containers = [dict(name=k, image=v, ports=[dict(name='https-slot', protocol='TCP', containerPort=28443)] if k == 'caddy' else []) for k, v in images.items()]
        return dict(metadata=dict(name=name, uid=uid, namespace=c['namespace'], resourceVersion='1',
                    labels={'fugue.io/edge-group-id': c['group'], 'fugue.io/edge-slot': 'b'},
                    annotations={'fugue.pro/source-commit': target['worker_source_commit']},
                    ownerReferences=[dict(kind='DaemonSet', name='workers-b', uid='workload-uid', controller=True)]),
                    spec=dict(nodeName=node, containers=containers), status=dict(phase='Running', podIP='10.0.0.1',
                    containerStatuses=[dict(name=k, ready=True, imageID=v, state=dict(running={'startedAt':'2026-01-01T00:00:00Z'})) for k,v in images.items()]))
    reference = worker(c['referenceWorker'], c['referenceWorkerUID'], 'node-reference')
    old = worker(c['worker'], c['workerUID'], c['node'])
    old['metadata']['annotations']['fugue.pro/source-commit'] = 'f' * 40
    old['spec']['containers'][0]['image'] = 'registry.example.test/worker@sha256:' + 'e' * 64
    old['status']['containerStatuses'][0]['imageID'] = old['spec']['containers'][0]['image']
    ds = dict(metadata=dict(name='workers-b', uid='workload-uid', namespace=c['namespace']),
              spec=dict(updateStrategy=dict(type='OnDelete'), template=dict(metadata=copy.deepcopy(reference['metadata']),
                   spec=dict(containers=copy.deepcopy(reference['spec']['containers']), nodeSelector={'role': 'edge'}))))
    node = dict(metadata=dict(name=c['node'], uid='node-uid', labels={'role': 'edge'}), spec={})
    return c, ds, node, target, previous, old, reference


class MemberRecoveryTest(unittest.TestCase):
    def test_inactive_refresh_rejects_active_foreign_or_uncommitted_replacements(self):
        args = inactive_fixture()
        self.assertTrue(validate_inactive_refresh(*args))
        for mutation in [lambda a:a[4].update(active_slot='b'), lambda a:a[1]['metadata'].update(uid='replacement'),
                         lambda a:a[2]['metadata'].update(uid='replacement'), lambda a:a[2]['spec'].update(unschedulable=True),
                         lambda a:a[1]['spec']['updateStrategy'].update(type='RollingUpdate'),
                         lambda a:a[1]['spec']['template']['spec']['containers'][0].update(image='registry.example.test/worker@sha256:'+'f'*64),
                         lambda a:a[1]['spec']['template']['spec']['nodeSelector'].update(role='other'),
                         lambda a:a[5]['metadata']['ownerReferences'][0].update(uid='foreign'),
                         lambda a:a[5]['status']['containerStatuses'][0].update(imageID='unverified'),
                         lambda a:a[6]['metadata']['annotations'].update({'fugue.pro/source-commit':'f'*40})]:
            changed = copy.deepcopy(args)
            mutation(changed)
            with self.assertRaises(ValueError):
                validate_inactive_refresh(*changed)

    def test_inactive_refresh_deletes_only_declared_uid_and_reuses_replacement(self):
        c, ds, node, target, previous, old, reference = inactive_fixture()
        replacement = copy.deepcopy(reference)
        replacement['metadata'].update(name='worker-replacement', uid='replacement-uid')
        replacement['spec']['nodeName'] = c['node']
        owner = dict(metadata=dict(resourceVersion='1'))
        lease = dict(metadata=dict(name='release-lease', uid='lease-uid', resourceVersion='1'),
                     spec=dict(renewTime=now().isoformat(), leaseDurationSeconds=120))
        class FakeRecovery(Recovery):
            def __init__(self):
                super().__init__(copy.deepcopy(c))
                self.deletes, self.renewed = [], False
            def get(self, kind, name):
                return {'daemonset':ds, 'node':node, 'lease':lease}[kind]
            def snapshot(self, allow_refresh=False):
                return owner, target, previous, 'original', replacement if self.deletes else old, reference
            def kube(self, *args, body=None):
                if args[0] == 'delete':
                    self.deletes.append((args, json.loads(body)))
                    return '{}'
                return json.dumps(dict(items=[replacement]))
            def renew_leases(self, held):
                self.renewed = True
        recovery = FakeRecovery()
        recovery.refresh_inactive_worker([lease], owner, target, previous, 'original', old, reference)
        self.assertTrue(recovery.renewed)
        self.assertEqual(len(recovery.deletes), 1)
        command, body = recovery.deletes[0]
        self.assertEqual(command[2], '/api/v1/namespaces/system/pods/worker-returned')
        self.assertEqual(body['preconditions'], dict(uid='worker-uid', resourceVersion='1'))
        self.assertNotIn('gracePeriodSeconds', body)
        self.assertEqual(recovery.config['workerUID'], 'replacement-uid')
        recovery.refresh_inactive_worker([lease], owner, target, previous, 'original', replacement, reference)
        self.assertEqual(len(recovery.deletes), 1)
        # A fresh process can discover the previously verified committed Pod.
        recovery.config.update(worker=c['worker'], workerUID=c['workerUID'])
        self.assertEqual(recovery.current_worker(target)['metadata']['uid'], 'replacement-uid')

    def test_recovery_requires_exact_committed_authority_and_predecessor(self):
        args=fixture()
        validate(*args)
        changed=list(copy.deepcopy(args));changed[3]=dict(changed[2],updated_at='2026-01-02T00:00:00Z');validate(*changed)
        for change in [lambda a:a[1].update(currentFrontGeneration=11), lambda a:a[3].update(generation=2),
                       lambda a:a[2].update(worker_image_digest='sha256:'+'d'*64), lambda a:a[2].update(operation='initialize'),
                       lambda a:a[0].update(workerPort=18443), lambda a:a[0].update(probes=[]),
                       lambda a:a[0].update(arbitraryPath='/tmp/other')]:
            a=copy.deepcopy(fixture());change(a)
            with self.assertRaises(ValueError):validate(*a)

    def test_recreated_current_worker_can_reconcile_same_slot_and_committed_rollback(self):
        config, authority, state, previous = fixture()
        previous['active_slot'] = state['active_slot']
        config['previousState'] = copy.deepcopy(previous)
        state.update(operation='rollback', rollback_of_generation=9)
        validate(config, authority, state, previous)
        for generation in [state['generation'], state['generation'] + 1]:
            changed = dict(previous, generation=generation)
            config['previousState'] = changed
            with self.assertRaises(ValueError):
                validate(config, authority, state, changed)

    def test_atomic_replacement_keeps_backup_and_rejects_changed_local_state(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);state=root/'activation.json';prepared=root/'activation.json.prepared'
            original=canonical(fixture()[3])+'\n';target=canonical(fixture()[2])+'\n'
            state.write_text(original);prepared.write_text('{}\n')
            # macOS lacks the flock CLI; this shim uses the real kernel flock
            # on the inherited descriptor, matching the Linux command used in CI.
            if not shutil.which('flock'):
                lock=root/'flock';lock.write_text('#!/usr/bin/env python3\nimport fcntl,sys\nfcntl.flock(int(sys.argv[-1]),fcntl.LOCK_EX|fcntl.LOCK_NB)\n');lock.chmod(0o755)
            env=dict(os.environ,PATH=str(root)+os.pathsep+os.environ['PATH'])
            command=['sh','-ceu',ACTIVATION_CAS,'sh',str(state),digest(original),digest('{}\n'),base64.b64encode(target.encode()).decode()]
            done=subprocess.run(command,capture_output=True,text=True,env=env)
            self.assertEqual(done.returncode,0,done.stderr)
            self.assertEqual(state.read_text(),target)
            self.assertEqual(Path(str(state)+'.recovery-before-'+digest(original)).read_text(),original)
            again=subprocess.run(command,capture_output=True,text=True,env=env)
            self.assertNotEqual(again.returncode,0)
            self.assertEqual(state.read_text(),target)

    def test_same_slot_record_repair_needs_no_inactive_preparation(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / 'activation.json'
            previous = dict(fixture()[3], active_slot='b')
            original, target = canonical(previous) + '\n', canonical(fixture()[2]) + '\n'
            state.write_text(original)
            if not shutil.which('flock'):
                lock = root / 'flock'
                lock.write_text('#!/usr/bin/env python3\nimport fcntl,sys\nfcntl.flock(int(sys.argv[-1]),fcntl.LOCK_EX|fcntl.LOCK_NB)\n')
                lock.chmod(0o755)
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'])
            command = ['sh', '-ceu', ACTIVATION_CAS, 'sh', str(state), digest(original), '', base64.b64encode(target.encode()).decode()]
            done = subprocess.run(command, capture_output=True, text=True, env=env)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual(state.read_text(), target)
            self.assertEqual(Path(str(state) + '.recovery-before-' + digest(original)).read_text(), original)


if __name__=='__main__':unittest.main()
