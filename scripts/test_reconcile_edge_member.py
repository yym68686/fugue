import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from scripts.reconcile_edge_member import ACTIVATION_CAS, canonical, digest, validate
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


class MemberRecoveryTest(unittest.TestCase):
    def test_recovery_requires_exact_committed_authority_and_predecessor(self):
        args=fixture()
        validate(*args)
        changed=list(copy.deepcopy(args));changed[3]=dict(changed[2],updated_at='2026-01-02T00:00:00Z');validate(*changed)
        for change in [lambda a:a[1].update(currentFrontGeneration=11), lambda a:a[3].update(generation=2),
                       lambda a:a[2].update(worker_image_digest='sha256:'+'d'*64), lambda a:a[2].update(operation='rollback'),
                       lambda a:a[0].update(workerPort=18443), lambda a:a[0].update(probes=[]),
                       lambda a:a[0].update(arbitraryPath='/tmp/other')]:
            a=copy.deepcopy(fixture());change(a)
            with self.assertRaises(ValueError):validate(*a)

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


if __name__=='__main__':unittest.main()
