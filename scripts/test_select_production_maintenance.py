import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts import select_production_maintenance as plan
from scripts.recover_longhorn_snapshot import canonical
from scripts.test_recover_longhorn_snapshot import approval, config


class SelectionTests(unittest.TestCase):
    outputs = [*plan.DECLARATIONS, 'cleanup_observer', 'cnpg_retention', 'local_node_resources']

    def setUp(self):
        self.repo = tempfile.TemporaryDirectory()
        self.addCleanup(self.repo.cleanup)
        self.root = Path(self.repo.name)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.test')
        self.git('config', 'commit.gpgsign', 'false')
        git_patch = patch.object(plan, 'git', side_effect=self.git)
        git_patch.start()
        self.addCleanup(git_patch.stop)
        self.write(plan.REVIEW + '/review.json', config())
        self.write(plan.REVIEW + '/approval.json', approval(config()))
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(['git', '-C', self.repo.name, *args], text=True)

    def write(self, path, content):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(content))

    def commit(self):
        self.git('add', '-A')
        self.git('commit', '-qm', 'fixture')
        return self.git('rev-parse', 'HEAD').strip()

    def request(self, c, a=None):
        path = plan.REQUESTS + hashlib.sha256(canonical(c).encode()).hexdigest()
        self.write(path + '/review.json', c)
        self.write(path + '/approval.json', a or approval(c))
        return path

    def test_code_docs_workflow_or_legacy_changes_do_not_select_a_writer(self):
        for path in ('scripts/recover_longhorn_snapshot.py', 'scripts/test_expand_postgres_volume.py',
                     'scripts/reconcile_postgres_protection.py', 'scripts/reconcile_node_participation.py',
                     '.github/workflows/ci.yml', '.github/workflows/daily-audit.yml', 'docs/notes.md',
                     'scripts/test_cnpg_backup_retention.py', 'scripts/cnpg_backup_history.py',
                     'scripts/publish_node_resource_policy.py', 'scripts/test_install_local_node_resource_policy.py',
                     plan.REVIEW + '/approval.json'):
            self.write(path, {'changed': True})
        self.assertEqual(plan.select(self.base, self.commit()), dict.fromkeys(self.outputs, False))

    def test_configuration_intents_select_only_their_own_writer(self):
        for name, path in plan.DECLARATIONS.items():
            with self.subTest(name=name):
                before = self.git('rev-parse', 'HEAD').strip()
                self.write(path, {'changed': True})
                self.assertEqual(plan.select(before, self.commit()), {key: key == name for key in self.outputs})

    def test_observer_changes_never_select_snapshot_recovery(self):
        self.write(plan.OBSERVER + 'observe.py', {'updated': True})
        self.assertEqual(plan.select(self.base, self.commit()),
                         {**dict.fromkeys(self.outputs, False), 'cleanup_observer': True})

    def test_cnpg_retention_installs_only_when_its_inputs_change(self):
        before = self.base
        for path in plan.CNPG_RETENTION_EXECUTOR:
            with self.subTest(path=path):
                self.write(path, {'updated': True})
                after = self.commit()
                self.assertEqual(plan.select(before, after), {key: key == 'cnpg_retention' for key in self.outputs})
                before = after

    def test_local_node_resources_publish_on_declaration_or_local_executor_changes(self):
        before = self.base
        for path in plan.LOCAL_NODE_RESOURCE_EXECUTOR:
            with self.subTest(path=path):
                self.write(path, {'updated': True})
                after = self.commit()
                self.assertEqual(plan.select(before, after), {key: key == 'local_node_resources' for key in self.outputs})
                before = after

    def test_formatting_does_not_replay_configuration_intent(self):
        path = plan.DECLARATIONS['expansion']
        self.write(path, {'size': '10Gi'})
        before = self.commit()
        (self.root / path).write_text(' { "size" : "10Gi" }\n')
        self.assertFalse(plan.select(before, self.commit())['expansion'])

    def test_new_approval_targets_exact_declaration_and_cannot_be_modified(self):
        c = {**config(), 'snapshotUID': 'new-snapshot-uid'}
        path = self.request(c)
        approved = self.commit()
        self.assertEqual(plan.select(self.base, approved)['snapshot_request'], path)
        self.write(path + '/approval.json', {**approval(c), 'maxWaitSeconds': 61})
        with self.assertRaisesRegex(ValueError, 'append-only'):
            plan.select(approved, self.commit())
        self.git('rm', '-r', path)
        removed = self.commit()
        self.request(c)
        with self.assertRaisesRegex(ValueError, 'previously used digest'):
            plan.select(removed, self.commit())

    def test_historical_approval_and_wrong_digest_never_select_apply(self):
        self.request(config())
        with self.assertRaisesRegex(ValueError, 'legacy snapshot authorization'):
            plan.select(self.base, self.commit())
        before = self.git('rev-parse', 'HEAD').strip()
        self.request({**config(), 'snapshotUID': 'new-snapshot-uid'}, approval(config()))
        with self.assertRaisesRegex(ValueError, 'approval targets another snapshot'):
            plan.select(before, self.commit())
