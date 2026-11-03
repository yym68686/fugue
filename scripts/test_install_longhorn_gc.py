import unittest
from unittest.mock import patch

from scripts.install_longhorn_gc import manager_engine_patch, quiescent, volumes


class EngineGCInstallTests(unittest.TestCase):
    def test_utility_is_selected_via_manager_startup_without_patching_read_only_setting(self):
        manager = {
            'metadata': {'uid': 'manager-uid', 'resourceVersion': '23'},
            'spec': {'template': {'spec': {'containers': [
                {'name': 'manager', 'command': ['longhorn-manager', 'daemon', '--engine-image', 'old', '--other', 'kept']},
            ]}}},
        }
        patches = manager_engine_patch(manager, 'manager', 'old', 'new')
        self.assertEqual(patches[-1], {'op': 'replace', 'path': '/spec/template/spec/containers/0/command/3', 'value': 'new'})
        self.assertEqual([p['path'] for p in patches if p['op'] == 'test'], [
            '/metadata/uid', '/metadata/resourceVersion', '/spec/template/spec/containers/0/name',
            '/spec/template/spec/containers/0/command',
        ])
        manager['spec']['template']['spec']['containers'][0]['command'][3] = 'new'
        self.assertEqual(manager_engine_patch(manager, 'manager', 'old', 'new'), [])
        manager['spec']['template']['spec']['containers'][0]['command'][3] = 'unexpected'
        with self.assertRaises(ValueError):
            manager_engine_patch(manager, 'manager', 'old', 'new')

    def test_never_switches_during_backup_delete_or_restore(self):
        for backup in [
            {'metadata': {'deletionTimestamp': 'now'}, 'status': {'state': 'Completed'}},
            {'metadata': {}, 'status': {'state': 'InProgress'}},
        ]:
            with patch('scripts.install_longhorn_gc.kubectl', return_value={'items': [backup]}):
                self.assertFalse(quiescent('storage'))
        with patch('scripts.install_longhorn_gc.kubectl', side_effect=[
            {'items': [{'metadata': {}, 'status': {'state': 'Completed'}}]},
            {'items': [{'status': {'restoreRequired': True}}]},
        ]):
            self.assertFalse(quiescent('storage'))

    def test_observes_existing_engine_identity_without_modifying_it(self):
        volume = {'metadata': {'uid': 'volume-uid'}, 'spec': {'image': 'engine@sha256:abc'}}
        with patch('scripts.install_longhorn_gc.kubectl', return_value={'items': [volume]}) as command:
            self.assertEqual(volumes('storage'), {'volume-uid': 'engine@sha256:abc'})
            self.assertEqual(command.call_args.args, ('-n', 'storage', 'get', 'volumes.longhorn.io', '-o', 'json'))
