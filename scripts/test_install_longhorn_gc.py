import unittest
from unittest.mock import patch

from scripts.install_longhorn_gc import quiescent, volumes


class EngineGCInstallTests(unittest.TestCase):
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
