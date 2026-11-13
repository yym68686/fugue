import unittest
from unittest.mock import patch

from scripts.artifact_content_storage import compatible_revision, check_readers


class ArtifactContentGateTest(unittest.TestCase):
    @patch('scripts.artifact_content_storage.command', return_value='legacy inline reader')
    def test_old_rollback_is_refused(self, command):
        with self.assertRaises(ValueError):
            compatible_revision('a'*40)

    @patch('scripts.artifact_content_storage.compatible_revision')
    @patch('scripts.artifact_content_storage.readers', return_value=[{'revision': 'a'*40, 'rollback_revision': 'b'*40}])
    def test_current_and_rollback_are_both_required(self, readers, compatible):
        check_readers()
        self.assertEqual([x.args[0] for x in compatible.call_args_list], ['a'*40, 'b'*40])
