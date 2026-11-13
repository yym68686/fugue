import unittest
from unittest.mock import patch

from scripts.artifact_content_storage import compatible_revision, check_readers, check_candidate


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

    @patch('scripts.artifact_content_storage.command')
    def test_invalid_source_is_refused_before_git(self, command):
        for revision in ('', 'main', '--help', 'a'*39, 'z'*40):
            with self.assertRaises(ValueError):
                compatible_revision(revision)
        command.assert_not_called()

    @patch('scripts.artifact_content_storage.compatible_revision')
    def test_every_registered_store_candidate_is_gated(self, compatible):
        for component in ('api', 'controller', 'schema', 'ssh-control'):
            self.assertTrue(check_candidate(component, 'a'*40)['artifact_store_reader'])
        self.assertEqual(compatible.call_count, 4)

    @patch('scripts.artifact_content_storage.compatible_revision', side_effect=ValueError('old reader'))
    def test_incompatible_candidate_cannot_bypass_with_writer_disabled(self, compatible):
        with self.assertRaises(ValueError):
            check_candidate('api', 'a'*40)

    @patch('scripts.artifact_content_storage.compatible_revision')
    def test_non_store_component_and_unknown_component(self, compatible):
        self.assertFalse(check_candidate('telemetry', 'a'*40)['artifact_store_reader'])
        with self.assertRaises(ValueError):
            check_candidate('unregistered-component', 'a'*40)
        compatible.assert_not_called()
