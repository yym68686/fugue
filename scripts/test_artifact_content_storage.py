import json
import unittest
from unittest.mock import patch

from scripts.artifact_content_storage import compatible_revision, check_readers, check_candidate, reader_registry


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

    def registry_fixture(self, store_worker=False):
        base = {'components': [{'id': 'service', 'sourceRoots': ['internal/store']}],
                'edgeGroupRegistryPath': 'releases/groups.json'}
        groups = {'apiVersion': 'release.fugue.dev/v1', 'kind': 'EdgeGroupRegistry',
                  'groups': [{key: {'id': key+'-region', 'sourceRoots':
                              ['internal/store'] if key == 'worker' and store_worker else ['internal/edge']}
                              for key in ('client', 'control', 'worker')}]}
        return base, groups

    @patch('scripts.artifact_content_storage.Path.read_text')
    @patch('scripts.artifact_content_storage.compatible_revision')
    def test_group_components_are_registered_without_exempting_store_readers(self, compatible, read):
        for store_worker in (False, True):
            base, groups = self.registry_fixture(store_worker)
            read.side_effect = [json.dumps(base), json.dumps(groups)]
            result = check_candidate('worker-region', 'a'*40)
            self.assertEqual(result['artifact_store_reader'], store_worker)
        compatible.assert_called_once_with('a'*40)

    @patch('scripts.artifact_content_storage.Path.read_text')
    @patch('scripts.artifact_content_storage.readers', return_value=[])
    def test_current_and_rollback_inventory_includes_all_group_roles(self, readers, read):
        base, groups = self.registry_fixture(True)
        read.side_effect = [json.dumps(base), json.dumps(groups)]
        check_readers()
        self.assertEqual([c['id'] for c in readers.call_args.args[0]['components']],
                         ['service', 'client-region', 'control-region', 'worker-region'])

    @patch('scripts.artifact_content_storage.Path.read_text')
    def test_group_inventory_fails_closed_on_duplicates_and_invalid_paths(self, read):
        base, groups = self.registry_fixture()
        groups['groups'][0]['worker']['id'] = 'service'
        read.side_effect = [json.dumps(base), json.dumps(groups)]
        with self.assertRaisesRegex(ValueError, 'ambiguous'):
            reader_registry()
        for path in ('../outside.json', '/outside.json', './groups.json'):
            read.side_effect = [json.dumps({**base, 'edgeGroupRegistryPath': path})]
            with self.assertRaisesRegex(ValueError, 'repository-relative'):
                reader_registry()

    @patch('scripts.artifact_content_storage.Path.read_text')
    def test_static_only_inventory_remains_supported(self, read):
        base, _ = self.registry_fixture()
        del base['edgeGroupRegistryPath']
        read.return_value = json.dumps(base)
        self.assertEqual(reader_registry(), base)
