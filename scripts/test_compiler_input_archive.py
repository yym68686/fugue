import copy
import json
import unittest
from unittest.mock import patch

from scripts.compiler_input_archive import validate_operation, managed_reader


class ArchiveOperationTest(unittest.TestCase):
    def fixture(self):
        return dict(version=1, mode='archive', digest='sha256:'+'a'*64, limit=1,
                    apply=False, expectedExecutorRevision='b'*40, minimumFreeBytes=512 << 20)

    def test_explicit_identity_bounds_and_no_hidden_mutations(self):
        self.assertEqual(validate_operation(self.fixture())['apply'], False)
        for key, value in [('mode', 'archive-retire'), ('limit', 0), ('limit', True),
                           ('limit', 1001), ('minimumFreeBytes', (512 << 20)-1),
                           ('digest', '*'), ('expectedExecutorRevision', 'main'), ('apply', 'true')]:
            with self.subTest(key=key, value=value):
                item = self.fixture(); item[key] = value
                with self.assertRaises(ValueError): validate_operation(item)
        item = self.fixture(); item['mode'] = 'archive-route'
        with self.assertRaises(ValueError): validate_operation(item)
        item['apply'] = True
        validate_operation(item)
        batch = self.fixture(); batch.update(mode='archive-batch', digest='', limit=1000)
        validate_operation(batch)
        batch['digest'] = self.fixture()['digest']
        with self.assertRaises(ValueError): validate_operation(batch)

    @patch('scripts.compiler_input_archive.command')
    def test_managed_reader_requires_shared_configuration_and_activation_barrier(self, command):
        def source(data_key='FUGUE_BUNDLE_SIGNING_KEY', data_optional=False, bundle=True):
            env = [{'name': 'FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'valueFrom': {'secretKeyRef': {
                'name': 'fugue-fugue-config', 'key': data_key, 'optional': data_optional}}}]
            if bundle:
                env.append({'name': 'FUGUE_BUNDLE_SIGNING_KEY', 'valueFrom': {'secretKeyRef': {
                    'name': 'fugue-fugue-config', 'key': 'FUGUE_BUNDLE_SIGNING_KEY'}}})
            return json.dumps({'items': [{'kind': 'Deployment', 'spec': {'template': {'spec': {'containers': [
                {'env': env}
            ]}}}}]})
        good = source()
        command.side_effect = ['resolveCompilerInputArchiveConfig(ctx, db, false)\nconfigured || s.db != nil', 'decryptDataBackendSecret(sealed)', good, good, good]
        managed_reader('a'*40)
        for reader, config, refs in [('environment-only-reader', 'decryptDataBackendSecret(sealed)', [good, good, good]),
                                     ('resolveCompilerInputArchiveConfig(ctx, db, false)', 'decryptDataBackendSecret(sealed)', [good, source('FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', True, False), good])]:
            command.side_effect = [reader, config, *refs]
            with self.assertRaises(ValueError): managed_reader('a'*40)


if __name__ == '__main__': unittest.main()
