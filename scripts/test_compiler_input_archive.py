import base64
import copy
import hashlib
import json
import unittest
from unittest.mock import patch

from scripts.compiler_input_archive import validate_operation, managed_reader, hot_reader, hot_restore_executor, credential_key, shared_reader_keys, execute_item


class ArchiveOperationTest(unittest.TestCase):
    @patch('scripts.compiler_input_archive.time.monotonic', return_value=1)
    @patch('scripts.compiler_input_archive.free_bytes', return_value=1024 << 20)
    @patch('scripts.compiler_input_archive.command', return_value='receipt')
    def test_reviewed_duration_cancels_remote_executor_without_kill_or_replay(self, command, space, clock):
        argv = ['kubectl', 'exec', 'synthetic', '--', 'env', 'FEATURE=true',
                '/usr/local/bin/fugue-input-storage', '--mode', 'archive']
        self.assertEqual(execute_item(argv, {}, 512 << 20, 600, 540), 'receipt')
        command.assert_called_once_with(argv[:6]+['timeout', '-s', 'TERM', '540']+argv[6:], timeout=570)
        failure = ValueError('storage command failed stage=maintenance-executor category=deadline')
        command.reset_mock()
        command.side_effect = failure
        with self.assertRaises(ValueError) as raised:
            execute_item(argv, {}, 512 << 20, 600, 540)
        self.assertIs(raised.exception, failure)
        command.assert_called_once()

    def test_duration_is_explicit_and_bounded(self):
        for seconds in (True, 0, 59, 1801, '540'):
            item = self.fixture(); item['maxDurationSeconds'] = seconds
            with self.subTest(seconds=seconds), self.assertRaises(ValueError):
                validate_operation(item)
        item = self.fixture(); item['maxDurationSeconds'] = 540
        validate_operation(item)

    @patch('scripts.compiler_input_archive.time.monotonic', return_value=1)
    @patch('scripts.compiler_input_archive.free_bytes', return_value=1024 << 20)
    @patch('scripts.compiler_input_archive.command')
    def test_command_is_not_replayed_after_a_possible_partial_copy(self, command, space, clock):
        command.return_value = 'receipt'
        self.assertEqual(execute_item(['synthetic'], {}, 512 << 20, 100), 'receipt')
        space.assert_called_once()
        # Any of these failures can follow an already committed lease or
        # receipt. In particular, 55P03 must survive as the original failure;
        # replaying now would usually replace it with active-owner conflict.
        for category in ('55P03', 'conflict', 'deadline', 'unclassified'):
            command.reset_mock()
            failure = ValueError('storage command failed stage=maintenance-executor category='+category)
            command.side_effect = [failure, ValueError('storage command failed stage=maintenance-executor category=conflict')]
            with self.assertRaises(ValueError) as raised:
                execute_item(['synthetic'], {}, 512 << 20, 100)
            self.assertIs(raised.exception, failure)
            command.assert_called_once()
        command.reset_mock()
        space.return_value = 511 << 20
        with self.assertRaisesRegex(ValueError, 'free-space reserve'):
            execute_item(['synthetic'], {}, 512 << 20, 100)
        command.assert_not_called()
        space.return_value = 1024 << 20
        command.reset_mock()
        clock.return_value = 100
        with self.assertRaisesRegex(ValueError, 'time budget'):
            execute_item(['synthetic'], {}, 512 << 20, 100)
        command.assert_not_called()

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
        batch.update(mode='archive-route-batch', digest='', apply=False)
        with self.assertRaises(ValueError): validate_operation(batch)
        batch['apply'] = True
        validate_operation(batch)

    @patch('scripts.compiler_input_archive.command')
    def test_hot_reader_rejects_previous_r2_first_reader(self, command):
        command.side_effect = [
            'func (s *Store) compilerInputArchiveRouted(ctx context.Context) {\nif m.Location == "hot" {\nreturn false, nil\n}\n}',
            'func compilerInputChunksTable(location string) {\ncase "hot": return "fugue_platform_input_hot_chunks"\n}',
        ]
        hot_reader('a'*40)
        for body in ('func (s *Store) compilerInputArchiveRouted(ctx context.Context) {\nreturn state == "routed", nil\n}',
                     'func (s *Store) compilerInputArchiveRouted(ctx context.Context) {\nif m.Location == "hot" {\nreturn true, nil\n}\n}'):
            command.side_effect = [body]
            with self.assertRaises(ValueError): hot_reader('a'*40)

    def test_hot_copy_reserves_both_extra_copy_and_wal(self):
        item = self.fixture()
        item.update(mode='archive-hot-copy', maxStoredBytes=16 << 20, minimumFreeBytes=576 << 20)
        validate_operation(item)
        for key, value in [('maxStoredBytes', True), ('maxStoredBytes', 0), ('maxStoredBytes', (2 << 30)+1),
                           ('minimumFreeBytes', (576 << 20)-1), ('limit', 2), ('digest', '')]:
            invalid = copy.deepcopy(item); invalid[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError): validate_operation(invalid)
        del item['maxStoredBytes']
        with self.assertRaises(ValueError): validate_operation(item)

    def test_hot_restore_requires_exact_digest_and_copy_wal_budget(self):
        item = self.fixture()
        item.update(mode='archive-hot-restore', maxStoredBytes=16 << 20,
                    minimumFreeBytes=576 << 20, maxDurationSeconds=540)
        validate_operation(item)
        for key, value in [('maxStoredBytes', True), ('maxStoredBytes', 0),
                           ('minimumFreeBytes', (576 << 20)-1), ('limit', 2),
                           ('digest', ''), ('digest', '*')]:
            invalid = copy.deepcopy(item); invalid[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                validate_operation(invalid)
        del item['maxStoredBytes']
        with self.assertRaises(ValueError):
            validate_operation(item)

    @patch('scripts.compiler_input_archive.command')
    def test_hot_restore_requires_executor_support_before_remote_execution(self, command):
        command.return_value = 'run = store.RunCompilerInputArchiveHotRestore'
        hot_restore_executor('a'*40)
        command.assert_called_once_with(['git', 'show', 'a'*40+':cmd/fugue-input-storage/main.go'])
        command.return_value = 'store.RunCompilerInputArchiveHotCopy'
        with self.assertRaises(ValueError):
            hot_restore_executor('b'*40)

    @patch('scripts.compiler_input_archive.command')
    def test_managed_reader_requires_shared_configuration_and_activation_barrier(self, command):
        command.side_effect = ['resolveCompilerInputArchiveConfig(ctx, db, false)\nconfigured || s.db != nil', 'decryptDataBackendSecret(sealed)']
        managed_reader('a'*40)
        for reader, config in [('environment-only-reader', 'decryptDataBackendSecret(sealed)'),
                               ('resolveCompilerInputArchiveConfig(ctx, db, false)', 'decryptDataBackendSecret(sealed)')]:
            command.side_effect = [reader, config]
            with self.assertRaises(ValueError): managed_reader('a'*40)

    @patch('scripts.compiler_input_archive.get')
    def test_actual_secret_precedence_empty_fallback_and_alias(self, get):
        get.return_value = {'data': {'empty': '', 'bundle': base64.b64encode(b'synthetic-shared-value').decode()}}
        def ref(name, key):
            return {'name': name, 'valueFrom': {'secretKeyRef': {'name': 'synthetic-secret', 'key': key}}}
        api = {'env': [ref('FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'empty'), ref('FUGUE_BUNDLE_SIGNING_KEY', 'bundle')]}
        ssh = {'env': [ref('FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'bundle')]}
        cache = {}
        self.assertEqual(credential_key('synthetic', api, cache), credential_key('synthetic', ssh, cache))
        get.assert_called_once()
        ssh['env'][0] = ref('FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'empty')
        with self.assertRaises(ValueError): credential_key('synthetic', ssh, cache)
        key = b'k'*32
        encoded = {'env': [{'name': 'FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'value': base64.b64encode(key).decode()}]}
        literal = {'env': [{'name': 'FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'value': key.decode()}]}
        self.assertEqual(credential_key('synthetic', encoded, {}), credential_key('synthetic', literal, {}))
        encoded['env'][0]['value'] = encoded['env'][0]['value'][:12]+'\r\n'+encoded['env'][0]['value'][12:]
        self.assertEqual(credential_key('synthetic', encoded, {}), credential_key('synthetic', literal, {}))
        with self.assertRaises(ValueError): credential_key('synthetic', {'envFrom': [{}]}, {})

    @patch('scripts.compiler_input_archive.get')
    def test_actual_rollback_key_must_match_even_when_current_manifest_is_safe(self, get):
        registry = {'components': [{'id': 'api', 'sourceRoots': ['internal/store'], 'workload': {
            'kind': 'Deployment', 'name': 'synthetic-api', 'namespace': 'synthetic', 'container': 'api'}}]}
        def workload(key, revision):
            return {'kind': 'Deployment', 'metadata': {'name': 'synthetic-api', 'namespace': 'synthetic'},
                    'spec': {'template': {'metadata': {'annotations': {'fugue.pro/source-commit': revision}},
                                         'spec': {'containers': [{'name': 'api', 'env': [
                                             {'name': 'FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY', 'value': key}]}]}}}}
        current = [{'component': 'api', 'revision': 'a'*40, 'rollback_revision': 'b'*40}]
        live = workload('synthetic-current-key', 'a'*40)
        pointer = {'data': {'recordName': 'synthetic-record'}}
        def record_for(key):
            body = json.dumps({'items': [workload(key, 'b'*40)]})
            return {'immutable': True, 'data': {'lkg.json': body, 'record.json': json.dumps({
                'component': 'api', 'configSha': 'a'*40, 'lkgManifestDigest': 'sha256:'+hashlib.sha256(body.encode()).hexdigest()})}}
        record = record_for('synthetic-other-key')
        get.side_effect = [live, pointer, record]
        with self.assertRaisesRegex(ValueError, 'differs for api rollback'):
            shared_reader_keys(registry, current)
        record = record_for('synthetic-current-key')
        get.side_effect = [live, pointer, record]
        shared_reader_keys(registry, current)
        record['immutable'] = False
        get.side_effect = [live, pointer, record]
        with self.assertRaisesRegex(ValueError, 'release record changed'):
            shared_reader_keys(registry, current)


if __name__ == '__main__': unittest.main()
