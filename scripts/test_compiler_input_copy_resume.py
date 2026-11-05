import copy
import io
import json
import unittest
import zipfile
from unittest.mock import patch

from scripts import compiler_input_copy_resume as verifier
from scripts import compiler_input_readonly as readonly
from scripts.test_compiler_input_copy import declaration, receipt


def fixture():
    prior = declaration()
    prior['copy']['max_put_bytes'] = 10000
    prior['copy']['max_get_bytes'] = 20000
    source = dict(digest=prior['copy']['digest'], format=prior['copy']['source_format'], location='legacy',
                  raw_bytes=65537, chunk_size=32769, chunk_count=2, created_utc='2026-01-02T03:04:05.123456Z')
    prefix = 'inputs/neutral/<shared>&\u2028'
    segment = dict(ordinal=0, key=prefix+'/00000-'+'1'*64+'.gz', raw_bytes=65536, stored_bytes=1234,
                   raw_hash='sha256:'+'2'*64, stored_hash='sha256:'+'1'*64)
    source_hash = verifier.go_identity(dict(Digest=source['digest'], Format=source['format'], Location=source['location'],
                                           RawBytes=source['raw_bytes'], ChunkSize=source['chunk_size'],
                                           ChunkCount=source['chunk_count'], CreatedUTC=source['created_utc']))
    receipt_data = {'declaration-sha256': readonly.identity(prior), 'intent': 'single-copy-attempt-no-retry'}
    uid = 'abcdefab-1234-1234-1234-abcdefabcdef'
    native = copy.deepcopy(prior['copy'])
    native.update(version=2, max_put_requests=1, max_put_bytes=8766, max_get_bytes=18766,
                  resume=dict(source_manifest_sha256=source_hash, receipts_sha256=verifier.go_identity([segment]),
                              prefix=prefix, epoch=0, token=1, verified_segments=1, verified_stored_bytes=1234,
                              prior_attempt_sha256='sha256:'+readonly.identity(prior), prior_receipt_uid=uid,
                              prior_receipt_sha256=verifier.sha256(verifier.canonical(receipt_data))))
    cm = dict(apiVersion='v1', kind='ConfigMap', immutable=True, data=receipt_data,
              metadata=dict(namespace=prior['namespace'], name='fugue-input-copy-'+readonly.identity(prior)[:20]+'-receipt', uid=uid))
    partial = receipt(prior)
    partial.update(state='', verified_raw_bytes=0, segments=0, source_preserved=False, route_unchanged=False)
    partial['usage'].update(put_requests=1, get_requests=1, put_payload_bytes=1234, get_payload_bytes=1234, source_raw_bytes=65536)
    report = dict(declaration_sha256=readonly.identity(prior), job='fugue-input-copy-'+readonly.identity(prior)[:20],
                  job_uid=uid, status='completion-not-confirmed', cleanup_confirmed=True,
                  retirement_authorized=False, route_changes_authorized=False, native_result=partial,
                  execution=dict(image_id=prior['image'], restart_count=0, node_uid=prior['nodeUID'], pod_uid=uid))
    raw_report = json.dumps(report, separators=(',', ':')).encode()
    run_commit = 'f'*40
    member = 'input-copy-result-'+run_commit+'.json'
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, raw_report)
    raw_zip = buffer.getvalue()
    artifact = dict(id=123, name='fugue-input-copy-'+run_commit, expired=False, digest=verifier.sha256(raw_zip),
                    size_in_bytes=len(raw_zip), workflow_run=dict(id=456, head_sha=run_commit))
    expected = dict(repository='example/neutral', declaration_path='deploy/example/copy.json', artifact_id=123, run_id=456, run_attempt=1,
                    run_commit=run_commit, artifact_sha256=verifier.sha256(raw_zip),
                    report_member=member, report_sha256=verifier.sha256(raw_report))
    run = dict(id=456, run_attempt=1, head_sha=run_commit, head_branch='main', event='push',
               path='.github/workflows/ci.yml', status='completed', conclusion='failure', repository=dict(full_name='example/neutral'))
    proof = dict(prior_declaration=prior, artifact=expected,
                 cumulative_budget=dict(max_put_bytes=10000, max_get_bytes=20000, max_put_requests=2,
                                        max_get_requests=5, max_source_raw_bytes=131073))
    observed = dict(source=source, receipts=[segment], epoch=0, active_leases=0, fenced=False, retired=False, incomplete_receipts=0,
                    archive=dict(archive_format='compiler-input-archive-v1', state='uploading', verified=False, verified_digest='', backend_hash=native['backend_hash'],
                                 prefix=prefix, epoch=0, token=1, source_format=native['source_format'], raw_bytes=65537,
                                 segment_bytes=65536, segment_count=2))
    return dict(native=native, proof=proof, receipt=cm, artifact=artifact, run=run, raw_zip=raw_zip, observed=observed)


class ResumeProofTest(unittest.TestCase):
    def test_complete_proof_performs_no_external_actions_and_grants_no_authority(self):
        value = fixture()
        with patch.object(verifier.runner, 'execute', side_effect=AssertionError('no execution')):
            result = verifier.verify(**value)
        self.assertTrue(result['proof_verified'])
        self.assertIs(result['execution_authorized'], False)
        self.assertNotIn('prefix', json.dumps(result))
        self.assertNotIn('archive', result)

    def test_receipt_uid_immutability_intent_and_content_are_all_required(self):
        for section, key, changed in [('metadata', 'uid', '0'*36), ('metadata', 'name', 'another'),
                                      ('metadata', 'namespace', 'another'), ('data', 'intent', 'single-copy-attempt'),
                                      ('data', 'declaration-sha256', 'a'*64), ('metadata', 'deletionTimestamp', 'now')]:
            value = fixture(); value['receipt'][section][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)
        for key, changed in [('immutable', False), ('kind', 'Secret'), ('binaryData', {'unexpected': 'x'})]:
            value = fixture(); value['receipt'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)

    def test_artifact_requires_exact_run_digest_archive_and_result_bytes(self):
        for key, changed in [('id', 124), ('name', 'different'), ('expired', True), ('digest', 'sha256:'+'0'*64), ('size_in_bytes', 1)]:
            value = fixture(); value['artifact'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)
        for key, changed in [('id', 457), ('head_sha', '0'*40)]:
            value = fixture(); value['artifact']['workflow_run'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)
        for key in ('artifact_sha256', 'report_sha256'):
            value = fixture(); value['proof']['artifact'][key] = 'sha256:'+'0'*64
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)
        value = fixture(); value['raw_zip'] += b'changed'
        with self.assertRaises(ValueError): verifier.verify(**value)
        for key, changed in [('id', 457), ('head_branch', 'feature'), ('head_sha', '0'*40), ('event', 'pull_request'),
                             ('path', '.github/workflows/other.yml'), ('run_attempt', 2), ('status', 'in_progress'),
                             ('conclusion', 'success'), ('repository', dict(full_name='example/other'))]:
            value = fixture(); value['run'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)

    def test_unbounded_or_ambiguous_artifacts_fail_before_decoding(self):
        for payload, count in [(b'x'*(verifier.MAX_REPORT+1), 1), (b'{}', 17)]:
            value = fixture(); expected = value['proof']['artifact']; buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
                for index in range(count): archive.writestr(expected['report_member'] if index == 0 else str(index), payload)
            value['raw_zip'] = buffer.getvalue()
            digest = verifier.sha256(value['raw_zip'])
            expected['artifact_sha256'] = digest
            value['artifact'].update(digest=digest, size_in_bytes=len(value['raw_zip']))
            with self.subTest(count=count), self.assertRaises(ValueError): verifier.verify(**value)
        for body in (b'{"usage":{},"usage":{}}', b'{"usage":NaN}'):
            with self.assertRaises(ValueError): verifier.strict_json(body)

    def test_fresh_same_snapshot_metadata_and_contiguous_receipts_are_required(self):
        for key, changed in [('epoch', 1), ('active_leases', 1), ('fenced', True), ('retired', True), ('incomplete_receipts', 1), ('active_leases', False)]:
            value = fixture(); value['observed'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)
        for section, key, changed in [('source', 'created_utc', '2026-01-02T03:04:05.123457Z'), ('source', 'chunk_size', 32768),
                                      ('source', 'location', 'archive'), ('archive', 'token', 2), ('archive', 'token', True),
                                      ('archive', 'prefix', 'another'), ('archive', 'verified', True)]:
            value = fixture(); value['observed'][section][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)
        for key, changed in [('ordinal', 1), ('raw_bytes', 65535), ('stored_bytes', 1233), ('raw_hash', 'sha256:'+'0'*64), ('key', 'other/key')]:
            value = fixture(); value['observed']['receipts'][0][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)

    def test_fresh_declaration_cannot_replay_an_old_claim_or_skip_prefix_reads(self):
        for key, changed in [('max_put_requests', 2), ('max_get_requests', 3), ('max_put_requests', True), ('version', 1)]:
            value = fixture(); value['native'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)
        for key, changed in [('token', 0), ('token', 2), ('epoch', 1), ('verified_segments', 2),
                             ('prior_attempt_sha256', 'sha256:'+'0'*64), ('receipts_sha256', 'sha256:'+'0'*64)]:
            value = fixture(); value['native']['resume'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**value)

    def test_unconfirmed_usage_or_cleanup_never_shrinks_the_cumulative_budget(self):
        value = fixture()
        prior, resume = value['proof']['prior_declaration'], value['native']['resume']
        report = verifier.verify_artifact(value['artifact'], value['run'], value['raw_zip'], value['proof']['artifact'])
        for key, changed in [('cleanup_confirmed', False), ('status', 'running'), ('retirement_authorized', True), ('declaration_sha256', '0'*64)]:
            invalid = copy.deepcopy(report); invalid[key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify_partial_result(prior, invalid, resume)
        for key, changed in [('put_requests', 2), ('get_payload_bytes', 1233), ('source_raw_bytes', 0)]:
            invalid = copy.deepcopy(report); invalid['native_result']['usage'][key] = changed
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify_partial_result(prior, invalid, resume)
        for key in value['proof']['cumulative_budget']:
            invalid = copy.deepcopy(value); invalid['proof']['cumulative_budget'][key] -= 1
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**invalid)
        for key in ('max_put_bytes', 'max_get_bytes'):
            invalid = copy.deepcopy(value); invalid['proof']['cumulative_budget'][key] += 1
            with self.subTest(key=key), self.assertRaises(ValueError): verifier.verify(**invalid)

    def test_json_identity_matches_encoding_json_html_and_unicode_rules(self):
        self.assertEqual(verifier.go_identity({'text': '<>&\u2028\u2029'}),
                         verifier.sha256(b'{"text":"\\u003c\\u003e\\u0026\\u2028\\u2029"}'))
        # Also checked by TestCompilerInputArchiveResumeIdentityGolden in Go.
        resume = fixture()['native']['resume']
        self.assertEqual(resume['source_manifest_sha256'], 'sha256:8f028c5cdb3a1560b5288aee315c6d483742f6a89383a2e14f9a84b0d7522e70')
        self.assertEqual(resume['receipts_sha256'], 'sha256:1fe01b3610813f3ed16cb645db4819b0104d0c6eebb3c94f38182dfb2ff3d331')


if __name__ == '__main__': unittest.main()
