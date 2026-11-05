"""Pure verification for a separately authorized copy-resume attempt.

No Kubernetes, GitHub, database or object calls occur here. A trusted runner
must fetch the exact immutable receipt, GitHub artifact and one fresh metadata
snapshot before calling this verifier. A successful proof grants no execution
authority; a v2 runner must separately validate a new authorized declaration.
"""
import copy
import hashlib
import io
import json
import posixpath
import re
import zipfile

from scripts import compiler_input_copy as runner
from scripts import compiler_input_readonly as readonly

require = readonly.require
SHA = r'sha256:[a-f0-9]{64}'
UID = r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'
MAX_ZIP = 1 << 20
MAX_REPORT = 256 << 10
MAX_METADATA = 1 << 20
USAGE_TO_LIMIT = {'put_payload_bytes': 'max_put_bytes', 'get_payload_bytes': 'max_get_bytes',
                  'put_requests': 'max_put_requests', 'get_requests': 'max_get_requests',
                  'source_raw_bytes': 'max_source_raw_bytes'}


def sha256(body):
    return 'sha256:' + hashlib.sha256(body).hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def go_identity(value):
    # Source identity and Segment structs have declared field order in Go.
    # Match encoding/json's UTF-8 and HTML escaping, without sorting fields.
    body = json.dumps(value, separators=(',', ':'), ensure_ascii=False)
    for text, escaped in [('&', r'\u0026'), ('<', r'\u003c'), ('>', r'\u003e'),
                          ('\u2028', r'\u2028'), ('\u2029', r'\u2029')]:
        body = body.replace(text, escaped)
    return sha256(body.encode())


def strict_json(body):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'resume evidence has duplicate JSON fields')
            result[key] = value
        return result
    try:
        return json.loads(body, object_pairs_hook=pairs,
                          parse_constant=lambda _: require(False, 'resume evidence has nonfinite JSON'))
    except (ValueError, UnicodeError):
        raise readonly.Refusal('resume evidence JSON is invalid') from None


def validate_contract(native, prior):
    require(type(native) is dict and type(native.get('version')) is int and native['version'] == 2,
            'resume requires an explicit native v2 contract')
    require(set(native) == set(prior['copy']) | {'resume'}, 'resume native fields differ')
    resume = native['resume']
    fields = {'source_manifest_sha256', 'receipts_sha256', 'prefix', 'epoch', 'token',
              'verified_segments', 'verified_stored_bytes', 'prior_attempt_sha256',
              'prior_receipt_uid', 'prior_receipt_sha256'}
    require(type(resume) is dict and set(resume) == fields, 'resume identity fields differ')
    for key in ('source_manifest_sha256', 'receipts_sha256', 'prior_attempt_sha256', 'prior_receipt_sha256'):
        require(isinstance(resume[key], str) and re.fullmatch(SHA, resume[key]), 'resume identity is not exact')
    require(isinstance(resume['prior_receipt_uid'], str) and re.fullmatch(UID, resume['prior_receipt_uid']),
            'resume immutable receipt UID is invalid')
    for key, low, high in [('epoch', 0, (1 << 63)-1), ('token', 1, (1 << 63)-2),
                           ('verified_segments', 1, 16384), ('verified_stored_bytes', 1, 2 << 30)]:
        require(type(resume[key]) is int and low <= resume[key] <= high, 'resume prefix bound is invalid')
    prefix = resume['prefix']
    require(isinstance(prefix, str) and 0 < len(prefix) <= 512 and posixpath.normpath(prefix) == prefix
            and not prefix.startswith(('/', '../')) and prefix not in ('.', '..')
            and not any(x in prefix for x in ('\\', '\0', '\r', '\n')), 'resume object prefix is invalid')
    for key in ('digest', 'source_format', 'raw_bytes', 'segment_bytes', 'backend_hash'):
        require(native[key] == prior['copy'][key], 'resume source or representation differs from prior attempt')
    segments = (native['raw_bytes']+native['segment_bytes']-1)//native['segment_bytes']
    require(type(native['max_put_requests']) is int and type(native['max_get_requests']) is int
            and resume['verified_segments'] < segments
            and native['max_put_requests'] == segments-resume['verified_segments']
            and native['max_get_requests'] == 2*segments, 'resume must reuse the prefix and retain both complete GET passes')
    # Apply the same strict common native bounds as the connected v1 runner.
    common = copy.deepcopy(prior)
    common['copy'] = {k: v for k, v in native.items() if k != 'resume'}
    common['copy'].update(version=1, max_put_requests=segments)
    runner.validate(common)
    require(resume['verified_stored_bytes'] <= resume['verified_segments']*(native['segment_bytes']+(1 << 20)),
            'resume stored prefix exceeds its bound')
    require(resume['prior_attempt_sha256'] == 'sha256:'+readonly.identity(prior), 'resume prior declaration differs')
    return resume


def verify_immutable_receipt(prior, resume, receipt):
    name = 'fugue-input-copy-'+readonly.identity(prior)[:20]+'-receipt'
    require(type(receipt) is dict and receipt.get('apiVersion') == 'v1' and receipt.get('kind') == 'ConfigMap'
            and receipt.get('immutable') is True and not receipt.get('binaryData'), 'resume prior receipt is not immutable')
    metadata = receipt.get('metadata', {})
    require(metadata.get('namespace') == prior['namespace'] and metadata.get('name') == name
            and metadata.get('uid') == resume['prior_receipt_uid'] and not metadata.get('deletionTimestamp'),
            'resume prior receipt identity changed')
    expected = {'declaration-sha256': readonly.identity(prior), 'intent': 'single-copy-attempt-no-retry'}
    require(receipt.get('data') == expected and sha256(canonical(expected)) == resume['prior_receipt_sha256'],
            'resume prior immutable receipt content differs')


def validate_artifact_contract(expected):
    fields = {'repository', 'declaration_path', 'artifact_id', 'run_id', 'run_attempt', 'run_commit', 'artifact_sha256', 'report_member', 'report_sha256'}
    require(type(expected) is dict and set(expected) == fields, 'resume artifact contract fields differ')
    require(all(type(expected[k]) is int and expected[k] > 0 for k in ('artifact_id', 'run_id', 'run_attempt'))
            and isinstance(expected['repository'], str) and re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', expected['repository'])
            and isinstance(expected['run_commit'], str) and re.fullmatch('[a-f0-9]{40}', expected['run_commit'])
            and all(isinstance(expected[k], str) and re.fullmatch(SHA, expected[k]) for k in ('artifact_sha256', 'report_sha256')),
            'resume artifact identity is invalid')
    source = expected['declaration_path']
    require(isinstance(source, str) and re.fullmatch(r'[A-Za-z0-9_./-]{1,512}\.json', source)
            and not source.startswith(('/', '../')) and posixpath.normpath(source) == source,
            'resume prior declaration source is invalid')
    name = 'fugue-input-copy-'+expected['run_commit']
    member = 'input-copy-result-'+expected['run_commit']+'.json'
    require(expected['report_member'] == member, 'resume artifact result member differs')
    return name, member


def verify_artifact(artifact, run, raw_zip, expected):
    name, member = validate_artifact_contract(expected)
    require(type(run) is dict and type(run.get('id')) is int and run['id'] == expected['run_id']
            and type(run.get('run_attempt')) is int and run['run_attempt'] == expected['run_attempt']
            and run.get('head_sha') == expected['run_commit'] and run.get('head_branch') == 'main'
            and run.get('event') == 'push' and run.get('path') == '.github/workflows/ci.yml'
            and run.get('status') == 'completed' and run.get('conclusion') == 'failure'
            and run.get('repository', {}).get('full_name') == expected['repository'],
            'resume artifact is not from the exact terminal main workflow attempt')
    require(type(artifact) is dict and artifact.get('id') == expected['artifact_id']
            and artifact.get('name') == name and artifact.get('expired') is False
            and artifact.get('digest') == expected['artifact_sha256']
            and artifact.get('workflow_run', {}).get('id') == expected['run_id']
            and artifact.get('workflow_run', {}).get('head_sha') == expected['run_commit'],
            'resume trusted artifact identity differs')
    require(type(raw_zip) is bytes and 0 < len(raw_zip) <= MAX_ZIP
            and type(artifact.get('size_in_bytes')) is int and artifact['size_in_bytes'] == len(raw_zip)
            and sha256(raw_zip) == expected['artifact_sha256'], 'resume artifact bytes differ or exceed budget')
    try:
        with zipfile.ZipFile(io.BytesIO(raw_zip)) as archive:
            entries = archive.infolist()
            names = [x.filename for x in entries]
            require(1 <= len(entries) <= 16 and len(names) == len(set(names))
                    and sum(x.file_size for x in entries) <= 2 << 20, 'resume artifact inventory exceeds budget')
            require(member in names, 'resume artifact result is absent')
            info = archive.getinfo(member)
            require(0 < info.file_size <= MAX_REPORT and not info.is_dir()
                    and not info.flag_bits & 1 and (info.external_attr >> 16) & 0o170000 != 0o120000,
                    'resume artifact result is not a bounded regular entry')
            with archive.open(info) as body:
                result = body.read(MAX_REPORT+1)
            require(len(result) == info.file_size and sha256(result) == expected['report_sha256'],
                    'resume artifact result bytes differ')
    except (zipfile.BadZipFile, RuntimeError, OSError):
        raise readonly.Refusal('resume artifact cannot be verified') from None
    return strict_json(result)


def verify_partial_result(prior, report, resume):
    require(type(report) is dict and report.get('declaration_sha256') == readonly.identity(prior)
            and report.get('job') == 'fugue-input-copy-'+readonly.identity(prior)[:20]
            and report.get('status') == 'completion-not-confirmed' and report.get('cleanup_confirmed') is True
            and report.get('retirement_authorized') is False and report.get('route_changes_authorized') is False,
            'resume prior attempt has no confirmed terminal cleanup')
    execution = report.get('execution', {})
    require(execution.get('image_id', '').removeprefix('docker-pullable://') == prior['image']
            and type(execution.get('restart_count')) is int and execution['restart_count'] == 0
            and execution.get('node_uid') == prior['nodeUID']
            and isinstance(execution.get('pod_uid'), str) and re.fullmatch(UID, execution['pod_uid'])
            and isinstance(report.get('job_uid'), str) and re.fullmatch(UID, report['job_uid']),
            'resume prior execution identity is not proven')
    native = report.get('native_result')
    require(type(native) is dict, 'resume prior native usage is absent')
    result = runner.result_contract(json.dumps(native, separators=(',', ':')), prior, success=False)
    require(result['state'] == '' and result['verified_raw_bytes'] == 0, 'resume prior attempt already completed')
    usage = result['usage']
    # No ambiguous, uncheckpointed PUT may be inferred away. Such an attempt
    # needs a separate reviewed proof, rather than silently reducing its usage.
    require(usage['put_requests'] == usage['get_requests'] == resume['verified_segments']
            and usage['put_payload_bytes'] == usage['get_payload_bytes'] == resume['verified_stored_bytes']
            and usage['source_raw_bytes'] >= resume['verified_segments']*prior['copy']['segment_bytes'],
            'resume prior usage differs from the fully checkpointed prefix')
    return usage


def verify_metadata(native, observed):
    require(type(observed) is dict and len(canonical(observed)) <= MAX_METADATA
            and set(observed) == {'source', 'archive', 'receipts', 'epoch', 'active_leases', 'fenced', 'retired', 'incomplete_receipts'},
            'resume fresh metadata is absent or exceeds budget')
    resume = native['resume']
    require(type(observed['epoch']) is int and observed['epoch'] == resume['epoch']
            and type(observed['active_leases']) is int and observed['active_leases'] == 0
            and type(observed['incomplete_receipts']) is int and observed['incomplete_receipts'] == 0
            and observed['fenced'] is False and observed['retired'] is False, 'resume maintenance state changed')
    source = observed['source']
    require(type(source) is dict and set(source) == {'digest', 'format', 'location', 'raw_bytes', 'chunk_size', 'chunk_count', 'created_utc'}
            and source['digest'] == native['digest'] and source['format'] == native['source_format']
            and source['location'] == 'legacy' and type(source['raw_bytes']) is int and source['raw_bytes'] == native['raw_bytes']
            and all(type(source[k]) is int and source[k] > 0 for k in ('chunk_size', 'chunk_count'))
            and (source['raw_bytes']+source['chunk_size']-1)//source['chunk_size'] == source['chunk_count']
            and isinstance(source['created_utc'], str)
            and re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{0,8}[1-9])?Z', source['created_utc']),
            'resume source metadata differs')
    identity = dict(Digest=source['digest'], Format=source['format'], Location=source['location'], RawBytes=source['raw_bytes'],
                    ChunkSize=source['chunk_size'], ChunkCount=source['chunk_count'], CreatedUTC=source['created_utc'])
    require(go_identity(identity) == resume['source_manifest_sha256'], 'resume complete source identity changed')
    expected_archive = dict(archive_format='compiler-input-archive-v1', state='uploading', verified=False, verified_digest='', backend_hash=native['backend_hash'],
                            prefix=resume['prefix'], epoch=resume['epoch'], token=resume['token'],
                            source_format=native['source_format'], raw_bytes=native['raw_bytes'],
                            segment_bytes=native['segment_bytes'], segment_count=native['max_get_requests']//2)
    archive = observed['archive']
    require(type(archive) is dict and archive == expected_archive and archive['verified'] is False
            and all(type(archive[k]) is int for k in ('epoch', 'token', 'raw_bytes', 'segment_bytes', 'segment_count')),
            'resume archive representation or token changed')
    receipts = observed['receipts']
    require(type(receipts) is list and len(receipts) == resume['verified_segments'], 'resume receipt prefix count changed')
    ordered = []
    for ordinal, receipt in enumerate(receipts):
        fields = ('ordinal', 'key', 'raw_bytes', 'stored_bytes', 'raw_hash', 'stored_hash')
        require(type(receipt) is dict and set(receipt) == set(fields)
                and type(receipt['ordinal']) is int and receipt['ordinal'] == ordinal
                and type(receipt['raw_bytes']) is int and receipt['raw_bytes'] == native['segment_bytes']
                and type(receipt['stored_bytes']) is int and 1 <= receipt['stored_bytes'] <= receipt['raw_bytes']+(1 << 20)
                and all(isinstance(receipt[k], str) and re.fullmatch(SHA, receipt[k]) for k in ('raw_hash', 'stored_hash')),
                'resume receipt prefix is not contiguous or valid')
        require(receipt['key'] == resume['prefix']+'/'+str(ordinal).zfill(5)+'-'+receipt['stored_hash'][7:]+'.gz',
                'resume immutable segment key changed')
        ordered.append({k: receipt[k] for k in fields})
    require(sum(x['stored_bytes'] for x in ordered) == resume['verified_stored_bytes']
            and go_identity(ordered) == resume['receipts_sha256'], 'resume ordered receipts changed')


def verify_budget(native, prior, usage, cumulative):
    require(type(cumulative) is dict and set(cumulative) == set(USAGE_TO_LIMIT.values())
            and all(type(x) is int and x > 0 for x in cumulative.values()), 'resume cumulative budget is invalid')
    require(cumulative['max_put_bytes'] <= prior['copy']['max_put_bytes']
            and cumulative['max_get_bytes'] <= prior['copy']['max_get_bytes']
            and cumulative['max_put_requests'] <= 16384 and cumulative['max_get_requests'] <= 65536
            and cumulative['max_source_raw_bytes'] <= 2*native['raw_bytes'], 'resume cumulative ceiling exceeds the reviewed envelope')
    future = {k: native.get(k, native['raw_bytes']) for k in cumulative}
    for key, limit in USAGE_TO_LIMIT.items():
        require(usage[key]+future[limit] <= cumulative[limit], 'resume prior plus future budget exceeds cumulative ceiling')
    # PUT and GET ceilings are independent stop limits, not a promise that
    # both can be exhausted while completing. At least the existing prefix
    # must fit twice; the object wrapper enforces every future actual byte.
    require(native['max_get_bytes'] >= 2*native['resume']['verified_stored_bytes'],
            'resume GET budget cannot cover both prefix passes')


def validate_proof(native, proof):
    require(type(proof) is dict and set(proof) == {'prior_declaration', 'artifact', 'cumulative_budget'},
            'resume proof contract fields differ')
    prior = runner.validate(proof['prior_declaration'])
    require(prior['version'] == 1, 'resume proof must bind the original single attempt')
    resume = validate_contract(native, prior)
    validate_artifact_contract(proof['artifact'])
    # Before any trusted fetch, reject envelopes that cannot accommodate even
    # this attempt. The verified prior actual usage is added after the fetch.
    verify_budget(native, prior, {key: 0 for key in USAGE_TO_LIMIT}, proof['cumulative_budget'])
    return resume


def verify(native, proof, receipt, artifact, run, raw_zip, observed):
    """Verify only trusted inputs already fetched by a separately gated runner."""
    resume = validate_proof(native, proof)
    prior = proof['prior_declaration']
    verify_immutable_receipt(prior, resume, receipt)
    report = verify_artifact(artifact, run, raw_zip, proof['artifact'])
    usage = verify_partial_result(prior, report, resume)
    verify_metadata(native, observed)
    verify_budget(native, prior, usage, proof['cumulative_budget'])
    return {'execution_authorized': False, 'proof_verified': True,
            'prior_attempt_sha256': resume['prior_attempt_sha256'],
            'prior_report_sha256': proof['artifact']['report_sha256'],
            'source_manifest_sha256': resume['source_manifest_sha256'], 'receipts_sha256': resume['receipts_sha256'],
            'prior_usage': usage, 'cumulative_budget': proof['cumulative_budget']}
