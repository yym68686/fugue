"""Trusted fetch and preflight helpers for an explicit v2 copy declaration.

These functions never infer permission from a prior attempt or a verified
proof. The ordinary copy executor validates the new declaration first. No R2
calls or database writes occur in this module.
"""
import json
import os
import select
import subprocess
import time

from scripts import compiler_input_copy_resume as proof
from scripts import compiler_input_readonly as readonly
from scripts.compiler_input_storage import command, get

require = readonly.require


def metadata_sql(c):
    # Embedded in the existing single bounded RR READ ONLY statement, never
    # fetched from a second snapshot or a source-body query.
    digest = c['copy']['digest']
    require(isinstance(digest, str) and proof.re.fullmatch(proof.SHA, digest), 'resume metadata digest is not exact')
    count = c['copy']['resume']['verified_segments']
    require(type(count) is int and 1 <= count <= 16384, 'resume metadata count is not bounded')
    return """(SELECT json_build_object(
'source',(SELECT json_build_object('digest',content_hash,'format',format,'location',storage_location,
 'raw_bytes',size_bytes,'chunk_size',chunk_size,'chunk_count',chunk_count,
 'created_utc',rtrim(rtrim(to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US'),'0'),'.')||'Z')
 FROM fugue_platform_input_manifests WHERE content_hash='"""+digest+"""'),
'archive',(SELECT json_build_object('archive_format',archive_format,'state',state,'verified',verified_at IS NOT NULL,'verified_digest',verified_digest,
 'backend_hash',backend_hash,'prefix',object_prefix,'epoch',source_epoch,'token',fencing_token,'source_format',source_format,
 'raw_bytes',raw_bytes,'segment_bytes',segment_bytes,'segment_count',segment_count)
 FROM fugue_platform_input_archives WHERE content_hash='"""+digest+"""'),
'receipts',(SELECT COALESCE(json_agg(json_build_object('ordinal',ordinal,'key',object_key,'raw_bytes',raw_bytes,
 'stored_bytes',stored_bytes,'raw_hash',segment_hash,'stored_hash',stored_hash) ORDER BY ordinal),'[]'::json)
 FROM (SELECT ordinal,object_key,raw_bytes,stored_bytes,segment_hash,stored_hash FROM fugue_platform_input_archive_segments
 WHERE content_hash='"""+digest+"""' AND uploaded_at IS NOT NULL AND verified_at IS NOT NULL ORDER BY ordinal LIMIT """+str(count+1)+""") s),
'incomplete_receipts',(SELECT count(*) FROM fugue_platform_input_archive_segments WHERE content_hash='"""+digest+"""' AND (uploaded_at IS NULL OR verified_at IS NULL)),
'epoch',(SELECT epoch FROM fugue_platform_input_archive_control WHERE singleton),
'fenced',(SELECT writes_fenced FROM fugue_platform_input_archive_control WHERE singleton),
'retired',(SELECT retirement_started_at IS NOT NULL FROM fugue_platform_input_archive_control WHERE singleton),
'active_leases',(SELECT count(*) FROM fugue_platform_input_archive_leases WHERE expires_at>transaction_timestamp())))"""


def github_bytes(path, limit):
    """One CLI request, bounded in memory and time; no credential/error output."""
    require(type(limit) is int and 0 < limit <= proof.MAX_ZIP, 'resume GitHub response bound is invalid')
    process = subprocess.Popen(['gh', 'api', '--hostname', 'github.com', path],
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    data = bytearray()
    deadline = time.monotonic()+20
    try:
        while True:
            remaining = deadline-time.monotonic()
            require(remaining > 0, 'resume GitHub request timed out')
            readable, _, _ = select.select([process.stdout], [], [], remaining)
            require(bool(readable), 'resume GitHub request timed out')
            chunk = os.read(process.stdout.fileno(), min(64 << 10, limit+1-len(data)))
            if not chunk:
                break
            data.extend(chunk)
            require(len(data) <= limit, 'resume GitHub response exceeds budget')
        require(process.wait(timeout=max(0.01, deadline-time.monotonic())) == 0, 'resume GitHub request failed')
        return bytes(data)
    finally:
        process.stdout.close()
        if process.poll() is None:
            # This is only the local gh client, never a workload/DB process.
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=3)


def fetch_proof(c, observed):
    require(c['version'] == 2 and c['executionAuthorized'] is True, 'resume new attempt is not authorized')
    expected = c['resumeProof']['artifact']
    require(expected['repository'] == os.environ.get('GITHUB_REPOSITORY')
            and bool(os.environ.get('GH_TOKEN')), 'resume trusted GitHub repository or token is unavailable')
    prior = c['resumeProof']['prior_declaration']
    original = command(['git', 'show', expected['run_commit']+':'+expected['declaration_path']])
    require(len(original.encode()) <= 64 << 10 and proof.strict_json(original) == prior,
            'resume prior declaration does not match its trusted main commit')
    name = 'fugue-input-copy-'+readonly.identity(prior)[:20]+'-receipt'
    receipt = get(prior['namespace'], 'configmap', name)
    prefix = 'repos/'+expected['repository']+'/actions/'
    run = proof.strict_json(github_bytes(prefix+'runs/'+str(expected['run_id'])+'/attempts/'+str(expected['run_attempt']), 128 << 10))
    artifact = proof.strict_json(github_bytes(prefix+'artifacts/'+str(expected['artifact_id']), 128 << 10))
    # Check the metadata's advertised size before permitting even the bounded
    # ZIP request. The verifier also checks exact bytes/hash and run identity.
    require(type(artifact.get('size_in_bytes')) is int and 0 < artifact['size_in_bytes'] <= proof.MAX_ZIP,
            'resume artifact advertised size exceeds budget')
    raw_zip = github_bytes(prefix+'artifacts/'+str(expected['artifact_id'])+'/zip', proof.MAX_ZIP)
    return proof.verify(c['copy'], c['resumeProof'], receipt, artifact, run, raw_zip, observed['resume'])


def cumulative_usage(c, prior_usage, native_usage):
    result = {}
    for key, limit in proof.USAGE_TO_LIMIT.items():
        require(type(prior_usage.get(key)) is int and type(native_usage.get(key)) is int
                and prior_usage[key] >= 0 and native_usage[key] >= 0,
                'resume cumulative usage is absent')
        value = prior_usage[key]+native_usage[key]
        require(0 <= value <= c['resumeProof']['cumulative_budget'][limit], 'resume cumulative usage exceeds the signed ceiling')
        result[key] = value
    return result


def terminal_receipt(c, report):
    # Called only after exact execution/session cleanup. Retain the original
    # one-shot reservation, and make the new terminal receipt append-only too.
    require(c['version'] == 2 and c['terminalReceiptRequired'] is True
            and report.get('job_uid') and report.get('cleanup_confirmed') is True
            and type(report.get('native_result')) is dict, 'resume terminal receipt prerequisites are absent')
    fields = ('declaration_sha256', 'job', 'job_uid', 'status', 'cleanup_confirmed', 'retirement_authorized',
              'route_changes_authorized', 'execution', 'native_result', 'cumulative_usage', 'finished_at')
    value = {key: report[key] for key in fields}
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'))
    require(len(encoded.encode()) <= 32 << 10, 'resume terminal receipt exceeds budget')
    data = {'declaration-sha256': readonly.identity(c), 'result.json': encoded}
    name = report['job']+'-result'
    created = readonly.create({'apiVersion': 'v1', 'kind': 'ConfigMap', 'immutable': True,
                              'metadata': {'namespace': c['namespace'], 'name': name}, 'data': data})
    require(proof.re.fullmatch(proof.UID, created.get('metadata', {}).get('uid', '')), 'resume terminal receipt UID is absent')
    observed = get(c['namespace'], 'configmap', name)
    require(observed.get('metadata', {}).get('uid') == created['metadata']['uid'] and observed.get('immutable') is True
            and observed.get('data') == data, 'resume terminal receipt readback differs')
    return {'uid': created['metadata']['uid'], 'data_sha256': proof.sha256(proof.canonical(data)), 'confirmed': True}
