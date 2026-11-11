"""Bounded public transfer observer, executed outside the serving cluster.

Input contains signed, single-use permits, never a signing key. The API validates
the complete report and executor attestations independently before admission.
"""
import base64
import argparse
import concurrent.futures
import datetime
import hashlib
import http.client
import json
import ipaddress
import os
from pathlib import Path
import socket
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace('+00:00', 'Z')


def receive(response, permit, result):
    result['http_status'] = response.status
    headers = response.getheaders()
    values = lambda name: [v for k, v in headers if k.lower() == name.lower()]
    attested = values('X-Fugue-Network-Attestation')
    if response.status != 200 or values('Content-Length') != ['1048576'] or values('Content-Encoding') or len(attested) != 1 or len(attested[0]) > 16384:
        result['failure'] = 'response'
        return
    try:
        result['attestation'] = json.loads(base64.urlsafe_b64decode(attested[0] + '=' * (-len(attested[0]) % 4)))
    except (ValueError, UnicodeError):
        result['failure'] = 'integrity'
        return
    started = time.monotonic()
    body = response.read(1048576)
    result['body_seconds'] = time.monotonic() - started
    result['bytes_received'] = len(body)
    result['body_sha256'] = 'sha256:' + hashlib.sha256(body).hexdigest()
    if len(body) != 1048576:
        result['failure'] = 'body'
    elif result['body_sha256'] != permit['body_sha256']:
        result['failure'] = 'integrity'


def measure(permit, deadline):
    result = dict(attempt_id=permit['attempt_id'], started_at=stamp(), body_seconds=0,
                  body_sha256='', bytes_received=0, failure='', http_status=0)
    raw, conn, timer = None, None, None
    stage = 'connect'
    try:
        remaining = min(deadline - time.time(), 5)
        if remaining <= 0:
            raise TimeoutError('observer deadline expired')
        raw = socket.create_connection((permit['address'], 443), timeout=remaining)
        def expire():
            try:
                (conn.sock if conn and conn.sock else raw).shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        timer = threading.Timer(max(0, deadline - time.time()), expire)
        timer.start()
        stage = 'tls'
        context = ssl.create_default_context()
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        conn = http.client.HTTPSConnection(permit['hostname'], timeout=remaining, context=context)
        conn.sock = context.wrap_socket(raw, server_hostname=permit['hostname'])
        conn.sock.settimeout(max(0.001, deadline - time.time()))
        stage = 'response'
        token = base64.urlsafe_b64encode(json.dumps(permit, separators=(',', ':')).encode()).decode().rstrip('=')
        conn.request('GET', permit['path'], headers={'Host': permit['hostname'], 'X-Fugue-Network-Probe': token,
                     'X-Fugue-Route-Probe': '1', 'Accept-Encoding': 'identity', 'Connection': 'close'})
        with conn.getresponse() as response:
            stage = 'body'
            receive(response, permit, result)
    except (OSError, ValueError, http.client.HTTPException):
        result['failure'] = stage
    finally:
        if timer:
            timer.cancel()
        if conn:
            conn.close()
        if raw:
            raw.close()
        result['completed_at'] = stamp()
    return result


def measure_plan(plan):
    if plan['schema'] != 'fugue.client-probe-plan/v1' or len(plan['permits']) != 2:
        raise ValueError('bounded paired plan required')
    for permit in plan['permits']:
        if not ipaddress.ip_address(permit['address']).is_global or permit['body_bytes'] != 1048576 or not permit['path'].startswith('/') or '\r' in permit['path'] or '\n' in permit['path']:
            raise ValueError('bounded public transfer required')
    deadline = time.time() + 8
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda permit: measure(permit, deadline), plan['permits']))
    return dict(schema='fugue.client-probe-report/v1', plan=plan, outcomes=outcomes)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def api_post(base, key, path, payload=None):
    parsed = urllib.parse.urlsplit(base)
    if parsed.scheme != 'https' or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/'):
        raise ValueError('HTTPS API origin required')
    body = None if payload is None else json.dumps(payload, separators=(',', ':')).encode()
    request = urllib.request.Request(base.rstrip('/') + path, data=body, method='POST',
                                     headers={'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'})
    with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
        if response.status == 204:
            return None
        raw = response.read(131073)
        if len(raw) > 131072:
            raise ValueError('API response exceeds bound')
        return json.loads(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args()
    base, key = os.environ['FUGUE_API_URL'], os.environ['FUGUE_API_KEY']
    plan = api_post(base, key, '/v1/admin/edge-recovery/client-probes')
    if plan is None:
        print(json.dumps({'status': 'no_due_capacity_round'}))
        return
    report = measure_plan(plan)
    if args.report:
        args.report.write_text(json.dumps(report, separators=(',', ':')) + '\n')
    result = api_post(base, key, '/v1/admin/edge-recovery/client-probes/report', report)
    print(json.dumps({'round_id': plan['round_id'], 'result': result,
                      'outcomes': [{k: outcome[k] for k in ('attempt_id', 'http_status', 'failure', 'bytes_received', 'started_at', 'completed_at')} for outcome in report['outcomes']]}))


if __name__ == '__main__':
    try:
        main()
    except urllib.error.HTTPError as error:
        print('Recovery API rejected request: HTTP ' + str(error.code), file=sys.stderr)
        raise SystemExit(1)
