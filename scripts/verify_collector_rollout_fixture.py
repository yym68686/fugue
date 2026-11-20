"""Verification-branch fixture; no cluster credentials or production endpoints."""

import base64
from collections import Counter
import datetime as dt
import hashlib
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlsplit


PHASES = ('current', 'candidate', 'rollback')
REVISIONS = ('f10c2de45771f323f8360aa0c66b28038d3720fe',
             '69868474d2596067170616addf52b1ddf40d3089',
             '7df4bb0ce4791ed8c5f92e5eda4c154bb43ecf1c')
LEASE = '/apis/coordination.k8s.io/v1/namespaces/fixture/leases'
ANNOTATION = 'fugue.pro/log-checkpoint'
POD_UID = 'c7202ed6-7ace-4c20-a934-479b5781e170'
SOURCE = 'fixture/log-source/app/' + POD_UID + '/containerd://fixture'
SOURCE_HASH = base64.urlsafe_b64encode(hashlib.sha256(SOURCE.encode()).digest()).decode().rstrip('=')
LOGS = '/api/v1/namespaces/fixture/pods/log-source/log'
DIRECTORY = f'/api/v1/nodes/fixture-node/proxy/logs/pods/fixture_log-source_{POD_UID}/app/'
FRAMES = ('forward-fixture-frame', 'rollback-fixture-frame')


def require(ok, reason):
    if not ok:
        raise AssertionError(reason)


def serve(root):
    lock = threading.Lock()
    epoch = int(time.time()) - 60
    second = False
    lease = None
    writes = 0
    reads, blocked = Counter(), Counter()
    accepted = {p: Counter() for p in PHASES}
    releases = []
    pod = {'metadata': {'name': 'log-source', 'namespace': 'fixture', 'uid': POD_UID},
           'spec': {'nodeName': 'fixture-node', 'containers': [{'name': 'app', 'image': 'fixture'}]},
           'status': {'phase': 'Running', 'containerStatuses': [
               {'name': 'app', 'containerID': 'containerd://fixture', 'state': {'running': {}}}]}}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def reply(self, code, value=None, content_type='application/json'):
            payload = (value if isinstance(value, bytes) else json.dumps(value).encode()) if value is not None else b''
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def read_json(self):
            size = int(self.headers.get('Content-Length', '0'))
            require(0 < size <= 256 << 10, 'fixture request budget')
            data = self.rfile.read(size)
            if self.headers.get_content_type() == 'application/vnd.kubernetes.protobuf':
                require(self.server.server_port == 8443, 'unexpected protobuf sink payload')
                data = subprocess.run(['/fixture-lease-decode'], input=data, capture_output=True,
                                      check=True, timeout=3).stdout
            return json.loads(data)

        def status(self, code, reason):
            self.reply(code, {'kind': 'Status', 'apiVersion': 'v1', 'status': 'Failure', 'reason': reason, 'code': code})

        def kube(self):
            nonlocal lease, writes
            path = urlsplit(self.path).path
            phase = self.headers.get('Authorization', '').removeprefix('Bearer fixture-')
            if phase not in PHASES:
                self.status(401, 'Unauthorized')
                return
            with lock:
                if path in (LEASE, LEASE + '/cursor'):
                    if self.command == 'GET':
                        if lease is None:
                            self.status(404, 'NotFound')
                        else:
                            self.reply(200, lease)
                        return
                    value = self.read_json()
                    metadata = value['metadata']
                    require(metadata['name'] == 'cursor', 'unexpected Lease name')
                    if self.command == 'POST' and path == LEASE:
                        if lease is not None:
                            self.status(409, 'AlreadyExists')
                            return
                    elif self.command == 'PUT' and path == LEASE + '/cursor':
                        if lease is None or metadata.get('resourceVersion') != lease['metadata']['resourceVersion']:
                            self.status(409, 'Conflict')
                            return
                    else:
                        self.status(405, 'MethodNotAllowed')
                        return
                    require(value['spec']['leaseDurationSeconds'] == 60, 'production fence duration changed')
                    writes += 1
                    metadata['resourceVersion'] = str(writes)
                    lease = {**value, 'kind': 'Lease', 'apiVersion': 'coordination.k8s.io/v1'}
                    if not lease['spec'].get('holderIdentity'):
                        state = json.loads(metadata['annotations'][ANNOTATION])
                        releases.append({'phase': phase, 'state': state})
                    self.reply(201 if self.command == 'POST' else 200, lease)
                    return
                if self.command != 'GET':
                    self.status(405, 'MethodNotAllowed')
                    return
                if path == '/api/v1/pods':
                    reads[phase] += 1
                    self.reply(200, {'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {}, 'items': [pod]})
                    return
                if path == DIRECTORY:
                    reads[phase] += 1
                    self.reply(200, b'<a href="0.log">current</a>', 'text/html')
                    return
                if path in (LOGS, DIRECTORY + '0.log'):
                    reads[phase] += 1
                    marker = ' ' if path == LOGS else ' stdout F '
                    frames = FRAMES if second else FRAMES[:1]
                    lines = ''.join(dt.datetime.fromtimestamp(epoch + i, dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ') +
                                    marker + message + '\n' for i, message in enumerate(frames))
                    self.reply(200, lines.encode(), 'text/plain')
                    return
                self.status(404, 'NotFound')

        def sink(self):
            phase, _, suffix = self.path.lstrip('/').partition('/')
            require(phase in PHASES and suffix == 'loki/api/v1/push', 'unexpected synthetic sink endpoint')
            payload = self.read_json()
            messages = [json.loads(value[1])['message'] for stream in payload['streams'] for value in stream['values']]
            require(messages and all(m in FRAMES for m in messages), 'unexpected synthetic frame')
            held = FRAMES[0] if phase == 'current' else FRAMES[1] if phase == 'candidate' else None
            with lock:
                if held in messages:
                    blocked[phase] += 1
                else:
                    accepted[phase].update(messages)
            if held in messages:
                # Never acknowledge this batch; the harness must observe the
                # in-flight request before stopping this exact predecessor.
                self.connection.settimeout(30)
                try:
                    self.connection.recv(1)
                except (OSError, TimeoutError):
                    pass
                self.close_connection = True
                return
            self.reply(204)

        def do_GET(self):
            self.kube()

        def do_PUT(self):
            self.kube()

        def do_POST(self):
            self.kube() if self.server.server_port == 8443 else self.sink()

    kube = ThreadingHTTPServer(('127.0.0.1', 8443), Handler)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.load_cert_chain(root/'cert.pem', root/'key.pem')
    kube.socket = tls.wrap_socket(kube.socket, server_side=True)
    sink = ThreadingHTTPServer(('127.0.0.1', 8081), Handler)
    for server in (kube, sink):
        threading.Thread(target=server.serve_forever, daemon=True).start()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as ctl:
        ctl.bind(str(root/'control.sock'))
        os.chmod(root/'control.sock', 0o666)
        ctl.listen(4)
        while True:
            conn, _ = ctl.accept()
            with conn, lock:
                conn.settimeout(3)
                opcode = conn.recv(1)
                if opcode == b'2':
                    second = True
                require(opcode in (b'1', b'2'), 'invalid fixture control')
                data = {'epoch': epoch, 'reads': reads, 'blocked': blocked, 'accepted': accepted,
                        'releases': releases, 'lease': lease}
                conn.sendall(json.dumps(data).encode())


class PrivateSocket(HTTPConnection):
    def __init__(self, path):
        super().__init__('localhost', timeout=2)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(2)
        self.sock.connect(str(self.path))


def exercise(images):
    def docker(*args, timeout=20):
        return subprocess.run(['docker', *args], capture_output=True, text=True, timeout=timeout, check=True).stdout.strip()

    require(len(images) == 3, 'expected current, candidate, rollback')
    decoder = Path(os.environ['FIXTURE_LEASE_DECODE']).resolve(strict=True)
    for image, revision in zip(images, REVISIONS):
        value = json.loads(docker('image', 'inspect', image))[0]
        require(value['Os'] == 'linux' and value['Architecture'] == 'amd64', 'wrong OCI platform')
        require((value['Config']['Labels'] or {}).get('org.opencontainers.image.revision') == revision, 'wrong OCI revision')
        if '@sha256:' in image:
            require(image in value.get('RepoDigests', []), 'wrong OCI repository digest')
        print(f'verified image revision={revision} id={value["Id"]}', flush=True)

    with tempfile.TemporaryDirectory(prefix='collector-fixture-') as temp:
        root = Path(temp)
        root.chmod(0o755)
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                        '-keyout', str(root/'key.pem'), '-out', str(root/'cert.pem'), '-subj', '/CN=collector-fixture',
                        '-addext', 'subjectAltName=IP:127.0.0.1'], capture_output=True, check=True, timeout=10)
        containers = {}

        def wait(label, check, timeout=14):
            stop = time.monotonic() + timeout
            while time.monotonic() < stop:
                try:
                    if check():
                        return
                except (OSError, ConnectionError):
                    pass
                time.sleep(0.08)
            raise AssertionError('timed out: ' + label)

        def state(op=b'1'):
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as ctl:
                ctl.settimeout(2)
                ctl.connect(str(root/'control.sock'))
                ctl.sendall(op)
                with ctl.makefile('rb') as fp:
                    raw = fp.read((256 << 10) + 1)
                require(len(raw) <= 256 << 10, 'fixture state budget')
            return json.loads(raw)

        def launch(phase, image, index):
            service = root/('identity-' + phase)
            service.mkdir()
            (service/'token').write_text('fixture-' + phase)
            (service/'ca.crt').write_bytes((root/'cert.pem').read_bytes())
            observation = root/('private-' + phase)
            observation.mkdir(mode=0o1777)
            env = {'KUBERNETES_SERVICE_HOST': '127.0.0.1', 'KUBERNETES_SERVICE_PORT': '8443',
                   'FUGUE_TELEMETRY_AGENT_BIND_ADDR': f'127.0.0.1:{7800 + index}',
                   'FUGUE_TELEMETRY_LOG_CHECKPOINT_NAMESPACE': 'fixture', 'FUGUE_TELEMETRY_LOG_CHECKPOINT_NAME': 'cursor',
                   'FUGUE_OBSERVABILITY_ENABLED': 'true', 'FUGUE_OBSERVABILITY_KUBERNETES_LOGS_ENABLED': 'true',
                   'FUGUE_OBSERVABILITY_KUBERNETES_LOG_NAMESPACES': 'fixture',
                   'FUGUE_OBSERVABILITY_KUBERNETES_LOG_POLL_INTERVAL': '400ms',
                   'FUGUE_OBSERVABILITY_LOKI_URL': f'http://127.0.0.1:8081/{phase}',
                   'FUGUE_OBSERVABILITY_BATCH_SIZE': '1', 'FUGUE_OBSERVABILITY_EXPORT_TIMEOUT': '40s',
                   'FUGUE_OBSERVABILITY_RETRY_MAX_ATTEMPTS': '1'}
            opts = [arg for k, v in env.items() for arg in ('-e', f'{k}={v}')]
            containers[phase] = docker('run', '-d', '--network', 'container:' + containers['fixture'],
                                       '--memory', '512m', '--cpus', '1', '--stop-timeout', '20',
                                       '-v', f'{service}:/var/run/secrets/kubernetes.io/serviceaccount:ro',
                                       '-v', f'{observation}:/tmp', *opts, image)

        def standby(phase):
            def observed():
                if not (root/('private-' + phase)/'fugue-observation.sock').is_socket():
                    return False
                # Read with the original root-only socket permissions, inside
                # the same no-network fixture; do not chmod the socket.
                return docker('exec', containers['fixture'], 'python', '/fixture.py', '--checkpoint-state',
                              '/fixture/private-' + phase + '/fugue-observation.sock') == 'standby'
            wait(phase + ' standby', observed)
            s = state()
            require(not s['reads'].get(phase) and not s['accepted'][phase] and not s['blocked'].get(phase),
                    'standby accessed source or sink before ownership transfer')

        def stop(phase):
            docker('stop', '--time', '20', containers[phase], timeout=25)
            status = json.loads(docker('inspect', containers[phase]))[0]['State']
            require(status['ExitCode'] == 0 and not status['OOMKilled'], 'predecessor did not exit gracefully')

        def release(index, phase, unacknowledged_index):
            s = state()
            require(len(s['releases']) == index + 1, 'no unambiguous graceful checkpoint release')
            record = s['releases'][index]
            entry = record['state']['s'].get(SOURCE_HASH)
            require(record['phase'] == phase and record['state']['v'] == 1 and entry and not entry.get('c') and
                    entry['t'] <= (s['epoch'] + unacknowledged_index) * 10**9,
                    'release skipped an unacknowledged source position')

        def ack(phase, index):
            def saved():
                s = state()
                current = s['lease']
                pos = json.loads(current['metadata']['annotations'][ANNOTATION])['s'].get(SOURCE_HASH, {})
                return (s['accepted'][phase].get(FRAMES[index], 0) == 1 and bool(current['spec'].get('holderIdentity'))
                        and pos.get('t') == (s['epoch'] + index) * 10**9 and sum(pos.get('b', {}).values()) == 1)
            wait(phase + ' acknowledged and durably saved frame', saved)

        try:
            containers['fixture'] = docker('run', '-d', '--network', 'none', '--memory', '128m', '--cpus', '1',
                                           '-v', f'{root}:/fixture', '-v', f'{Path(__file__).resolve()}:/fixture.py:ro',
                                           '-v', f'{decoder}:/fixture-lease-decode:ro',
                                           'python:3.12-alpine', 'python', '-u', '/fixture.py', '--serve', '/fixture')
            wait('isolated fixture', lambda: bool(state()), timeout=12)
            launch('current', images[0], 0)
            wait('current in-flight frame', lambda: state()['blocked'].get('current', 0) == 1)
            launch('candidate', images[1], 1)
            standby('candidate')
            stop('current')
            release(0, 'current', 0)
            ack('candidate', 0)
            state(b'2')
            wait('candidate in-flight frame', lambda: state()['blocked'].get('candidate', 0) == 1)
            launch('rollback', images[2], 2)
            standby('rollback')
            stop('candidate')
            release(1, 'candidate', 1)
            ack('rollback', 1)
            stop('rollback')
            final = state()
            require(final['accepted'] == {'current': {}, 'candidate': {FRAMES[0]: 1}, 'rollback': {FRAMES[1]: 1}} and
                    [r['phase'] for r in final['releases']] == list(PHASES) and
                    not final['lease']['spec'].get('holderIdentity'), 'unexpected delivery or release transcript')
            print('Verified isolated OCI checkpoint: active-only source reads, forward and rollback replay, no duplicate fixture delivery')
        except (AssertionError, RuntimeError, OSError, subprocess.SubprocessError):
            for phase, container in containers.items():
                try:
                    print(f'fixture diagnostics: {phase}\n{docker("logs", "--tail", "25", container)}', file=sys.stderr)
                except subprocess.SubprocessError:
                    pass
            raise
        finally:
            for phase in reversed(containers):
                try:
                    docker('rm', '-f', containers[phase])
                except subprocess.SubprocessError:
                    print(f'isolated runner cleanup failed: {phase}', file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) == 3 and sys.argv[1] == '--serve':
        serve(Path(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == '--checkpoint-state':
        sock = PrivateSocket(sys.argv[2])
        try:
            sock.request('GET', '/v1/snapshots/log-sources')
            response = sock.getresponse()
            require(response.status == 200, 'private diagnostic response failed')
            print(json.loads(response.read(1 << 20))['checkpoint']['state'])
        finally:
            sock.close()
    else:
        exercise(sys.argv[1:])
