import base64
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('capacity_observer', Path(__file__).with_name('observe_capacity_recovery.py'))
observer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(observer)


class Response(io.BytesIO):
    status = 200

    def __init__(self, body, headers=None):
        super().__init__(body)
        self.headers = headers if headers is not None else [('Content-Length', '1048576'), ('X-Fugue-Network-Attestation', base64.urlsafe_b64encode(b'{}').decode())]

    def getheaders(self):
        return self.headers


class CapacityObserverTest(unittest.TestCase):
    def test_exact_payload_and_full_failure_evidence(self):
        body = b'a' * 1048576
        permit = {'body_sha256': 'sha256:' + hashlib.sha256(body).hexdigest()}
        for scenario in ['valid', 'partial', 'changed', 'compressed', 'duplicate', 'rejected']:
            response = Response(body)
            if scenario == 'partial': response = Response(body[:-1])
            if scenario == 'changed': response = Response(b'b' * 1048576)
            if scenario == 'compressed': response.headers.append(('Content-Encoding', 'gzip'))
            if scenario == 'duplicate': response.headers.append(response.headers[-1])
            if scenario == 'rejected': response.status = 503
            result = dict(failure='', bytes_received=0)
            observer.receive(response, permit, result)
            self.assertEqual(result['failure'], {'valid': '', 'partial': 'body', 'changed': 'integrity'}.get(scenario, 'response'))
            if scenario == 'valid':
                self.assertEqual(result['bytes_received'], len(body))
                self.assertEqual(result['body_sha256'], permit['body_sha256'])


if __name__ == '__main__':
    unittest.main()
