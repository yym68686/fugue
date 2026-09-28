import base64
import datetime
import http.server
import json
from pathlib import Path
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

from scripts import front_connection_witness as witness


class HeldFrontConnectionTests(unittest.TestCase):
    def test_same_verified_tls_socket_survives_repeated_proofs_and_never_reconnects(self):
        with tempfile.TemporaryDirectory() as directory:
            key, cert = Path(directory)/"key.pem", Path(directory)/"cert.pem"
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-keyout", str(key), "-out", str(cert), "-subj", "/CN=api.example.test", "-addext", "subjectAltName=DNS:api.example.test"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            seen = []
            class Handler(http.server.BaseHTTPRequestHandler):
                protocol_version = "HTTP/1.1"
                def log_message(self, *_): pass
                def do_HEAD(self):
                    seen.append(self.client_address)
                    self.send_response(204)
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("X-Fugue-Route-Probe-Nonce", self.headers["X-Fugue-Route-Probe-Nonce"])
                    self.send_header("X-Fugue-Route-Proof", "sha256:" + "a" * 64)
                    self.send_header("X-Fugue-Route-Valid-Until", (datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=2)).isoformat())
                    self.send_header("X-Fugue-Route-Bundle-Version", "bundle-one")
                    self.send_header("X-Fugue-Route-Edge-Id", "node-a")
                    self.send_header("X-Fugue-Route-Edge-Group", "cell-a")
                    binding = {"release_id": "release-one", "fencing_token": 2, "release_channel": "full"}
                    self.send_header("X-Fugue-Traffic-Release", base64.urlsafe_b64encode(json.dumps(binding).encode()).decode().rstrip("="))
                    if len(seen) == 3: self.send_header("Connection", "close")
                    self.end_headers()
            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            trusted = ssl.create_default_context(cafile=str(cert))
            try:
                with patch.object(witness.ssl, "create_default_context", return_value=trusted):
                    held = witness.HeldTLS("127.0.0.1", "api.example.test", server.server_port)
                try:
                    first = held.proof("/")
                    second = held.proof("/")
                    self.assertEqual(first, second)
                    self.assertEqual(seen[0], seen[1])
                    self.assertEqual(held.requests, 2)
                    with self.assertRaises(ValueError): held.proof("/")
                    with self.assertRaises((OSError, http.client.HTTPException, ValueError)): held.proof("/")
                    self.assertEqual(len(seen), 3)
                    self.assertEqual(len(set(seen)), 1)
                finally:
                    held.close()
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_connection_fact_is_bound_to_exact_client_tuple_and_pod(self):
        from types import SimpleNamespace
        held = SimpleNamespace(client_address="192.0.2.5", client_port=43123)
        entry = {"id": "connection-one", "downstream_remote": "192.0.2.5:43123", "protocol": "https", "started_at": "2026-01-01T00:00:00Z", "slot": "a", "target": "192.0.2.9:18443"}
        pod = {"metadata": {"name": "front", "uid": "front-uid"}}
        for bad in [None, "missing", "port", "address", "duplicate", "truncated"]:
            row = dict(entry)
            if bad == "port": row["downstream_remote"] = "192.0.2.5:43124"
            if bad == "address": row["downstream_remote"] = "192.0.2.6:43123"
            active = [] if bad == "missing" else [row, row] if bad == "duplicate" else [row]
            value = {"count": len(active)+1 if bad == "truncated" else len(active), "active": active}
            with self.subTest(bad=bad), patch.object(witness.front, "read", return_value=value):
                if bad:
                    with self.assertRaises(ValueError): witness.fact({"namespace": "test"}, pod, held)
                else:
                    self.assertEqual(witness.fact({"namespace": "test"}, pod, held)["pod_uid"], "front-uid")


if __name__ == "__main__": unittest.main()
