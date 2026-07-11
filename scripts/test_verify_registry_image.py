#!/usr/bin/env python3

import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import threading
import unittest
import urllib.parse


REPO_ROOT = Path(__file__).resolve().parent.parent
VERIFIER = REPO_ROOT / "scripts" / "verify_registry_image.py"


def encoded(document):
    return json.dumps(document, separators=(",", ":"), sort_keys=True).encode()


def digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


class RegistryFixture:
    def __init__(self):
        config_body = encoded(
            {
                "architecture": "amd64",
                "config": {},
                "os": "linux",
                "rootfs": {"diff_ids": [], "type": "layers"},
            }
        )
        layer_body = b"synthetic-layer-content"
        self.config_digest = digest(config_body)
        self.layer_digest = digest(layer_body)
        manifest_body = encoded(
            {
                "config": {
                    "digest": self.config_digest,
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "size": len(config_body),
                },
                "layers": [
                    {
                        "digest": self.layer_digest,
                        "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                        "size": len(layer_body),
                    }
                ],
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "schemaVersion": 2,
            }
        )
        self.manifest_digest = digest(manifest_body)
        index_body = encoded(
            {
                "manifests": [
                    {
                        "digest": self.manifest_digest,
                        "mediaType": "application/vnd.oci.image.manifest.v1+json",
                        "platform": {"architecture": "amd64", "os": "linux"},
                        "size": len(manifest_body),
                    }
                ],
                "mediaType": "application/vnd.oci.image.index.v1+json",
                "schemaVersion": 2,
            }
        )
        self.index_digest = digest(index_body)
        self.manifests = {
            self.index_digest: index_body,
            self.manifest_digest: manifest_body,
        }
        self.blobs = {
            self.config_digest: config_body,
            self.layer_digest: layer_body,
        }
        self.requests = []
        self.token_requests = 0
        self.lock = threading.Lock()

    def record(self, method, path):
        with self.lock:
            self.requests.append((method, path))


def fixture_handler(fixture):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format, *_args):
            return

        def send_bytes(self, status, body=b"", content_type="application/octet-stream", digest_header=""):
            self.send_response(status)
            self.send_header("Connection", "close")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", content_type)
            if digest_header:
                self.send_header("Docker-Content-Digest", digest_header)
            self.end_headers()
            if self.command != "HEAD" and body:
                self.wfile.write(body)

        def require_auth(self):
            if self.headers.get("Authorization") == "Bearer fixture-token":
                return True
            host, port = self.server.server_address
            realm = f"http://{host}:{port}/token"
            self.send_response(401)
            self.send_header("Connection", "close")
            self.send_header("Content-Length", "0")
            self.send_header(
                "WWW-Authenticate",
                f'Bearer realm="{realm}",service="fixture-registry",scope="repository:acme/image:pull"',
            )
            self.end_headers()
            return False

        def do_GET(self):
            fixture.record("GET", self.path)
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == "/token":
                with fixture.lock:
                    fixture.token_requests += 1
                self.send_bytes(200, b'{"token":"fixture-token"}', "application/json")
                return
            if not self.require_auth():
                return
            manifest_prefix = "/v2/acme/image/manifests/"
            blob_prefix = "/v2/acme/image/blobs/"
            if parsed.path.startswith(manifest_prefix):
                requested_digest = parsed.path[len(manifest_prefix) :]
                body = fixture.manifests.get(requested_digest)
                if body is None:
                    self.send_bytes(404, b'{"errors":[{"code":"MANIFEST_UNKNOWN"}]}', "application/json")
                    return
                document = json.loads(body)
                self.send_bytes(200, body, document["mediaType"], requested_digest)
                return
            if parsed.path.startswith(blob_prefix):
                requested_digest = parsed.path[len(blob_prefix) :]
                body = fixture.blobs.get(requested_digest)
                if body is None:
                    self.send_bytes(404, b'{"errors":[{"code":"BLOB_UNKNOWN"}]}', "application/json")
                    return
                self.send_bytes(200, body, "application/octet-stream", requested_digest)
                return
            self.send_bytes(404)

        def do_HEAD(self):
            fixture.record("HEAD", self.path)
            if not self.require_auth():
                return
            parsed = urllib.parse.urlsplit(self.path)
            blob_prefix = "/v2/acme/image/blobs/"
            if parsed.path.startswith(blob_prefix):
                requested_digest = parsed.path[len(blob_prefix) :]
                body = fixture.blobs.get(requested_digest)
                if body is None:
                    self.send_bytes(404, b'{"errors":[{"code":"BLOB_UNKNOWN"}]}', "application/json")
                    return
                self.send_bytes(200, body, "application/octet-stream", requested_digest)
                return
            self.send_bytes(404)

    return Handler


def run_verifier(fixture, image_digest=None, platform="linux/amd64"):
    server = ThreadingHTTPServer(("127.0.0.1", 0), fixture_handler(fixture))
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        selected_digest = image_digest or fixture.index_digest
        return subprocess.run(
            [
                "python3",
                str(VERIFIER),
                "--image",
                f"{host}:{port}/acme/image@{selected_digest}",
                "--platform",
                platform,
                "--timeout-seconds",
                "5",
                "--insecure",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class RegistryImageVerificationTests(unittest.TestCase):
    def test_bearer_registry_index_manifest_config_and_layers_pass(self):
        fixture = RegistryFixture()
        result = run_verifier(fixture)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertEqual(payload["index_digest"], fixture.index_digest)
        self.assertEqual(payload["manifest_digest"], fixture.manifest_digest)
        self.assertEqual(payload["platform"], "linux/amd64")
        self.assertEqual(payload["blob_count"], 2)
        self.assertEqual(fixture.token_requests, 1)
        self.assertIn(("GET", f"/v2/acme/image/blobs/{fixture.config_digest}"), fixture.requests)
        self.assertIn(("HEAD", f"/v2/acme/image/blobs/{fixture.layer_digest}"), fixture.requests)

    def test_missing_layer_fails(self):
        fixture = RegistryFixture()
        del fixture.blobs[fixture.layer_digest]
        result = run_verifier(fixture)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("returned HTTP 404", result.stderr)

    def test_manifest_body_digest_mismatch_fails(self):
        fixture = RegistryFixture()
        fixture.manifests[fixture.index_digest] += b"\n"
        result = run_verifier(fixture)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("manifest body digest mismatch", result.stderr)

    def test_missing_platform_fails(self):
        fixture = RegistryFixture()
        result = run_verifier(fixture, platform="linux/arm64")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("found 0", result.stderr)

    def test_config_platform_mismatch_fails(self):
        fixture = RegistryFixture()
        config = json.loads(fixture.blobs[fixture.config_digest])
        config["architecture"] = "arm64"
        config_body = encoded(config)
        new_config_digest = digest(config_body)
        del fixture.blobs[fixture.config_digest]
        fixture.blobs[new_config_digest] = config_body

        manifest = json.loads(fixture.manifests[fixture.manifest_digest])
        manifest["config"]["digest"] = new_config_digest
        manifest["config"]["size"] = len(config_body)
        manifest_body = encoded(manifest)
        new_manifest_digest = digest(manifest_body)
        del fixture.manifests[fixture.manifest_digest]
        fixture.manifests[new_manifest_digest] = manifest_body

        index = json.loads(fixture.manifests[fixture.index_digest])
        index["manifests"][0]["digest"] = new_manifest_digest
        index["manifests"][0]["size"] = len(manifest_body)
        index_body = encoded(index)
        new_index_digest = digest(index_body)
        del fixture.manifests[fixture.index_digest]
        fixture.manifests[new_index_digest] = index_body
        fixture.index_digest = new_index_digest
        result = run_verifier(fixture)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("image config platform mismatch", result.stderr)

    def test_invalid_reference_fails_without_network(self):
        result = subprocess.run(
            ["python3", str(VERIFIER), "--image", "ghcr.io/acme/image:mutable"],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("registry/repository@sha256:digest", result.stderr)


if __name__ == "__main__":
    unittest.main()
