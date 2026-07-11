#!/usr/bin/env python3

import argparse
from email.message import Message
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.parse


DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
MANIFEST_ACCEPT = ", ".join(
    (
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    )
)
INDEX_MEDIA_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}
MANIFEST_MEDIA_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
MAX_CONFIG_BYTES = 4 * 1024 * 1024
MAX_MANIFEST_DESCRIPTORS = 64
MAX_BLOB_DESCRIPTORS = 512


class VerificationError(Exception):
    pass


def parse_image_ref(image_ref):
    if "://" in image_ref or "@" not in image_ref:
        raise VerificationError("image must be registry/repository@sha256:digest")
    name, digest = image_ref.rsplit("@", 1)
    if DIGEST_RE.fullmatch(digest) is None:
        raise VerificationError("image digest must be a complete lowercase sha256 digest")
    if "/" not in name or any(ch.isspace() for ch in name):
        raise VerificationError("image must include a registry and repository path")
    registry, repository = name.split("/", 1)
    if not registry or not repository or "@" in repository or "|" in repository:
        raise VerificationError("invalid registry image repository")
    return registry, repository, digest


def parse_platform(value):
    parts = value.split("/")
    if len(parts) not in (2, 3) or not parts[0] or not parts[1]:
        raise VerificationError("platform must be os/architecture or os/architecture/variant")
    return parts[0], parts[1], parts[2] if len(parts) == 3 else ""


def descriptor_digest(descriptor, label):
    if not isinstance(descriptor, dict):
        raise VerificationError(f"{label} descriptor must be an object")
    digest = descriptor.get("digest")
    if not isinstance(digest, str) or DIGEST_RE.fullmatch(digest) is None:
        raise VerificationError(f"{label} descriptor has an invalid digest")
    size = descriptor.get("size")
    if not isinstance(size, int) or size < 0:
        raise VerificationError(f"{label} descriptor has an invalid size")
    return digest, size


def content_digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


class RegistryClient:
    def __init__(self, registry, repository, timeout_seconds, insecure):
        self.registry = registry
        self.repository = repository
        self.deadline = time.monotonic() + timeout_seconds
        self.scheme = "http" if insecure else "https"
        self.token = ""
        self.request_count = 0
        self.insecure = insecure
        self.curl = shutil.which("curl")
        if not self.curl:
            raise VerificationError("curl is required for registry verification")

    def _remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise VerificationError("registry verification exceeded its total timeout")
        return remaining

    @staticmethod
    def _parse_final_headers(raw_headers):
        blocks = re.split(br"\r?\n\r?\n", raw_headers)
        selected = None
        for block in blocks:
            lines = block.splitlines()
            if lines and lines[0].startswith(b"HTTP/"):
                selected = lines[1:]
        if selected is None:
            raise VerificationError("curl did not return an HTTP response header block")
        message = Message()
        for raw_line in selected:
            if b":" not in raw_line:
                continue
            raw_name, raw_value = raw_line.split(b":", 1)
            try:
                name = raw_name.decode("ascii").strip()
                value = raw_value.decode("latin-1").strip()
            except UnicodeDecodeError as exc:
                raise VerificationError("registry returned invalid response headers") from exc
            if name:
                message.add_header(name, value)
        return message

    @staticmethod
    def _curl_config(headers):
        lines = []
        for name, value in headers.items():
            if any(ch in value for ch in "\r\n"):
                raise VerificationError(f"invalid registry request header: {name}")
            escaped = f"{name}: {value}".replace("\\", "\\\\").replace('"', '\\"')
            lines.append(f'header = "{escaped}"\n')
        return "".join(lines)

    def _open(self, method, url, headers, body_limit):
        self.request_count += 1
        remaining = self._remaining()
        connect_timeout = min(5.0, remaining)
        allowed_protocols = "=http,https" if self.insecure else "=https"
        with tempfile.TemporaryDirectory(prefix="fugue-registry-verify-") as temp_dir:
            headers_path = f"{temp_dir}/headers"
            body_path = f"{temp_dir}/body"
            command = [
                self.curl,
                "--silent",
                "--show-error",
                "--location",
                "--max-redirs",
                "5",
                "--proto",
                allowed_protocols,
                "--proto-redir",
                allowed_protocols,
                "--connect-timeout",
                f"{connect_timeout:.3f}",
                "--max-time",
                f"{remaining:.3f}",
                "--dump-header",
                headers_path,
                "--output",
                body_path,
                "--write-out",
                "%{http_code}",
                "--user-agent",
                "fugue-registry-verifier/1",
                "--config",
                "-",
            ]
            if method == "HEAD":
                command.append("--head")
            elif method != "GET":
                raise VerificationError(f"unsupported registry request method: {method}")
            if self.insecure:
                command.append("--insecure")
            if body_limit > 0:
                command.extend(("--max-filesize", str(body_limit)))
            command.append(url)
            completed = subprocess.run(
                command,
                input=self._curl_config(headers),
                capture_output=True,
                text=True,
                check=False,
            )
            if completed.returncode != 0:
                parsed = urllib.parse.urlsplit(url)
                safe_target = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
                detail = completed.stderr.strip() or f"curl exit {completed.returncode}"
                raise VerificationError(f"registry {method} {safe_target} failed: {detail}")
            try:
                status = int(completed.stdout.strip())
            except ValueError as exc:
                raise VerificationError("curl returned an invalid HTTP status") from exc
            try:
                with open(headers_path, "rb") as headers_file:
                    raw_headers = headers_file.read()
                if method == "HEAD":
                    body = b""
                else:
                    with open(body_path, "rb") as body_file:
                        body = body_file.read(body_limit + 1)
            except OSError as exc:
                raise VerificationError(f"could not read curl registry response: {exc}") from exc
            if method != "HEAD" and len(body) > body_limit:
                raise VerificationError(f"registry response exceeds {body_limit} bytes")
            return status, self._parse_final_headers(raw_headers), body

    def _authenticate(self, challenge):
        if not challenge or not challenge.lower().startswith("bearer "):
            raise VerificationError("registry requires unsupported authentication")
        params = {}
        for match in re.finditer(r'([A-Za-z][A-Za-z0-9_-]*)="([^"\\]*(?:\\.[^"\\]*)*)"', challenge[7:]):
            params[match.group(1).lower()] = match.group(2).replace(r'\"', '"').replace(r"\\", "\\")
        realm = params.get("realm", "")
        parsed = urllib.parse.urlsplit(realm)
        allowed_schemes = {"https"} if self.scheme == "https" else {"http", "https"}
        if parsed.scheme not in allowed_schemes or not parsed.netloc or parsed.username or parsed.password:
            raise VerificationError("registry returned an invalid bearer token realm")
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        present = {key for key, _ in query}
        for key in ("service", "scope"):
            value = params.get(key)
            if value and key not in present:
                query.append((key, value))
        token_url = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query), parsed.fragment)
        )
        status, _, body = self._open("GET", token_url, {"Accept": "application/json"}, 1024 * 1024)
        if status != 200:
            raise VerificationError(f"registry bearer token request returned HTTP {status}")
        try:
            payload = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise VerificationError("registry bearer token response is invalid JSON") from exc
        token = payload.get("token") or payload.get("access_token")
        if not isinstance(token, str) or not token:
            raise VerificationError("registry bearer token response has no token")
        self.token = token

    def request(self, method, path, accept, body_limit, extra_headers=None):
        url = f"{self.scheme}://{self.registry}{path}"
        for attempt in range(2):
            headers = {
                "Accept": accept,
                "Accept-Encoding": "identity",
                "User-Agent": "fugue-registry-verifier/1",
            }
            if extra_headers:
                headers.update(extra_headers)
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            status, response_headers, body = self._open(method, url, headers, body_limit)
            if status != 401 or attempt != 0:
                return status, response_headers, body
            self._authenticate(response_headers.get("WWW-Authenticate", ""))
        raise VerificationError("registry authentication retry was exhausted")

    def manifest(self, digest):
        path = f"/v2/{urllib.parse.quote(self.repository, safe='/')}/manifests/{digest}"
        status, headers, body = self.request("GET", path, MANIFEST_ACCEPT, MAX_MANIFEST_BYTES)
        if status != 200:
            raise VerificationError(f"registry manifest {digest} returned HTTP {status}")
        actual = content_digest(body)
        if actual != digest:
            raise VerificationError(f"registry manifest body digest mismatch: expected {digest}, got {actual}")
        declared = headers.get("Docker-Content-Digest", "").strip()
        if declared and declared != digest:
            raise VerificationError(f"registry manifest header digest mismatch: expected {digest}, got {declared}")
        try:
            document = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise VerificationError(f"registry manifest {digest} is invalid JSON") from exc
        if not isinstance(document, dict):
            raise VerificationError(f"registry manifest {digest} must be a JSON object")
        media_type = headers.get_content_type()
        document_media_type = document.get("mediaType")
        if isinstance(document_media_type, str) and document_media_type:
            media_type = document_media_type
        return document, media_type, len(body)

    def blob(self, digest, expected_size, read_body):
        path = f"/v2/{urllib.parse.quote(self.repository, safe='/')}/blobs/{digest}"
        method = "GET" if read_body else "HEAD"
        limit = MAX_CONFIG_BYTES if read_body else 0
        status, headers, body = self.request(method, path, "application/octet-stream", limit)
        if status != 200:
            raise VerificationError(f"registry blob {digest} returned HTTP {status}")
        declared = headers.get("Docker-Content-Digest", "").strip()
        if declared and declared != digest:
            raise VerificationError(f"registry blob header digest mismatch: expected {digest}, got {declared}")
        content_length = headers.get("Content-Length", "").strip()
        if content_length:
            try:
                observed_size = int(content_length)
            except ValueError as exc:
                raise VerificationError(f"registry blob {digest} returned an invalid Content-Length") from exc
            if observed_size != expected_size:
                raise VerificationError(
                    f"registry blob size mismatch for {digest}: expected {expected_size}, got {observed_size}"
                )
        if read_body:
            if len(body) != expected_size:
                raise VerificationError(
                    f"registry blob body size mismatch for {digest}: expected {expected_size}, got {len(body)}"
                )
            actual = content_digest(body)
            if actual != digest:
                raise VerificationError(f"registry blob body digest mismatch: expected {digest}, got {actual}")
        return body

    def probe_blob_get(self, digest, expected_size):
        if expected_size <= 0:
            raise VerificationError(f"registry layer {digest} has no bytes to probe")
        path = f"/v2/{urllib.parse.quote(self.repository, safe='/')}/blobs/{digest}"
        status, headers, body = self.request(
            "GET",
            path,
            "application/octet-stream",
            1,
            {"Range": "bytes=0-0"},
        )
        if status == 206:
            content_range = headers.get("Content-Range", "").strip()
            match = re.fullmatch(r"bytes\s+0-0/(\d+)", content_range, flags=re.IGNORECASE)
            if match is None or int(match.group(1)) != expected_size:
                raise VerificationError(
                    f"registry blob {digest} returned an invalid Content-Range for bounded GET"
                )
        elif status != 200 or expected_size != 1:
            raise VerificationError(f"registry blob {digest} bounded GET returned HTTP {status}")
        if len(body) != 1:
            raise VerificationError(
                f"registry blob {digest} bounded GET returned {len(body)} bytes instead of 1"
            )
        declared = headers.get("Docker-Content-Digest", "").strip()
        if declared and declared != digest:
            raise VerificationError(f"registry blob header digest mismatch: expected {digest}, got {declared}")


def select_platform_manifest(manifests, os_name, architecture, variant):
    if not isinstance(manifests, list) or not manifests:
        raise VerificationError("image index has no manifests")
    if len(manifests) > MAX_MANIFEST_DESCRIPTORS:
        raise VerificationError("image index has too many manifest descriptors")
    matches = []
    for descriptor in manifests:
        if not isinstance(descriptor, dict):
            raise VerificationError("image index contains a non-object descriptor")
        platform = descriptor.get("platform")
        if not isinstance(platform, dict):
            continue
        if platform.get("os") != os_name or platform.get("architecture") != architecture:
            continue
        descriptor_variant = platform.get("variant") or ""
        if descriptor_variant != variant:
            continue
        matches.append(descriptor)
    if len(matches) != 1:
        raise VerificationError(
            f"image index must contain exactly one {os_name}/{architecture}{('/' + variant) if variant else ''} manifest; found {len(matches)}"
        )
    return matches[0]


def verify_image(client, top_digest, platform):
    os_name, architecture, variant = platform
    top_document, media_type, _ = client.manifest(top_digest)
    index_digest = ""
    manifest_digest = top_digest

    if media_type in INDEX_MEDIA_TYPES:
        index_digest = top_digest
        descriptor = select_platform_manifest(top_document.get("manifests"), os_name, architecture, variant)
        manifest_digest, manifest_size = descriptor_digest(descriptor, "platform manifest")
        document, media_type, observed_size = client.manifest(manifest_digest)
        if observed_size != manifest_size:
            raise VerificationError(
                f"platform manifest size mismatch: expected {manifest_size}, got {observed_size}"
            )
    else:
        document = top_document

    if media_type not in MANIFEST_MEDIA_TYPES:
        raise VerificationError(f"unsupported image manifest media type: {media_type}")
    config = document.get("config")
    layers = document.get("layers")
    if not isinstance(layers, list):
        raise VerificationError("image manifest layers must be an array")
    if len(layers) + 1 > MAX_BLOB_DESCRIPTORS:
        raise VerificationError("image manifest has too many blob descriptors")

    config_digest, config_size = descriptor_digest(config, "image config")
    config_body = client.blob(config_digest, config_size, True)
    try:
        image_config = json.loads(config_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VerificationError("image config is invalid JSON") from exc
    if not isinstance(image_config, dict):
        raise VerificationError("image config must be a JSON object")
    observed_platform = (
        image_config.get("os"),
        image_config.get("architecture"),
        image_config.get("variant") or "",
    )
    if observed_platform != platform:
        expected = "/".join(item for item in platform if item)
        observed = "/".join(str(item) for item in observed_platform if item)
        raise VerificationError(f"image config platform mismatch: expected {expected}, got {observed or '<missing>'}")

    total_layer_bytes = 0
    layer_digests = set()
    for index, descriptor in enumerate(layers):
        digest, size = descriptor_digest(descriptor, f"layer {index}")
        if digest in layer_digests:
            raise VerificationError(f"image manifest repeats layer digest {digest}")
        layer_digests.add(digest)
        client.blob(digest, size, False)
        client.probe_blob_get(digest, size)
        total_layer_bytes += size

    return {
        "blob_count": len(layers) + 1,
        "index_digest": index_digest,
        "manifest_digest": manifest_digest,
        "layer_get_probe_count": len(layers),
        "platform": "/".join(item for item in platform if item),
        "request_count": client.request_count,
        "total_layer_bytes": total_layer_bytes,
        "verification": "registry_manifest_config_and_layer_get",
    }


def main():
    parser = argparse.ArgumentParser(description="Verify an immutable OCI registry image and all pull-critical blobs.")
    parser.add_argument("--image", required=True, help="Immutable registry/repository@sha256:digest reference")
    parser.add_argument("--platform", default="linux/amd64", help="Required image platform")
    parser.add_argument("--timeout-seconds", type=float, default=20.0, help="Total registry verification timeout")
    parser.add_argument("--insecure", action="store_true", help="Allow HTTP and unverified TLS for local tests only")
    args = parser.parse_args()

    try:
        if args.timeout_seconds <= 0:
            raise VerificationError("timeout must be greater than zero")
        registry, repository, digest = parse_image_ref(args.image)
        platform = parse_platform(args.platform)
        client = RegistryClient(registry, repository, args.timeout_seconds, args.insecure)
        result = verify_image(client, digest, platform)
        result["image"] = args.image
        print(json.dumps(result, separators=(",", ":"), sort_keys=True))
    except VerificationError as exc:
        print(f"[verify_registry_image] ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
