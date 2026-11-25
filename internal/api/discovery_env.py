import base64
import hashlib
import hmac
import json
import os
import shlex
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as fh:
    bundle = json.load(fh)

schema_version = str(bundle.get("schema_version", "")).strip()
if not schema_version:
    raise SystemExit("DiscoveryBundle schema_version is empty")
if schema_version.split(".", 1)[0] != "1":
    raise SystemExit(f"unsupported DiscoveryBundle schema_version: {schema_version}")
if not str(bundle.get("signature", "")).strip():
    raise SystemExit("DiscoveryBundle signature is empty")

def payload_for_signature(key_id, valid_until):
    payload = {}
    for key in (
        "schema_version",
        "generation",
        "previous_generation",
        "generated_at",
        "valid_until",
        "issuer",
        "key_id",
        "api_endpoints",
        "kubernetes",
        "registry",
        "edge_groups",
        "edge_nodes",
        "dns_nodes",
        "platform_routes",
        "public_runtime_env",
    ):
        value = bundle.get(key)
        if key == "valid_until":
            value = valid_until
        elif key == "key_id":
            value = key_id
        if value in ("", None, [], {}):
            continue
        payload[key] = value
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def verify_signature():
    keys = {}
    active_key = os.environ.get("FUGUE_BUNDLE_SIGNING_KEY", "").strip()
    active_key_id = os.environ.get("FUGUE_BUNDLE_SIGNING_KEY_ID", bundle.get("key_id", "")).strip()
    previous_key = os.environ.get("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY", "").strip()
    previous_key_id = os.environ.get("FUGUE_BUNDLE_SIGNING_PREVIOUS_KEY_ID", "").strip()
    if active_key and active_key_id:
        keys[active_key_id.lower()] = active_key
    if previous_key and previous_key_id:
        keys[previous_key_id.lower()] = previous_key
    if not keys:
        return
    revoked = {item.strip().lower() for item in os.environ.get("FUGUE_BUNDLE_REVOKED_KEY_IDS", "").replace(";", ",").split(",") if item.strip()}
    candidates = [(bundle.get("key_id", ""), bundle.get("signature", ""), bundle.get("valid_until", ""))]
    for item in bundle.get("signatures") or []:
        candidates.append((item.get("key_id", ""), item.get("signature", ""), item.get("valid_until", bundle.get("valid_until", ""))))
    for key_id, signature, valid_until in candidates:
        key_id = str(key_id or "").strip()
        signature = str(signature or "").strip()
        if not key_id or not signature or key_id.lower() in revoked:
            continue
        key = keys.get(key_id.lower())
        if not key:
            continue
        digest = hmac.new(key.encode("utf-8"), payload_for_signature(key_id, valid_until), hashlib.sha256).digest()
        expected = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        if hmac.compare_digest(signature, expected):
            return
    raise SystemExit("DiscoveryBundle signature verification failed")

verify_signature()

def first_named(items, name):
    for item in items or []:
        if str(item.get("name", "")).strip() == name:
            return item
    return {}

def emit(key, value):
    if value is None:
        value = ""
    if isinstance(value, list):
        value = ",".join(str(item) for item in value)
    print(f"{key}={shlex.quote(str(value))}")
