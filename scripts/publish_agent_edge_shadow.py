#!/usr/bin/env python3
"""Publish an explicit observational Agent policy through the artifact API.

This lane cannot publish full/active authority or attest LKG verification. It
creates, validates and releases only the declared immutable shadow policy.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request

SCOPE = "agent-edge-control"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return "sha256:" + hashlib.sha256(canonical(value).encode()).hexdigest()


def validate(config):
    if set(config) != {"apiVersion", "kind", "origin", "expectedPreviousGeneration", "policy"} or config["apiVersion"] != "configuration.fugue.dev/v1" or config["kind"] != "AgentEdgeShadowPolicy":
        raise ValueError("invalid shadow policy declaration")
    url = urllib.parse.urlsplit(config["origin"])
    if url.scheme != "https" or config["origin"] != "https://" + str(url.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(url.hostname)):
        raise ValueError("canonical HTTPS policy origin required")
    policy = config["policy"]
    if policy.get("schema_version") != "fugue.agent-edge-policy/v1" or policy.get("scope") != SCOPE or policy.get("mode") != "shadow" or policy.get("origin") != config["origin"] or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", policy.get("generation", "")):
        raise ValueError("only an explicit shadow policy is accepted")
    previous = config["expectedPreviousGeneration"]
    if not isinstance(previous, str) or previous == policy["generation"] or previous and not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}", previous):
        raise ValueError("explicit distinct predecessor generation required")
    return config


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class API:
    def __init__(self, origin, token, response_limit=1 << 20, timeout=30):
        if type(response_limit) is not int or not 1 <= response_limit <= 128 << 20:
            raise ValueError("invalid bounded API response size")
        if type(timeout) is not int or not 1 <= timeout <= 120:
            raise ValueError("invalid bounded API timeout")
        self.origin, self.token = origin, token
        self.response_limit = response_limit
        self.timeout = timeout
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def __call__(self, method, path, body=None):
        if not path.startswith("/v1/") or "\n" in path:
            raise ValueError("invalid artifact API path")
        request = urllib.request.Request(self.origin + path, method=method, data=None if body is None else canonical(body).encode(), headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read(self.response_limit + 1)
        except urllib.error.HTTPError as error:
            raise RuntimeError("artifact API returned HTTP " + str(error.code)) from None
        except Exception:
            raise RuntimeError("artifact API transport unavailable") from None
        if len(raw) > self.response_limit:
            raise ValueError("artifact API response exceeds bound")
        return json.loads(raw)


def current(api, channel):
    return api("GET", "/v1/platform-state/artifacts/policy_snapshot?scope_key=" + SCOPE + "&channel=" + channel)


def target_matches(artifact, policy):
    return artifact.get("artifact_kind") == "policy_snapshot" and artifact.get("scope_key") == SCOPE and artifact.get("generation") == policy["generation"] and artifact.get("content") == policy and artifact.get("content_hash") == digest(policy)


def check_predecessor(state, config):
    artifact = state.get("artifact")
    if artifact and artifact.get("generation") == config["policy"]["generation"]:
        if not target_matches(artifact, config["policy"]):
            raise ValueError("same-generation Agent policy content changed")
        return True
    if (artifact or {}).get("generation", "") != config["expectedPreviousGeneration"]:
        raise ValueError("shadow policy predecessor differs from declaration")
    return False


def publish(config, api):
    # Once full authority exists this initial observation lane must stop. It
    # cannot override active authority by publishing a newer shadow generation.
    if current(api, "full").get("artifact"):
        raise ValueError("full Agent authority exists; initial shadow lane cannot change it")
    policy = config["policy"]
    before = current(api, "shadow")
    if check_predecessor(before, config):
        if before.get("release", {}).get("status") != "active" or before["artifact"].get("status") != "validated":
            raise ValueError("existing shadow publication is not validated and active")
        return before
    candidates = api("GET", "/v1/admin/artifacts?kind=policy_snapshot&scope=" + SCOPE + "&limit=100").get("artifacts", [])
    matches = [a for a in candidates if a.get("generation") == policy["generation"]]
    if len(matches) > 1:
        raise ValueError("ambiguous policy generation")
    if matches:
        artifact = matches[0]
    else:
        artifact = api("POST", "/v1/admin/artifacts", {"artifact_kind": "policy_snapshot", "scope": {"scope_type": "global", "key": SCOPE}, "generation": policy["generation"], "content": policy})["artifact"]
    if not target_matches(artifact, policy) or not re.fullmatch(r"[a-zA-Z0-9_.-]+", artifact.get("id", "")):
        raise ValueError("stored immutable policy differs from declaration")
    result = api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/validate", {"dry_run": False})
    if result.get("pass") is not True or result.get("artifact", {}).get("status") != "validated" or not target_matches(result["artifact"], policy):
        raise ValueError("typed Agent policy validation failed")
    latest = current(api, "shadow")
    if current(api, "full").get("artifact") or latest.get("artifact") != before.get("artifact") or latest.get("release") != before.get("release"):
        raise ValueError("Agent policy authority changed during validation")
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "shadow", "idempotency_key": "git-agent-shadow/" + policy["generation"] + "/" + digest(policy), "reason": "declared Agent Edge observation policy; no control traffic activation"})
    after = current(api, "shadow")
    if not check_predecessor(after, config) or after.get("release", {}).get("status") != "active" or after["release"].get("release_channel") != "shadow":
        raise ValueError("shadow publication did not converge to declared authority")
    return after


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    args = parser.parse_args()
    raw = Path(args.config).read_bytes()
    if len(raw) > 65536:
        raise ValueError("policy declaration exceeds bound")
    config = validate(json.loads(raw))
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token:
        raise ValueError("independent configuration writer credential missing")
    state = publish(config, API(config["origin"], token))
    print(canonical({"published": True, "mode": "shadow", "artifact_id": state["artifact"]["id"], "generation": state["artifact"]["generation"], "release_id": state["release"]["id"], "fencing_token": state["release"]["fencing_token"]}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("Agent Edge shadow publication failed: " + type(error).__name__) from None
