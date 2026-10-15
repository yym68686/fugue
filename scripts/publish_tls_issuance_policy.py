#!/usr/bin/env python3
"""Publish generation-fenced certificate intent independently of code releases."""
import argparse
import json
import os
from pathlib import Path
import urllib.error

try:
    from .publish_node_resource_policy import API
except ImportError:
    from publish_node_resource_policy import API


def publish(api, desired):
    try:
        current = api("GET", "/v1/admin/tls-issuance/policy")
    except RuntimeError as exc:
        if "returned 404" not in str(exc):
            raise
        current = {"generation": 0}
    if current == desired:
        return {"changed": False, "generation": desired["generation"]}
    if desired["generation"] != current["generation"] + 1:
        raise ValueError("TLS policy generation does not follow the observed configuration")
    actual = api("PUT", "/v1/admin/tls-issuance/policy", desired)
    if actual != desired:
        raise ValueError("TLS policy publication receipt differs")
    return {"changed": True, "generation": actual["generation"], "enabled": actual["enabled"]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    args = parser.parse_args()
    config_path = Path(args.config)
    config = json.loads(config_path.read_text())
    if not config["publish"]:
        print(json.dumps({"published": False, "reason": "configuration publication not activated"}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token:
        raise ValueError("configuration writer credential missing")
    desired = json.loads((config_path.parent / config["policy_file"]).read_text())
    print(json.dumps(publish(API(config["origin"], token), desired)))


if __name__ == "__main__":
    main()
