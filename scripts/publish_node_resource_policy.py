#!/usr/bin/env python3
"""Publish node resource intent through the normal authenticated task protocol."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "internal/api"))
from node_resource_policy import validate

TASK = "reconcile-host-inotify-policy"


class API:
    def __init__(self, origin, token):
        parsed = urllib.parse.urlsplit(origin)
        if parsed.scheme != "https" or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("policy API origin must be HTTPS without credentials")
        self.origin, self.token = origin.rstrip("/"), token

    def __call__(self, method, path, body=None):
        request = urllib.request.Request(self.origin + path, method=method, data=json.dumps(body).encode() if body is not None else None, headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        # Credentials must never follow an API redirect to another authority.
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args, **kwargs):
                return None
        try:
            with urllib.request.build_opener(NoRedirect).open(request, timeout=30) as response:
                if path == "/install/node-updater.sh":
                    return response.read().decode()
                return json.load(response)
        except urllib.error.HTTPError as exc:
            raise RuntimeError("node policy API %s %s returned %d" % (method, path.split("?")[0], exc.code)) from None


def version_number(value):
    return int(value[1:]) if isinstance(value, str) and re.fullmatch(r"v[1-9][0-9]*", value) else 0


def publish(api, targets, policy, apply=False, wait_seconds=0, upgrade_updaters=False, minimum_updater_version=None):
    validate(policy)
    minimum = version_number(minimum_updater_version)
    if minimum_updater_version is not None and not minimum:
        raise ValueError("minimum_updater_version must be a numbered v release")
    if not targets or len(set(targets)) != len(targets):
        raise ValueError("explicit unique node target list required")
    deadline = time.monotonic() + wait_seconds
    remaining, submitted, results, upgrades = set(targets), {}, {}, set()
    while remaining:
        updaters = api("GET", "/v1/node-updaters")["node_updaters"]
        by_node = {}
        upgrade_candidates = []
        for updater in updaters:
            if updater.get("status") != "active":
                continue
            stamp = updater.get("last_heartbeat_at") or updater.get("last_seen_at") or ""
            try:
                age = (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(stamp.replace("Z", "+00:00"))).total_seconds()
            except ValueError:
                continue
            if age > 900:
                continue
            node = updater.get("cluster_node_name")
            if node in by_node:
                raise ValueError("multiple active updaters for target " + node)
            by_node[node] = updater
        for node in sorted(remaining):
            updater = by_node.get(node)
            if not updater:
                continue
            capable = TASK in updater.get("capabilities", [])
            current = version_number(updater.get("updater_version"))
            upgrade_needed = not capable or (upgrade_updaters and current < minimum)
            if upgrade_needed and node not in upgrades and upgrade_updaters:
                upgrade_candidates.append((node, updater["id"], current))
            if not capable:
                continue
            if node not in submitted:
                task = api("POST", "/v1/node-update-tasks", {"node_updater_id": updater["id"], "type": TASK, "payload": {"policy_json": json.dumps(policy, sort_keys=True, separators=(",", ":")), "dry_run": "false" if apply else "true", "reason": "git-node-resource-policy"}})["task"]
                submitted[node] = task["id"]
            response = api("GET", "/v1/node-update-tasks?" + urllib.parse.urlencode({"task_id": submitted[node], "limit": 1}))
            tasks = response.get("tasks", [])
            if not tasks:
                continue
            task = tasks[0]
            if task["status"] in ("failed", "canceled"):
                raise RuntimeError("node policy task %s on %s %s: %s" % (task["id"], node, task["status"], task.get("error_message", "")))
            if task["status"] == "completed":
                results[node] = {"task_id": task["id"], "status": task["status"], "message": task.get("result_message", "")}
                if not upgrade_needed:
                    remaining.remove(node)
        # Publish configuration to all capable targets before depending on code
        # availability. A script download/build failure must not gate recovery.
        if upgrade_candidates:
            script = api("GET", "/install/node-updater.sh")
            match = re.search(r'^FUGUE_NODE_UPDATER_SCRIPT_VERSION="(v[1-9][0-9]*)"$', script, re.MULTILINE)
            served = version_number(match[1]) if match else 0
            for node, updater_id, current in upgrade_candidates:
                if served >= minimum and served > current and TASK in script:
                    api("POST", "/v1/node-update-tasks", {"node_updater_id": updater_id, "type": "upgrade-node-updater", "payload": {"target_version": match[1], "reason": "node-resource-policy-version"}})
                    upgrades.add(node)
        if not remaining:
            return results
        if time.monotonic() >= deadline:
            raise RuntimeError("node policy unverified; pending targets: " + ", ".join(sorted(remaining)))
        time.sleep(min(15, max(0, deadline - time.monotonic())))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--upgrade-updaters", action="store_true")
    parser.add_argument("--wait-seconds", type=int, default=900)
    args = parser.parse_args()
    config_path = Path(args.config)
    config = json.loads(config_path.read_text())
    policy = json.loads((config_path.parent / config["policy_file"]).read_text())
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token:
        raise ValueError("configuration writer credential missing")
    results = publish(API(config["origin"], token), config["nodes"], policy, args.apply, args.wait_seconds, args.upgrade_updaters, config.get("minimum_updater_version"))
    print(json.dumps({"applied": args.apply, "results": results}, sort_keys=True))


if __name__ == "__main__":
    main()
