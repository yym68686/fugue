#!/usr/bin/env python3
"""Enroll exact signed cell inputs and a shadow-only configuration producer.

This operation cannot publish traffic channels, select transport, or attest LKG.
The configuration lane is independent of component image builds and deployment.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import urllib.parse

from scripts.publish_agent_edge_shadow import API, canonical

CELL = re.compile(r"cell-[a-z0-9]+(?:-[a-z0-9]+)*")
IDENTITY = re.compile(r"[a-zA-Z0-9_.-]+")


def digest(value):
    # Artifact storage hashes Go encoding/json's map representation, including
    # its HTML and line-separator escaping. These inputs contain integer bounds.
    raw = canonical(value)
    for char in ["&", "<", ">", "\u2028", "\u2029"]:
        raw = raw.replace(char, "\\u%04x" % ord(char))
    return "sha256:" + hashlib.sha256(raw.encode()).hexdigest()


def validate(config):
    if set(config) != {"schema", "generation", "origin", "authority_cell_id", "expected_previous_policy", "static_intent", "projection_policy", "producer"} or config["schema"] != "fugue.cell-producer-bootstrap/v1" or type(config["generation"]) is not int or config["generation"] < 1:
        raise ValueError("invalid cell producer declaration")
    cell = config["authority_cell_id"]
    if not isinstance(cell, str) or len(cell) > 128 or not CELL.fullmatch(cell):
        raise ValueError("canonical cell required")
    url = urllib.parse.urlsplit(config["origin"])
    if url.scheme != "https" or config["origin"] != "https://" + str(url.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(url.hostname)):
        raise ValueError("canonical HTTPS API origin required")
    scope = "authority-cell:" + cell
    intent, policy, producer = config["static_intent"], config["projection_policy"], config["producer"]
    for value in [intent, policy]:
        if value.get("schema_version") != "fugue.platform.config/v1" or value.get("scope") != scope or value.get("authority_cell_id") != cell or not IDENTITY.fullmatch(value.get("generation", "")):
            raise ValueError("input scope or authority differs")
    topology = intent.get("edge_topology", {})
    edges, dns = topology.get("edges", []), intent.get("dns_consumers", [])
    if topology.get("authority_cells") != [{"id": cell}] or not edges or not dns or len(edges) > 10000 or len(dns) > 4096 or any(e.get("authority_cell_id") != cell for e in edges) or any(d.get("edge_group_id") != cell for d in dns):
        raise ValueError("complete neutral cell membership required")
    edge_ids, dns_ids = sorted(e["id"] for e in edges), sorted(d["node_id"] for d in dns)
    if len(set(edge_ids)) != len(edge_ids) or len(set(dns_ids)) != len(dns_ids):
        raise ValueError("duplicate declared member")
    # Match the typed TrafficConsumerTopology encoding, whose field order is
    # part of its digest contract. The API independently validates this pin.
    membership = {"schema_version": "fugue.traffic-consumer-topology/v1", "authority_cell_id": cell, "edge_node_ids": edge_ids, "dns_node_ids": dns_ids}
    membership_digest = "sha256:" + hashlib.sha256(json.dumps(membership, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    if policy.get("consumer_topology_digest") != membership_digest or policy.get("dns_placement_mode") != "consumer_readiness":
        raise ValueError("projection policy membership or placement differs")
    required = {"schema_version", "generation", "authority_cell_id", "mode", "input_source", "target_scope", "interval_seconds", "refresh_seconds", "require_application_domains", "require_route_defaults", "require_dns_query_policy", "hosted_zone_templates"}
    if set(producer) != required or producer["schema_version"] != "fugue.platform.producer/v1" or producer["mode"] != "shadow" or producer["input_source"] != "business-static-intent" or producer["target_scope"] != scope or producer["authority_cell_id"] != cell or not IDENTITY.fullmatch(producer["generation"]) or any(producer[k] is not True for k in ["require_application_domains", "require_route_defaults", "require_dns_query_policy"]):
        raise ValueError("only explicit shadow producer enrollment is allowed")
    previous = config["expected_previous_policy"]
    if previous is not None:
        if set(previous) != {"artifact_id", "content_hash", "release_id", "fencing_token"} or not IDENTITY.fullmatch(previous["artifact_id"]) or not IDENTITY.fullmatch(previous["release_id"]) or not re.fullmatch(r"sha256:[a-f0-9]{64}", previous["content_hash"]) or type(previous["fencing_token"]) is not int or previous["fencing_token"] < 1:
            raise ValueError("exact predecessor policy required")
    return config


def selected(api, scope, channel, kind="policy_snapshot"):
    state = api("GET", "/v1/platform-state/artifacts/" + kind + "?" + urllib.parse.urlencode({"scope_key": scope, "channel": channel}))
    artifact, release = state.get("artifact"), state.get("release")
    if artifact or release:
        if not artifact or not release or artifact.get("artifact_kind") != kind or artifact.get("scope_key") != scope or release.get("artifact_id") != artifact.get("id") or release.get("artifact_kind") != kind or release.get("scope_key") != scope or release.get("release_channel") != channel:
            raise ValueError("selected authority does not match requested lane")
    return state


def exact(a, kind, scope, content):
    return a.get("artifact_kind") == kind and a.get("scope_key") == scope and a.get("generation") == content["generation"] and a.get("content") == content and a.get("content_hash") == digest(content) and bool(IDENTITY.fullmatch(a.get("id", "")))


def ensure_input(api, kind, scope, content):
    # Generation is immutable within kind/scope. Read full content before
    # reuse; metadata list views do not prove that a draft matches the intent.
    query = urllib.parse.urlencode({"kind": kind, "scope": scope, "limit": 100})
    candidates = api("GET", "/v1/admin/artifacts?" + query).get("artifacts", [])
    matches = [a for a in candidates if a.get("generation") == content["generation"]]
    if len(matches) > 1:
        raise ValueError("ambiguous immutable input")
    if matches:
        ident = matches[0].get("id", "")
        if not IDENTITY.fullmatch(ident):
            raise ValueError("invalid input identity")
        artifact = api("GET", "/v1/admin/artifacts/" + ident)["artifact"]
    else:
        artifact = api("POST", "/v1/admin/artifacts", {"artifact_kind": kind, "scope": {"scope_type": "global", "key": scope}, "generation": content["generation"], "content": content})["artifact"]
    if not exact(artifact, kind, scope, content):
        raise ValueError("immutable input differs from declaration")
    if artifact.get("status") != "validated":
        result = api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/validate", {"dry_run": False})
        artifact = result.get("artifact", {})
        if result.get("pass") is not True or artifact.get("status") != "validated" or not exact(artifact, kind, scope, content):
            raise ValueError("typed input validation failed")
    return artifact


def authority_identity(state):
    a, r = state.get("artifact"), state.get("release")
    if not a and not r:
        return None
    if not a or not r or r.get("status") != "active" or a.get("status") != "validated":
        raise ValueError("incomplete producer authority")
    return {"artifact_id": a["id"], "content_hash": a["content_hash"], "release_id": r["id"], "fencing_token": r["fencing_token"]}


def reject_serving(api, target, owner):
    for scope, kind in [(target, "release_set"), (owner, "policy_snapshot")]:
        for channel in ["gray", "full"]:
            if selected(api, scope, channel, kind).get("artifact"):
                raise ValueError("bootstrap cannot alter established serving authority")


def publish(config, api):
    config = validate(config)
    target, owner = config["producer"]["target_scope"], "platform-config-producer:" + config["authority_cell_id"]
    reject_serving(api, target, owner)
    before = selected(api, owner, "shadow")
    previous = authority_identity(before)
    producer = copy.deepcopy(config["producer"])
    if previous != config["expected_previous_policy"] and (before.get("artifact") or {}).get("generation") != producer["generation"]:
        raise ValueError("producer predecessor differs")
    if previous and before["artifact"].get("content", {}).get("mode") not in ["shadow", "paused"]:
        raise ValueError("bootstrap cannot replace serving producer")
    intent = ensure_input(api, "platform_intent", target, config["static_intent"])
    policy = ensure_input(api, "policy_snapshot", target, config["projection_policy"])
    producer.update(static_intent_artifact_id=intent["id"], static_intent_digest=intent["content_hash"], dns_policy_artifact_id=policy["id"], dns_policy_digest=policy["content_hash"])
    artifact = ensure_input(api, "policy_snapshot", owner, producer)
    preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + artifact["id"])
    if preview.get("intent", {}).get("scope") != target or preview["intent"].get("authority_cell_id") != config["authority_cell_id"] or preview.get("policy", {}).get("consumer_topology_digest") != policy["content"]["consumer_topology_digest"] or not preview.get("business_snapshot_revision"):
        raise ValueError("current business projection does not match declared cell")
    reject_serving(api, target, owner)
    latest = selected(api, owner, "shadow")
    if authority_identity(latest) != previous:
        raise ValueError("producer authority changed during preparation")
    if previous and before["artifact"]["id"] == artifact["id"]:
        return latest
    api("POST", "/v1/admin/artifacts/" + artifact["id"] + "/release", {"release_channel": "shadow", "idempotency_key": "git-cell-producer/" + artifact["content_hash"], "reason": "explicit isolated cell shadow enrollment; declaration " + digest(config)})
    after = selected(api, owner, "shadow")
    if not exact(after.get("artifact", {}), "policy_snapshot", owner, producer) or authority_identity(after) is None:
        raise ValueError("declared shadow authority not selected")
    return after


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--evidence")
    args = parser.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 1 << 20:
        raise ValueError("declaration exceeds bound")
    config = validate(json.loads(raw))
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(config)}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token or not args.evidence:
        raise ValueError("configuration credential and evidence path required")
    result = publish(config, API(config["origin"], token))
    evidence = {"schema": "fugue.cell-producer-bootstrap-result/v1", "declaration_digest": digest(config), "scope": config["producer"]["target_scope"], "mode": "shadow", "authority": authority_identity(result), "authorizes_traffic": False}
    Path(args.evidence).write_text(canonical(evidence)+"\n")
    print(canonical(evidence))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("cell producer bootstrap failed: " + str(error)) from None
