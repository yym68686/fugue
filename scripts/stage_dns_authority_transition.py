#!/usr/bin/env python3
"""Stage one explicit DNS authority transition from retained immutable inputs.

This configuration lane can compile and publish shadow only. It never selects
public transport, publishes gray/full, or attests a positive LKG. The API repeats
signature, current-reference and whole-route behavior checks.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import time
import urllib.error
import urllib.parse
import urllib.request

from scripts.bootstrap_cell_producer import authority_identity, digest, selected
from scripts.observe_front_candidate import now, timestamp
from scripts.publish_agent_edge_shadow import canonical, NoRedirect

IDENTITY = re.compile(r"[a-zA-Z0-9_.-]{1,256}")
HASH = re.compile(r"sha256:[a-f0-9]{64}")
CELL = re.compile(r"cell-[a-z0-9]+(?:-[a-z0-9]+)*")


class API:
    def __init__(self, origin, token):
        self.origin, self.token = origin, token
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

    def __call__(self, method, path, body=None):
        if not path.startswith("/v1/") or "\n" in path:
            raise ValueError("invalid artifact API path")
        encoded = None if body is None else canonical(body).encode()
        if encoded is not None and len(encoded) > 16 << 20:
            raise ValueError("DNS compiler request exceeds bound")
        request = urllib.request.Request(self.origin + path, method=method, data=encoded,
                                         headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
        try:
            with self.opener.open(request, timeout=45) as response:
                raw = response.read((16 << 20) + 1)
        except urllib.error.HTTPError as error:
            detail = error.read(8193).decode("utf-8", "replace")
            if len(detail) > 8192:
                detail = detail[:8192] + "…"
            # The control plane error body contains typed validation details,
            # not credentials. Keep it bounded so a rejected immutable compile
            # is actionable without dumping a large artifact or token.
            raise RuntimeError("artifact API returned HTTP " + str(error.code) + ": " + detail) from None
        except Exception:
            raise RuntimeError("artifact API transport unavailable") from None
        if len(raw) > 16 << 20:
            raise ValueError("DNS compiler response exceeds bound")
        return json.loads(raw)


def validate(config):
    dynamic = config.get("schema") in ["fugue.dns-authority-transition-stage/v2", "fugue.dns-authority-transition-stage/v3"]
    runtime_sources = config.get("schema") == "fugue.dns-authority-transition-stage/v3"
    route_field = "route_sources" if dynamic else "route_publications"
    required = {"schema", "generation", "origin", "authority_cell_id", "previous_topology", route_field, "dns_node_ids", "expected_previous_shadow", "capture"}
    if runtime_sources:
        required.add("global_producer_policy")
    if set(config) != required or config["schema"] not in ["fugue.dns-authority-transition-stage/v1", "fugue.dns-authority-transition-stage/v2", "fugue.dns-authority-transition-stage/v3"] or type(config["generation"]) is not int or config["generation"] < 1:
        raise ValueError("explicit versioned DNS shadow staging declaration required")
    if not CELL.fullmatch(config.get("authority_cell_id", "")):
        raise ValueError("neutral DNS authority required")
    origin = urllib.parse.urlsplit(config["origin"])
    if origin.scheme != "https" or config["origin"] != "https://" + str(origin.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(origin.hostname)):
        raise ValueError("canonical HTTPS artifact origin required")
    capture = config["capture"]
    if set(capture) != {"timeout_seconds", "poll_seconds", "max_source_age_seconds"} or any(type(v) is not int for v in capture.values()) or not 30 <= capture["timeout_seconds"] <= 900 or not 5 <= capture["poll_seconds"] <= 30 or not 30 <= capture["max_source_age_seconds"] <= 180:
        raise ValueError("bounded source capture required")
    nodes = config["dns_node_ids"]
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 16 or nodes != sorted(set(nodes)) or any(not IDENTITY.fullmatch(n) for n in nodes):
        raise ValueError("explicit unique physical DNS nodes required")
    previous = config["expected_previous_shadow"]
    if previous is not None and (set(previous) != {"artifact_id", "content_hash", "release_id", "fencing_token"} or any(not IDENTITY.fullmatch(previous[k]) for k in ["artifact_id", "release_id"]) or not HASH.fullmatch(previous["content_hash"]) or type(previous["fencing_token"]) is not int or previous["fencing_token"] < 1):
        raise ValueError("exact previous shadow publication required")
    topology = config["previous_topology"]
    if not isinstance(topology, dict) or topology.get("schema_version") != "edge-topology/v1" or set(topology) != {"schema_version", "authority_cells", "serving_pools", "edges"}:
        raise ValueError("complete explicit previous topology required")
    cells = topology["authority_cells"]
    if not 1 <= len(cells) <= 16 or any(set(c) != {"id", "legacy_group_id"} or not CELL.fullmatch(c["id"]) or not c["legacy_group_id"].startswith("edge-group-") for c in cells):
        raise ValueError("explicit old aliases for every routing cell required")
    aliases = {c["legacy_group_id"]: c["id"] for c in cells}
    if len(aliases) != len(cells) or len({c["id"] for c in cells}) != len(cells) or config["authority_cell_id"] in aliases.values():
        raise ValueError("routing and DNS authority must be unique and independent")
    refs = config[route_field]
    if dynamic:
        if not isinstance(refs, list) or [r.get("authority_cell_id") for r in refs] != sorted(aliases.values()):
            raise ValueError("each routing cell requires one explicit producer policy authority")
        for source in refs:
            if set(source) != {"authority_cell_id", "producer_policy"}:
                raise ValueError("only exact producer policy authorities may select current full publications")
            ref = source["producer_policy"]
            if not isinstance(ref, dict) or set(ref) != {"artifact_id", "content_hash", "release_id", "fencing_token"} or any(not IDENTITY.fullmatch(ref.get(k, "")) for k in ["artifact_id", "release_id"]) or not HASH.fullmatch(ref.get("content_hash", "")) or type(ref["fencing_token"]) is not int or ref["fencing_token"] < 1:
                raise ValueError("exact producer policy artifact, digest, release and fence required")
        if runtime_sources:
            ref = config["global_producer_policy"]
            if not isinstance(ref, dict) or set(ref) != {"artifact_id", "content_hash", "release_id", "fencing_token"} or any(not IDENTITY.fullmatch(ref.get(k, "")) for k in ["artifact_id", "release_id"]) or not HASH.fullmatch(ref.get("content_hash", "")) or type(ref["fencing_token"]) is not int or ref["fencing_token"] < 1:
                raise ValueError("exact transitional global producer policy required")
        refs = []
    fields = {"authority_cell_id", "release_set_id", "release_set_digest", "release_id", "release_channel", "fencing_token", "route_artifact_id", "route_artifact_digest", "tls_artifact_id", "tls_artifact_digest"}
    if not dynamic and (not isinstance(refs, list) or len(refs) != len(cells) or [r.get("authority_cell_id") for r in refs] != sorted(aliases.values())):
        raise ValueError("each declared cell requires one sorted exact full publication")
    for ref in refs:
        if set(ref) != fields or ref["release_channel"] != "full" or type(ref["fencing_token"]) is not int or ref["fencing_token"] < 1 or any(not HASH.fullmatch(ref[k]) for k in ["release_set_digest", "route_artifact_digest", "tls_artifact_digest"]) or any(not IDENTITY.fullmatch(ref[k]) for k in ["release_set_id", "release_id", "route_artifact_id", "tls_artifact_id"]):
            raise ValueError("exact full route and TLS pins required")
    edges = topology["edges"]
    if not isinstance(edges, list) or not 1 <= len(edges) <= 10000 or len({e.get("id") for e in edges}) != len(edges) or any(e.get("authority_cell_id") not in aliases.values() for e in edges):
        raise ValueError("unique declared physical Edge membership required")
    return config


def artifact(api, ident, kind, scope):
    value = api("GET", "/v1/admin/artifacts/" + ident)["artifact"]
    if value.get("id") != ident or value.get("artifact_kind") != kind or value.get("scope_key") != scope or value.get("status") != "validated" or digest(value["content"]) != value.get("content_hash"):
        raise ValueError("retained artifact identity, validation or digest differs")
    return value


def full_source(api, scope):
    state = selected(api, scope, "full", "release_set")
    a, release = state.get("artifact"), state.get("release")
    if not a or not release or release.get("verification_state") != "verified" or release.get("verified_lkg_generation") != a["generation"]:
        raise ValueError("source is not a verified full publication")
    lkg = api("GET", "/v1/admin/artifacts/" + a["id"] + "/lkg").get("lkg")
    if not lkg or lkg.get("artifact_id") != a["id"] or lkg.get("content_hash") != a["content_hash"] or lkg.get("verified_by_release_id") != release["id"] or timestamp(lkg["expires_at"]) <= now():
        raise ValueError("source positive LKG differs or expired")
    return state


def publication(api, state):
    a = artifact(api, state["artifact"]["id"], "release_set", state["artifact"]["scope_key"])
    if a["content_hash"] != state["artifact"]["content_hash"]:
        raise ValueError("source parent changed")
    kinds, ids = a["content"]["artifact_kinds"], a["content"]["artifact_ids"]
    if len(kinds) != len(ids) or len(set(kinds)) != len(kinds):
        raise ValueError("source composition ambiguous")
    return {"parent": a, "release": state["release"], "children": {kind: artifact(api, ident, kind, a["scope_key"]) for kind, ident in zip(kinds, ids)}}


def reference(pub, cell=None):
    a, r, children = pub["parent"], pub["release"], pub["children"]
    out = {"release_set_id": a["id"], "release_set_digest": a["content_hash"], "release_id": r["id"], "fencing_token": r["fencing_token"]}
    for name, kind in [("route", "edge_route_bundle"), ("tls", "caddy_route_config")]:
        out[name + "_artifact_id"], out[name + "_artifact_digest"] = children[kind]["id"], children[kind]["content_hash"]
    if cell:
        out.update(authority_cell_id=cell, release_channel="full")
    else:
        out["dns_artifact_id"], out["dns_artifact_digest"] = children["dns_answer_bundle"]["id"], children["dns_answer_bundle"]["content_hash"]
    return out


def authorize_route_source(api, source, parent):
    cell, expected = source["authority_cell_id"], source["producer_policy"]
    scope = "platform-config-producer:" + cell
    current = selected(api, scope, "shadow", "policy_snapshot")
    if authority_identity(current) != expected:
        raise ValueError("Cell producer policy authority changed")
    policy = artifact(api, expected["artifact_id"], "policy_snapshot", scope)
    content = policy["content"]
    if policy["content_hash"] != expected["content_hash"] or content.get("authority_cell_id") != cell or content.get("target_scope") != "authority-cell:" + cell or content.get("publication_role") != "cell-routes" or content.get("mode") != "serving" or content.get("serving", {}).get("single_publication") is not False:
        raise ValueError("continuous serving producer authority is required")
    if parent.get("scope_key") != "authority-cell:" + cell or parent.get("content", {}).get("publication_role") != "cell-routes" or parent.get("metadata", {}).get("producer_policy_release_id") != expected["release_id"]:
        raise ValueError("Cell full publication is not owned by the declared producer policy")


def authorize_global_source(api, expected, parent):
    current = selected(api, "platform-config-producer", "shadow", "policy_snapshot")
    if authority_identity(current) != expected:
        raise ValueError("global producer policy authority changed")
    policy = artifact(api, expected["artifact_id"], "policy_snapshot", "platform-config-producer")
    content = policy["content"]
    if policy["content_hash"] != expected["content_hash"] or content.get("target_scope") != "global" or content.get("publication_role", "") != "" or content.get("mode") != "serving" or content.get("serving", {}).get("single_publication", False) is not False:
        raise ValueError("continuous global serving producer required")
    if parent.get("scope_key") != "global" or parent.get("metadata", {}).get("producer_policy_release_id") != expected["release_id"]:
        raise ValueError("global publication is not owned by declared producer")


def runtime_source_approvals(config):
    if "global_producer_policy" not in config:
        return []
    bindings = [("authority-cell:" + item["authority_cell_id"], item["producer_policy"]) for item in config["route_sources"]]
    bindings.append(("global", config["global_producer_policy"]))
    return [{"scope_key": scope, "policy_artifact_id": pin["artifact_id"], "policy_digest": pin["content_hash"]} for scope, pin in bindings]


def resolve_route_publications(config, api):
    """Resolve intent selectors to concrete immutable references on each attempt."""
    cells = []
    for source in config.get("route_sources", config.get("route_publications", [])):
        cell = source["authority_cell_id"]
        pub = publication(api, full_source(api, "authority-cell:" + cell))
        if "route_sources" in config:
            authorize_route_source(api, source, pub["parent"])
        elif reference(pub, cell) != source:
            raise ValueError("Cell full publication differs from pinned declaration")
        cells.append(pub)
    return cells


def source_input(api, parent, kind, generation_key):
    generation = parent["content"]["lineage"][generation_key]
    rows = api("GET", "/v1/admin/artifacts?" + urllib.parse.urlencode({"kind": kind, "scope": parent["scope_key"], "generation": generation, "limit": 2})).get("artifacts", [])
    if len(rows) != 1 or rows[0].get("generation") != generation:
        raise ValueError("source input generation is missing or ambiguous")
    value = artifact(api, rows[0]["id"], kind, parent["scope_key"])
    key = "intent_digest" if kind == "platform_intent" else "policy_digest"
    if value.get("metadata", {}).get(key) != parent["content"]["lineage"][key]:
        raise ValueError("source input differs from signed parent lineage")
    return value["content"]


def compose(config, previous, cells, source_intent, source_policy, snapshot):
    """Mechanical projection only; API/compiler validates every resulting field."""
    aliases = {c["legacy_group_id"]: c["id"] for c in config["previous_topology"]["authority_cells"]}
    remap = lambda value: aliases.get(value, value)
    intent, policy, facts = copy.deepcopy(source_intent), copy.deepcopy(source_policy), copy.deepcopy(snapshot)
    cell = config["authority_cell_id"]
    intent.update(publication_role="cell-dns", authority_cell_id=cell, scope="authority-cell:" + cell, generation="dns-transition-intent-" + digest({"declaration": config, "previous": reference(previous)})[7:])
    for field in ["routes", "tls", "cache_policies", "application_domains"]:
        intent.pop(field, None)
    topology = copy.deepcopy(config["previous_topology"])
    topology["authority_cells"] = [{"id": c["id"]} for c in topology["authority_cells"]]
    intent["edge_topology"] = topology
    refs, inputs = [], []
    for pub in cells:
        authority = pub["parent"]["content"]["consumer_topology"]["authority_cell_id"]
        ref = reference(pub, authority)
        refs.append(ref)
        inputs.append({"reference": ref, "parent": pub["parent"], "route": pub["children"]["edge_route_bundle"], "tls": pub["children"]["caddy_route_config"]})
    if "global_producer_policy" in config:
        bindings = {item["authority_cell_id"]: item["producer_policy"] for item in config["route_sources"]}
        for item in inputs:
            item["producer_policy"] = copy.deepcopy(bindings[item["reference"]["authority_cell_id"]])
        intent["dns_route_sources"] = runtime_source_approvals(config)
    intent["cell_route_publications"] = refs
    if "route_sources" in config:
        intent["generation"] = "dns-transition-intent-" + digest({"declaration": config, "previous": reference(previous), "route_publications": refs})[7:]
    prior = {"reference": reference(previous), "parent": previous["parent"], "route": previous["children"]["edge_route_bundle"], "tls": previous["children"]["caddy_route_config"], "dns": previous["children"]["dns_answer_bundle"]}
    if "global_producer_policy" in config:
        prior["producer_policy"] = copy.deepcopy(config["global_producer_policy"])
    intent["route_authority_transition"] = {"previous_topology": copy.deepcopy(config["previous_topology"]), "previous_publication": prior["reference"]}
    if sorted(c["node_id"] for c in intent.get("dns_consumers", [])) != config["dns_node_ids"]:
        raise ValueError("DNS physical membership differs from explicit declaration")
    for consumer in intent["dns_consumers"]:
        consumer["edge_group_id"] = cell
    for record in intent.get("dns", []):
        for key in ["edge_group_id", "fallback_edge_group_id"]:
            if key in record:
                record[key] = remap(record[key])
    policy.update(publication_role="cell-dns", authority_cell_id=cell, scope=intent["scope"], generation="dns-transition-policy-" + digest(config)[7:])
    for field in ["route_constraints", "traffic_constraints", "tls_readiness"]:
        policy.pop(field, None)
    policy["traffic_rollout_cohorts"] = [{"id": "complete", "edge_group_ids": [cell]}]
    topology_encoding = {"publication_role": "cell-dns", "schema_version": "fugue.traffic-consumer-topology/v1", "authority_cell_id": cell, "edge_node_ids": [], "dns_node_ids": config["dns_node_ids"]}
    import hashlib
    policy["consumer_topology_digest"] = "sha256:" + hashlib.sha256(json.dumps(topology_encoding, separators=(",", ":")).encode()).hexdigest()
    for rule in policy.get("dns_answer_rules", []):
        for key in ["preferred_edge_groups", "fallback_edge_groups"]:
            if key in rule:
                rule[key] = [remap(g) for g in rule[key]]
    facts.update(intent_generation=intent["generation"], policy_generation=policy["generation"])
    for field in ["origins", "releases", "tls_domains", "dns_placements", "facts"]:
        facts.pop(field, None)
    members = {e["id"]: e["authority_cell_id"] for e in topology["edges"]}
    for endpoint in facts.get("dns_edge_endpoints", []):
        if members.get(endpoint["edge_id"]) != remap(endpoint["edge_group_id"]):
            raise ValueError("source endpoint lacks unchanged physical membership")
        endpoint["edge_group_id"] = members[endpoint["edge_id"]]
    for consumer in facts.get("dns_consumers", []):
        if consumer["node_id"] not in config["dns_node_ids"]:
            raise ValueError("source DNS observation has undeclared member")
        consumer["edge_group_id"] = cell
    for selection in facts.get("dns_selections", []):
        for scope in [selection] + selection.get("scoped_candidates", []):
            for key in ["selected_edge_group_id", "shadow_selected_edge_group_id"]:
                if key in scope:
                    scope[key] = remap(scope[key])
            for candidate in scope.get("candidates", []):
                if members.get(candidate["edge_id"]) != remap(candidate["edge_group_id"]):
                    raise ValueError("source selection has undeclared physical member")
                candidate["edge_group_id"] = members[candidate["edge_id"]]
    return {"intent": intent, "policy": policy, "runtime_snapshot": facts, "cell_route_publications": inputs, "previous_traffic_publication": prior}


def identity(state):
    a, r = state.get("artifact"), state.get("release")
    if not a and not r:
        return None
    if not a or not r:
        raise ValueError("incomplete publication state")
    return {"artifact_id": a["id"], "content_hash": a["content_hash"], "release_id": r["id"], "fencing_token": r["fencing_token"]}


def no_serving_authority(config, api):
    scope = "authority-cell:" + config["authority_cell_id"]
    for channel in ["gray", "full"]:
        if selected(api, scope, channel, "release_set").get("artifact"):
            raise ValueError("serving DNS authority exists; initial shadow staging cannot continue")


def stage(config, api, save):
    validate(config)
    scope = "authority-cell:" + config["authority_cell_id"]
    no_serving_authority(config, api)
    before = selected(api, scope, "shadow", "release_set")
    prefix = "dns-transition-shadow/" + digest(config) + "/"
    if before.get("artifact") and before.get("release", {}).get("idempotency_key") == prefix + before["artifact"]["content_hash"]:
        parent = artifact(api, before["artifact"]["id"], "release_set", scope)
        if parent["content"].get("publication_role") != "cell-dns" or parent["content"].get("artifact_kinds") != ["dns_answer_bundle"] or len(parent["content"].get("artifact_ids", [])) != 1:
            raise ValueError("completed DNS staging has foreign composition")
        dns = artifact(api, parent["content"]["artifact_ids"][0], "dns_answer_bundle", scope)
        source = dns["content"].get("cell_dns_source", {}).get("intent", {})
        if source.get("dns_route_sources", []) != runtime_source_approvals(config):
            raise ValueError("completed DNS runtime source approval differs")
        transition = source.get("route_authority_transition", {})
        refs = source.get("cell_route_publications", [])
        if source.get("authority_cell_id") != config["authority_cell_id"] or transition.get("previous_topology") != config["previous_topology"]:
            raise ValueError("completed DNS staging differs from declaration")
        if "route_sources" in config:
            if [ref.get("authority_cell_id") for ref in refs] != [item["authority_cell_id"] for item in config["route_sources"]]:
                raise ValueError("completed DNS staging has different routing authorities")
            for item, ref in zip(config["route_sources"], refs):
                route_parent = artifact(api, ref["release_set_id"], "release_set", "authority-cell:" + item["authority_cell_id"])
                if route_parent["content_hash"] != ref["release_set_digest"]:
                    raise ValueError("completed DNS route source digest differs")
                authorize_route_source(api, item, route_parent)
        elif refs != config["route_publications"]:
            raise ValueError("completed DNS staging differs from pinned route publications")
        api("POST", "/v1/admin/platform-config/release-set/prepare-consumers", {"release_set_id": parent["id"], "artifact_release_id": before["release"]["id"]})
        result = {"schema": "fugue.dns-authority-transition-stage-result/v1", "declaration_digest": digest(config), "release_set_id": parent["id"], "release_set_digest": parent["content_hash"], "dns_artifact_id": dns["id"], "dns_artifact_digest": dns["content_hash"], "shadow_release_id": before["release"]["id"], "fencing_token": before["release"]["fencing_token"], "previous_publication": transition["previous_publication"], "route_publications": refs, "public_transport_changed": False, "serving_published": False, "completed_at": now().isoformat(), "resumed": True}
        save(result)
        return result
    if identity(before) != config["expected_previous_shadow"]:
        raise ValueError("DNS shadow predecessor differs from declaration")
    cells = None if "route_sources" in config else resolve_route_publications(config, api)
    deadline, last_error = time.monotonic() + config["capture"]["timeout_seconds"], "no fresh complete source"
    while time.monotonic() < deadline:
        try:
            if "route_sources" in config:
                cells = resolve_route_publications(config, api)
            source = full_source(api, "global")
            age = (now() - timestamp(source["release"]["released_at"])).total_seconds()
            if not 0 <= age <= config["capture"]["max_source_age_seconds"]:
                raise ValueError("waiting for a fresh verified full source")
            previous = publication(api, source)
            if "global_producer_policy" in config:
                authorize_global_source(api, config["global_producer_policy"], previous["parent"])
            inputs = api("GET", "/v1/admin/artifacts/" + previous["parent"]["id"] + "/compiler-input")
            request = compose(config, previous, cells, source_input(api, previous["parent"], "platform_intent", "intent_generation"), source_input(api, previous["parent"], "policy_snapshot", "policy_generation"), inputs["runtime_snapshot"])
            save({"schema": "fugue.dns-authority-transition-stage-result/v1", "declaration_digest": digest(config), "previous_publication": request["previous_traffic_publication"]["reference"], "route_publications": request["intent"]["cell_route_publications"], "public_transport_changed": False, "serving_published": False, "captured_at": now().isoformat()})
            result = api("POST", "/v1/admin/platform-config/compile", request)
            break
        except (ValueError, RuntimeError) as error:
            last_error = str(error)
            source_mismatch = "route_sources" in config and "HTTP 400" in last_error and "authority transition changes compiled route behavior or hard constraints" in last_error
            if isinstance(error, RuntimeError) and "HTTP 409" not in last_error and not source_mismatch:
                raise
            time.sleep(config["capture"]["poll_seconds"])
    else:
        raise ValueError(last_error)
    parent, dns = result["release_artifact"], result["dns_artifact"]
    if result.get("route_artifact") or result.get("tls_artifact") or parent["content"].get("publication_role") != "cell-dns" or dns["content"].get("previous_traffic_publication") != request["previous_traffic_publication"]:
        raise ValueError("compiler output differs from DNS-only transition")
    evidence = {"schema": "fugue.dns-authority-transition-stage-result/v1", "declaration_digest": digest(config), "previous_publication": request["previous_traffic_publication"]["reference"], "route_publications": request["intent"]["cell_route_publications"], "release_set_id": parent["id"], "release_set_digest": parent["content_hash"], "dns_artifact_id": dns["id"], "dns_artifact_digest": dns["content_hash"], "public_transport_changed": False, "serving_published": False}
    save(evidence)
    no_serving_authority(config, api)
    if identity(selected(api, scope, "shadow", "release_set")) != identity(before):
        raise ValueError("DNS shadow authority changed while compiling")
    if "route_sources" in config:
        for item, pub in zip(config["route_sources"], cells):
            authorize_route_source(api, item, pub["parent"])
    if "global_producer_policy" in config:
        authorize_global_source(api, config["global_producer_policy"], previous["parent"])
    api("POST", "/v1/admin/artifacts/" + parent["id"] + "/release", {"release_channel": "shadow", "idempotency_key": "dns-transition-shadow/" + digest(config) + "/" + parent["content_hash"], "reason": "explicit DNS transition staging; no public transport or serving publication"})
    current = selected(api, scope, "shadow", "release_set")
    if current["artifact"]["id"] != parent["id"] or current["artifact"]["content_hash"] != parent["content_hash"]:
        raise ValueError("compiled DNS shadow is not current")
    api("POST", "/v1/admin/platform-config/release-set/prepare-consumers", {"release_set_id": parent["id"], "artifact_release_id": current["release"]["id"]})
    evidence.update(shadow_release_id=current["release"]["id"], fencing_token=current["release"]["fencing_token"], completed_at=now().isoformat())
    save(evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--evidence")
    args = parser.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 1 << 20:
        raise ValueError("DNS staging declaration exceeds bound")
    config = validate(json.loads(raw))
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(config)}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token or not args.evidence:
        raise ValueError("independent configuration writer and retained evidence required")
    def save(value):
        Path(args.evidence).write_text(canonical(value) + "\n")
    print(canonical(stage(config, API(config["origin"], token), save)))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("DNS shadow transition stopped: " + str(error)) from None
