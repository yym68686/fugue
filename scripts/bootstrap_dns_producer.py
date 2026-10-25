"""Enroll a DNS-only producer above an explicit existing positive baseline.

Uses the ordinary cell producer CI lane. No transport or workload is selected.
After activation the API's producer transactions own all candidate transitions.
"""
import re
import urllib.parse

from scripts.bootstrap_cell_producer import authority_identity, digest, ensure_input, exact, selected
from scripts.dns_publication_precondition import check, validate as validate_baseline, validate_ref
from scripts.stage_dns_authority_transition import public_api_connect_address

SCHEMA = "fugue.dns-producer-bootstrap/v1"


def validate(c):
    fields = {"schema", "generation", "origin", "authority_cell_id", "expected_previous_policy", "baseline", "producer"}
    if "api_connect_address" in c:
        fields.add("api_connect_address")
        public_api_connect_address(c["api_connect_address"])
    if set(c) != fields or c.get("schema") != SCHEMA or type(c.get("generation")) is not int or c["generation"] < 1 or c.get("expected_previous_policy") is not None:
        raise ValueError("initial DNS producer declaration with absent predecessor required")
    cell = c["authority_cell_id"]
    if not isinstance(cell, str) or not re.fullmatch(r"cell-[a-z0-9]+(?:-[a-z0-9]+)*", cell) or len(cell) > 128:
        raise ValueError("explicit neutral DNS authority required")
    origin = urllib.parse.urlsplit(c["origin"])
    if c["origin"] != "https://" + str(origin.hostname) or not re.fullmatch(r"[a-z0-9][a-z0-9.-]+[a-z0-9]", str(origin.hostname)):
        raise ValueError("canonical HTTPS origin required")
    validate_baseline(c["baseline"])
    if c["baseline"]["lanes"]["full"] != c["baseline"]["lkg_publication"]:
        raise ValueError("DNS enrollment requires the current full positive baseline")
    p = c["producer"]
    if not isinstance(p, dict) or set(p) != {"schema_version", "generation", "mode", "input_source", "publication_role", "authority_cell_id", "target_scope", "interval_seconds", "refresh_seconds", "serving", "dns_source"}:
        raise ValueError("explicit DNS-only producer settings required")
    if p["schema_version"] != "fugue.platform.producer/v1" or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,256}", p["generation"]) or p["mode"] != "serving" or p["input_source"] != "verified-global-dns" or p["publication_role"] != "cell-dns" or p["authority_cell_id"] != cell or p["target_scope"] != "authority-cell:" + cell:
        raise ValueError("producer must own only the declared independent DNS scope")
    if type(p["interval_seconds"]) is not int or type(p["refresh_seconds"]) is not int or not 30 <= p["interval_seconds"] <= 900 or not max(120, p["interval_seconds"]) <= p["refresh_seconds"] <= 3600:
        raise ValueError("bounded DNS scheduling required")
    v = p["serving"]
    if not isinstance(v, dict) or set(v) != {"canary_rule_ref", "gray_min_seconds", "full_min_seconds", "rollout_timeout_seconds"} or v["canary_rule_ref"] != "cohort=complete" or any(type(v[k]) is not int for k in ["gray_min_seconds", "full_min_seconds", "rollout_timeout_seconds"]) or not 60 <= v["gray_min_seconds"] <= 1800 or not 60 <= v["full_min_seconds"] <= 1800 or not max(v["gray_min_seconds"], v["full_min_seconds"]) + p["interval_seconds"] <= v["rollout_timeout_seconds"] <= 3600:
        raise ValueError("bounded complete-cohort serving observation required")
    source = p["dns_source"]
    if not isinstance(source, dict) or set(source) != {"producer_policy", "previous_topology", "dns_node_ids"}:
        raise ValueError("exact DNS source declaration required")
    validate_ref(source["producer_policy"])
    nodes = source["dns_node_ids"]
    if not isinstance(nodes, list) or not 1 <= len(nodes) <= 16 or nodes != sorted(set(nodes)) or any(not isinstance(n, str) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,256}", n) for n in nodes):
        raise ValueError("sorted explicit DNS physical members required")
    topology = source["previous_topology"]
    if not isinstance(topology, dict) or set(topology) != {"schema_version", "authority_cells", "serving_pools", "edges"} or topology["schema_version"] != "edge-topology/v1":
        raise ValueError("complete source topology required")
    return c


def key(c):
    return "git-dns-producer/" + digest(c)


def baseline(c, api):
    scope = c["producer"]["target_scope"]
    check(api, scope, c["baseline"])
    full = selected(api, scope, "full", "release_set")
    a, r = full["artifact"], full["release"]
    content = a.get("content", {})
    topology = content.get("consumer_topology", {})
    if r.get("verification_state") != "verified" or r.get("verified_lkg_generation") != a.get("generation") or content.get("publication_role") != "cell-dns" or content.get("artifact_kinds") != ["dns_answer_bundle"] or topology != {"schema_version": "fugue.traffic-consumer-topology/v1", "publication_role": "cell-dns", "authority_cell_id": c["authority_cell_id"], "edge_node_ids": [], "dns_node_ids": c["producer"]["dns_source"]["dns_node_ids"]}:
        raise ValueError("DNS baseline is not an exact verified independent authority")


def publish(c, api):
    validate(c)
    owner = "platform-config-producer:" + c["authority_cell_id"]
    for channel in ["gray", "full"]:
        if selected(api, owner, channel).get("artifact"):
            raise ValueError("DNS producer has an unexpected serving policy lane")
    before = selected(api, owner, "shadow")
    if before.get("artifact"):
        if not exact(before["artifact"], "policy_snapshot", owner, c["producer"]) or before.get("release", {}).get("idempotency_key") != key(c) or authority_identity(before) is None:
            raise ValueError("DNS producer already belongs to another declaration")
        return before
    baseline(c, api)
    pin = c["producer"]["dns_source"]["producer_policy"]
    if authority_identity(selected(api, "platform-config-producer", "shadow")) != pin:
        raise ValueError("approved global source producer changed")
    a = ensure_input(api, "policy_snapshot", owner, c["producer"])
    preview = api("GET", "/v1/admin/platform-config/routes/project?producer_policy_artifact_id=" + a["id"] + "&view=configuration")
    intent, policy = preview.get("intent", {}), preview.get("policy", {})
    for item in [intent, policy]:
        if item.get("publication_role") != "cell-dns" or item.get("authority_cell_id") != c["authority_cell_id"] or item.get("scope") != c["producer"]["target_scope"]:
            raise ValueError("DNS preview acquired a different authority")
    if intent.get("routes") or intent.get("tls") or sorted(n["node_id"] for n in intent.get("dns_consumers", [])) != c["producer"]["dns_source"]["dns_node_ids"]:
        raise ValueError("DNS preview contains route execution or wrong physical membership")
    baseline(c, api)
    if authority_identity(selected(api, owner, "shadow")) is not None or authority_identity(selected(api, "platform-config-producer", "shadow")) != pin:
        raise ValueError("producer authority changed during preparation")
    api("POST", "/v1/admin/artifacts/" + a["id"] + "/release", {"release_channel": "shadow", "idempotency_key": key(c), "reason": "explicit DNS-only producer enrollment above verified baseline; declaration " + digest(c)})
    after = selected(api, owner, "shadow")
    if not exact(after.get("artifact", {}), "policy_snapshot", owner, c["producer"]) or after.get("release", {}).get("idempotency_key") != key(c) or authority_identity(after) is None:
        raise ValueError("DNS producer activation did not select the exact policy")
    return after
