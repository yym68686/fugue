"""Read-only observations shared by node-local release and participation recovery."""
import json
import os
import urllib.parse
import urllib.request

from scripts.configure_edge_release_hold import ANNOTATION, hold
from scripts import observe_front_candidate as front
from scripts import reconcile_front_probe_transport as kube
from scripts import replace_front_executor as replacement
from scripts import stage_front_serving_transport as stage

STATE = "fugue.io/automatic-edge-participation"


def items(value, limit=256):
    if value.get("metadata", {}).get("continue") or len(value["items"]) > limit:
        raise ValueError("Participation inventory is incomplete")
    return value["items"]


def node_ready(node):
    ready = [c["status"] for c in node.get("status", {}).get("conditions", []) if c["type"] == "Ready"]
    if len(ready) != 1 or ready[0] not in ("True", "False", "Unknown"):
        raise ValueError("Node readiness observation is missing or ambiguous")
    return ready[0] == "True"


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def read_edge_node(policy, member):
    key = os.environ.get("FUGUE_API_KEY", "")
    if not key:
        raise ValueError("Edge participation observation credential unavailable")
    path = "/v1/edge/nodes/" + urllib.parse.quote(member["node"], safe="")
    origin = policy.get("_local_origin", policy["origin"])
    # The installed controller must not use an ambient proxy or redirect
    # when sending its credential to the in-process API.
    req = urllib.request.Request(origin + path, headers={"Authorization": "Bearer " + key})
    with urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(req, timeout=15) as response:
        raw = response.read(131073)
    if len(raw) > 131072:
        raise ValueError("Edge participation observation exceeds bound")
    value = json.loads(raw)["node"]
    if value["id"] != member["node"] or value["edge_group_id"] != member["group"]:
        raise ValueError("Edge participation observation belongs to another node")
    return value


def serving_services(namespace):
    selector = "app.kubernetes.io/managed-by=" + stage.MANAGER
    return items(front.read("get", "services", "-n", namespace, "-l", selector, "-o", "json"))


def node_service(services, node):
    selected = [s for s in services if s["spec"].get("selector", {}).get("fugue.io/edge-node") == node
                and s["metadata"].get("annotations", {}).get("transport.fugue.dev/phase") == "serving"]
    if len(selected) > 1:
        raise ValueError("Multiple serving Front Services select the same node")
    return selected[0] if selected else None


def require_isolated_service(namespace, svc, node):
    """An Unknown Node or one failed HTTP probe alone is not a traffic fence."""
    meta, spec = svc["metadata"], svc["spec"]
    if (spec["selector"].get("fugue.io/edge-node") != node or
            meta["annotations"].get(stage.DIGEST) != kube.digest(replacement.projection(spec))):
        raise ValueError("Cannot isolate a peer without its exact node-local public transport")
    slices = items(front.read("get", "endpointslices", "-n", namespace,
                              "-l", "kubernetes.io/service-name=" + meta["name"], "-o", "json"))
    uids = []
    for item in slices:
        m = item["metadata"]
        if (not m.get("uid") or m.get("deletionTimestamp") or
                not any(o.get("controller") is True and o.get("kind") == "Service" and o.get("uid") == meta["uid"]
                        for o in m.get("ownerReferences", []))):
            raise ValueError("Offline peer EndpointSlice has foreign ownership")
        uids.append(m["uid"])
        for endpoint in item.get("endpoints", []):
            conditions = endpoint.get("conditions", {})
            # An omitted ready flag means ready per the Kubernetes contract.
            if (endpoint.get("nodeName") != node or conditions.get("ready") is not False or
                    conditions.get("serving") is True):
                raise ValueError("Offline peer still has a serving or unproven Front endpoint")
    return sorted(uids)


def isolation(policy, member, node, svc):
    """Require policy authorization, exact live hold, and verified DNS exclusion."""
    if member is None or node_ready(node):
        return None
    meta = node["metadata"]
    ann = meta.get("annotations", {})
    owned = json.loads(ann.get(ANNOTATION, "null"))
    # The current main policy is standing authorization. A missed CI tick
    # must not require a redundant Node annotation before a local handoff.
    # A conflicting/manual hold, however, still belongs to its own owner.
    if meta["uid"] != member["node_uid"] or ANNOTATION in ann and owned != hold(member):
        return None
    observed = read_edge_node(policy, member)
    o = observed.get("observation", {})
    intent = o.get("dns_admission_intent", {})
    if (o.get("heartbeat_fresh") is not False or o.get("reason") != "heartbeat_stale" or
            intent.get("state") != "suspended" or
            intent.get("serving_evidence") not in ("excluded_from_verified_full", "excluded_across_full_transition") or
            svc["spec"]["externalIPs"] != [observed.get("public_ipv4")]):
        return None
    slices = require_isolated_service(policy["namespace"], svc, member["node"])
    return {"state": "isolated", "node_uid": meta["uid"], "authorization": hold(member), "service_uid": svc["metadata"]["uid"],
            "service": svc["spec"], "endpoint_slices": slices}
