"""Offline diagnostic only. No cluster client, credentials or write path.

Proves a narrow structural statement about a supplied Kubernetes snapshot,
not network failure, signed assignment, rollback safety, or release authority.
In particular this is NOT an alternate handoff_backend/failed_predecessor.
"""
import ipaddress
import re
from uuid import UUID


def require(ok, why):
    if not ok:
        raise ValueError(why)


def identity(obj):
    m = obj["metadata"]
    uid, rv = m["uid"], m["resourceVersion"]
    require(isinstance(uid, str) and str(UUID(uid)) == uid and UUID(uid).int != 0 and
            isinstance(rv, str) and re.fullmatch(r"[1-9][0-9]*", rv) and
            not m.get("deletionTimestamp"), "absent, invalid or terminating object identity")
    return (m["name"], uid, rv)


def only(values, why):
    require(isinstance(values, list) and len(values) == 1, why)
    return values[0]


def explicit_unready_snapshot(svc, slices, pods, node, declaration):
    """Validate one IPv4 local listener, one selected pod and one endpoint.

    Conservative: reject an empty, multi-family, paginated, duplicated or
    contradictory inventory. Return only identity fences, never authorization.
    """
    ns, address, socket, target = (declaration[k] for k in ("namespace", "address", "port", "targetPort"))
    selector = declaration["selector"]
    require(isinstance(ns, str) and ns and isinstance(selector, dict) and selector and
            all(isinstance(k, str) and k and isinstance(v, str) and v for k, v in selector.items()) and
            all(type(x) is int and 1 <= x <= 65535 for x in (socket, target)) and
            svc["apiVersion"] == "v1" and svc["kind"] == "Service" and
            svc["metadata"]["namespace"] == ns and svc["metadata"]["name"] == declaration["name"] and
            ipaddress.ip_address(address).version == 4, "not the declared IPv4 Service")
    s = svc["spec"]
    require(s.get("type") == "ClusterIP" and s.get("externalIPs") == [address] and
            s.get("externalTrafficPolicy") == "Local" and s.get("internalTrafficPolicy") == "Local" and
            s.get("publishNotReadyAddresses", False) is False and s.get("selector") == selector and
            len(s["ports"]) == 2 and {(x["protocol"], x["port"], x["targetPort"]) for x in s["ports"]} ==
            {("UDP", socket, target), ("TCP", socket, target)}, "socket, selector or readiness policy mismatch")
    name, uid, rv = identity(svc)
    require(node["apiVersion"] == "v1" and node["kind"] == "Node", "node identity is absent")
    node_id = identity(node)
    require(any(x["address"] == address for x in node["status"]["addresses"]) and
            only([x for x in node["status"]["conditions"] if x["type"] == "Ready"], "ambiguous node readiness")["status"] == "True",
            "node does not own the address or is not ready")
    for inventory, kind, api in ((slices, "EndpointSliceList", "discovery.k8s.io/v1"), (pods, "PodList", "v1")):
        require(inventory["kind"] == kind and inventory["apiVersion"] == api and
                not inventory["metadata"].get("continue") and not inventory["metadata"].get("remainingItemCount"),
                "incomplete or wrong inventory")
    pod = only(pods["items"], "requires one selected pod")
    pm = pod["metadata"]
    pod_id = identity(pod)
    require(pm.get("namespace") == ns and pod["spec"].get("nodeName") == node_id[0] and
            all(pm.get("labels", {}).get(k) == v for k, v in selector.items()) and
            pod["status"].get("phase") == "Running" and
            only([c for c in pod["status"]["conditions"] if c["type"] == "Ready"], "ambiguous pod readiness")["status"] == "False",
            "selected pod identity or readiness mismatch")
    pod_ip = only(pod["status"]["podIPs"], "ambiguous pod address")["ip"]
    require(ipaddress.ip_address(pod_ip).version == 4, "not an IPv4 pod")
    sl = only(slices["items"], "requires one complete EndpointSlice")
    sm = sl["metadata"]
    slice_id = identity(sl)
    owner = only(sm["ownerReferences"], "ambiguous slice owner")
    require(sm.get("namespace") == ns and sm.get("labels", {}).get("kubernetes.io/service-name") == name and
            owner.get("apiVersion") == "v1" and owner.get("kind") == "Service" and owner.get("name") == name and
            owner.get("uid") == uid and owner.get("controller") is True and sl.get("addressType") == "IPv4" and
            len(sl["ports"]) == 2 and {(p["protocol"], p["port"]) for p in sl["ports"]} ==
            {("UDP", target), ("TCP", target)}, "slice does not describe the declared listener")
    ep = only(sl["endpoints"], "ambiguous endpoint membership")
    ref, conditions = ep["targetRef"], ep["conditions"]
    require(ep.get("nodeName") == node_id[0] and ep.get("addresses") == [pod_ip] and
            ref.get("kind") == "Pod" and ref.get("name") == pod_id[0] and ref.get("namespace") == ns and
            ref.get("uid") == pod_id[1] and all(conditions.get(k) is False for k in ("ready", "serving", "terminating")),
            "missing, ambiguous, terminating or potentially serving endpoint")
    return {"schema": "fugue.dns.explicit-unready-diagnostic/v1", "authorizes_traffic": False,
            "namespace": ns, "address": address, "port": socket, "target_port": target, "pod_ip": pod_ip,
            "selector": dict(selector), "service": (name, uid, rv), "pod": pod_id, "slice": slice_id, "node": node_id}


def stable_observation(first, second):
    """Both argument tuples must be collected independently by the caller."""
    a, b = explicit_unready_snapshot(*first), explicit_unready_snapshot(*second)
    require(a == b, "resource versions or object identities changed")
    return a
