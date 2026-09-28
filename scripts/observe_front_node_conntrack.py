"""Read one kernel tuple through an explicitly declared existing node observer."""
import ipaddress
import json
from pathlib import Path
import re

try:
    from . import observe_front_candidate as front
    from . import read_front_conntrack as conntrack
except ImportError:
    import observe_front_candidate as front
    import read_front_conntrack as conntrack


CONFIG = Path("deploy/environments/production/front-connection-observation/observer.json")


def validate(config):
    if set(config) != {"schema", "namespace", "selector", "daemonSet", "container", "image", "hostRoot", "python"} or config["schema"] != "fugue.front-connection-observer/v1":
        raise ValueError("explicit node connection observer required")
    if any(not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", config[k]) for k in ["namespace", "daemonSet", "container"]):
        raise ValueError("canonical observer identity required")
    if not isinstance(config["selector"], dict) or not config["selector"] or any(not isinstance(k, str) or not isinstance(v, str) or any(c in k+v for c in "\n\r,= ") for k, v in config["selector"].items()):
        raise ValueError("explicit observer selector required")
    if not re.fullmatch(r"[a-zA-Z0-9./_-]+@sha256:[0-9a-f]{64}", config["image"]):
        raise ValueError("immutable observer image required")
    for key in ["hostRoot", "python"]:
        if not re.fullmatch(r"/(?:[a-zA-Z0-9_-]+/)*[a-zA-Z0-9_.-]+", config[key]) or any(p in [".", ".."] for p in Path(config[key]).parts):
            raise ValueError("explicit absolute observer paths required")
    return config


def observer(config, source):
    nodes = front.read("get", "nodes", "-o", "json")
    if nodes.get("metadata", {}).get("continue"):
        raise ValueError("node identity observation is incomplete")
    matches = [n for n in nodes["items"] if not n["metadata"].get("deletionTimestamp") and source in [a.get("address") for a in n.get("status", {}).get("addresses", [])]]
    if len(matches) != 1 or not matches[0]["metadata"].get("uid"):
        raise ValueError("held socket source does not identify exactly one node")
    node = matches[0]
    query = ",".join(k+"="+v for k, v in sorted(config["selector"].items()))
    listed = front.read("get", "pods", "-n", config["namespace"], "-l", query, "--field-selector", "spec.nodeName="+node["metadata"]["name"], "-o", "json")
    if listed.get("metadata", {}).get("continue") or len(listed["items"]) != 1:
        raise ValueError("node observer is absent or ambiguous")
    pod = listed["items"][0]
    meta, spec, status = pod["metadata"], pod["spec"], pod["status"]
    containers = [c for c in spec["containers"] if c["name"] == config["container"]]
    states = [c for c in status.get("containerStatuses", []) if c["name"] == config["container"]]
    if meta.get("deletionTimestamp") or not meta.get("uid") or meta.get("namespace") != config["namespace"] or spec.get("nodeName") != node["metadata"]["name"] or spec.get("hostPID") is not True or len(containers) != 1 or len(states) != 1:
        raise ValueError("node observer identity or host namespace differs")
    if status.get("phase") != "Running" or states[0].get("ready") is not True or states[0].get("imageID", "").removeprefix("docker-pullable://") != config["image"] or not any(o.get("kind") == "DaemonSet" and o.get("name") == config["daemonSet"] and o.get("controller") is True for o in meta.get("ownerReferences", [])):
        raise ValueError("node observer is not the declared Ready executor")
    container = containers[0]
    security = container.get("securityContext", {})
    mounts = [m for m in container.get("volumeMounts", []) if m.get("mountPath") == config["hostRoot"] and not m.get("subPath") and not m.get("subPathExpr")]
    if security.get("privileged") is not True or security.get("runAsUser") != 0 or len(mounts) != 1 or not any(v.get("name") == mounts[0]["name"] and v.get("hostPath", {}).get("path") == "/" for v in spec.get("volumes", [])):
        raise ValueError("existing observer cannot read the declared host namespace")
    return node, pod


def read(held, pod_ip, config=None):
    config = validate(config if config is not None else json.loads(CONFIG.read_text()))
    source, destination, candidate = (str(ipaddress.IPv4Address(x)) for x in [held.client_address, held.target_address, pod_ip])
    if any(type(p) is not int or not 1 <= p <= 65535 for p in [held.client_port, held.target_port]):
        raise ValueError("bounded held socket ports required")
    node, pod = observer(config, source)
    # Execute only the repository's fixed CT_GET reader, in memory. No file is
    # installed, no namespace is modified, and Python bytecode writes are off.
    program = Path(conntrack.__file__).read_text()
    result = front.read("-n", config["namespace"], "exec", pod["metadata"]["name"], "-c", config["container"], "--", "nsenter", "-t", "1", "-n", "--", "chroot", config["hostRoot"], config["python"], "-I", "-B", "-c", program, "--source", source, "--destination", destination, "--source-port", str(held.client_port), "--destination-port", str(held.target_port), "--pod", candidate)
    if set(result) != {"address", "port"} or type(result["port"]) is not int or not 1 <= result["port"] <= 65535:
        raise ValueError("node observer returned an invalid exact tuple")
    latest_node, latest_pod = observer(config, source)
    if latest_node["metadata"]["uid"] != node["metadata"]["uid"] or latest_pod["metadata"]["uid"] != pod["metadata"]["uid"] or latest_pod["spec"] != pod["spec"]:
        raise ValueError("node observer changed during exact tuple lookup")
    held.nat_observer = {"node_uid": node["metadata"]["uid"], "pod_uid": pod["metadata"]["uid"], "image": config["image"]}
    return str(ipaddress.IPv4Address(result["address"])), result["port"]
