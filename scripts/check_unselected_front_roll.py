"""Fail closed before an explicitly authorized, unselected Front rollout."""
import argparse
import ipaddress
import json
from pathlib import Path
import subprocess

from scripts import retire_front_hostport as transport


def validate(config):
    require = transport.require
    require(type(config) is dict and set(config) == set(
        "schema namespace group node address daemonSet daemonSetUID daemonSetGeneration oldUID frontUID maxSessions serving probe".split()
    ) and config["schema"] == "fugue.unselected-front-roll/v1", "exact unselected Front declaration required")
    require(all(transport.NAME.fullmatch(config[key]) for key in ("namespace", "group", "node", "daemonSet"))
            and all(transport.UID.fullmatch(config[key]) for key in ("daemonSetUID", "oldUID", "frontUID"))
            and config["oldUID"] != config["frontUID"], "invalid Front identities")
    require(type(config["daemonSetGeneration"]) is int and config["daemonSetGeneration"] > 0
            and type(config["maxSessions"]) is int and config["maxSessions"] >= 0, "invalid rollout generation or session budget")
    address = ipaddress.ip_address(config["address"])
    require(address.version == 4 and address.is_global and str(address) == config["address"], "invalid public ingress address")
    for key in ("serving", "probe"):
        item = config[key]
        require(set(item) == {"name", "uid", "generation"} and transport.NAME.fullmatch(item["name"])
                and transport.UID.fullmatch(item["uid"]) and type(item["generation"]) is int and item["generation"] > 0,
                "exact ingress generations required")
    require(config["serving"]["uid"] != config["probe"]["uid"] and config["serving"]["name"] != config["probe"]["name"],
            "probe ingress is not independent")
    return config


def check_workload(config, workload, pods):
    require = transport.require
    meta, status, spec = workload["metadata"], workload["status"], workload["spec"]
    template = spec["template"]["spec"]
    require(meta["uid"] == config["daemonSetUID"] and meta["generation"] == config["daemonSetGeneration"]
            and not meta.get("deletionTimestamp") and status["observedGeneration"] == meta["generation"]
            and status["desiredNumberScheduled"] == status["numberReady"] == status["currentNumberScheduled"] == 1
            and spec["updateStrategy"]["type"] == "RollingUpdate"
            and spec["updateStrategy"]["rollingUpdate"]["maxSurge"] == 0
            and template["nodeSelector"]["kubernetes.io/hostname"] == config["node"]
            and not template.get("hostNetwork") and not transport.has_host_ports(template),
            "Front controller is not the exact, no-surge, unselected one-node workload")
    require(len(pods) == 1, "old Front selector does not have exactly one Pod")
    pod = pods[0]
    require(pod["metadata"]["uid"] == config["oldUID"] and not pod["metadata"].get("deletionTimestamp")
            and transport.owner(pod, config["daemonSet"], config["daemonSetUID"])
            and pod["spec"]["nodeName"] == config["node"] and pod["status"]["phase"] == "Running"
            and not pod["spec"].get("hostNetwork") and not transport.has_host_ports(pod["spec"]),
            "old Front Pod identity or transport changed")
    return pod


def check_sessions(raw, maximum):
    require = transport.require
    rows = [line.split() for line in raw.splitlines() if line.strip()]
    require(sum(row[:2] == ["sl", "local_address"] for row in rows) == 2
            and any(len(row) >= 4 and row[1].endswith(":0050") and row[3] == "0A" for row in rows)
            and any(len(row) >= 4 and row[1].endswith(":01BB") and row[3] == "0A" for row in rows),
            "complete TCP inventories and both Front listeners are required")
    sessions = transport.retirable_tcp(raw)
    require(sessions <= maximum, f"old Front has {sessions} sessions, exceeding authorized limit {maximum}")
    return sessions


def observe(config, cluster):
    workload = cluster.get("daemonset", config["daemonSet"])
    labels = workload["spec"]["selector"]["matchLabels"]
    transport.require(labels and not workload["spec"]["selector"].get("matchExpressions"), "ambiguous standby selector")
    selected = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
    old = check_workload(config, workload, cluster.list("pods", "-l", selected))
    cluster.no_worker_transaction()
    current, _ = cluster.ingress(workload["spec"]["template"]["metadata"]["labels"], old)
    # Verify the old Front after probing the current public and probe inventory,
    # as close to the write as possible. The direct writer also checks the
    # prepared resource identity and immutable predecessor.
    raw = subprocess.run(
        ["kubectl", "-n", config["namespace"], "exec", old["metadata"]["name"], "-c", "edge-front",
         "--", "cat", "/proc/net/tcp", "/proc/net/tcp6"],
        check=True, capture_output=True, text=True, timeout=12,
    ).stdout
    transport.require(len(raw) < 2 << 20, "old Front kernel TCP inventory is not bounded")
    result = check_sessions(raw, config["maxSessions"])
    return {"oldUID": config["oldUID"], "servingUID": current["metadata"]["uid"], "sessions": result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("declaration", type=Path)
    args = parser.parse_args()
    config = validate(json.loads(args.declaration.read_text()))
    print(json.dumps(observe(config, transport.Cluster(config)), sort_keys=True))


if __name__ == "__main__":
    main()
