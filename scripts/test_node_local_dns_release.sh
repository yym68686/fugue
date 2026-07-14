#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

export MOCK_LOG="${TMP_DIR}/kubectl.log"
export MOCK_APPLY="${TMP_DIR}/applied.json"
export MOCK_PROBE_SCRIPTS="${TMP_DIR}/probe-scripts.log"
export MOCK_DIG_LOG="${TMP_DIR}/dig.log"
export MOCK_STATE="${TMP_DIR}/state.json"
MOCK_STATE_CTL="${TMP_DIR}/statectl"
MOCK_KUBECTL="${TMP_DIR}/kubectl"

cat >"${MOCK_STATE_CTL}" <<'PY'
#!/usr/bin/env python3
import json
import sys

path = sys.argv[1]
action = sys.argv[2]


def base_state():
    return {
        "counter": 0,
        "ds": {
            "exists": False,
            "layout": "current",
            "mode": "shadow",
            "targets": [],
            "revision": "rev-none",
            "generation": 1,
            "observed_generation": 1,
        },
        "pods": [],
        "missing_nodes": [],
        "failure_modes": {},
        "hostports": {},
        "node_overrides": {},
        "notready_after_delete": {},
    }


def load():
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def save(state):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, separators=(",", ":"))


def create_pod(state, node, revision=None, mode=None, layout=None):
    state["counter"] += 1
    ds = state["ds"]
    selected_mode = mode or ds["mode"]
    selected_layout = layout or ds["layout"]
    failure_mode = state["failure_modes"].get(node, "")
    pod = {
        "name": f"fugue-fugue-node-local-dns-{node}-{state['counter']}",
        "uid": f"uid-{node}-{state['counter']}",
        "node": node,
        "mode": selected_mode,
        "revision": revision or ds["revision"],
        "ready": True,
        "phase": "Running",
        "restart_count": 0,
        "state": "running",
        "state_reason": "",
        "last_state": "",
        "last_state_reason": "",
        "config_key": "Corefile" if selected_layout == "legacy" else f"Corefile.{selected_mode}",
    }
    if failure_mode == "crash":
        pod.update({"ready": False, "restart_count": 7, "state": "waiting", "state_reason": "CrashLoopBackOff"})
    elif failure_mode == "oom":
        pod.update({
            "ready": False,
            "restart_count": 1,
            "state": "waiting",
            "state_reason": "ContainerCreating",
            "last_state": "terminated",
            "last_state_reason": "OOMKilled",
        })
    elif failure_mode == "failed":
        pod.update({"ready": False, "phase": "Failed", "state": "terminated", "state_reason": "Error"})
    return pod


def reconcile(state):
    ds = state["ds"]
    if not ds["exists"]:
        state["pods"] = []
        return
    targets = set(ds["targets"])
    state["pods"] = [pod for pod in state["pods"] if pod["node"] in targets]
    present = {pod["node"] for pod in state["pods"]}
    for node in ds["targets"]:
        if node not in present and node not in state["missing_nodes"]:
            state["pods"].append(create_pod(state, node))


if action == "reset":
    save(base_state())
    raise SystemExit(0)

state = load()
if action == "seed":
    layout, mode, targets_raw, ds_revision = sys.argv[3:7]
    targets = [item for item in targets_raw.split(",") if item]
    revisions = [item for item in (sys.argv[7] if len(sys.argv) > 7 else ds_revision).split(",") if item]
    if len(revisions) == 1 and len(targets) > 1:
        revisions *= len(targets)
    if len(revisions) != len(targets):
        raise SystemExit("pod revision count must equal target count")
    state = base_state()
    state["ds"].update({
        "exists": True,
        "layout": layout,
        "mode": mode,
        "targets": targets,
        "revision": ds_revision,
    })
    state["pods"] = [create_pod(state, node, revision, mode, layout) for node, revision in zip(targets, revisions)]
elif action == "template":
    layout, mode, targets_raw, revision = sys.argv[3:7]
    ds = state["ds"]
    ds.update({
        "exists": True,
        "layout": layout,
        "mode": mode,
        "targets": [item for item in targets_raw.split(",") if item],
        "revision": revision,
        "generation": int(ds.get("generation") or 0) + 1,
    })
    ds["observed_generation"] = ds["generation"]
    reconcile(state)
elif action == "failure":
    kind, nodes_raw = sys.argv[3:5]
    nodes = [item for item in nodes_raw.split(",") if item]
    if kind == "missing":
        state["missing_nodes"] = nodes
    elif kind in {"crash", "oom", "failed"}:
        for node in nodes:
            state["failure_modes"][node] = kind
    else:
        raise SystemExit(f"unknown Pod failure mode: {kind}")
elif action == "clear-failures":
    state["missing_nodes"] = []
    state["failure_modes"] = {}
elif action == "hostports":
    node, mode = sys.argv[3:5]
    if mode == "none":
        state["hostports"].pop(node, None)
    else:
        state["hostports"][node] = {"mode": mode, "uid": f"auth-{node}", "ready": True, "restarts": 0}
elif action == "hostport-drift":
    node, field, value = sys.argv[3:6]
    record = state["hostports"][node]
    if field == "restarts":
        record[field] = int(value)
    elif field == "ready":
        record[field] = value == "true"
    elif field == "uid":
        record[field] = value
    else:
        raise SystemExit(f"unknown hostPort drift field: {field}")
elif action == "pod-revision":
    node, revision = sys.argv[3:5]
    matches = [pod for pod in state["pods"] if pod["node"] == node]
    if len(matches) != 1:
        raise SystemExit(f"expected one Pod on {node}")
    matches[0]["revision"] = revision
elif action == "pod-health":
    node, field, value = sys.argv[3:6]
    matches = [pod for pod in state["pods"] if pod["node"] == node]
    if len(matches) != 1:
        raise SystemExit(f"expected one Pod on {node}")
    pod = matches[0]
    if field == "restart_count":
        pod[field] = int(value)
    elif field == "ready":
        pod[field] = value == "true"
    elif field in {"phase", "state", "state_reason", "last_state", "last_state_reason"}:
        pod[field] = value
    else:
        raise SystemExit(f"unknown Pod health field: {field}")
elif action == "node-ready":
    node, ready = sys.argv[3:5]
    state["node_overrides"].setdefault(node, {})["ready"] = ready == "true"
elif action == "node-condition":
    node, condition, value = sys.argv[3:6]
    if condition not in {"MemoryPressure", "DiskPressure", "PIDPressure"}:
        raise SystemExit(f"unknown node condition: {condition}")
    normalized = {"true": "True", "false": "False"}.get(value.lower(), value)
    state["node_overrides"].setdefault(node, {}).setdefault("conditions", {})[condition] = normalized
elif action == "notready-after-delete":
    trigger_node, target_node = sys.argv[3:5]
    state["notready_after_delete"][trigger_node] = target_node
elif action == "remove-pod":
    node = sys.argv[3]
    state["pods"] = [pod for pod in state["pods"] if pod["node"] != node]
elif action == "dump":
    print(json.dumps(state, separators=(",", ":")))
    raise SystemExit(0)
else:
    raise SystemExit(f"unknown state action: {action}")

save(state)
PY
chmod +x "${MOCK_STATE_CTL}"

cat >"${MOCK_KUBECTL}" <<'PY'
#!/usr/bin/env python3
import copy
import json
import os
import shlex
import sys

argv = sys.argv[1:]
with open(os.environ["MOCK_LOG"], "a", encoding="utf-8") as handle:
    handle.write(shlex.join(argv) + "\n")


def load_state():
    with open(os.environ["MOCK_STATE"], encoding="utf-8") as handle:
        return json.load(handle)


def save_state(state):
    with open(os.environ["MOCK_STATE"], "w", encoding="utf-8") as handle:
        json.dump(state, handle, separators=(",", ":"))


def create_pod(state, node):
    state["counter"] += 1
    ds = state["ds"]
    failure_mode = state["failure_modes"].get(node, "")
    pod = {
        "name": f"fugue-fugue-node-local-dns-{node}-{state['counter']}",
        "uid": f"uid-{node}-{state['counter']}",
        "node": node,
        "mode": ds["mode"],
        "revision": ds["revision"],
        "ready": True,
        "phase": "Running",
        "restart_count": 0,
        "state": "running",
        "state_reason": "",
        "last_state": "",
        "last_state_reason": "",
        "config_key": "Corefile" if ds["layout"] == "legacy" else f"Corefile.{ds['mode']}",
    }
    if failure_mode == "crash":
        pod.update({"ready": False, "restart_count": 7, "state": "waiting", "state_reason": "CrashLoopBackOff"})
    elif failure_mode == "oom":
        pod.update({
            "ready": False,
            "restart_count": 1,
            "state": "waiting",
            "state_reason": "ContainerCreating",
            "last_state": "terminated",
            "last_state_reason": "OOMKilled",
        })
    elif failure_mode == "failed":
        pod.update({"ready": False, "phase": "Failed", "state": "terminated", "state_reason": "Error"})
    return pod


def reconcile(state):
    ds = state["ds"]
    if not ds["exists"]:
        state["pods"] = []
        return
    targets = set(ds["targets"])
    state["pods"] = [pod for pod in state["pods"] if pod["node"] in targets]
    present = {pod["node"] for pod in state["pods"]}
    for node in ds["targets"]:
        if node not in present and node not in state["missing_nodes"]:
            state["pods"].append(create_pod(state, node))


NODE_DEFS = {
    "node-a": ("203.0.113.11", {}),
    "node-b": ("203.0.113.12", {}),
    "node-c": ("203.0.113.13", {}),
    "node-edge": ("198.51.100.20", {"fugue.io/role.edge": "true", "fugue.io/role.dns": "true"}),
    "node-control": ("192.0.2.10", {"node-role.kubernetes.io/control-plane": "true"}),
    "dns-us": ("203.0.113.10", {"fugue.io/role.dns": "true", "fugue.io/schedulable": "true", "fugue.io/location-country-code": "us"}),
    "dns-de": ("198.51.100.20", {"fugue.io/role.dns": "true", "fugue.io/schedulable": "true", "fugue.io/location-country-code": "de"}),
}


def node_json(name, external_ip=None, ready=None):
    default_ip, extra_labels = NODE_DEFS[name]
    overrides = state.get("node_overrides", {}).get(name, {})
    if ready is None:
        ready = overrides.get("ready", True)
    labels = {"kubernetes.io/os": "linux", "kubernetes.io/arch": "amd64", "kubernetes.io/hostname": name}
    labels.update(extra_labels)
    condition_overrides = overrides.get("conditions", {})
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {"name": name, "labels": labels},
        "spec": {"unschedulable": bool(overrides.get("unschedulable", False))},
        "status": {
            "conditions": [
                {"type": "Ready", "status": "True" if ready else "False"},
                {"type": "MemoryPressure", "status": condition_overrides.get("MemoryPressure", "False")},
                {"type": "DiskPressure", "status": condition_overrides.get("DiskPressure", "False")},
                {"type": "PIDPressure", "status": condition_overrides.get("PIDPressure", "False")},
            ],
            "addresses": [{"type": "ExternalIP", "address": external_ip or default_ip}],
        },
    }


def pod_json(pod):
    def container_state(kind, reason):
        if kind == "running":
            return {"running": {"startedAt": "2026-07-13T00:00:00Z"}}
        if kind == "waiting":
            return {"waiting": {"reason": reason}}
        if kind == "terminated":
            return {"terminated": {"reason": reason, "exitCode": 1}}
        return {}

    condition = {"type": "Ready", "status": "True" if pod["ready"] else "False"}
    status = {
        "phase": pod["phase"],
        "conditions": [condition],
        "containerStatuses": [{
            "name": "node-cache",
            "restartCount": pod["restart_count"],
            "ready": pod["ready"],
            "state": container_state(pod.get("state", "running"), pod.get("state_reason", "")),
            "lastState": container_state(pod.get("last_state", ""), pod.get("last_state_reason", "")),
        }],
    }
    listen_ips = "169.254.20.10" if pod["mode"] == "shadow" else "169.254.20.10,10.43.0.10"
    container = {
        "name": "node-cache",
        "image": "registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739",
        "imagePullPolicy": "IfNotPresent",
        "args": ["-localip", listen_ips, "-conf", "/etc/Corefile", "-upstreamsvc", "fugue-fugue-dns-upstream"],
        "securityContext": {"capabilities": {"add": ["NET_ADMIN"]}},
        "volumeMounts": [
            {"name": "xtables-lock", "mountPath": "/run/xtables.lock"},
            {"name": "config-volume", "mountPath": "/etc/coredns"},
        ],
    }
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": pod["name"],
            "namespace": "kube-system",
            "uid": pod["uid"],
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "fugue-fugue-node-local-dns", "uid": "uid-ds", "controller": True}],
            "labels": {
                "app.kubernetes.io/name": "fugue",
                "app.kubernetes.io/instance": "fugue",
                "app.kubernetes.io/component": "node-local-dns",
                "fugue.io/node-local-dns-mode": pod["mode"],
                "fugue.io/node-local-dns-cohort": "active",
                "controller-revision-hash": pod["revision"],
            },
        },
        "spec": {
            "nodeName": pod["node"],
            "hostNetwork": True,
            "dnsPolicy": "Default",
            "priorityClassName": "fugue-fugue-node-local-dns",
            "serviceAccountName": "fugue-fugue-node-local-dns",
            "automountServiceAccountToken": False,
            "containers": [container],
            "volumes": [
                {"name": "xtables-lock", "hostPath": {"path": "/run/xtables.lock", "type": "FileOrCreate"}},
                {"name": "config-volume", "configMap": {"name": "fugue-fugue-node-local-dns", "items": [{"key": pod["config_key"], "path": "Corefile.base"}]}},
            ],
        },
        "status": status,
    }


def corefile(bind_ips):
    return f"""cluster.local:53 {{
  errors
  cache {{
    success 9984 30
    denial 9984 5
  }}
  reload
  loop
  bind {bind_ips}
  forward . __PILLAR__CLUSTER__DNS__ {{
    force_tcp
  }}
  prometheus :9253
  health 169.254.20.10:8080
}}
in-addr.arpa:53 {{
  errors
  cache 30
  reload
  loop
  bind {bind_ips}
  forward . __PILLAR__CLUSTER__DNS__ {{
    force_tcp
  }}
  prometheus :9253
}}
ip6.arpa:53 {{
  errors
  cache 30
  reload
  loop
  bind {bind_ips}
  forward . __PILLAR__CLUSTER__DNS__ {{
    force_tcp
  }}
  prometheus :9253
}}
.:53 {{
  errors
  cache 30
  reload
  loop
  bind {bind_ips}
  forward . __PILLAR__CLUSTER__DNS__ {{
    force_tcp
  }}
  prometheus :9253
}}
"""


def daemonset_json(state):
    ds = state["ds"]
    targets = ds["targets"]
    listen_ips = "169.254.20.10" if ds["mode"] == "shadow" else "169.254.20.10,10.43.0.10"
    selector = {"kubernetes.io/os": "linux"}
    affinity = None
    update_strategy = {"type": "OnDelete"}
    config_key = f"Corefile.{ds['mode']}"
    if ds["layout"] == "legacy":
        selector["kubernetes.io/hostname"] = targets[0]
        update_strategy = {"type": "RollingUpdate"}
        config_key = "Corefile"
    else:
        affinity = {
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [{"matchExpressions": [{"key": "kubernetes.io/hostname", "operator": "In", "values": targets}]}]
                }
            }
        }
    image = "registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739"
    container = {
        "name": "node-cache",
        "image": image,
        "imagePullPolicy": "IfNotPresent",
        "args": ["-localip", listen_ips, "-conf", "/etc/Corefile", "-upstreamsvc", "fugue-fugue-dns-upstream"],
        "securityContext": {"capabilities": {"add": ["NET_ADMIN"]}},
        "volumeMounts": [
            {"name": "xtables-lock", "mountPath": "/run/xtables.lock"},
            {"name": "config-volume", "mountPath": "/etc/coredns"},
        ],
    }
    drift = os.environ.get("MOCK_ARTIFACT_DRIFT", "")
    mode_label = ds["mode"]
    if drift == "image":
        container["image"] = "registry.k8s.io/dns/k8s-dns-node-cache@sha256:" + "0" * 64
    elif drift == "args":
        container["args"][1] = "169.254.20.11"
    elif drift == "capabilities":
        container["securityContext"]["capabilities"]["add"] = []
    elif drift == "ports":
        container["ports"] = [{"containerPort": 53, "protocol": "UDP"}]
    elif drift == "mode-label":
        mode_label = "iptables" if ds["mode"] == "shadow" else "shadow"
    pod_spec = {
        "automountServiceAccountToken": False,
        "containers": [container],
        "dnsPolicy": "Default",
        "hostNetwork": True,
        "nodeSelector": selector,
        "priorityClassName": "fugue-fugue-node-local-dns",
        "serviceAccountName": "fugue-fugue-node-local-dns",
        "volumes": [
            {"name": "xtables-lock", "hostPath": {"path": "/run/xtables.lock", "type": "FileOrCreate"}},
            {"name": "config-volume", "configMap": {"name": "fugue-fugue-node-local-dns", "items": [{"key": config_key, "path": "Corefile.base"}]}},
        ],
    }
    if affinity is not None:
        pod_spec["affinity"] = affinity
    if drift == "affinity-extra":
        pod_spec["affinity"]["podAffinity"] = {}
    elif drift == "affinity-operator":
        pod_spec["affinity"]["nodeAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"]["nodeSelectorTerms"][0]["matchExpressions"][0]["operator"] = "NotIn"
    elif drift == "selector-extra":
        pod_spec["nodeSelector"]["fugue.io/unexpected"] = "true"
    if drift == "host-network":
        pod_spec["hostNetwork"] = False
    elif drift == "dns-policy":
        pod_spec["dnsPolicy"] = "ClusterFirst"
    ready = sum(1 for pod in state["pods"] if pod["node"] in targets and pod["ready"])
    status = {
        "observedGeneration": ds["observed_generation"],
        "desiredNumberScheduled": len(targets),
        "numberReady": ready,
    }
    if len(targets) - ready:
        status["numberUnavailable"] = len(targets) - ready
    return {
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {"name": "fugue-fugue-node-local-dns", "namespace": "kube-system", "uid": "uid-ds", "generation": ds["generation"], "labels": {}, "annotations": {}},
        "spec": {"updateStrategy": update_strategy, "template": {"metadata": {"labels": {
            "app.kubernetes.io/component": "node-local-dns",
            "fugue.io/node-local-dns-mode": mode_label,
            "fugue.io/node-local-dns-cohort": "active",
        }}, "spec": pod_spec}},
        "status": status,
    }


def controller_revision_list_json(state):
    ds = state["ds"]
    daemonset = daemonset_json(state)
    revision_hash = ds["revision"]
    template = copy.deepcopy(daemonset["spec"]["template"])
    template["$patch"] = "replace"
    item = {
        "apiVersion": "apps/v1",
        "kind": "ControllerRevision",
        "metadata": {
            "name": daemonset["metadata"]["name"] + "-" + revision_hash,
            "namespace": "kube-system",
            "labels": {
                "app.kubernetes.io/name": "fugue",
                "app.kubernetes.io/instance": "fugue",
                "app.kubernetes.io/component": "node-local-dns",
                "controller-revision-hash": revision_hash,
            },
            "ownerReferences": [{
                "apiVersion": "apps/v1",
                "kind": "DaemonSet",
                "name": daemonset["metadata"]["name"],
                "uid": daemonset["metadata"]["uid"],
                "controller": True,
            }],
        },
        "revision": 1,
        "data": {"spec": {"template": template}},
    }
    drift = os.environ.get("MOCK_CONTROLLER_REVISION_DRIFT", "")
    if drift == "owner":
        item["metadata"]["ownerReferences"][0]["uid"] = "uid-other-ds"
    elif drift == "template":
        item["data"]["spec"]["template"]["metadata"].setdefault("annotations", {})["test.fugue.io/drift"] = "true"
    elif drift == "hash":
        item["metadata"]["labels"]["controller-revision-hash"] = revision_hash + "-mismatch"
    elif drift == "ambiguous":
        duplicate = copy.deepcopy(item)
        duplicate_hash = revision_hash + "-duplicate"
        duplicate["metadata"]["name"] = daemonset["metadata"]["name"] + "-" + duplicate_hash
        duplicate["metadata"]["labels"]["controller-revision-hash"] = duplicate_hash
        return {"apiVersion": "apps/v1", "kind": "ControllerRevisionList", "items": [item, duplicate]}
    elif drift:
        raise SystemExit(f"unsupported ControllerRevision drift: {drift}")
    return {"apiVersion": "apps/v1", "kind": "ControllerRevisionList", "items": [item]}


state = load_state()
command = next((item for item in argv if item in {"get", "apply", "delete", "exec", "logs", "describe", "rollout"}), "")

if command == "apply" and "-f" in argv:
    payload_raw = sys.stdin.read()
    with open(os.environ["MOCK_APPLY"], "w", encoding="utf-8") as handle:
        handle.write(payload_raw)
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        payload = {}
    if payload.get("kind") == "Pod":
        script = (((payload.get("spec") or {}).get("containers") or [{}])[0].get("command") or ["", "", ""])[-1]
        with open(os.environ["MOCK_PROBE_SCRIPTS"], "a", encoding="utf-8") as handle:
            handle.write(script + "\n--- probe ---\n")
        name = str((payload.get("metadata") or {}).get("name") or "")
        if any("metadata.uid" in item for item in argv):
            sys.stdout.write(f"{name}|uid-probe-{name}")
    elif payload.get("kind") == "DaemonSet":
        template = ((payload.get("spec") or {}).get("template") or {})
        pod_spec = template.get("spec") or {}
        labels = (template.get("metadata") or {}).get("labels") or {}
        selector = pod_spec.get("nodeSelector") or {}
        legacy_target = selector.get("kubernetes.io/hostname")
        if legacy_target:
            layout = "legacy"
            targets = [legacy_target]
        else:
            layout = "current"
            terms = (((pod_spec.get("affinity") or {}).get("nodeAffinity") or {}).get("requiredDuringSchedulingIgnoredDuringExecution") or {}).get("nodeSelectorTerms") or []
            matches = [
                expression.get("values") or []
                for term in terms
                for expression in term.get("matchExpressions") or []
                if expression.get("key") == "kubernetes.io/hostname" and expression.get("operator") == "In"
            ]
            if len(matches) != 1:
                raise SystemExit(1)
            targets = matches[0]
        state["ds"].update({
            "exists": True,
            "layout": layout,
            "mode": labels.get("fugue.io/node-local-dns-mode"),
            "targets": targets,
            "revision": f"rev-restored-{state['counter'] + 1}",
            "generation": int(state["ds"].get("generation") or 0) + 1,
        })
        state["ds"]["observed_generation"] = state["ds"]["generation"]
        reconcile(state)
        save_state(state)
    raise SystemExit(0)

if command == "get" and "service" in argv:
    if any("jsonpath=" in item for item in argv):
        sys.stdout.write("10.43.0.10")
    else:
        print('{"spec":{"clusterIP":"10.43.0.10","selector":{"k8s-app":"kube-dns"}}}')
    raise SystemExit(0)

if command == "get" and "endpointslices.discovery.k8s.io" in argv:
    print('{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","items":[{"addressType":"IPv4","endpoints":[{"addresses":["10.42.0.2"],"conditions":{"ready":true}}]}]}')
    raise SystemExit(0)

if command == "get" and "configmap" in argv:
    resource = argv[argv.index("configmap") + 1]
    if resource == "coredns":
        sys.stdout.write(".:53 { kubernetes cluster.local in-addr.arpa ip6.arpa }")
        raise SystemExit(0)
    if resource == "fugue-fugue-node-local-dns":
        ds = state["ds"]
        shadow = corefile("169.254.20.10")
        iptables = corefile("169.254.20.10 10.43.0.10")
        selected = shadow if ds["mode"] == "shadow" else iptables
        if ds["layout"] == "legacy":
            data = {"Corefile": selected}
        else:
            data = {"Corefile": selected, "Corefile.shadow": shadow, "Corefile.iptables": iptables}
        if os.environ.get("MOCK_COREFILE_DRIFT") == "true":
            data["Corefile"] = data["Corefile"].replace("force_tcp", "prefer_udp", 1)
        print(json.dumps({"data": data}, separators=(",", ":")))
        raise SystemExit(0)

if command == "get" and "controllerrevisions.apps" in argv:
    if not state["ds"]["exists"]:
        print('{"apiVersion":"apps/v1","kind":"ControllerRevisionList","items":[]}')
    else:
        print(json.dumps(controller_revision_list_json(state), separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and "daemonsets" in argv:
    print('{"apiVersion":"apps/v1","kind":"DaemonSetList","items":[]}')
    raise SystemExit(0)

if command == "get" and ("daemonset" in argv or any(item.startswith("ds/") for item in argv)):
    ds = state["ds"]
    ignore_not_found = "--ignore-not-found" in argv
    if "daemonset" in argv:
        requested_name = argv[argv.index("daemonset") + 1]
    else:
        requested_name = next(item.split("/", 1)[1] for item in argv if item.startswith("ds/"))
    if requested_name != "fugue-fugue-node-local-dns":
        raise SystemExit(0 if ignore_not_found else 1)
    if not ds["exists"]:
        raise SystemExit(0 if ignore_not_found else 1)
    if "-o" in argv:
        output = argv[argv.index("-o") + 1]
        if output == "name":
            sys.stdout.write("daemonset.apps/fugue-fugue-node-local-dns")
        elif output == "json":
            print(json.dumps(daemonset_json(state), separators=(",", ":")))
        elif output.startswith("jsonpath="):
            if "updateStrategy.type" in output:
                sys.stdout.write("OnDelete" if ds["layout"] == "current" else "RollingUpdate")
            elif "desiredNumberScheduled" in output:
                desired = len(ds["targets"])
                ready = sum(1 for pod in state["pods"] if pod["node"] in ds["targets"] and pod["ready"])
                unavailable = "" if desired == ready else str(desired - ready)
                sys.stdout.write(f"{ds['generation']}|{ds['observed_generation']}|{desired}|{ready}|{unavailable}")
            elif "node-local-dns-mode" in output:
                sys.stdout.write(ds["mode"])
            elif "kubernetes\\.io/hostname" in output and ds["layout"] == "legacy":
                sys.stdout.write(ds["targets"][0])
    else:
        print(json.dumps(daemonset_json(state), separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and "nodes" in argv:
    selector = argv[argv.index("-l") + 1] if "-l" in argv else ""
    if "fugue.io/location-country-code=" in selector:
        country = selector.rsplit("=", 1)[-1]
        name = "dns-de" if country == "de" else "dns-us"
        ip = NODE_DEFS[name][0]
        ready = os.environ.get("MOCK_DNS_NODE_NOT_READY") != "true"
        if os.environ.get("MOCK_DNS_EXTERNAL_IP_DRIFT") == "true":
            ip = "192.0.2.99"
        items = [node_json(name, ip, ready)]
        if os.environ.get("MOCK_DNS_MULTIPLE_NODES") == "true":
            extra = node_json("node-c", "192.0.2.100", True)
            items.append(extra)
        print(json.dumps({"items": items}, separators=(",", ":")))
        raise SystemExit(0)
    if "--no-headers" in argv:
        for name in NODE_DEFS:
            print(f"{name} Ready <none> 1d v1.35.4")
    else:
        print(json.dumps({"items": [node_json(name) for name in NODE_DEFS]}, separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and "node" in argv:
    name = argv[argv.index("node") + 1]
    if name not in NODE_DEFS:
        raise SystemExit(1)
    if "-o" in argv:
        output = argv[argv.index("-o") + 1]
        if output == "json":
            print(json.dumps(node_json(name), separators=(",", ":")))
        elif "status.conditions" in output:
            ready = state.get("node_overrides", {}).get(name, {}).get("ready", True)
            sys.stdout.write("True" if ready else "False")
        elif "kubernetes\\.io/os" in output:
            sys.stdout.write("linux")
        elif "kubernetes\\.io/arch" in output:
            sys.stdout.write("amd64")
    raise SystemExit(0)

if command == "get" and "pods" in argv and "--all-namespaces" in argv:
    selector = argv[argv.index("--field-selector") + 1]
    node = selector.split("=", 1)[1]
    record = state["hostports"].get(node)
    items = []
    if record:
        host_ip = NODE_DEFS[node][0] if record["mode"] == "scoped" else ""
        ports = []
        for protocol in ("UDP", "TCP"):
            port = {"containerPort": 53, "hostPort": 53, "protocol": protocol}
            if host_ip:
                port["hostIP"] = host_ip
            ports.append(port)
        items.append({
            "metadata": {"namespace": "fugue-system", "name": f"authoritative-{node}", "uid": record["uid"]},
            "spec": {"hostNetwork": False, "containers": [{"name": "dns", "ports": ports}]},
            "status": {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True" if record["ready"] else "False"}],
                "containerStatuses": [{"name": "dns", "restartCount": record["restarts"]}],
            },
        })
    print(json.dumps({"items": items}, separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and "pods" in argv:
    reconcile(state)
    save_state(state)
    pods = [pod_json(pod) for pod in state["pods"]]
    if "--no-headers" in argv:
        for pod in state["pods"]:
            ready = "1/1" if pod["ready"] else "0/1"
            print(f"{pod['name']} {ready} {pod['phase']} {pod['restart_count']} 1m")
    elif "-o" in argv:
        output = argv[argv.index("-o") + 1]
        if output == "json":
            print(json.dumps({"items": pods}, separators=(",", ":")))
        elif output.startswith("jsonpath="):
            if ".metadata.name" in output and ".spec.nodeName" in output:
                for pod in state["pods"]:
                    print(f"{pod['name']}\t{pod['node']}")
            elif ".spec.nodeName" in output:
                for pod in state["pods"]:
                    print(pod["node"])
    raise SystemExit(0)

if command == "get" and "pod" in argv:
    name = argv[argv.index("pod") + 1]
    if name.startswith("fugue-nld-") and any("status.phase" in item for item in argv):
        fail_purpose = os.environ.get("MOCK_PROBE_FAIL_PURPOSE", "")
        safe_fail_purpose = "".join(
            char.lower() if char.isalnum() else "-" if char == "_" else char
            for char in fail_purpose
            if char.isalnum() or char in "-_"
        )[:12].strip("-")
        phase = "Failed" if safe_fail_purpose and f"fugue-nld-{safe_fail_purpose}-" in name else "Succeeded"
        sys.stdout.write(f"uid-probe-{name}|{phase}")
        raise SystemExit(0)
    reconcile(state)
    save_state(state)
    match = next((pod for pod in state["pods"] if pod["name"] == name), None)
    if not match:
        raise SystemExit(1)
    if "-o" in argv and argv[argv.index("-o") + 1] == "json":
        print(json.dumps(pod_json(match), separators=(",", ":")))
    raise SystemExit(0)

if command == "delete" and "pod" in argv:
    name = argv[argv.index("pod") + 1]
    match = next((pod for pod in state["pods"] if pod["name"] == name), None)
    if match:
        node = match["node"]
        state["pods"] = [pod for pod in state["pods"] if pod["name"] != name]
        if state["ds"]["exists"] and node in state["ds"]["targets"] and node not in state["missing_nodes"]:
            state["pods"].append(create_pod(state, node))
        target = state.get("notready_after_delete", {}).get(node)
        if target:
            state["node_overrides"].setdefault(target, {})["ready"] = False
        save_state(state)
    raise SystemExit(0)

if command == "delete" and "daemonset" in argv:
    state["ds"]["exists"] = False
    state["pods"] = []
    save_state(state)
    raise SystemExit(0)

if command == "exec":
    raise SystemExit(1 if os.environ.get("MOCK_EXEC_FAIL") == "true" else 0)

if command in {"logs", "describe"}:
    raise SystemExit(0)

if command == "rollout":
    raise SystemExit(0)

raise SystemExit(1)
PY
chmod +x "${MOCK_KUBECTL}"

"${MOCK_STATE_CTL}" "${MOCK_STATE}" reset
: >"${MOCK_LOG}"
: >"${MOCK_PROBE_SCRIPTS}"
: >"${MOCK_DIG_LOG}"

export FUGUE_UPGRADE_LIB_ONLY=true
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/upgrade_fugue_control_plane.sh"

# Unit tests exercise the release state machine, not external public smoke endpoints.
smoke_test() {
  [[ "${MOCK_SMOKE_FAIL:-false}" != "true" ]]
}

# Keep authoritative checks deterministic, fully intercept dig, and emit the
# exact OPT/EDNS/ECS shape produced by the release command.
dig() {
  printf '%s\n' "$*" >>"${MOCK_DIG_LOG}"
  cat <<'EOF'
;; Got answer:
;; ->>HEADER<<- opcode: QUERY, status: NOERROR, id: 4242
;; flags: qr aa; QUERY: 1, ANSWER: 1, AUTHORITY: 0, ADDITIONAL: 1

;; OPT PSEUDOSECTION:
; EDNS: version: 0, flags:; udp: 1232
; CLIENT-SUBNET: 0.0.0.0/0/0
;; QUESTION SECTION:
;example.test. IN SOA

;; ANSWER SECTION:
example.test. 60 IN SOA ns1.example.test. hostmaster.example.test. 1 300 60 3600 60
EOF
}
[[ "$(type -t dig)" == "function" ]] || {
  echo "the NodeLocal release test must intercept dig without network access" >&2
  exit 1
}

if command -v docker >/dev/null 2>&1; then
  node_local_dns_test_image='registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc7c80faba5261a740a9f878ab8f7403e72444b0a2fa0a9a42ed26577a48290a'
  pulled=false
  for attempt in 1 2 3; do
    if docker pull --platform linux/amd64 "${node_local_dns_test_image}"; then
      pulled=true
      break
    fi
    [[ "${attempt}" == "3" ]] || sleep $((attempt * 2))
  done
  [[ "${pulled}" == "true" ]] || {
    echo "failed to pull the pinned NodeLocal DNSCache test image after 3 attempts" >&2
    exit 1
  }
  docker run --rm --pull=never --platform linux/amd64 --entrypoint /bin/sh \
    "${node_local_dns_test_image}" \
    -ec 'command -v sh >/dev/null; command -v grep >/dev/null; command -v iptables >/dev/null; command -v iptables-save >/dev/null'
fi

set_common_values() {
  KUBECTL="${MOCK_KUBECTL}"
  FUGUE_RELEASE_NAME=fugue
  FUGUE_RELEASE_FULLNAME=fugue-fugue
  FUGUE_NAMESPACE=fugue-system
  FUGUE_COREDNS_NAMESPACE=kube-system
  FUGUE_NODE_LOCAL_DNS_ENABLED=true
  FUGUE_NODE_LOCAL_DNS_MODE=shadow
  FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=false
  FUGUE_NODE_LOCAL_DNS_NODE_NAME=node-a
  FUGUE_NODE_LOCAL_DNS_NODE_NAMES=""
  FUGUE_NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES=""
  FUGUE_NODE_LOCAL_DNS_NAMESPACE=kube-system
  FUGUE_NODE_LOCAL_DNS_LOCAL_IP=169.254.20.10
  FUGUE_NODE_LOCAL_DNS_CLUSTER_DOMAIN=cluster.local
  FUGUE_NODE_LOCAL_DNS_KUBE_DNS_SERVICE_NAME=kube-dns
  FUGUE_NODE_LOCAL_DNS_COREDNS_CONFIGMAP_NAME=coredns
  FUGUE_NODE_LOCAL_DNS_EXPECTED_KUBE_DNS_SERVICE_IP=10.43.0.10
  FUGUE_NODE_LOCAL_DNS_EXTERNAL_PROBE_NAME=api.example.test
  FUGUE_NODE_LOCAL_DNS_IMAGE_REPOSITORY=registry.k8s.io/dns/k8s-dns-node-cache
  FUGUE_NODE_LOCAL_DNS_IMAGE_TAG=1.26.8
  FUGUE_NODE_LOCAL_DNS_IMAGE_DIGEST=sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739
  FUGUE_NODE_LOCAL_DNS_IMAGE_PULL_POLICY=IfNotPresent
  FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE=docker.io/library/busybox@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028
  FUGUE_NODE_LOCAL_DNS_PROBE_TIMEOUT_SECONDS=2
  FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS=2
  FUGUE_NODE_LOCAL_DNS_NODE_WATCH_SECONDS=0
  FUGUE_HELM_TIMEOUT=2s
  FUGUE_ROLLOUT_TIMEOUT=2s
  FUGUE_SMOKE_RETRIES=1
  FUGUE_SMOKE_DELAY_SECONDS=0
  FUGUE_SMOKE_URL=https://api.example.test/healthz
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_WAIT_SECONDS=120
  FUGUE_CONTROL_PLANE_BACKUP_DRAIN_POLL_SECONDS=5
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_COMMAND_TIMEOUT_SECONDS=15
  FUGUE_CONTROL_PLANE_BACKUP_COORDINATION_DB_QUERY_TIMEOUT_SECONDS=20
  FUGUE_DEPLOY_JOB_BUDGET_SECONDS=20400
  FUGUE_DEPLOY_ROLLBACK_RESERVE_SECONDS=10200
  FUGUE_DEPLOY_ARTIFACT_RESERVE_SECONDS=600
  FUGUE_RELEASE_KUBERNETES_OPERATION_OUTER_TIMEOUT_SECONDS=900
  CONTROL_PLANE_RELEASE_JOB_DEADLINE_EPOCH=$(( $(date +%s) + FUGUE_DEPLOY_JOB_BUDGET_SECONDS ))
  NODE_LOCAL_DNS_ROLLBACK_BUDGET_SECONDS=0
  FUGUE_DNS_ZONE=example.test
  FUGUE_DNS_NAMESERVERS=ns1.example.test
  FUGUE_DNS_TTL=60
  FUGUE_DNS_CONTAINER_NAME=dns
  FUGUE_DNS_UDP_CONTAINER_PORT=53
  FUGUE_DNS_TCP_CONTAINER_PORT=53
  NODE_LOCAL_DNS_PREVIOUS_ENABLED=false
  NODE_LOCAL_DNS_PREVIOUS_MODE=""
  NODE_LOCAL_DNS_PREVIOUS_TARGET_NODES=""
  NODE_LOCAL_DNS_PREVIOUS_PODS_JSON='[]'
  NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME=""
  NODE_LOCAL_DNS_ACTIVE_SERVICE_NAME=""
  NODE_LOCAL_DNS_ACTIVE_COMPONENT="node-local-dns"
  NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME=""
  NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES=""
  NODE_LOCAL_DNS_PRESERVED_MODE=""
  NODE_LOCAL_DNS_PRESERVED_PODS_JSON='[]'
  NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON=""
  NODE_LOCAL_DNS_SPLIT_COHORT=false
  NODE_LOCAL_DNS_PREFLIGHT_TARGET_NODES=""
  NODE_LOCAL_DNS_ADDED_NODES=""
  NODE_LOCAL_DNS_REPLACED_NODES=""
  NODE_LOCAL_DNS_FAILED_NODE=""
  NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT=""
  NODE_LOCAL_DNS_TARGET_NODES=""
  NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP=""
  NODE_LOCAL_DNS_HELM_SET_ARGS=()
  MOCK_ARTIFACT_DRIFT=""
  MOCK_CONTROLLER_REVISION_DRIFT=""
  MOCK_COREFILE_DRIFT=false
  MOCK_EXEC_FAIL=false
  MOCK_PROBE_FAIL_PURPOSE=""
  MOCK_SMOKE_FAIL=false
  MOCK_DNS_NODE_NOT_READY=false
  MOCK_DNS_EXTERNAL_IP_DRIFT=false
  MOCK_DNS_MULTIPLE_NODES=false
  export MOCK_ARTIFACT_DRIFT MOCK_CONTROLLER_REVISION_DRIFT MOCK_COREFILE_DRIFT MOCK_EXEC_FAIL MOCK_PROBE_FAIL_PURPOSE MOCK_SMOKE_FAIL
  export MOCK_DNS_NODE_NOT_READY MOCK_DNS_EXTERNAL_IP_DRIFT MOCK_DNS_MULTIPLE_NODES
}

state_json() {
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" dump
}

assert_state() {
  local expression="$1"
  STATE_JSON="$(state_json)" STATE_EXPRESSION="${expression}" python3 -c '
import json
import os

state = json.loads(os.environ["STATE_JSON"])
if not eval(os.environ["STATE_EXPRESSION"], {"__builtins__": {}, "all": all, "len": len}, {"state": state}):
    raise SystemExit("state assertion failed: " + os.environ["STATE_EXPRESSION"] + "\n" + json.dumps(state, indent=2))
'
}

echo "[test_node_local_dns_release] exact-residue rollback diagnostics are bounded and hostPID-scoped"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" reset
set_common_values
NODE_LOCAL_DNS_TARGET_NODES=node-a
: >"${MOCK_PROBE_SCRIPTS}"
MOCK_PROBE_FAIL_PURPOSE=rule-cleanup
export MOCK_PROBE_FAIL_PURPOSE
if node_local_dns_recover_exact_residue 10.43.0.10 >/dev/null 2>&1; then
  echo "a failed rule-cleanup probe must fail exact-residue recovery" >&2
  exit 1
fi
MOCK_PROBE_FAIL_PURPOSE=""
export MOCK_PROBE_FAIL_PURPOSE
APPLIED_MANIFEST="${MOCK_APPLY}" python3 - <<'PY'
import json
import os

with open(os.environ["APPLIED_MANIFEST"], encoding="utf-8") as handle:
    manifest = json.load(handle)
spec = manifest["spec"]
container = spec["containers"][0]
assert manifest["metadata"]["name"].startswith("fugue-nld-rule-cleanup-")
assert spec.get("hostPID") is True
assert spec.get("hostNetwork") is True
assert spec.get("automountServiceAccountToken") is False
assert container.get("securityContext") == {"capabilities": {"add": ["NET_ADMIN"]}}
script = container["command"][-1]
required = [
    "iptables --version > /tmp/fugue-nld-iptables.version",
    "iptables-save --version > /tmp/fugue-nld-iptables-save.version",
    "backend=%s",
    "for comm_file in /proc/[0-9]*/comm",
    "[ \"${comm}\" = 'node-cache' ]",
    "node-cache-comm-count total=%s shown=%s limit=%s truncated=%s",
    "node_cache_limit=32",
    "rule_diagnostic_limit=64",
    "rule-before table=%s index=%s exact-count=%s rule=%s",
    "rule-delete table=%s index=%s rc=%s",
    "rule-after table=%s index=%s exact-count=%s rule=%s",
    "rule-observation sample=0 exact-comment-count=%s",
    "observation_limit=4",
]
missing = [item for item in required if item not in script]
assert not missing, (missing, repr(script))
assert "/cmdline" not in script
assert "/environ" not in script
PY

# The optional host PID namespace is diagnostic-only. Every existing six-arg
# probe call must retain the safer default and must not gain another capability.
node_local_dns_run_probe_pod node-a default-hostpid \
  "${FUGUE_NODE_LOCAL_DNS_PROBE_IMAGE}" false false true
APPLIED_MANIFEST="${MOCK_APPLY}" python3 - <<'PY'
import json
import os

with open(os.environ["APPLIED_MANIFEST"], encoding="utf-8") as handle:
    manifest = json.load(handle)
spec = manifest["spec"]
container = spec["containers"][0]
assert not spec.get("hostPID", False)
assert spec.get("automountServiceAccountToken") is False
assert "securityContext" not in container
PY
: >"${MOCK_PROBE_SCRIPTS}"

echo "[test_node_local_dns_release] initial single-node shadow and current artifact"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" reset
set_common_values
prepare_node_local_dns_helm_args
helm_args="${NODE_LOCAL_DNS_HELM_SET_ARGS[*]}"
grep -Fq 'nodeLocalDNS.kubeDNSServiceIP=10.43.0.10' <<<"${helm_args}"
grep -Fq 'nodeLocalDNS.nodeSelector={"kubernetes.io/os":"linux"}' <<<"${helm_args}"
grep -Fq 'nodeLocalDNS.nodeSelector.kubernetes\.io/hostname=' <<<"${helm_args}"
grep -Fq 'nodeLocalDNS.targetNodes=["node-a"]' <<<"${helm_args}"
grep -Fq 'nodeLocalDNS.updateStrategy.type=OnDelete' <<<"${helm_args}"
[[ "${NODE_LOCAL_DNS_ADDED_NODES}" == "node-a" ]]
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a rev-shadow-1
"${MOCK_KUBECTL}" -n kube-system get daemonset fugue-fugue-node-local-dns -o json | python3 -c '
import json
import sys

if "updateRevision" in (json.load(sys.stdin).get("status") or {}):
    raise SystemExit("the DaemonSet mock must use the real apps/v1 status schema")
'
node_local_dns_reconcile_after_helm
assert_state 'len(state["pods"]) == 1 and state["pods"][0]["ready"] and state["pods"][0]["config_key"] == "Corefile.shadow"'
grep -Fq 'metric_total coredns_dns_requests_total' "${MOCK_PROBE_SCRIPTS}"
grep -Fq 'server="dns://169.254.20.10:53"' "${MOCK_PROBE_SCRIPTS}"
grep -Fq 'dns_tcp_query kubernetes.default.svc.cluster.local' "${MOCK_PROBE_SCRIPTS}"
grep -Fq "nc -w 5 '169.254.20.10' 53" "${MOCK_PROBE_SCRIPTS}"
PROBE_SCRIPTS="${MOCK_PROBE_SCRIPTS}" python3 - <<'PY'
import os

with open(os.environ["PROBE_SCRIPTS"], encoding="utf-8") as handle:
    scripts = handle.read()
required = [
    "grep -F 'coredns_panics_total'",
    "grep -F 'coredns_reload_failed_total'",
    "panics=\"$(awk '/^coredns_panics_total/",
    "reload_failures=\"$(awk '/^coredns_reload_failed_total/",
    "[ \"${panics}\" = '0' ]",
    "[ \"${reload_failures}\" = '0' ]",
]
missing = [item for item in required if item not in scripts]
if missing:
    raise SystemExit("NodeLocal DNS probe omitted zero-value panic/reload gates: " + repr(missing))
PY

echo "[test_node_local_dns_release] current ControllerRevision identity fails closed"
set_common_values
node_local_dns_configure_cohort_names
NODE_LOCAL_DNS_TARGET_NODES=node-a
[[ "$(node_local_dns_current_controller_revision shadow)" == "rev-shadow-1" ]]
for controller_revision_drift in owner template hash ambiguous; do
  if (
    set_common_values
    node_local_dns_configure_cohort_names
    NODE_LOCAL_DNS_TARGET_NODES=node-a
    MOCK_CONTROLLER_REVISION_DRIFT="${controller_revision_drift}"
    export MOCK_CONTROLLER_REVISION_DRIFT
    node_local_dns_current_controller_revision shadow
  ) >/dev/null 2>&1; then
    echo "NodeLocal DNSCache accepted ${controller_revision_drift} ControllerRevision drift" >&2
    exit 1
  fi
done

echo "[test_node_local_dns_release] first release and legacy layout transition guards"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" reset
if (
  set_common_values
  FUGUE_NODE_LOCAL_DNS_MODE=iptables
  prepare_node_local_dns_helm_args
) >/dev/null 2>&1; then
  echo "the first NodeLocal DNSCache release must reject iptables mode" >&2
  exit 1
fi

echo "[test_node_local_dns_release] preflight rejects restart and lastState history"
for health_case in restart last-terminated; do
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-health rev-health
  case "${health_case}" in
    restart)
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a restart_count 1
      ;;
    last-terminated)
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a last_state terminated
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a last_state_reason Error
      ;;
  esac
  if (set_common_values; prepare_node_local_dns_helm_args) >/dev/null 2>&1; then
    echo "a live NodeLocal DNSCache Pod with ${health_case} history must fail the pure-cohort preflight" >&2
    exit 1
  fi
done

echo "[test_node_local_dns_release] current affinity is structurally exact"
for affinity_drift in affinity-extra affinity-operator selector-extra; do
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-affinity rev-affinity
  if (
    set_common_values
    MOCK_ARTIFACT_DRIFT="${affinity_drift}"
    export MOCK_ARTIFACT_DRIFT
    prepare_node_local_dns_helm_args
  ) >/dev/null 2>&1; then
    echo "NodeLocal DNSCache preflight accepted malformed affinity drift: ${affinity_drift}" >&2
    exit 1
  fi
done
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed legacy shadow node-a rev-legacy rev-legacy-pod
if (
  set_common_values
  FUGUE_NODE_LOCAL_DNS_MODE=iptables
  prepare_node_local_dns_helm_args
) >/dev/null 2>&1; then
  echo "legacy Corefile layout must not change mode in the migration release" >&2
  exit 1
fi

echo "[test_node_local_dns_release] additive shadow expansion is exactly one node"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-old rev-old
if (
  set_common_values
  FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
  FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
  FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-a,node-b,node-c
  prepare_node_local_dns_helm_args
) >/dev/null 2>&1; then
  echo "a shadow expansion must not add more than one node per release" >&2
  exit 1
fi
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-old rev-old
set_common_values
FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-a,node-b
old_node_a_uid="$(state_json | python3 -c 'import json,sys; print(json.load(sys.stdin)["pods"][0]["uid"])')"
prepare_node_local_dns_helm_args
[[ "${NODE_LOCAL_DNS_ADDED_NODES}" == "node-b" ]]
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a,node-b rev-expanded
node_local_dns_reconcile_after_helm
OLD_UID="${old_node_a_uid}" STATE_JSON="$(state_json)" python3 -c '
import json, os
s = json.loads(os.environ["STATE_JSON"])
pods = {pod["node"]: pod for pod in s["pods"]}
assert pods["node-a"]["uid"] == os.environ["OLD_UID"]
assert pods["node-a"]["revision"] == "rev-old"
assert pods["node-b"]["revision"] == "rev-expanded"
'

echo "[test_node_local_dns_release] added Pod must carry observed DaemonSet revision"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-old rev-old
set_common_values
FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-a,node-b
prepare_node_local_dns_helm_args
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a,node-b rev-expanded
"${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-revision node-b rev-stale
if node_local_dns_reconcile_after_helm >/dev/null 2>&1; then
  echo "an added Pod from a stale ControllerRevision must not pass reconciliation" >&2
  exit 1
fi

echo "[test_node_local_dns_release] terminal startup states bypass the critical timeout"
for terminal_case in last-oom phase-failed; do
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current iptables node-a rev-terminal rev-terminal
  set_common_values
  FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS=8
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a ready false
  case "${terminal_case}" in
    last-oom)
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a restart_count 1
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a state waiting
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a state_reason ContainerCreating
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a last_state terminated
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a last_state_reason OOMKilled
      ;;
    phase-failed)
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a phase Failed
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a state terminated
      "${MOCK_STATE_CTL}" "${MOCK_STATE}" pod-health node-a state_reason Error
      ;;
  esac
  terminal_started_at="${SECONDS}"
  if node_local_dns_wait_for_pod_on_node node-a "" iptables rev-terminal "${FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS}" >/dev/null 2>&1; then
    echo "terminal Pod state ${terminal_case} unexpectedly became Ready" >&2
    exit 1
  fi
  terminal_elapsed=$((SECONDS - terminal_started_at))
  if (( terminal_elapsed >= FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS )); then
    echo "terminal Pod state ${terminal_case} did not bypass the critical timeout; elapsed=${terminal_elapsed}s" >&2
    exit 1
  fi
done

echo "[test_node_local_dns_release] mode switch replaces ordinary, edge, then control-plane Pods"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-control,node-edge,node-a rev-shadow rev-control,rev-edge,rev-a
set_common_values
FUGUE_NODE_LOCAL_DNS_MODE=iptables
FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-control,node-edge,node-a
prepare_node_local_dns_helm_args
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current iptables node-control,node-edge,node-a rev-iptables
: >"${MOCK_LOG}"
node_local_dns_reconcile_after_helm
delete_order="$(awk '/delete pod fugue-fugue-node-local-dns-/ {for (i = 1; i <= NF; i++) if ($i == "pod") print $(i + 1)}' "${MOCK_LOG}")"
DELETE_ORDER="${delete_order}" python3 -c '
import os

names = os.environ["DELETE_ORDER"].splitlines()
assert len(names) == 3, names
assert "node-a" in names[0], names
assert "node-edge" in names[1], names
assert "node-control" in names[2], names
'
assert_state 'len(state["pods"]) == 3 and all(pod["mode"] == "iptables" and pod["ready"] and pod["revision"] == "rev-iptables" for pod in state["pods"])'

echo "[test_node_local_dns_release] a later NotReady node is never deleted"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a,node-b rev-shadow rev-a,rev-b
"${MOCK_STATE_CTL}" "${MOCK_STATE}" notready-after-delete node-a node-b
set_common_values
FUGUE_NODE_LOCAL_DNS_MODE=iptables
FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-a,node-b
prepare_node_local_dns_helm_args
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current iptables node-a,node-b rev-iptables
: >"${MOCK_LOG}"
if node_local_dns_reconcile_after_helm >/dev/null 2>&1; then
  echo "mode reconciliation must stop when the next node becomes NotReady" >&2
  exit 1
fi
grep -Fq 'delete pod fugue-fugue-node-local-dns-node-a-' "${MOCK_LOG}"
if grep -Fq 'delete pod fugue-fugue-node-local-dns-node-b-' "${MOCK_LOG}"; then
  echo "the NotReady second node was deleted" >&2
  exit 1
fi

echo "[test_node_local_dns_release] node pressure blocks deletion"
for pressure_condition in MemoryPressure DiskPressure PIDPressure; do
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-pressure rev-pressure
  set_common_values
  FUGUE_NODE_LOCAL_DNS_MODE=iptables
  prepare_node_local_dns_helm_args
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" template current iptables node-a rev-pressure-new
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" node-condition node-a "${pressure_condition}" true
  : >"${MOCK_LOG}"
  if node_local_dns_reconcile_after_helm >/dev/null 2>&1; then
    echo "mode reconciliation accepted ${pressure_condition}=True" >&2
    exit 1
  fi
  if grep -Fq 'delete pod fugue-fugue-node-local-dns-node-a-' "${MOCK_LOG}"; then
    echo "NodeLocal DNSCache Pod was deleted while ${pressure_condition}=True" >&2
    exit 1
  fi
done

echo "[test_node_local_dns_release] mixed old revisions survive exact Helm rollback"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a,node-b rev-old-template rev-old-a,rev-old-b
set_common_values
FUGUE_NODE_LOCAL_DNS_MODE=iptables
FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-a,node-b
old_node_b_uid="$(state_json | python3 -c 'import json,sys; print([p for p in json.load(sys.stdin)["pods"] if p["node"] == "node-b"][0]["uid"])')"
prepare_node_local_dns_helm_args
"${MOCK_STATE_CTL}" "${MOCK_STATE}" failure crash node-a
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current iptables node-a,node-b rev-new
crash_output="${TMP_DIR}/crash-reconcile.log"
FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS=8
crash_started_at="${SECONDS}"
if node_local_dns_reconcile_after_helm >"${crash_output}" 2>&1; then
  echo "a CrashLooping replacement Pod must fail the mode transition" >&2
  exit 1
fi
crash_elapsed=$((SECONDS - crash_started_at))
if (( crash_elapsed >= FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS )); then
  echo "CrashLoopBackOff did not use the terminal fast-failure path; elapsed=${crash_elapsed}s" >&2
  exit 1
fi
"${MOCK_STATE_CTL}" "${MOCK_STATE}" clear-failures
# This template mutation represents the completed Helm rollback before the rollback hook runs.
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a,node-b rev-old-template
node_local_dns_restore_previous_after_helm_rollback
OLD_UID="${old_node_b_uid}" STATE_JSON="$(state_json)" python3 -c '
import json, os
s = json.loads(os.environ["STATE_JSON"])
pods = {pod["node"]: pod for pod in s["pods"]}
assert set(pods) == {"node-a", "node-b"}
assert all(pod["mode"] == "shadow" and pod["ready"] for pod in pods.values())
assert pods["node-b"]["uid"] == os.environ["OLD_UID"]
assert pods["node-b"]["revision"] == "rev-old-b"
assert pods["node-a"]["revision"] == "rev-old-template"
'

echo "[test_node_local_dns_release] rollback restores the failed node before earlier replacements"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a,node-b,node-c rev-rollback-old rev-old-a,rev-old-b,rev-old-c
set_common_values
FUGUE_NODE_LOCAL_DNS_MODE=iptables
FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-a,node-b,node-c
old_node_c_uid="$(state_json | python3 -c 'import json,sys; print([p for p in json.load(sys.stdin)["pods"] if p["node"] == "node-c"][0]["uid"])')"
prepare_node_local_dns_helm_args
"${MOCK_STATE_CTL}" "${MOCK_STATE}" failure crash node-b
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current iptables node-a,node-b,node-c rev-rollback-new
FUGUE_NODE_LOCAL_DNS_CRITICAL_READY_TIMEOUT_SECONDS=8
if node_local_dns_reconcile_after_helm >/dev/null 2>&1; then
  echo "a failed second replacement must stop the mode transition" >&2
  exit 1
fi
[[ "${NODE_LOCAL_DNS_REPLACED_NODES}" == "node-a" ]]
[[ "${NODE_LOCAL_DNS_FAILED_NODE}" == "node-b" ]]
"${MOCK_STATE_CTL}" "${MOCK_STATE}" clear-failures
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a,node-b,node-c rev-rollback-old
: >"${MOCK_LOG}"
node_local_dns_restore_previous_after_helm_rollback
rollback_delete_order="$(awk '/delete pod fugue-fugue-node-local-dns-/ {for (i = 1; i <= NF; i++) if ($i == "pod") print $(i + 1)}' "${MOCK_LOG}")"
ROLLBACK_DELETE_ORDER="${rollback_delete_order}" OLD_NODE_C_UID="${old_node_c_uid}" STATE_JSON="$(state_json)" python3 - <<'PY'
import json
import os

names = os.environ["ROLLBACK_DELETE_ORDER"].splitlines()
assert len(names) == 2, names
assert "node-b" in names[0], names
assert "node-a" in names[1], names
pods = {pod["node"]: pod for pod in json.loads(os.environ["STATE_JSON"])["pods"]}
assert set(pods) == {"node-a", "node-b", "node-c"}
assert all(pod["mode"] == "shadow" and pod["ready"] for pod in pods.values())
assert pods["node-c"]["uid"] == os.environ["OLD_NODE_C_UID"]
PY

for new_pod_failure in missing crash; do
  echo "[test_node_local_dns_release] added ${new_pod_failure} Pod rolls back to previous shadow cohort"
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-before rev-before
  set_common_values
  FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
  FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
  FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-a,node-b
  old_uid="$(state_json | python3 -c 'import json,sys; print(json.load(sys.stdin)["pods"][0]["uid"])')"
  prepare_node_local_dns_helm_args
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" failure "${new_pod_failure}" node-b
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a,node-b rev-bad-addition
  if node_local_dns_reconcile_after_helm >/dev/null 2>&1; then
    echo "a ${new_pod_failure} added Pod must fail reconciliation" >&2
    exit 1
  fi
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" clear-failures
  "${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a rev-before
  node_local_dns_restore_previous_after_helm_rollback
  OLD_UID="${old_uid}" STATE_JSON="$(state_json)" python3 -c '
import json, os
s = json.loads(os.environ["STATE_JSON"])
assert len(s["pods"]) == 1
pod = s["pods"][0]
assert pod["node"] == "node-a" and pod["uid"] == os.environ["OLD_UID"]
assert pod["mode"] == "shadow" and pod["ready"]
'
done

echo "[test_node_local_dns_release] failed edge addition preserves authoritative DNS through rollback"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-before rev-before
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostports node-edge scoped
set_common_values
FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
FUGUE_NODE_LOCAL_DNS_NODE_NAMES=node-a,node-edge
prepare_node_local_dns_helm_args
"${MOCK_STATE_CTL}" "${MOCK_STATE}" failure crash node-edge
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a,node-edge rev-edge-bad
if node_local_dns_reconcile_after_helm >/dev/null 2>&1; then
  echo "a CrashLooping edge addition must fail reconciliation" >&2
  exit 1
fi
"${MOCK_STATE_CTL}" "${MOCK_STATE}" clear-failures
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current shadow node-a rev-before
: >"${MOCK_DIG_LOG}"
node_local_dns_restore_previous_after_helm_rollback
grep -Fq -- '@198.51.100.20 +time=3 +tries=1 +norecurse +subnet=0.0.0.0/0 +noall +comments +question +answer example.test SOA' "${MOCK_DIG_LOG}"
grep -Fq -- '@198.51.100.20 +time=3 +tries=1 +norecurse +subnet=0.0.0.0/0 +noall +comments +question +answer +tcp example.test SOA' "${MOCK_DIG_LOG}"

echo "[test_node_local_dns_release] scoped authoritative rows retain tab fields and TCP probes"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-edge rev-edge rev-edge
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostports node-edge scoped
: >"${MOCK_DIG_LOG}"
set_common_values
FUGUE_NODE_LOCAL_DNS_NODE_NAME=node-edge
prepare_node_local_dns_helm_args
[[ "$(printf '%s\n' "${NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT}" | sed '/^[[:space:]]*$/d' | wc -l | awk '{print $1}')" == "2" ]]
if ! awk -F $'\t' 'NF != 7 {exit 1}' <<<"${NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT}"; then
  echo "scoped authoritative hostPort snapshot lost its tab-delimited fields" >&2
  exit 1
fi
node_local_dns_verify_authoritative_coexistence node-edge
grep -Fq -- '@198.51.100.20 +time=3 +tries=1 +norecurse +subnet=0.0.0.0/0 +noall +comments +question +answer example.test SOA' "${MOCK_DIG_LOG}"
grep -Fq -- '@198.51.100.20 +time=3 +tries=1 +norecurse +subnet=0.0.0.0/0 +noall +comments +question +answer +tcp example.test SOA' "${MOCK_DIG_LOG}"

echo "[test_node_local_dns_release] verified DNS manifest transactions refresh authoritative Pod UIDs"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge uid auth-node-edge-forward
if node_local_dns_verify_authoritative_coexistence node-edge >/dev/null 2>&1; then
  echo "a replacement authoritative Pod UID must differ from the pre-transaction baseline" >&2
  exit 1
fi
node_local_dns_refresh_authoritative_hostport_snapshot node-edge "DNS manifest"
node_local_dns_verify_authoritative_coexistence node-edge
awk -F $'\t' '$1 == "node-edge" && $2 == "198.51.100.20" && $3 == "TCP" && $5 == "auth-node-edge-forward" && $6 == "0" && $7 == "true" {found=1} END {exit !found}' <<<"${NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT}"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge uid auth-node-edge-rollback
if node_local_dns_verify_authoritative_coexistence node-edge >/dev/null 2>&1; then
  echo "a restored authoritative Pod UID must differ from the forward transaction baseline" >&2
  exit 1
fi
node_local_dns_refresh_authoritative_hostport_snapshot node-edge "DNS manifest rollback"
node_local_dns_verify_authoritative_coexistence node-edge
awk -F $'\t' '$1 == "node-edge" && $2 == "198.51.100.20" && $3 == "UDP" && $5 == "auth-node-edge-rollback" && $6 == "0" && $7 == "true" {found=1} END {exit !found}' <<<"${NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT}"

# Restore the original mock identity so the drift matrix below starts from a
# stable baseline of its own.
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge uid auth-node-edge
node_local_dns_refresh_authoritative_hostport_snapshot node-edge "test reset"

for drift_case in uid restarts ready rules; do
  case "${drift_case}" in
    uid) "${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge uid auth-replaced ;;
    restarts) "${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge restarts 1 ;;
    ready) "${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge ready false ;;
    rules) MOCK_PROBE_FAIL_PURPOSE=hostport-scope; export MOCK_PROBE_FAIL_PURPOSE ;;
  esac
  if node_local_dns_verify_authoritative_coexistence node-edge >/dev/null 2>&1; then
    echo "authoritative DNS ${drift_case} drift must fail coexistence verification" >&2
    exit 1
  fi
  case "${drift_case}" in
    uid) "${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge uid auth-node-edge ;;
    restarts) "${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge restarts 0 ;;
    ready) "${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge ready true ;;
    rules) MOCK_PROBE_FAIL_PURPOSE=""; export MOCK_PROBE_FAIL_PURPOSE ;;
  esac
done

echo "[test_node_local_dns_release] edge authoritative drift blocks OnDelete replacement"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-edge rev-edge rev-edge
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostports node-edge scoped
set_common_values
FUGUE_NODE_LOCAL_DNS_MODE=iptables
FUGUE_NODE_LOCAL_DNS_NODE_NAME=node-edge
prepare_node_local_dns_helm_args
"${MOCK_STATE_CTL}" "${MOCK_STATE}" template current iptables node-edge rev-edge-new
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostport-drift node-edge restarts 1
: >"${MOCK_LOG}"
if node_local_dns_reconcile_after_helm >/dev/null 2>&1; then
  echo "authoritative DNS drift must stop edge replacement" >&2
  exit 1
fi
if grep -Fq 'delete pod fugue-fugue-node-local-dns-node-edge-' "${MOCK_LOG}"; then
  echo "edge Pod was deleted after authoritative DNS drift" >&2
  exit 1
fi

echo "[test_node_local_dns_release] edge disable snapshots coexistence and restores on teardown failure"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-edge rev-edge rev-edge
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostports node-edge scoped
set_common_values
FUGUE_NODE_LOCAL_DNS_ENABLED=false
prepare_node_local_dns_helm_args
[[ "$(printf '%s\n' "${NODE_LOCAL_DNS_HOSTPORT_POD_SNAPSHOT}" | sed '/^[[:space:]]*$/d' | wc -l | awk '{print $1}')" == "2" ]]
: >"${MOCK_DIG_LOG}"
node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"
assert_state 'state["ds"]["exists"] is False and len(state["pods"]) == 0'
grep -Fq -- '@198.51.100.20 +time=3 +tries=1 +norecurse +subnet=0.0.0.0/0 +noall +comments +question +answer +tcp example.test SOA' "${MOCK_DIG_LOG}"

"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-edge rev-edge rev-edge
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostports node-edge scoped
set_common_values
FUGUE_NODE_LOCAL_DNS_ENABLED=false
prepare_node_local_dns_helm_args
MOCK_PROBE_FAIL_PURPOSE=teardown
export MOCK_PROBE_FAIL_PURPOSE
if node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" >/dev/null 2>&1; then
  echo "a failed teardown probe must fail the disable transaction" >&2
  exit 1
fi
MOCK_PROBE_FAIL_PURPOSE=""
export MOCK_PROBE_FAIL_PURPOSE
assert_state 'state["ds"]["exists"] is True and len(state["pods"]) == 1 and state["pods"][0]["ready"] and state["pods"][0]["mode"] == "shadow"'
node_local_dns_verify_running shadow "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"

echo "[test_node_local_dns_release] unsafe hostPorts and current ConfigMap drift fail closed"
"${MOCK_STATE_CTL}" "${MOCK_STATE}" reset
"${MOCK_STATE_CTL}" "${MOCK_STATE}" hostports node-a unscoped
if (set_common_values; prepare_node_local_dns_helm_args) >/dev/null 2>&1; then
  echo "an unscoped DNS hostPort owner must block NodeLocal DNSCache" >&2
  exit 1
fi
"${MOCK_STATE_CTL}" "${MOCK_STATE}" seed current shadow node-a rev-stable rev-stable
if (
  set_common_values
  MOCK_COREFILE_DRIFT=true
  export MOCK_COREFILE_DRIFT
  prepare_node_local_dns_helm_args
) >/dev/null 2>&1; then
  echo "a current-layout ConfigMap drift must fail preflight" >&2
  exit 1
fi

echo "[test_node_local_dns_release] public DNS hostIP validation remains exact"
set_common_values
FUGUE_DNS_ENABLED=true
FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED=true
FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE=us
FUGUE_DNS_PUBLIC_HOST_IP=203.0.113.10
FUGUE_DNS_EXTRA_GROUPS='country-de|edge-country-de|de|198.51.100.20|dns-de-token'
validate_dns_public_host_port_targets
for invalid_target in not-ready external-ip-drift multiple-nodes; do
  if (
    set_common_values
    FUGUE_DNS_ENABLED=true
    FUGUE_DNS_PUBLIC_HOSTPORTS_ENABLED=true
    FUGUE_DNS_NODE_SELECTOR_COUNTRY_CODE=us
    FUGUE_DNS_PUBLIC_HOST_IP=203.0.113.10
    FUGUE_DNS_EXTRA_GROUPS='country-de|edge-country-de|de|198.51.100.20|dns-de-token'
    case "${invalid_target}" in
      not-ready) MOCK_DNS_NODE_NOT_READY=true; export MOCK_DNS_NODE_NOT_READY ;;
      external-ip-drift) MOCK_DNS_EXTERNAL_IP_DRIFT=true; export MOCK_DNS_EXTERNAL_IP_DRIFT ;;
      multiple-nodes) MOCK_DNS_MULTIPLE_NODES=true; export MOCK_DNS_MULTIPLE_NODES ;;
    esac
    validate_dns_public_host_port_targets
  ) >/dev/null 2>&1; then
    echo "public DNS host-port target validation must reject ${invalid_target}" >&2
    exit 1
  fi
done

echo "[test_node_local_dns_release] split cohort preserves an offline legacy Pod while active canary rolls independently"
export DUAL_MOCK_STATE="${TMP_DIR}/dual-state.json"
export DUAL_MOCK_LOG="${TMP_DIR}/dual-kubectl.log"
DUAL_MOCK_STATE_CTL="${TMP_DIR}/dual-statectl"
DUAL_MOCK_KUBECTL="${TMP_DIR}/dual-kubectl"

cat >"${DUAL_MOCK_STATE_CTL}" <<'PY'
#!/usr/bin/env python3
import json
import sys

path = sys.argv[1]
action = sys.argv[2]


def initial_state():
    return {
        "nodes": {
            "dmit": {
                "ready": False,
                "unschedulable": False,
                "labels": {"fugue.io/schedulable": "false"},
                "taints": [{"key": "node.kubernetes.io/unreachable", "effect": "NoExecute"}],
            },
            "node-a": {"ready": True, "unschedulable": False, "labels": {}, "taints": []},
            "node-b": {"ready": True, "unschedulable": False, "labels": {}, "taints": []},
            "node-c": {"ready": True, "unschedulable": True, "labels": {}, "taints": []},
        },
        "preserved": {
            "exists": True,
            "layout": "legacy",
            "mode": "iptables",
            "targets": ["dmit"],
            "uid": "uid-preserved-ds",
            "revision": "rev-preserved-legacy",
            "generation": 1,
            "observed_generation": 1,
            "pod": {
                "name": "fugue-fugue-node-local-dns-dmit-fixed",
                "uid": "uid-preserved-pod",
                "revision": "rev-preserved-pod",
                "restart_count": 1,
                "deletion_timestamp": "",
                "config_key": "Corefile",
                "listen_ips": "169.254.20.10,10.43.0.10",
            },
        },
        "active": {
            "exists": False,
            "mode": "shadow",
            "targets": [],
            "uid": "uid-active-ds",
            "revision": "rev-active-shadow",
            "generation": 1,
            "observed_generation": 1,
            "pods": [],
        },
    }


def load():
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def save(state):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(state, handle, separators=(",", ":"))


if action == "reset":
    save(initial_state())
    raise SystemExit(0)

state = load()
if action == "forward":
    preserved = state["preserved"]
    preserved.update({
        "layout": "current",
        "revision": "rev-preserved-current",
        "generation": 2,
        "observed_generation": 2,
    })
    active = state["active"]
    active.update({
        "exists": True,
        "mode": "shadow",
        "targets": ["node-a"],
        "revision": "rev-active-shadow",
        "generation": 1,
        "observed_generation": 1,
        "pods": [{
            "name": "fugue-fugue-node-local-dns-active-node-a-fixed",
            "uid": "uid-active-node-a",
            "node": "node-a",
            "revision": "rev-active-shadow",
        }],
    })
elif action == "rollback-base":
    preserved = state["preserved"]
    preserved.update({
        "layout": "legacy",
        "revision": "rev-preserved-legacy",
        "generation": 3,
        "observed_generation": 3,
    })
elif action == "preserved-drift":
    field = sys.argv[3]
    pod = state["preserved"]["pod"]
    if field == "uid":
        pod["uid"] = "uid-preserved-pod-drift"
    elif field == "args":
        pod["listen_ips"] = "169.254.20.11,10.43.0.10"
    elif field == "config":
        pod["config_key"] = "Corefile.shadow"
    elif field == "restart":
        pod["restart_count"] = 2
    elif field == "deletion":
        pod["deletion_timestamp"] = "2026-07-13T12:00:00Z"
    else:
        raise SystemExit(f"unsupported preserved drift: {field}")
elif action == "node-ready":
    node, value = sys.argv[3:5]
    state["nodes"][node]["ready"] = value == "true"
elif action == "dump":
    print(json.dumps(state, separators=(",", ":")))
    raise SystemExit(0)
else:
    raise SystemExit(f"unsupported dual state action: {action}")

save(state)
PY
chmod +x "${DUAL_MOCK_STATE_CTL}"

cat >"${DUAL_MOCK_KUBECTL}" <<'PY'
#!/usr/bin/env python3
import copy
import json
import os
import shlex
import sys

argv = sys.argv[1:]
with open(os.environ["DUAL_MOCK_LOG"], "a", encoding="utf-8") as handle:
    handle.write(shlex.join(argv) + "\n")


def load():
    with open(os.environ["DUAL_MOCK_STATE"], encoding="utf-8") as handle:
        return json.load(handle)


def save(state):
    with open(os.environ["DUAL_MOCK_STATE"], "w", encoding="utf-8") as handle:
        json.dump(state, handle, separators=(",", ":"))


def corefile(bind_ips):
    return f"""cluster.local:53 {{
  errors
  cache {{
    success 9984 30
    denial 9984 5
  }}
  reload
  loop
  bind {bind_ips}
  forward . __PILLAR__CLUSTER__DNS__ {{
    force_tcp
  }}
  prometheus :9253
  health 169.254.20.10:8080
}}
in-addr.arpa:53 {{
  errors
  cache 30
  reload
  loop
  bind {bind_ips}
  forward . __PILLAR__CLUSTER__DNS__ {{
    force_tcp
  }}
  prometheus :9253
}}
ip6.arpa:53 {{
  errors
  cache 30
  reload
  loop
  bind {bind_ips}
  forward . __PILLAR__CLUSTER__DNS__ {{
    force_tcp
  }}
  prometheus :9253
}}
.:53 {{
  errors
  cache 30
  reload
  loop
  bind {bind_ips}
  forward . __PILLAR__CLUSTER__DNS__ {{
    force_tcp
  }}
  prometheus :9253
}}
"""


def node_json(name, record):
    labels = {
        "kubernetes.io/os": "linux",
        "kubernetes.io/arch": "amd64",
        "kubernetes.io/hostname": name,
    }
    labels.update(record.get("labels") or {})
    metadata = {"name": name, "labels": labels}
    padding_bytes = int(os.environ.get("DUAL_MOCK_NODE_PADDING_BYTES", "0") or "0")
    if name == "dmit" and padding_bytes > 0:
        metadata["annotations"] = {"test.fugue.io/padding": "x" * padding_bytes}
    return {
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": metadata,
        "spec": {
            "unschedulable": bool(record.get("unschedulable")),
            "taints": record.get("taints") or [],
        },
        "status": {
            "conditions": [
                {"type": "Ready", "status": "True" if record.get("ready") else "False"},
                {"type": "MemoryPressure", "status": "False"},
                {"type": "DiskPressure", "status": "False"},
                {"type": "PIDPressure", "status": "False"},
            ],
            "addresses": [{"type": "ExternalIP", "address": "203.0.113.10"}],
        },
    }


def container(mode, listen_ips):
    return {
        "name": "node-cache",
        "image": "registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739",
        "imagePullPolicy": "IfNotPresent",
        "args": ["-localip", listen_ips, "-conf", "/etc/Corefile", "-upstreamsvc", "fugue-fugue-dns-upstream"],
        "securityContext": {"capabilities": {"add": ["NET_ADMIN"]}},
        "volumeMounts": [
            {"name": "xtables-lock", "mountPath": "/run/xtables.lock"},
            {"name": "config-volume", "mountPath": "/etc/coredns"},
        ],
    }


def pod_json(kind, state):
    if kind == "preserved":
        record = state["preserved"]["pod"]
        mode = state["preserved"]["mode"]
        node = "dmit"
        component = "node-local-dns"
        cohort = ""
        owner_name = "fugue-fugue-node-local-dns"
        owner_uid = state["preserved"]["uid"]
        ready = False
        restart_count = record["restart_count"]
        config_key = record["config_key"]
        listen_ips = record["listen_ips"]
    else:
        record = kind
        mode = state["active"]["mode"]
        node = record["node"]
        component = "node-local-dns-active"
        cohort = "active"
        owner_name = "fugue-fugue-node-local-dns-active"
        owner_uid = state["active"]["uid"]
        ready = True
        restart_count = 0
        config_key = f"Corefile.{mode}"
        listen_ips = "169.254.20.10" if mode == "shadow" else "169.254.20.10,10.43.0.10"
    metadata = {
        "name": record["name"],
        "namespace": "kube-system",
        "uid": record["uid"],
        "ownerReferences": [{
            "apiVersion": "apps/v1",
            "kind": "DaemonSet",
            "name": owner_name,
            "uid": owner_uid,
            "controller": True,
        }],
        "labels": {
            "app.kubernetes.io/name": "fugue",
            "app.kubernetes.io/instance": "fugue",
            "app.kubernetes.io/component": component,
            "fugue.io/node-local-dns-mode": mode,
            "controller-revision-hash": record["revision"],
        },
    }
    if cohort:
        metadata["labels"]["fugue.io/node-local-dns-cohort"] = cohort
    if record.get("deletion_timestamp"):
        metadata["deletionTimestamp"] = record["deletion_timestamp"]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": {
            "nodeName": node,
            "hostNetwork": True,
            "dnsPolicy": "Default",
            "priorityClassName": "fugue-fugue-node-local-dns",
            "serviceAccountName": "fugue-fugue-node-local-dns",
            "automountServiceAccountToken": False,
            "containers": [container(mode, listen_ips)],
            "volumes": [
                {"name": "xtables-lock", "hostPath": {"path": "/run/xtables.lock", "type": "FileOrCreate"}},
                {"name": "config-volume", "configMap": {"name": "fugue-fugue-node-local-dns", "items": [{"key": config_key, "path": "Corefile.base"}]}},
            ],
        },
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
            "containerStatuses": [{
                "name": "node-cache",
                "ready": True,
                "restartCount": restart_count,
                "state": {"running": {"startedAt": "2026-07-13T00:00:00Z"}},
                "lastState": {},
            }],
        },
    }


def daemonset_json(kind, state):
    record = state[kind]
    preserved = kind == "preserved"
    name = "fugue-fugue-node-local-dns" if preserved else "fugue-fugue-node-local-dns-active"
    component = "node-local-dns" if preserved else "node-local-dns-active"
    mode = record["mode"]
    targets = record["targets"]
    layout = record.get("layout", "current")
    current = layout == "current"
    listen_ips = "169.254.20.10" if mode == "shadow" else "169.254.20.10,10.43.0.10"
    selector = {"kubernetes.io/os": "linux"}
    template_labels = {
        "app.kubernetes.io/component": component,
        "fugue.io/node-local-dns-mode": mode,
    }
    pod_spec = {
        "nodeSelector": selector,
        "hostNetwork": True,
        "dnsPolicy": "Default",
        "priorityClassName": "fugue-fugue-node-local-dns",
        "serviceAccountName": "fugue-fugue-node-local-dns",
        "automountServiceAccountToken": False,
        "containers": [container(mode, listen_ips)],
        "volumes": [
            {"name": "xtables-lock", "hostPath": {"path": "/run/xtables.lock", "type": "FileOrCreate"}},
            {"name": "config-volume", "configMap": {
                "name": "fugue-fugue-node-local-dns",
                "items": [{"key": f"Corefile.{mode}" if current else "Corefile", "path": "Corefile.base"}],
            }},
        ],
    }
    update_strategy = {"type": "OnDelete"}
    if current:
        template_labels["fugue.io/node-local-dns-cohort"] = "preserved" if preserved else "active"
        pod_spec["affinity"] = {"nodeAffinity": {"requiredDuringSchedulingIgnoredDuringExecution": {
            "nodeSelectorTerms": [{"matchExpressions": [{
                "key": "kubernetes.io/hostname",
                "operator": "In",
                "values": targets,
            }]}],
        }}}
    else:
        selector["kubernetes.io/hostname"] = targets[0]
        update_strategy = {"type": "RollingUpdate"}
    if preserved:
        pods = [pod_json("preserved", state)]
    else:
        pods = [pod_json(item, state) for item in record["pods"]]
    ready = sum(1 for pod in pods if any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod["status"]["conditions"]))
    status = {
        "observedGeneration": record["observed_generation"],
        "desiredNumberScheduled": len(targets),
        "numberReady": ready,
    }
    if len(targets) - ready:
        status["numberUnavailable"] = len(targets) - ready
    return {
        "apiVersion": "apps/v1",
        "kind": "DaemonSet",
        "metadata": {
            "name": name,
            "namespace": "kube-system",
            "uid": record["uid"],
            "generation": record["generation"],
            "labels": {"app.kubernetes.io/component": component},
            "annotations": {},
        },
        "spec": {
            "updateStrategy": update_strategy,
            "template": {"metadata": {"labels": template_labels}, "spec": pod_spec},
        },
        "status": status,
    }


def controller_revision_list_json(state):
    daemonset = daemonset_json("active", state)
    revision_hash = state["active"]["revision"]
    template = copy.deepcopy(daemonset["spec"]["template"])
    template["$patch"] = "replace"
    return {
        "apiVersion": "apps/v1",
        "kind": "ControllerRevisionList",
        "items": [{
            "apiVersion": "apps/v1",
            "kind": "ControllerRevision",
            "metadata": {
                "name": daemonset["metadata"]["name"] + "-" + revision_hash,
                "namespace": "kube-system",
                "labels": {
                    "app.kubernetes.io/name": "fugue",
                    "app.kubernetes.io/instance": "fugue",
                    "app.kubernetes.io/component": "node-local-dns-active",
                    "controller-revision-hash": revision_hash,
                },
                "ownerReferences": [{
                    "apiVersion": "apps/v1",
                    "kind": "DaemonSet",
                    "name": daemonset["metadata"]["name"],
                    "uid": daemonset["metadata"]["uid"],
                    "controller": True,
                }],
            },
            "revision": 1,
            "data": {"spec": {"template": template}},
        }],
    }


state = load()
command = next((item for item in argv if item in {"get", "delete", "apply", "exec", "rollout", "logs", "describe"}), "")

if command == "get" and "service" in argv:
    if any("jsonpath=" in item for item in argv):
        sys.stdout.write("10.43.0.10")
    else:
        print('{"spec":{"clusterIP":"10.43.0.10","selector":{"k8s-app":"kube-dns"}}}')
    raise SystemExit(0)

if command == "get" and "endpointslices.discovery.k8s.io" in argv:
    print('{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","items":[{"addressType":"IPv4","endpoints":[{"addresses":["10.42.0.2"],"conditions":{"ready":true}}]}]}')
    raise SystemExit(0)

if command == "get" and "configmap" in argv:
    resource = argv[argv.index("configmap") + 1]
    if resource == "coredns":
        sys.stdout.write(".:53 { kubernetes cluster.local in-addr.arpa ip6.arpa }")
    else:
        print(json.dumps({"data": {
            "Corefile": corefile("169.254.20.10 10.43.0.10"),
            "Corefile.shadow": corefile("169.254.20.10"),
            "Corefile.iptables": corefile("169.254.20.10 10.43.0.10"),
        }}, separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and "controllerrevisions.apps" in argv:
    if not state["active"]["exists"]:
        print('{"apiVersion":"apps/v1","kind":"ControllerRevisionList","items":[]}')
    else:
        print(json.dumps(controller_revision_list_json(state), separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and ("daemonset" in argv or any(item.startswith("ds/") for item in argv)):
    if "daemonset" in argv:
        name = argv[argv.index("daemonset") + 1]
    else:
        name = next(item.split("/", 1)[1] for item in argv if item.startswith("ds/"))
    kind = "preserved" if name == "fugue-fugue-node-local-dns" else "active" if name == "fugue-fugue-node-local-dns-active" else ""
    exists = bool(kind and state[kind]["exists"])
    if not exists:
        raise SystemExit(0 if "--ignore-not-found" in argv else 1)
    if "-o" in argv:
        output = argv[argv.index("-o") + 1]
        if output == "name":
            sys.stdout.write(f"daemonset.apps/{name}")
        elif output == "json":
            print(json.dumps(daemonset_json(kind, state), separators=(",", ":")))
        elif output.startswith("jsonpath=") and "updateStrategy.type" in output:
            sys.stdout.write(daemonset_json(kind, state)["spec"]["updateStrategy"]["type"])
    else:
        print(json.dumps(daemonset_json(kind, state), separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and "nodes" in argv:
    items = [node_json(name, record) for name, record in state["nodes"].items()]
    if "--no-headers" in argv:
        for item in items:
            status = "Ready" if item["status"]["conditions"][0]["status"] == "True" else "NotReady"
            print(f"{item['metadata']['name']} {status} <none> 1d v1.35.4")
    else:
        print(json.dumps({"items": items}, separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and "node" in argv:
    name = argv[argv.index("node") + 1]
    if name not in state["nodes"]:
        raise SystemExit(1)
    node = node_json(name, state["nodes"][name])
    output = argv[argv.index("-o") + 1] if "-o" in argv else ""
    if output == "json":
        print(json.dumps(node, separators=(",", ":")))
    elif "status.conditions" in output:
        sys.stdout.write("True" if state["nodes"][name]["ready"] else "False")
    elif "kubernetes\\.io/os" in output:
        sys.stdout.write("linux")
    elif "kubernetes\\.io/arch" in output:
        sys.stdout.write("amd64")
    elif "spec.unschedulable" in output:
        sys.stdout.write("true" if state["nodes"][name]["unschedulable"] else "false")
    raise SystemExit(0)

if command == "get" and "pods" in argv and "--all-namespaces" in argv:
    padding_bytes = int(os.environ.get("DUAL_MOCK_POD_PADDING_BYTES", "0") or "0")
    items = []
    if padding_bytes > 0:
        items.append({
            "metadata": {"annotations": {"test.fugue.io/padding": "x" * padding_bytes}},
            "spec": {"containers": []},
            "status": {"phase": "Running"},
        })
    print(json.dumps({"items": items}, separators=(",", ":")))
    raise SystemExit(0)

if command == "get" and "pods" in argv:
    selector = argv[argv.index("-l") + 1] if "-l" in argv else ""
    component = ""
    for part in selector.split(","):
        if part.startswith("app.kubernetes.io/component="):
            component = part.split("=", 1)[1]
    if component == "node-local-dns-active":
        pods = [pod_json(item, state) for item in state["active"]["pods"]] if state["active"]["exists"] else []
    elif component == "node-local-dns":
        pods = [pod_json("preserved", state)] if state["preserved"]["exists"] else []
    else:
        pods = []
    if "--no-headers" in argv:
        for pod in pods:
            ready = "1/1" if pod["status"]["conditions"][0]["status"] == "True" else "0/1"
            print(f"{pod['metadata']['name']} {ready} Running 0 1m")
    elif "-o" in argv:
        output = argv[argv.index("-o") + 1]
        if output == "json":
            print(json.dumps({"items": pods}, separators=(",", ":")))
        elif output.startswith("jsonpath="):
            for pod in pods:
                print(f"{pod['metadata']['name']}\t{pod['spec']['nodeName']}")
    raise SystemExit(0)

if command == "delete" and "daemonset" in argv:
    name = argv[argv.index("daemonset") + 1]
    if name != "fugue-fugue-node-local-dns-active":
        raise SystemExit("refusing mock deletion of preserved DaemonSet")
    state["active"]["exists"] = False
    state["active"]["pods"] = []
    save(state)
    raise SystemExit(0)

if command == "rollout":
    raise SystemExit(0)

if command in {"apply", "exec", "logs", "describe"}:
    raise SystemExit(0)

raise SystemExit(1)
PY
chmod +x "${DUAL_MOCK_KUBECTL}"

dual_state_json() {
  "${DUAL_MOCK_STATE_CTL}" "${DUAL_MOCK_STATE}" dump
}

set_dual_cohort_values() {
  set_common_values
  KUBECTL="${DUAL_MOCK_KUBECTL}"
  FUGUE_NODE_LOCAL_DNS_PRESERVED_OFFLINE_NODES=dmit
  FUGUE_NODE_LOCAL_DNS_NODE_NAME=node-a
  FUGUE_NODE_LOCAL_DNS_NODE_NAMES=""
  FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=false
}

# The dual-cohort tests keep the exact artifact and preserved snapshot checks
# from the release library. Only live DNS probes are replaced with an exact
# active-Pod runtime check because the mock has no network namespace.
node_local_dns_verify_central_coredns_ready() {
  return 0
}

node_local_dns_observe_one_node() {
  return 0
}

node_local_dns_verify_running() {
  local mode="$1"
  local service_ip="$2"
  local pods_json=""

  pods_json="$(node_local_dns_capture_pods_json)" || return 1
  node_local_dns_validate_pure_pod_snapshot "${pods_json}" "${NODE_LOCAL_DNS_TARGET_NODES}" "${mode}" || return 1
  node_local_dns_validate_active_pod_runtime \
    "${pods_json}" "${NODE_LOCAL_DNS_TARGET_NODES}" "${mode}" "${service_ip}" true
}

node_local_dns_verify_teardown() {
  return 0
}

"${DUAL_MOCK_STATE_CTL}" "${DUAL_MOCK_STATE}" reset
: >"${DUAL_MOCK_LOG}"
set_dual_cohort_values
prepare_node_local_dns_helm_args

# Real Node objects include image inventories and can exceed Linux's 128 KiB
# per-argument/per-environment-entry limit. Both whole-cluster consumers must
# stream the document over stdin instead of exporting it as one environment
# variable.
export DUAL_MOCK_NODE_PADDING_BYTES=140000
node_local_dns_verify_preserved_nodes_isolated
[[ "$(node_local_dns_replacement_order node-a)" == "node-a" ]]
unset DUAL_MOCK_NODE_PADDING_BYTES

# A busy node can also return more than 128 KiB of Pod inventory before the
# first Helm mutation. Keep both documents on stdin so the host-port gate
# remains exact without depending on the per-environment-entry size limit.
export DUAL_MOCK_POD_PADDING_BYTES=1450000
pod_inventory_bytes="$(${KUBECTL} get pods --all-namespaces --field-selector spec.nodeName=node-a -o json | wc -c)"
(( pod_inventory_bytes > 131072 ))
[[ -z "$(node_local_dns_pod_dns_host_port_conflicts node-a)" ]]
unset DUAL_MOCK_POD_PADDING_BYTES

[[ "${NODE_LOCAL_DNS_SPLIT_COHORT}" == "true" ]]
[[ "${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}" == "fugue-fugue-node-local-dns" ]]
[[ "${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}" == "fugue-fugue-node-local-dns-active" ]]
[[ "${NODE_LOCAL_DNS_ACTIVE_COMPONENT}" == "node-local-dns-active" ]]
[[ "${NODE_LOCAL_DNS_PREVIOUS_ENABLED}" == "false" ]]
[[ "${NODE_LOCAL_DNS_ADDED_NODES}" == "node-a" ]]
[[ "${NODE_LOCAL_DNS_TARGET_NODES}" == "node-a" ]]
dual_helm_args="${NODE_LOCAL_DNS_HELM_SET_ARGS[*]}"
grep -Fq 'nodeLocalDNS.mode=shadow' <<<"${dual_helm_args}"
grep -Fq 'nodeLocalDNS.legacyMode=iptables' <<<"${dual_helm_args}"
grep -Fq 'nodeLocalDNS.nodeSelector.kubernetes\.io/hostname=' <<<"${dual_helm_args}"
grep -Fq 'nodeLocalDNS.targetNodes=["node-a"]' <<<"${dual_helm_args}"
grep -Fq 'nodeLocalDNS.preservedOfflineNodes=["dmit"]' <<<"${dual_helm_args}"

PRESERVED_DS="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_JSON}" \
  PRESERVED_PODS="${NODE_LOCAL_DNS_PRESERVED_PODS_JSON}" python3 - <<'PY'
import json
import os

daemonset = json.loads(os.environ["PRESERVED_DS"])
pods = json.loads(os.environ["PRESERVED_PODS"])
assert daemonset["metadata"]["name"] == "fugue-fugue-node-local-dns"
assert daemonset["metadata"]["uid"] == "uid-preserved-ds"
assert daemonset["spec"]["template"]["metadata"]["labels"]["fugue.io/node-local-dns-mode"] == "iptables"
assert daemonset["spec"]["template"]["spec"]["nodeSelector"] == {
    "kubernetes.io/os": "linux",
    "kubernetes.io/hostname": "dmit",
}
assert len(pods) == 1
pod = pods[0]
assert pod["name"] == "fugue-fugue-node-local-dns-dmit-fixed"
assert pod["uid"] == "uid-preserved-pod"
assert pod["node"] == "dmit"
assert pod["mode"] == "iptables"
assert pod["ready"] is False
assert pod["phase"] == "Running"
assert pod["restart_count"] == 1
assert pod["deletion_timestamp"] == ""
assert pod["image"] == "registry.k8s.io/dns/k8s-dns-node-cache@sha256:bc6e64e2c85956af2fcc0aa720086410d41b4f31f378c9a92646fecc85cd4739"
assert pod["args"] == [
    "-localip", "169.254.20.10,10.43.0.10", "-conf", "/etc/Corefile",
    "-upstreamsvc", "fugue-fugue-dns-upstream",
]
assert pod["config_items"] == [{"key": "Corefile", "path": "Corefile.base"}]
PY

preserved_pod_before_forward="$(dual_state_json | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["preserved"]["pod"],sort_keys=True))')"
"${DUAL_MOCK_STATE_CTL}" "${DUAL_MOCK_STATE}" forward
active_pods_json="$(node_local_dns_capture_pods_json)"
ACTIVE_PODS="${active_pods_json}" python3 - <<'PY'
import json
import os

pods = json.loads(os.environ["ACTIVE_PODS"])
assert len(pods) == 1
assert pods[0]["node"] == "node-a"
assert pods[0]["component"] == "node-local-dns-active"
assert pods[0]["cohort"] == "active"
assert all(pod["node"] != "dmit" for pod in pods)
PY
preserved_pods_json="$(node_local_dns_capture_preserved_pods_json)"
PRESERVED_PODS="${preserved_pods_json}" python3 -c 'import json,os; pods=json.loads(os.environ["PRESERVED_PODS"]); assert len(pods)==1 and pods[0]["node"]=="dmit"'

node_local_dns_reconcile_after_helm
preserved_pod_after_reconcile="$(dual_state_json | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["preserved"]["pod"],sort_keys=True))')"
[[ "${preserved_pod_after_reconcile}" == "${preserved_pod_before_forward}" ]]
node_local_dns_verify_preserved_state_unchanged

dual_forward_baseline="${TMP_DIR}/dual-forward-baseline.json"
cp "${DUAL_MOCK_STATE}" "${dual_forward_baseline}"
for preserved_drift in uid args config restart deletion; do
  cp "${dual_forward_baseline}" "${DUAL_MOCK_STATE}"
  "${DUAL_MOCK_STATE_CTL}" "${DUAL_MOCK_STATE}" preserved-drift "${preserved_drift}"
  if node_local_dns_verify_preserved_state_unchanged >/dev/null 2>&1; then
    echo "preserved offline NodeLocal DNSCache ${preserved_drift} drift must fail closed" >&2
    exit 1
  fi
done
cp "${dual_forward_baseline}" "${DUAL_MOCK_STATE}"
node_local_dns_verify_preserved_state_unchanged

saved_active_daemonset_name="${NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME}"
NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME="${NODE_LOCAL_DNS_PRESERVED_DAEMONSET_NAME}"
if node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}" >/dev/null 2>&1; then
  echo "split-cohort delete guard must refuse the preserved DaemonSet" >&2
  exit 1
fi
NODE_LOCAL_DNS_ACTIVE_DAEMONSET_NAME="${saved_active_daemonset_name}"

preserved_before_active_delete="$(dual_state_json | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["preserved"],sort_keys=True))')"
: >"${DUAL_MOCK_LOG}"
node_local_dns_delete_daemonset_safely "${NODE_LOCAL_DNS_KUBE_DNS_SERVICE_IP}"
preserved_after_active_delete="$(dual_state_json | python3 -c 'import json,sys; print(json.dumps(json.load(sys.stdin)["preserved"],sort_keys=True))')"
[[ "${preserved_after_active_delete}" == "${preserved_before_active_delete}" ]]
dual_state_json | python3 -c 'import json,sys; state=json.load(sys.stdin); assert state["preserved"]["exists"] is True; assert state["active"]["exists"] is False'
grep -Fq 'delete daemonset fugue-fugue-node-local-dns-active' "${DUAL_MOCK_LOG}"
if grep -Eq 'delete daemonset fugue-fugue-node-local-dns([[:space:]]|$)' "${DUAL_MOCK_LOG}"; then
  echo "active rollback deleted the preserved base DaemonSet" >&2
  exit 1
fi

"${DUAL_MOCK_STATE_CTL}" "${DUAL_MOCK_STATE}" rollback-base
node_local_dns_verify_preserved_snapshot_after_helm_rollback
if grep -Eq 'delete (daemonset|pod) fugue-fugue-node-local-dns-dmit' "${DUAL_MOCK_LOG}"; then
  echo "Helm rollback touched the preserved dmit Pod or DaemonSet" >&2
  exit 1
fi

echo "[test_node_local_dns_release] allow-all excludes cordoned nodes from the first active canary"
"${DUAL_MOCK_STATE_CTL}" "${DUAL_MOCK_STATE}" reset
"${DUAL_MOCK_STATE_CTL}" "${DUAL_MOCK_STATE}" node-ready node-b false
: >"${DUAL_MOCK_LOG}"
set_dual_cohort_values
FUGUE_NODE_LOCAL_DNS_NODE_NAME=""
FUGUE_NODE_LOCAL_DNS_NODE_NAMES=""
FUGUE_NODE_LOCAL_DNS_ALLOW_ALL_NODES=true
prepare_node_local_dns_helm_args
[[ "${NODE_LOCAL_DNS_TARGET_NODES}" == "node-a" ]]
[[ "${NODE_LOCAL_DNS_ADDED_NODES}" == "node-a" ]]
if grep -Fq 'get node node-c ' "${DUAL_MOCK_LOG}"; then
  echo "cordoned node-c reached the explicit target preflight" >&2
  exit 1
fi

echo "[test_node_local_dns_release] ok"
