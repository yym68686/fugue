#!/usr/bin/env python3
"""Apply an independent namespace memory reservation policy without code rollout."""
import argparse
import json
import re
import subprocess
from pathlib import Path

MANAGER = "fugue-workload-memory-policy"
POLICY_NAME = "fugue-default-memory-request"


def load_policy(path):
    policy = json.loads(Path(path).read_text())
    if policy.get("apiVersion") != "constraints.fugue.dev/v1" or policy.get("kind") != "WorkloadMemoryPolicy":
        raise ValueError("unsupported policy")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", policy.get("controlPlaneNamespace", "")):
        raise ValueError("controlPlaneNamespace is required")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", policy.get("controllerServiceAccount", "")):
        raise ValueError("controllerServiceAccount is required")
    memory = policy.get("defaultMemoryRequest", "")
    if not re.fullmatch(r"[1-9][0-9]*(Mi|Gi)", memory):
        raise ValueError("defaultMemoryRequest must be a positive Mi/Gi quantity")
    prefixes = policy.get("namespacePrefixes")
    if not isinstance(prefixes, list) or not prefixes or any(not isinstance(p, str) or not re.fullmatch(r"[a-z][a-z0-9-]+-", p) for p in prefixes):
        raise ValueError("explicit namespace prefixes ending with '-' are required")
    if type(policy.get("resizeExisting")) is not bool:
        raise ValueError("resizeExisting must be boolean")
    return policy


def limit_range(namespace, memory):
    return {"apiVersion": "v1", "kind": "LimitRange",
            "metadata": {"name": POLICY_NAME, "namespace": namespace,
                         "labels": {"app.kubernetes.io/managed-by": MANAGER}},
            "spec": {"limits": [{"type": "Container", "defaultRequest": {"memory": memory}}]}}


def controller_config_access(policy):
    namespace = policy["controlPlaneNamespace"]
    role = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
            "metadata": {"name": MANAGER, "namespace": namespace},
            "rules": [{"apiGroups": [""], "resources": ["configmaps"],
                       "resourceNames": [MANAGER], "verbs": ["get"]}]}
    binding = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
               "metadata": {"name": MANAGER, "namespace": namespace},
               "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": MANAGER},
               "subjects": [{"kind": "ServiceAccount", "name": policy["controllerServiceAccount"], "namespace": namespace}]}
    return role, binding


def resize_patch(pod, memory):
    # Increasing only a request does not change a container's memory limit or
    # executable. Existing requests, including intentionally small ones, win.
    if pod.get("status", {}).get("phase") != "Running" or pod["metadata"].get("deletionTimestamp"):
        return None
    containers = []
    for container in pod["spec"].get("containers", []):
        resources = container.get("resources", {})
        if "memory" in resources.get("requests", {}):
            continue
        # Kubernetes defaults a request from an explicit limit. Respect that
        # bound instead of proposing a request above the container's limit.
        value = resources.get("limits", {}).get("memory", memory)
        containers.append({"name": container["name"], "resources": {"requests": {"memory": value}}})
    if not containers:
        return None
    return {"metadata": {"resourceVersion": pod["metadata"]["resourceVersion"]},
            "spec": {"containers": containers}}


def kubectl(*args, body=None):
    result = subprocess.run(["kubectl", *args], input=body, text=True, capture_output=True, timeout=45)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def reconcile(policy, apply=False):
    config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": {
        "name": MANAGER, "namespace": policy["controlPlaneNamespace"],
        "labels": {"app.kubernetes.io/managed-by": MANAGER}}, "data": {"policy.json": json.dumps(policy)}}
    role = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
            "metadata": {"name": MANAGER}, "rules": [{"apiGroups": [""], "resources": ["limitranges"],
                                                      "verbs": ["get", "list", "create"]}]}
    binding = {"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
               "metadata": {"name": MANAGER}, "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": MANAGER},
               "subjects": [{"kind": "ServiceAccount", "name": policy["controllerServiceAccount"], "namespace": policy["controlPlaneNamespace"]}]}
    args = ["apply", "--server-side", f"--field-manager={MANAGER}", "-f", "-"]
    if not apply:
        args.append("--dry-run=server")
    for obj in [role, binding, *controller_config_access(policy), config]:
        kubectl(*args, body=json.dumps(obj))
    if apply:
        # Validate as the actual consumer, not only as the privileged CI writer.
        kubectl("auth", "can-i", "get", "configmap/" + MANAGER,
                "-n", policy["controlPlaneNamespace"],
                "--as=system:serviceaccount:" + policy["controlPlaneNamespace"] + ":" + policy["controllerServiceAccount"])
    namespaces = json.loads(kubectl("get", "namespaces", "-o", "json"))["items"]
    names = sorted(n["metadata"]["name"] for n in namespaces
                   if any(n["metadata"]["name"].startswith(p) for p in policy["namespacePrefixes"])
                   and not n["metadata"].get("deletionTimestamp"))
    changed, resized, requires_rollout = 0, 0, []
    for namespace in names:
        existing = json.loads(kubectl("get", "limitranges", "-n", namespace, "-o", "json"))["items"]
        own = next((x for x in existing if x["metadata"]["name"] == POLICY_NAME), None)
        if own and own["metadata"].get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER:
            raise ValueError(f"{namespace}: policy name already owned by another manager")
        # Do not add a second default with nondeterministic admission ordering.
        if any(x["metadata"]["name"] != POLICY_NAME and any(
                "memory" in rule.get("defaultRequest", {}) or "memory" in rule.get("default", {})
                for rule in x.get("spec", {}).get("limits", [])) for x in existing):
            print(f"{namespace}: existing memory admission policy retained", flush=True)
            continue
        desired = limit_range(namespace, policy["defaultMemoryRequest"])
        if not own or own.get("spec") != desired["spec"]:
            args = ["apply", "--server-side", f"--field-manager={MANAGER}", "-f", "-"]
            if not apply:
                args.append("--dry-run=server")
            kubectl(*args, body=json.dumps(desired))
            changed += 1
        if policy["resizeExisting"]:
            pods = json.loads(kubectl("get", "pods", "-n", namespace, "-o", "json"))["items"]
            for pod in pods:
                patch = resize_patch(pod, policy["defaultMemoryRequest"])
                if patch is None:
                    continue
                if pod.get("status", {}).get("qosClass") == "BestEffort":
                    requires_rollout.append(namespace + "/" + pod["metadata"]["name"])
                    continue
                args = ["patch", "pod", pod["metadata"]["name"], "-n", namespace,
                        "--subresource=resize", "--type=strategic", "-p", json.dumps(patch)]
                if not apply:
                    args.append("--dry-run=server")
                # resourceVersion fencing prevents overwriting concurrent resize.
                kubectl(*args)
                resized += 1
        print(f"{namespace}: memory request policy checked", flush=True)
    print(json.dumps({"applied": apply, "namespaces": len(names), "policy_changes": changed, "pod_resizes": resized, "requires_rollout": requires_rollout}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("policy")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    reconcile(load_policy(args.policy), args.apply)
