"""Reconcile one explicit scrape job and reload the existing process.

This independent configuration lane preserves the Pod and its TSDB. It does
not deploy component code or derive serving policy from observed health.
"""
import argparse
import json
import re
import subprocess
import time
from pathlib import Path


def run(args, data=None):
    return subprocess.run(args, input=data, text=True, capture_output=True, check=True, timeout=40).stdout


def project(config, policy):
    if policy.get("version") != 1 or not policy.get("componentPorts"):
        raise ValueError("unsupported or empty scrape policy")
    entries = policy["componentPorts"]
    if len(entries) != len(set(entries)):
        raise ValueError("duplicate component/port policy")
    for entry in entries:
        component, port = entry.split(";")
        if port not in {"metrics", "health", "http"} or ".*" in component:
            raise ValueError("unbounded component or unsupported port")
        re.compile(component)
    match = list(re.finditer(r"(?m)^([ ]*)- job_name: *" + re.escape(policy["job"]) + r"\s*$", config))
    if len(match) != 1:
        raise ValueError("expected exactly one configured scrape job")
    start = match[0].start()
    indent = match[0].group(1)
    next_job = re.search(r"(?m)^" + re.escape(indent) + r"- job_name:", config[match[0].end():])
    end = match[0].end() + next_job.start() if next_job else len(config)
    block = config[start:end]
    # Only replace existing keep/drop gates over component and port labels;
    # retain instance, namespace, pod labels and every other scrape job.
    rule = re.compile(r"(?m)^([ ]*)- source_labels: \[(__meta_kubernetes_pod_label_app_kubernetes_io_component(?:, __meta_kubernetes_pod_container_port_name)?|__meta_kubernetes_pod_container_port_name)\]\n\1  regex: [^\n]+\n\1  action: (?:keep|drop)\n")
    gates = list(rule.finditer(block))
    if not gates:
        raise ValueError("expected explicit component/port relabel gates")
    prefix = gates[0].group(1)
    replacement = (prefix + "- source_labels: [__meta_kubernetes_pod_label_app_kubernetes_io_component, __meta_kubernetes_pod_container_port_name]\n" + prefix + "  regex: " + json.dumps("|".join(entries)) + "\n" + prefix + "  action: keep\n")
    first = True
    def replace(_):
        nonlocal first
        if first:
            first = False
            return replacement
        return ""
    block = rule.sub(replace, block)
    return config[:start] + block + config[end:]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("policy")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    policy = json.loads(Path(args.policy).read_text())
    base = ["kubectl", "-n", policy["namespace"]]
    cm = json.loads(run(base + ["get", "configmap", policy["configMap"], "-o", "json"]))
    old = cm["data"]["prometheus.yml"]
    new = project(old, policy)
    deployment = json.loads(run(base + ["get", "deployment", policy["deployment"], "-o", "json"]))
    selector = ",".join(k + "=" + v for k, v in deployment["spec"]["selector"]["matchLabels"].items())
    pods = json.loads(run(base + ["get", "pods", "-l", selector, "-o", "json"]))["items"]
    pods = [p for p in pods if any(c.get("type") == "Ready" and c.get("status") == "True" for c in p["status"].get("conditions", []))]
    if not pods:
        raise RuntimeError("no Ready Prometheus process to validate and reload")
    for pod in pods:
        run(base + ["exec", "-i", pod["metadata"]["name"], "-c", policy["container"], "--", "/bin/promtool", "check", "config", "/dev/stdin"], new)
    print(json.dumps({"changed": old != new, "validatedPods": len(pods), "apply": args.apply}))
    if not args.apply:
        return
    if old != new:
        patch = [{"op": "test", "path": "/metadata/resourceVersion", "value": cm["metadata"]["resourceVersion"]}, {"op": "replace", "path": "/data/prometheus.yml", "value": new}]
        run(base + ["patch", "configmap", policy["configMap"], "--type=json", "--patch-file=/dev/stdin"], json.dumps(patch))
    for pod in pods:
        name = pod["metadata"]["name"]
        deadline = time.monotonic() + 180
        while run(base + ["exec", name, "-c", policy["container"], "--", "cat", policy["configPath"]]) != new:
            if time.monotonic() >= deadline:
                raise RuntimeError("projected Prometheus config has not converged")
            time.sleep(5)
        run(base + ["exec", name, "-c", policy["container"], "--", "/bin/promtool", "check", "config", policy["configPath"]])
        run(base + ["exec", name, "-c", policy["container"], "--", "/bin/sh", "-c", "kill -HUP 1"])
    # Inspect the live loaded configuration, not just the mounted file.
    for pod in pods:
        proxy = "/api/v1/namespaces/" + policy["namespace"] + "/pods/" + pod["metadata"]["name"] + ":9090/proxy/api/v1/status/config"
        deadline = time.monotonic() + 30
        while True:
            loaded = json.loads(run(["kubectl", "get", "--raw", proxy]))
            yaml = loaded.get("data", {}).get("yaml", "")
            if "|".join(policy["componentPorts"]) in yaml:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Prometheus did not load the new scrape policy")
            time.sleep(2)
    print("Prometheus scrape policy loaded without replacing Pods or TSDB")


if __name__ == "__main__":
    main()
