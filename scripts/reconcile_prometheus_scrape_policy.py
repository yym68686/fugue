"""Reconcile explicit scrape policy and reload the existing process.

This independent configuration lane preserves the Pod and its TSDB. It does
not deploy component code or derive serving policy from observed health.
"""
import argparse
import json
import re
import subprocess
import time
from pathlib import Path
from urllib.parse import urlencode

MANAGED_BEGIN = "  # BEGIN Fugue additional scrape configs\n"
MANAGED_END = "  # END Fugue additional scrape configs\n"
LIFECYCLE_RULE_FILE = 'fugue-storage-lifecycle-rules.yml'
LIFECYCLE_RULE_PATH = '  - /etc/prometheus/fugue-storage-lifecycle-*.yml\n'
LIFECYCLE_RULE_OWNER = '# Managed Fugue storage lifecycle alerts v1\n'


def lifecycle_rules(policy):
    setting = policy.get('storageLifecycleAlerts', {'version': 1, 'enabled': False})
    if (not isinstance(setting, dict) or set(setting) != {'version', 'enabled'} or
            type(setting['version']) is not int or setting['version'] != 1 or type(setting['enabled']) is not bool):
        raise ValueError('invalid storage lifecycle alert configuration')
    if not setting['enabled']:
        return None
    rules = []
    for name, expression, duration in [
        ('FugueStorageLifecyclePolicyInvalid', 'fugue_storage_lifecycle_policy_read_failed == 1', '5m'),
        ('FugueStorageLifecycleAuditFailed', '(fugue_storage_lifecycle_last_run_failed == 1) and (fugue_storage_lifecycle_audit_enabled == 1)', '10m'),
        ('FugueStorageLifecycleAuditStale', '(time() - fugue_storage_lifecycle_last_success_timestamp_seconds > 2 * fugue_storage_lifecycle_interval_seconds) and (fugue_storage_lifecycle_audit_enabled == 1)', '10m'),
        ('FugueObservationRetentionFailed', '(fugue_observation_retention_last_run_failed == 1) and (fugue_observation_retention_enabled == 1)', '10m'),
    ]:
        rules.append({'alert': name, 'expr': expression, 'for': duration,
                      'labels': {'severity': 'warning'}, 'annotations': {'summary': name, 'description': 'Storage observation/cleanup needs investigation; this alert does not authorize deleting protected history.'}})
    # Capacity monitoring intentionally covers EVERY measured PVC in a namespace
    # opting into lifecycle audit, including but not falsely limited to PostgreSQL.
    free = '(100 * max by(namespace,persistentvolumeclaim) (kubelet_volume_stats_available_bytes) / max by(namespace,persistentvolumeclaim) (kubelet_volume_stats_capacity_bytes))'
    for level, severity in [('warning', 'warning'), ('pressure', 'warning'), ('critical', 'page')]:
        threshold = f'max by(namespace) (fugue_storage_lifecycle_free_percent_threshold{{level="{level}"}})'
        expression = free + ' < on(namespace) group_left ' + threshold
        # A removed/invalid/disabled policy must not keep old thresholds active.
        expression += ' and on(namespace) (max by(namespace) (fugue_storage_lifecycle_audit_enabled) == 1)'
        rules.append({'alert': 'FugueStorageVolumeFree' + level.title(), 'expr': expression, 'for': '10m',
                      'labels': {'severity': severity}, 'annotations': {'summary': 'Low available space on a measured Fugue namespace PVC', 'description': 'Investigate PVC space and WAL/maintenance headroom. Unknown and protected history must not be deleted to satisfy a threshold.'}})
    return LIFECYCLE_RULE_OWNER + json.dumps({'groups': [{'name': 'fugue-storage-lifecycle', 'rules': rules}]}, sort_keys=True) + '\n'


def lifecycle_rule_path(config, policy):
    if config.count(LIFECYCLE_RULE_PATH) > 1:
        raise ValueError('duplicate managed lifecycle rule path')
    config = config.replace(LIFECYCLE_RULE_PATH, '')
    if lifecycle_rules(policy) is None:
        return config
    headers = list(re.finditer(r'(?m)^rule_files:[ \t]*\n', config))
    if len(headers) != 1:
        raise ValueError('expected one block-style rule_files section')
    at = headers[0].end()
    return config[:at] + LIFECYCLE_RULE_PATH + config[at:]


def additional_scrapes(config, policy):
    """Own only a marked block; preserve every unrelated job and root setting.

    JSON field values are YAML flow values. promtool validates the complete
    configuration using the running Prometheus version before any mutation.
    An empty list removes the managed block, allowing configuration rollback.
    """
    jobs = policy.get("additionalScrapeConfigs", [])
    if not isinstance(jobs, list):
        raise ValueError("additionalScrapeConfigs must be a list")
    if config.count(MANAGED_BEGIN) != config.count(MANAGED_END) or config.count(MANAGED_BEGIN) > 1:
        raise ValueError("ambiguous managed scrape block")
    if MANAGED_BEGIN in config:
        start, end = config.index(MANAGED_BEGIN), config.index(MANAGED_END)
        if end < start:
            raise ValueError("invalid managed scrape block")
        config = config[:start] + config[end + len(MANAGED_END):]
    names = set()
    blocks = []
    for job in jobs:
        name = job.get("job_name", "") if isinstance(job, dict) else ""
        if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9_-]+", name) or name in names:
            raise ValueError("invalid or duplicate additional scrape job name")
        names.add(name)
        # A job outside the owned block belongs to Helm or another policy.
        if re.search(r"(?m)^\s*-\s*job_name:\s*[\"']?" + re.escape(name) + r"[\"']?\s*$", config):
            raise ValueError("additional scrape job conflicts with existing job: " + name)
        block = "  - job_name: " + name + "\n"
        for key, value in job.items():
            if not re.fullmatch(r"[a-z_]+", key):
                raise ValueError("invalid scrape config field")
            if key != "job_name":
                block += "    " + key + ": " + json.dumps(value, allow_nan=False) + "\n"
        blocks.append(block)
    if not blocks:
        return config
    headers = list(re.finditer(r"(?m)^scrape_configs:[ \t]*\n", config))
    if len(headers) != 1:
        raise ValueError("expected one block-style scrape_configs section")
    start = headers[0].end()
    next_root = re.search(r"(?m)^[a-zA-Z_][a-zA-Z0-9_]*:", config[start:])
    end = start + next_root.start() if next_root else len(config)
    return config[:end].rstrip("\n") + "\n" + MANAGED_BEGIN + "".join(blocks) + MANAGED_END + config[end:]


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
    return lifecycle_rule_path(additional_scrapes(config[:start] + block + config[end:], policy), policy)


def query_prometheus(proxy, expression):
    response = json.loads(run(["kubectl", "get", "--raw", proxy + "/api/v1/query?" + urlencode({"query": expression})]))
    if response.get("status") != "success":
        raise RuntimeError("Prometheus verification query failed")
    return response.get("data", {}).get("result", [])


def verify_queries(proxy, queries):
    # Each configured predicate returns a nonempty vector on success. A stale
    # metric or an absent target must not be accepted as a successful rollout.
    deadline = time.monotonic() + 120
    pending = list(queries)
    while pending:
        pending = [expression for expression in queries if not query_prometheus(proxy, expression)]
        if not pending:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("Prometheus metric verification failed: " + "; ".join(pending))
        time.sleep(5)


def public_dns_targets_match(services, endpoints, targets, check):
    """Compare selected backends, not merely a count of healthy DNS Pods."""
    selected = {service["metadata"]["name"] for service in services
                if re.fullmatch(check["serviceNamePattern"], service["metadata"]["name"])}
    if not selected:
        return False
    expected = set()
    for endpoint in endpoints:
        service = endpoint["metadata"]["name"]
        if service not in selected:
            continue
        for subset in endpoint.get("subsets", []):
            for address in subset.get("addresses", []) + subset.get("notReadyAddresses", []):
                ref = address.get("targetRef", {})
                if ref.get("kind") == "Pod" and ref.get("name"):
                    expected.add((service, ref["name"]))
    if {service for service, _ in expected} != selected:
        return False
    observed = {(target.get("labels", {}).get("service"), target.get("labels", {}).get("pod"))
                for target in targets if target.get("labels", {}).get("job") == check["job"] and target.get("health") == "up"}
    return expected == observed


def verify_public_dns_targets(base, proxy, check):
    deadline = time.monotonic() + 120
    while True:
        services = json.loads(run(base + ["get", "services", "-l", check["serviceLabelSelector"], "-o", "json"]))["items"]
        endpoints = json.loads(run(base + ["get", "endpoints", "-o", "json"]))["items"]
        targets = json.loads(run(["kubectl", "get", "--raw", proxy + "/api/v1/targets?state=active"]))["data"]["activeTargets"]
        if public_dns_targets_match(services, endpoints, targets, check):
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("public DNS metrics targets do not match the selected Service backends")
        time.sleep(5)


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
    rules = lifecycle_rules(policy)
    old_rules = cm['data'].get(LIFECYCLE_RULE_FILE)
    if old_rules is not None and not old_rules.startswith(LIFECYCLE_RULE_OWNER):
        raise ValueError('lifecycle rule filename is owned by another publisher')
    deployment = json.loads(run(base + ["get", "deployment", policy["deployment"], "-o", "json"]))
    selector = ",".join(k + "=" + v for k, v in deployment["spec"]["selector"]["matchLabels"].items())
    pods = json.loads(run(base + ["get", "pods", "-l", selector, "-o", "json"]))["items"]
    pods = [p for p in pods if any(c.get("type") == "Ready" and c.get("status") == "True" for c in p["status"].get("conditions", []))]
    if not pods:
        raise RuntimeError("no Ready Prometheus process to validate and reload")
    for pod in pods:
        if rules is not None:
            run(base + ["exec", "-i", pod["metadata"]["name"], "-c", policy["container"], "--", "/bin/promtool", "check", "rules", "/dev/stdin"], rules)
        run(base + ["exec", "-i", pod["metadata"]["name"], "-c", policy["container"], "--", "/bin/promtool", "check", "config", "/dev/stdin"], new)
    print(json.dumps({"changed": old != new or old_rules != rules, "validatedPods": len(pods), "apply": args.apply}))
    if not args.apply:
        return
    if old != new or old_rules != rules:
        patch = [{"op": "test", "path": "/metadata/resourceVersion", "value": cm["metadata"]["resourceVersion"]}, {"op": "replace", "path": "/data/prometheus.yml", "value": new}]
        if rules is not None:
            patch.append({'op': 'add', 'path': '/data/' + LIFECYCLE_RULE_FILE, 'value': rules})
        elif old_rules is not None:
            patch.append({'op': 'remove', 'path': '/data/' + LIFECYCLE_RULE_FILE})
        run(base + ["patch", "configmap", policy["configMap"], "--type=json", "--patch-file=/dev/stdin"], json.dumps(patch))
    for pod in pods:
        name = pod["metadata"]["name"]
        deadline = time.monotonic() + 180
        while run(base + ["exec", name, "-c", policy["container"], "--", "cat", policy["configPath"]]) != new:
            if time.monotonic() >= deadline:
                raise RuntimeError("projected Prometheus config has not converged")
            time.sleep(5)
        if rules is not None:
            rule_path = str(Path(policy['configPath']).parent / LIFECYCLE_RULE_FILE)
            while run(base + ['exec', name, '-c', policy['container'], '--', 'cat', rule_path]) != rules:
                if time.monotonic() >= deadline:
                    raise RuntimeError('projected lifecycle rules have not converged')
                time.sleep(5)
            run(base + ['exec', name, '-c', policy['container'], '--', '/bin/promtool', 'check', 'rules', rule_path])
        run(base + ["exec", name, "-c", policy["container"], "--", "/bin/promtool", "check", "config", policy["configPath"]])
        proxy = "/api/v1/namespaces/" + policy["namespace"] + "/pods/" + name + ":9090/proxy"
        before = query_prometheus(proxy, "prometheus_config_last_reload_success_timestamp_seconds")
        if not before:
            raise RuntimeError("Prometheus config reload timestamp is unavailable")
        previous_reload = max(float(sample["value"][1]) for sample in before)
        run(base + ["exec", name, "-c", policy["container"], "--", "/bin/sh", "-c", "kill -HUP 1"])
        verify_queries(proxy, ["prometheus_config_last_reload_successful == 1",
                               "prometheus_config_last_reload_success_timestamp_seconds > " + str(previous_reload)])
    # Inspect the live loaded configuration, not just the mounted file.
    for pod in pods:
        proxy = "/api/v1/namespaces/" + policy["namespace"] + "/pods/" + pod["metadata"]["name"] + ":9090/proxy/api/v1/status/config"
        deadline = time.monotonic() + 30
        while True:
            loaded = json.loads(run(["kubectl", "get", "--raw", proxy]))
            yaml = loaded.get("data", {}).get("yaml", "")
            extra_loaded = all(re.search(r"(?m)^\s*- job_name: " + re.escape(job["job_name"]) + r"\s*$", yaml)
                               for job in policy.get("additionalScrapeConfigs", []))
            if "|".join(policy["componentPorts"]) in yaml and extra_loaded:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Prometheus did not load the new scrape policy")
            time.sleep(2)
        verify_queries(proxy.removesuffix("/api/v1/status/config"), policy.get("verificationQueries", []))
        if rules is not None:
            loaded_rules = json.loads(run(['kubectl', 'get', '--raw', proxy.removesuffix('/api/v1/status/config') + '/api/v1/rules']))
            groups = loaded_rules.get('data', {}).get('groups', [])
            if not any(group.get('name') == 'fugue-storage-lifecycle' and len(group.get('rules', [])) == 7 for group in groups):
                raise RuntimeError('Prometheus did not load lifecycle alert rules')
        if policy.get("publicDNSTargets"):
            verify_public_dns_targets(base, proxy.removesuffix("/api/v1/status/config"), policy["publicDNSTargets"])
    print("Prometheus scrape policy loaded without replacing Pods or TSDB")


if __name__ == "__main__":
    main()
