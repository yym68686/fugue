"""Apply one admitted observation-writer transition and verify the consumer.

The production Actions lane must check Git freshness and artifact source
compatibility before invoking this command. It has no reverse authorizer.
"""

import argparse
from datetime import datetime
import json
import re
import time
from pathlib import Path
from urllib.parse import urlencode

from scripts.observation_rollup_writer_fence import handoff_sql
from scripts.provision_observation_worker_database import NAME, Provision, ProvisionError
from scripts.verify_observation_writer_handoff import (
    load_release, matches, observe_database, verify_workload, verify_workloads_and_progress, worker_metrics,
)


def verify_collector(provision, scrape_policy, pods, expression=None):
    namespace = provision.config["namespace"]
    prometheus = scrape_policy["deployment"]
    if scrape_policy["namespace"] != namespace or not matches(prometheus, NAME):
        raise ProvisionError("unexpected metrics collector identity")
    selection = provision.get("deployment", prometheus)["spec"]["selector"]
    labels = selection.get("matchLabels", {})
    if not labels or selection.get("matchExpressions"):
        raise ProvisionError("metrics collector selector is not exact")
    selector = ",".join(f"{key}={value}" for key, value in sorted(labels.items()))
    collectors = json.loads(provision.kube("get", "pods", "-l", selector, "-o", "json"))["items"]
    ready = [c["metadata"]["name"] for c in collectors if not c["metadata"].get("deletionTimestamp")
             and any(x.get("type") == "Ready" and x.get("status") == "True" for x in c["status"].get("conditions", []))]
    if not ready:
        raise ProvisionError("no ready independent metrics collector")
    if expression is None:
        expression = 'up{job="fugue-observation-worker"} == 1 and on(pod) (time() - timestamp(fugue_edge_quality_rollup_errors_total{job="fugue-observation-worker"}) < 90)'
    for collector in ready:
        if not matches(collector, NAME):
            raise ProvisionError("unexpected metrics collector Pod name")
        path = f"/api/v1/namespaces/{namespace}/pods/{collector}:9090/proxy/api/v1/query?"
        response = json.loads(provision.kube("get", "--raw", path + urlencode({"query": expression})))
        discovered = {item.get("metric", {}).get("pod") for item in response.get("data", {}).get("result", [])}
        if response.get("status") != "success" or not set(pods).issubset(discovered):
            raise ProvisionError("the independent collector has not observed every candidate worker Pod")


def verify_committed_progress(provision, release, primary, baseline, after_timestamp, *, wait_seconds=400):
    previous = datetime.fromisoformat(baseline["watermark"])
    deadline = time.monotonic() + wait_seconds
    while True:
        if provision.find_primary() != primary:
            raise ProvisionError("CNPG primary changed while verifying the completed transition")
        current = observe_database(provision, primary[0])
        if current["owner"] != "worker":
            raise ProvisionError("database did not retain the dedicated worker owner")
        if datetime.fromisoformat(current["watermark"]) > previous:
            workload = verify_workload(provision, release["worker"])
            for pod, pod_uid in workload[2]:
                metrics = worker_metrics(provision, pod)
                last = re.search(r"(?m)^fugue_edge_quality_rollup_last_success_timestamp_seconds ([0-9.]+)$", metrics)
                if last and float(last[1]) >= after_timestamp:
                    return {"watermark": current["watermark"], "pod": pod, "podUID": pod_uid,
                            "lastSuccessUnix": int(float(last[1]))}
        if time.monotonic() >= deadline:
            raise ProvisionError("worker has not confirmed a new committed watermark; do not restore API ownership")
        time.sleep(10)


def activate(provision, release, scrape_policy):
    api, worker, primary, _ = verify_workloads_and_progress(provision, release)
    verify_collector(provision, scrape_policy, [pod for pod, _ in worker[2]])
    credential = provision.get("secret", provision.config["secret"])
    secret_uid, _ = provision.credential(credential)
    if (verify_workload(provision, release["api"]) != api or verify_workload(provision, release["worker"]) != worker
            or provision.find_primary() != primary):
        raise ProvisionError("live workloads or primary changed during handoff admission")
    _, record = provision.sql(primary[0], handoff_sql(provision, secret_uid)).splitlines()
    baseline = json.loads(record)
    print("writer fence and owner persisted; checking worker execution", flush=True)
    return verify_committed_progress(provision, release, primary, baseline, float(baseline["cutoverUnix"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("handoff", type=Path)
    parser.add_argument("metrics", type=Path)
    parser.add_argument("--apply", action="store_true", required=True)
    args = parser.parse_args()
    try:
        provision = Provision(json.loads(args.database.read_text()))
        release = load_release(json.loads(args.handoff.read_text()))
        scrape = json.loads(args.metrics.read_text())
        print(json.dumps(activate(provision, release, scrape)))
    except ProvisionError as err:
        parser.exit(1, f"observation writer verification stopped: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "observation writer verification stopped on missing or malformed evidence\n")


if __name__ == "__main__":
    main()
