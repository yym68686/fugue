"""Read-only release preflight for an explicitly pinned rollup handoff."""

import argparse
import json
import re
from pathlib import Path

from scripts.provision_observation_worker_database import CHECKPOINT, NAME, Provision, ProvisionError


RELEASE_SCHEMA = "fugue.observation-rollup-handoff/v1"
COMMIT = r"[a-f0-9]{40}"
IMAGE = r"[a-z0-9.-]+(?:/[a-z0-9._-]+)+@sha256:[a-f0-9]{64}"


def matches(value, expression):
    return isinstance(value, str) and re.fullmatch(expression, value) is not None


def load_release(raw):
    if (not isinstance(raw, dict) or raw.keys() != {"schema", "api", "worker"}
            or raw["schema"] != RELEASE_SCHEMA):
        raise ProvisionError("invalid handoff declaration")
    for component in (raw["api"], raw["worker"]):
        if (not isinstance(component, dict)
                or component.keys() != {"deployment", "container", "image", "revision", "replicas"}
                or not all(matches(component[k], expr) for k, expr in (
                    ("deployment", NAME), ("container", NAME), ("image", IMAGE), ("revision", COMMIT)))
                or type(component["replicas"]) is not int or not 1 <= component["replicas"] <= 5):
            raise ProvisionError("invalid workload identity in handoff declaration")
    if raw["api"]["deployment"] == raw["worker"]["deployment"] or raw["api"]["image"] == raw["worker"]["image"]:
        raise ProvisionError("worker is not independent of the API")
    return raw


def controlled_by(obj, kind, uid):
    return any(ref.get("kind") == kind and ref.get("uid") == uid and ref.get("controller") is True
               for ref in obj["metadata"].get("ownerReferences", []))


def declares_artifact(template, component):
    return (template["metadata"].get("annotations", {}).get("fugue.pro/oci-revision") == component["revision"]
            and any(c.get("name") == component["container"] and c.get("image") == component["image"]
                    for c in template["spec"].get("containers", [])))


def verify_workload(provision, component, *, verify_template=None):
    deployment = provision.get("deployment", component["deployment"])
    meta, spec, status = deployment["metadata"], deployment["spec"], deployment["status"]
    count = component["replicas"]
    if (meta.get("deletionTimestamp") or spec.get("replicas") != count
            or status.get("observedGeneration") != meta.get("generation")
            or any(status.get(field) != count for field in ("replicas", "readyReplicas", "updatedReplicas", "availableReplicas"))
            or not declares_artifact(spec["template"], component)):
        raise ProvisionError(f"deployment {component['deployment']} is not settled on the declared revision")
    if verify_template is not None:
        verify_template(spec["template"], component)
    selector_spec = spec["selector"]
    labels = selector_spec.get("matchLabels", {})
    if not labels or selector_spec.get("matchExpressions"):
        raise ProvisionError("live selector cannot be matched exactly")
    selector = ",".join(f"{key}={value}" for key, value in sorted(labels.items()))
    replica_sets = json.loads(provision.kube("get", "replicasets", "-l", selector, "-o", "json"))["items"]
    pods = json.loads(provision.kube("get", "pods", "-l", selector, "-o", "json"))["items"]
    replica_uids = {rs["metadata"]["uid"] for rs in replica_sets if controlled_by(rs, "Deployment", meta["uid"])}
    pod_versions = []
    for pod in pods:
        if not any(controlled_by(pod, "ReplicaSet", uid) for uid in replica_uids):
            continue
        if (pod["metadata"].get("deletionTimestamp") or not declares_artifact(pod, component)
                or pod["status"].get("phase") != "Running"
                or not any(c.get("type") == "Ready" and c.get("status") == "True" for c in pod["status"].get("conditions", []))
                or not any(c.get("name") == component["container"] and c.get("ready") is True and c.get("imageID")
                           for c in pod["status"].get("containerStatuses", []))):
            raise ProvisionError(f"old, draining or unready {component['deployment']} Pod is still present")
        if verify_template is not None:
            verify_template(pod, component)
        pod_versions.append((pod["metadata"]["name"], pod["metadata"]["uid"]))
    if len(pod_versions) != count:
        raise ProvisionError("unexpected number of live Pods")
    return meta["uid"], meta["generation"], sorted(pod_versions)


def observe_database(provision, primary):
    raw = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT json_build_object('owner', COALESCE((SELECT value FROM public.fugue_meta WHERE key = '{CHECKPOINT}'), ''),
    'watermark', window_ended_at, 'ageSeconds', EXTRACT(EPOCH FROM (clock_timestamp() - window_ended_at))::int)
FROM public.fugue_edge_quality_rollup_watermarks WHERE window_name = '5m';
COMMIT;
""")
    _, record = raw.splitlines()
    value = json.loads(record)
    if value["owner"] not in ("", "worker") or not 0 <= value["ageSeconds"] <= 900:
        raise ProvisionError("persisted writer or watermarks are outside the handoff contract")
    return value


def worker_metrics(provision, pod):
    if not matches(pod, NAME):
        raise ProvisionError("cannot identify worker metrics endpoint")
    base = f"/api/v1/namespaces/{provision.config['namespace']}/pods/{pod}:9090/proxy"
    if provision.kube("get", "--raw", base + "/readyz") != "ok":
        raise ProvisionError("worker readiness endpoint failed")
    metrics = provision.kube("get", "--raw", base + "/metrics")
    if not re.search(r"(?m)^fugue_edge_quality_rollup_errors_total 0(?:\.0+)?$", metrics):
        raise ProvisionError("worker is reporting errors or has no independent metrics")
    return metrics


def verify_workloads_and_progress(provision, release):
    api = verify_workload(provision, release["api"])
    worker = verify_workload(provision, release["worker"])
    metrics_by_pod = [worker_metrics(provision, pod) for pod, _ in worker[2]]
    primary = provision.find_primary()
    db = observe_database(provision, primary[0])
    if db["owner"] == "" and any(not re.search(r"(?m)^fugue_edge_quality_rollup_runs_total 0(?:\.0+)?$", metrics)
                                 for metrics in metrics_by_pod):
        raise ProvisionError("worker reports activity while the API still owns the rollup")
    return api, worker, primary, db


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("handoff", type=Path)
    args = parser.parse_args()
    try:
        provision = Provision(json.loads(args.database.read_text()))
        release = load_release(json.loads(args.handoff.read_text()))
        api, worker, primary, db = verify_workloads_and_progress(provision, release)
        print(json.dumps({"apiPods": api[2], "workerPods": worker[2], "primary": primary[0], "database": db}))
    except ProvisionError as err:
        parser.exit(1, f"observation handoff preflight refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "observation handoff preflight received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
