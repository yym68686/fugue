"""Read only the deployed source/image binding of the configured mail provider."""

import json
import re

from scripts.provision_observation_worker_database import NAME, ProvisionError
from scripts.verify_observation_writer_handoff import COMMIT, IMAGE, controlled_by, matches


APP_ID = r"[a-z0-9](?:[a-z0-9_.-]{0,61}[a-z0-9])?"
REPO = r"https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+"
APP_LABEL = "fugue.pro/app-id"
SELECT_PROVIDER = """
WITH provider AS (
    SELECT app.id, app.tenant_id, app.spec_json->>'image' AS image,
        CASE WHEN jsonb_exists_any(app.source_json, ARRAY['origin_source','build_source'])
            THEN app.source_json->'build_source' ELSE app.source_json END AS source
    FROM public.fugue_mail_platform_config config
    JOIN public.fugue_apps app ON app.id = config.body->>'mailflare_app_id'
    WHERE config.id = 'platform' AND config.body->>'enabled' = 'true'
)
SELECT json_build_object('id', id, 'tenant', tenant_id, 'image', image,
    'revision', source->>'commit_sha', 'repository', source->>'repo_url',
    'sourceImage', source->>'resolved_image_ref') FROM provider;
"""


def read_provider_source(provision, primary):
    lines = provision.sql(primary, f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
{SELECT_PROVIDER}
COMMIT;
""").splitlines()
    if len(lines) != 2 or lines[0] != "1":
        raise ProvisionError("configured mail provider has no source/image record")
    value = json.loads(lines[1])
    if not isinstance(value, dict) or not all(matches(value.get(field), APP_ID) for field in ("id", "tenant")):
        raise ProvisionError("configured mail provider app identity is missing or malformed")
    # Classify the failure without emitting raw metadata: a malformed value
    # may contain a credential. Cutover still requires all four witnesses.
    checks = (
        ("repository", REPO, "configured mail provider has no canonical GitHub source repository"),
        ("revision", COMMIT, "configured mail provider has no full Git source revision"),
        ("sourceImage", IMAGE, "configured mail provider build has no immutable image reference"),
        ("image", IMAGE, "configured mail provider workload has no immutable image reference"),
    )
    missing = [reason for key, expression, reason in checks if not matches(value.get(key), expression)]
    if missing:
        raise ProvisionError("; ".join(missing))
    if value["sourceImage"].split("@")[1] != value["image"].split("@")[1]:
        raise ProvisionError("configured mail provider build and runtime image digests differ")
    return value


def observe_provider_runtime(provision, primary):
    source = read_provider_source(provision, primary)
    namespace = "fg-" + re.sub(r"[^a-z0-9]+", "-", source["tenant"]).strip("-")[:32]
    if not matches(namespace, NAME):
        raise ProvisionError("configured mail provider namespace is invalid")
    selector = f"{APP_LABEL}={source['id']}"

    def items(kind):
        return json.loads(provision.kube("get", kind, "--namespace", namespace,
                                         "-l", selector, "-o", "json"))["items"]

    deployments = items("deployments")
    ready = {}
    for dep in deployments:
        meta, spec, status = dep["metadata"], dep["spec"], dep.get("status", {})
        replicas = spec.get("replicas")
        if replicas == 0:
            continue
        image_names = [c["name"] for c in spec["template"]["spec"]["containers"]
                       if c["image"] == source["image"]]
        if (meta.get("deletionTimestamp") or not meta.get("uid") or not meta.get("generation")
                or meta.get("labels", {}).get(APP_LABEL) != source["id"]
                or len(image_names) != 1 or type(replicas) is not int or replicas < 1
                or status.get("observedGeneration") != meta.get("generation")
                or any(status.get(key) != replicas for key in
                       ("replicas", "readyReplicas", "updatedReplicas", "availableReplicas"))):
            raise ProvisionError("mail provider deployments are not settled on the bound image")
        ready[meta["uid"]] = (meta["generation"], image_names[0], replicas)
    if not ready:
        raise ProvisionError("no serving deployment for the configured mail provider")

    owners = {}
    for rs in items("replicasets"):
        for uid in ready:
            if controlled_by(rs, "Deployment", uid):
                owners[rs["metadata"]["uid"]] = uid
    observed = {uid: [] for uid in ready}
    expected_digest = source["image"].split("@")[1]
    for pod in items("pods"):
        pod_meta, status = pod["metadata"], pod["status"]
        uid = next((owner for rs, owner in owners.items() if controlled_by(pod, "ReplicaSet", rs)), None)
        if uid is None:
            raise ProvisionError("old or unbound provider Pod is still present")
        name = ready[uid][1]
        containers = [c for c in status.get("containerStatuses", []) if c["name"] == name]
        if (pod_meta.get("deletionTimestamp") or not pod_meta.get("uid") or status.get("phase") != "Running"
                or not any(c.get("type") == "Ready" and c.get("status") == "True"
                           for c in status.get("conditions", []))
                or not any(c.get("name") == name and c.get("image") == source["image"]
                           for c in pod["spec"]["containers"])
                or len(containers) != 1 or containers[0].get("ready") is not True
                or containers[0].get("imageID", "").rsplit("@", 1)[-1] != expected_digest):
            raise ProvisionError("mail provider Pod is not ready on the bound image digest")
        observed[uid].append(pod_meta["uid"])
    if any(len(set(observed[uid])) != record[2] for uid, record in ready.items()):
        raise ProvisionError("mail provider Pods do not cover the bound deployment")
    if read_provider_source(provision, primary) != source:
        raise ProvisionError("mail provider source changed during the runtime observation")
    witness = {uid: (ready[uid], sorted(pods)) for uid, pods in observed.items()}
    result = {"repository": source["repository"], "revision": source["revision"],
              "imageDigest": expected_digest, "readyPods": sum(len(pods) for pods in observed.values())}
    return result, witness
