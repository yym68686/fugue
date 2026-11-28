"""Verify the exact database import worker is Ready but not the executor."""

import argparse
import json
from pathlib import Path

from scripts.provision_database_import_worker import ImportProvision, SECRET
from scripts.provision_observation_worker_database import NAME, ProvisionError
from scripts.verify_observation_writer_handoff import COMMIT, IMAGE, matches, verify_workload


COMPONENT = "database-import-worker"
DEPLOYMENT = "fugue-" + COMPONENT
REPOSITORY = "ghcr.io/yym68686/" + DEPLOYMENT
DIGEST = r"sha256:[a-f0-9]{64}"
STANDBY_METRICS = {
    "fugue_database_import_worker_store_available 1",
    "fugue_database_import_worker_owns_execution 0",
}


def component_from_receipt(receipt, source):
    image = receipt["immutableRef"]
    digests = (receipt["topDigest"], receipt["platformManifestDigest"], receipt["configDigest"])
    if (receipt["apiVersion"] != "release.fugue.dev/v2" or receipt["kind"] != "ImmutableComponentArtifactReceipt"
            or receipt["component"] != COMPONENT or not matches(source, COMMIT) or not matches(image, IMAGE)
            or not all(receipt[k] == source for k in ("sourceSha", "configSha", "ociRevision"))
            or not all(matches(digest, DIGEST) for digest in digests)
            or image != f"{REPOSITORY}@{digests[0]}"):
        raise ProvisionError("database import standby receipt does not match the exact release")
    return {"deployment": DEPLOYMENT, "container": COMPONENT, "replicas": 1, "revision": source,
            "image": image, "runtimeDigests": list(digests)}


def verify_template(template, component):
    spec = template["spec"]
    if (spec.get("automountServiceAccountToken") is not False or spec.get("hostNetwork")
            or spec.get("hostPID") or spec.get("hostIPC") or spec.get("initContainers")
            or spec.get("ephemeralContainers") or len(spec["containers"]) != 1
            or spec.get("volumes") != [{"name": "scratch", "emptyDir": {"sizeLimit": "384Mi"}}]):
        raise ProvisionError("database import worker has unexpected Pod privileges or volumes")
    container = spec["containers"][0]
    credential = {"name": "FUGUE_DATABASE_IMPORT_WORKER_DATABASE_URL",
                  "valueFrom": {"secretKeyRef": {"name": SECRET, "key": "database-url"}}}
    mounts = container.get("volumeMounts", [])
    if (container["env"] != [credential, {"name": "GOMEMLIMIT", "value": "640MiB"}]
            or container.get("envFrom") or len(mounts) != 1 or mounts[0].get("name") != "scratch"
            or mounts[0].get("mountPath") != "/tmp" or mounts[0].get("subPath") or mounts[0].get("subPathExpr")
            or container.get("securityContext") != {"allowPrivilegeEscalation": False,
                "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
            or spec.get("securityContext", {}).get("runAsNonRoot") is not True):
        raise ProvisionError("database import worker has unexpected credentials or sandbox")
    if template.get("kind") == "Pod":
        statuses = template["status"].get("containerStatuses", [])
        if (len(statuses) != 1 or statuses[0].get("name") != COMPONENT
                or statuses[0].get("imageID", "").rsplit("@", 1)[-1].removeprefix("containerd://")
                not in component["runtimeDigests"]):
            raise ProvisionError("database import Pod is not running the admitted immutable image")


def verify_standby(provision, component):
    primary = provision.find_primary()
    observed = verify_workload(provision, component, verify_template=verify_template)
    pod = observed[2][0][0]
    if not matches(pod, NAME):
        raise ProvisionError("invalid database import worker Pod identity")
    path = f"/api/v1/namespaces/{provision.config['namespace']}/pods/{pod}:9090/proxy"
    if (provision.kube("get", "--raw", path + "/readyz") != "ok"
            or set(provision.kube("get", "--raw", path + "/metrics").splitlines()) != STANDBY_METRICS):
        raise ProvisionError("database import worker did not confirm healthy standby")
    result = provision.sql(primary[0], f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT json_build_object('owner', fugue_app_db_import_worker.owner_v1(),
    'activeFence', to_regnamespace('fugue_app_db_import_fence') IS NOT NULL);
COMMIT;
""").splitlines()
    if (len(result) != 2 or result[0] != "1" or json.loads(result[1]) != {"owner": "api", "activeFence": False}
            or provision.find_primary() != primary
            or verify_workload(provision, component, verify_template=verify_template) != observed):
        raise ProvisionError("database import ownership or workload identity changed during standby verification")
    return {"worker": component, "pods": observed[2], "primary": primary, "writer": "api", "writerTransferred": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("source")
    args = parser.parse_args()
    try:
        provision = ImportProvision(json.loads(args.database.read_text()))
        component = component_from_receipt(json.loads(args.receipt.read_text()), args.source)
        print(json.dumps(verify_standby(provision, component)))
    except ProvisionError as err:
        parser.exit(1, f"database import standby verification refused: {err}\n")
    except (KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "database import standby verification received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
