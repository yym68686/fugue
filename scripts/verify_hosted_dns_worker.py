"""Verify the exact worker image without transferring hosted DNS authority."""

import argparse
import json
from pathlib import Path
import re

from scripts.provision_hosted_dns_worker import HostedDNSProvision, SECRET
from scripts.provision_observation_worker_database import ProvisionError
from scripts.verify_observation_writer_handoff import COMMIT, IMAGE, matches, verify_workload


COMPONENT = "hosted-dns-worker"
DEPLOYMENT = "fugue-" + COMPONENT
REPOSITORY = "ghcr.io/yym68686/" + DEPLOYMENT


def component_from_receipt(receipt, source):
    image = receipt["immutableRef"]
    if (receipt["apiVersion"] != "release.fugue.dev/v2" or receipt["kind"] != "ImmutableComponentArtifactReceipt"
            or receipt["component"] != COMPONENT or not matches(source, COMMIT) or not matches(image, IMAGE)
            or not all(receipt[name] == source for name in ("sourceSha", "configSha", "ociRevision"))
            or image.split("@") != [REPOSITORY, receipt["topDigest"]]):
        raise ProvisionError("hosted DNS standby receipt does not match the exact release")
    return {"deployment": DEPLOYMENT, "container": COMPONENT, "replicas": 1, "revision": source, "image": image}


def verify_template(template, _):
    spec = template["spec"]
    credential = {"name": "FUGUE_HOSTED_DNS_WORKER_DATABASE_URL",
                  "valueFrom": {"secretKeyRef": {"name": SECRET, "key": "database-url"}}}
    if (spec.get("automountServiceAccountToken") is not False
            or spec.get("initContainers") or spec.get("ephemeralContainers") or spec.get("volumes")
            or len(spec["containers"]) != 1):
        raise ProvisionError("hosted DNS worker has unrelated workload privileges")
    env = spec["containers"][0]["env"]
    if env != [credential, {"name": "GOMEMLIMIT", "value": "96MiB"}] or spec["containers"][0].get("envFrom"):
        raise ProvisionError("hosted DNS worker has unrelated credentials")


def verify_standby_metrics(metrics):
    if (not re.search(r'(?m)^fugue_hosted_dns_owner\{owner="api"\} 1(?:\.0+)?$', metrics)
            or not re.search(r"(?m)^fugue_hosted_dns_worker_storage_available 1(?:\.0+)?$", metrics)
            or "fugue_hosted_dns_last_completed_at_seconds" in metrics):
        raise ProvisionError("hosted DNS worker did not confirm standby")


def verify_standby(provision, component):
    primary = provision.find_primary()
    observed = verify_workload(provision, component, verify_template=verify_template)
    namespace = provision.config["namespace"]
    pod = observed[2][0][0]
    path = f"/api/v1/namespaces/{namespace}/pods/{pod}:9090/proxy"
    if provision.kube("get", "--raw", path + "/readyz") != "ok":
        raise ProvisionError("hosted DNS worker is not ready")
    verify_standby_metrics(provision.kube("get", "--raw", path + "/metrics"))
    outcome = provision.sql(primary[0], f"""
BEGIN READ ONLY;
SET LOCAL statement_timeout = '3s';
{provision.identity_sql()}
SELECT json_build_object('owner', owner, 'completed', completed_at IS NOT NULL, 'updated', updated)
FROM fugue_hosted_dns_maintenance.progress_v1(NULL,NULL);
COMMIT;
""").splitlines()
    if (len(outcome) != 2 or outcome[0] != "1" or json.loads(outcome[1]) != {"owner": "api", "completed": False, "updated": None}
            or provision.find_primary() != primary
            or verify_workload(provision, component, verify_template=verify_template) != observed):
        raise ProvisionError("database or workload identity changed while confirming standby")
    return {"worker": component, "pods": observed[2], "primary": primary, "writer": "api"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("receipt", type=Path)
    parser.add_argument("source")
    args = parser.parse_args()
    try:
        provision = HostedDNSProvision(json.loads(args.database.read_text()))
        expected = component_from_receipt(json.loads(args.receipt.read_text()), args.source)
        print(json.dumps(verify_standby(provision, expected), sort_keys=True))
    except ProvisionError as err:
        parser.exit(1, f"hosted DNS standby verification stopped: {err}\n")
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        parser.exit(1, "hosted DNS standby verification received missing or malformed evidence\n")


if __name__ == "__main__":
    main()
