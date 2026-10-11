import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import urllib.request

from scripts.reconcile_agent_edge_trust import canonical, kubectl, strict_json


MANAGER = "fugue-managed-ssh-runtime"
GENERATION = "ssh.fugue.dev/runtime-generation"
DIGEST = "ssh.fugue.dev/runtime-digest"


def verify_artifact_run(config, repository, token, request=None, job_name="managed_ssh_artifact"):
    if job_name not in {"managed_ssh_artifact", "managed_ssh_front_artifact"}:
        raise ValueError("reviewed SSH artifact acceptance job required")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository or "") or not token:
        raise ValueError("authenticated repository identity required")
    if type(config.get("artifactRunID")) is not int or config["artifactRunID"] < 1 or not re.fullmatch(r"[a-f0-9]{40}", config.get("artifactSourceCommit", "")):
        raise ValueError("exact artifact run and source required")
    if request is None:
        def request(path):
            query = urllib.request.Request("https://api.github.com/repos/"+repository+path, headers={"Authorization":"Bearer "+token,"Accept":"application/vnd.github+json","X-GitHub-Api-Version":"2022-11-28"})
            try:
                with urllib.request.urlopen(query, timeout=30) as response:
                    raw = response.read(2*1024*1024+1)
            except Exception:
                raise ValueError("GitHub artifact provenance request failed") from None
            if len(raw)>2*1024*1024:
                raise ValueError("GitHub artifact provenance exceeds bound")
            return strict_json(raw)
    prefix = "/actions/runs/"+str(config["artifactRunID"])
    run = request(prefix)
    if run.get("head_sha") != config["artifactSourceCommit"] or run.get("event") != "push" or run.get("head_branch") != "main" or run.get("path") != ".github/workflows/ci.yml" or run.get("repository",{}).get("full_name") != repository:
        raise ValueError("artifact source differs from reviewed main workflow")
    for page in range(1,11):
        jobs = request(prefix+"/jobs?filter=latest&per_page=100&page="+str(page)).get("jobs",[])
        for job in jobs:
            if job.get("name") == job_name:
                if job.get("status") != "completed" or job.get("conclusion") != "success":
                    raise ValueError("managed SSH artifact acceptance did not succeed")
                return {"verified":True,"run_id":config["artifactRunID"],"source_commit":config["artifactSourceCommit"]}
        if len(jobs)<100:
            break
    raise ValueError("managed SSH artifact acceptance job missing")


def digest(value):
    return "sha256:"+hashlib.sha256(canonical(value).encode()).hexdigest()


def name(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value):
        raise ValueError("canonical Kubernetes name required")


def validate(config, receipt):
    expected = {"apiVersion", "kind", "generation", "expectedPreviousGeneration", "expectedPreviousDigest", "controllerNamespace", "controllerAccount", "runtimeConfigMap", "tenantNamespaces", "runtime", "artifactReceiptDigest", "artifactSourceCommit", "artifactRunID"}
    if set(config) != expected or config["apiVersion"] != "ssh.fugue.dev/v1" or config["kind"] != "ManagedSSHRuntimeIntent":
        raise ValueError("invalid managed SSH runtime declaration")
    previous, generation = config["expectedPreviousGeneration"], config["generation"]
    if type(previous) is not int or type(generation) is not int or not 0 <= previous < generation <= 2**53-1:
        raise ValueError("explicit runtime generation predecessor required")
    if (previous == 0 and config["expectedPreviousDigest"] != "") or (previous > 0 and not re.fullmatch(r"sha256:[a-f0-9]{64}", config["expectedPreviousDigest"])):
        raise ValueError("runtime predecessor digest required")
    for field in ["controllerNamespace", "controllerAccount", "runtimeConfigMap"]:
        name(config[field])
    options = config["runtime"]
    option_fields = {"schema", "namespace", "runtime_ids", "releases", "state_storage_class", "api_servers", "ingress_namespace", "ingress_pod_labels", "controller_namespace", "controller_pod_labels", "ingress_cidrs", "required_edges", "rollback_retention_seconds"}
    if not isinstance(options, dict) or set(options) != option_fields or options["schema"] != "fugue.managed-ssh-runtime/v1":
        raise ValueError("complete runtime configuration required")
    for field in ["namespace", "state_storage_class", "ingress_namespace", "controller_namespace"]:
        name(options[field])
    if options["namespace"] == config["controllerNamespace"] or options["controller_namespace"] != config["controllerNamespace"]:
        raise ValueError("dedicated SSH serving namespace required")
    tenants = config["tenantNamespaces"]
    if not isinstance(tenants, list) or not 1 <= len(tenants) <= 32 or len(set(tenants)) != len(tenants):
        raise ValueError("bounded explicit tenant namespace enrollment required")
    for namespace in tenants:
        name(namespace)
        if namespace in {options["namespace"], config["controllerNamespace"], "default", "kube-system", "kube-public", "kube-node-lease"}:
            raise ValueError("platform namespaces cannot be enrolled for tenant execution")
    if not isinstance(options["runtime_ids"], list) or not 1 <= len(options["runtime_ids"]) <= 32 or any(not isinstance(identifier, str) or not re.fullmatch(r"runtime_[A-Za-z0-9_-]+", identifier) for identifier in options["runtime_ids"]):
        raise ValueError("explicit runtime identities required")
    if type(config["artifactRunID"]) is not int or config["artifactRunID"] < 1 or not re.fullmatch(r"[a-f0-9]{40}",config["artifactSourceCommit"]):
        raise ValueError("exact CI artifact run and source required")
    if receipt.get("schema") != "fugue.managed-ssh-image-receipt/v1" or receipt.get("verified") is not True or receipt.get("network_policy_enforced") is not True or digest(receipt) != config["artifactReceiptDigest"] or receipt.get("source_commit") != config["artifactSourceCommit"]:
        raise ValueError("exact verified network-policy image receipt required")
    required_checks = {"candidate-publication", "native-ssh", "native-scp", "authorization-refresh", "graceful-retirement", "network-policy-allow-and-deny", "unchanged-workload"}
    if not required_checks.issubset(receipt.get("checks", [])) or not re.fullmatch(r"[a-f0-9]{40}", receipt.get("source_commit", "")):
        raise ValueError("artifact receipt omits required acceptance")
    expected_release = {"image":receipt.get("image"), "helper_digest":receipt.get("helper_digest"), "architecture":receipt.get("architecture")}
    if options["releases"] != {receipt.get("architecture"):expected_release} or not re.fullmatch(r"[^\s]+@sha256:[a-f0-9]{64}", expected_release["image"]) or not re.fullmatch(r"sha256:[a-f0-9]{64}", expected_release["helper_digest"]):
        raise ValueError("runtime release differs from verified immutable artifact")
    if not isinstance(options["api_servers"], list) or not 1 <= len(options["api_servers"]) <= 16:
        raise ValueError("bounded API egress required")
    for peer in options["api_servers"]:
        if set(peer) != {"cidr", "port"} or type(peer["port"]) is not int or not 1 <= peer["port"] <= 65535:
            raise ValueError("invalid API egress peer")
        exact_address(peer["cidr"])
    for cidr in options["ingress_cidrs"]:
        exact_address(cidr)
    for field in ["ingress_pod_labels", "controller_pod_labels"]:
        if not isinstance(options[field], dict) or not options[field] or len(options[field]) > 16:
            raise ValueError("explicit traffic source selectors required")
    if type(options["rollback_retention_seconds"]) is not int or not 3600 <= options["rollback_retention_seconds"] <= 30*86400:
        raise ValueError("bounded rollback retention required")
    if not isinstance(options["required_edges"],list) or len(options["required_edges"])>64:
        raise ValueError("bounded edge consumers required")
    seen_edges=set()
    for edge in options["required_edges"]:
        base_fields={"edge_id","edge_group_id","namespace","pod_labels","public_address"}
        service_fields={"public_service","service_uid","image"}
        if set(edge) not in (base_fields,base_fields|service_fields,base_fields|service_fields|{'binding_publication'}) or edge["edge_id"] in seen_edges:
            raise ValueError("invalid or duplicated edge consumer")
        if "public_service" in edge:
            name(edge["public_service"])
            if not isinstance(edge["service_uid"],str) or not re.fullmatch(r'[A-Za-z0-9-]{1,128}',edge["service_uid"]) or not isinstance(edge["image"],str) or not re.fullmatch(r'[^\s]+@sha256:[a-f0-9]{64}',edge["image"]):
                raise ValueError("Service-backed consumer requires exact Service UID and immutable image")
        if 'binding_publication' in edge:
            policy=edge['binding_publication']
            if set(policy)!={'port_start','port_end','intent_digest'} or type(policy['port_start']) is not int or type(policy['port_end']) is not int or not 1024<=policy['port_start']<=policy['port_end']<=65535 or not re.fullmatch(r'sha256:[a-f0-9]{64}',policy['intent_digest']):
                raise ValueError('explicit bounded managed binding publication required')
        name(edge["namespace"])
        name(edge["edge_id"])
        name(edge["edge_group_id"])
        address=ipaddress.ip_address(edge["public_address"])
        if address.is_unspecified or address.is_loopback or not isinstance(edge["pod_labels"],dict) or not edge["pod_labels"]:
            raise ValueError("edge consumer lacks an exact public address and selector")
        seen_edges.add(edge["edge_id"])
    return config


def exact_address(cidr):
    network = ipaddress.ip_network(cidr, strict=True)
    if network.prefixlen != network.max_prefixlen or network.network_address.is_unspecified:
        raise ValueError("network access must be limited to exact addresses")


def objects(config):
    options = config["runtime"]
    namespace, controller_namespace, account = options["namespace"], config["controllerNamespace"], config["controllerAccount"]
    labels = {"app.kubernetes.io/managed-by":MANAGER}
    resources = [{"apiVersion":"v1", "kind":"Namespace", "metadata":{"name":namespace, "labels":dict(labels, **{"fugue.pro/managed-ssh-control":"true"})}}, {"apiVersion":"networking.k8s.io/v1", "kind":"NetworkPolicy", "metadata":{"name":"managed-ssh-default-deny", "namespace":namespace, "labels":labels}, "spec":{"podSelector":{}, "policyTypes":["Ingress", "Egress"]}}]
    rules = [{"apiGroups":[""], "resources":["secrets", "serviceaccounts", "services", "persistentvolumeclaims"], "verbs":["get", "create", "update", "patch"]}, {"apiGroups":["apps"], "resources":["deployments"], "verbs":["get", "create", "update", "patch"]}, {"apiGroups":["networking.k8s.io"], "resources":["networkpolicies"], "verbs":["get", "create", "update", "patch"]}]
    assignments = [(namespace, rules)]
    for tenant in config["tenantNamespaces"]:
        assignments.append((tenant, [{"apiGroups":[""], "resources":["pods"], "verbs":["get", "list"]}, {"apiGroups":[""], "resources":["pods/exec"], "verbs":["get", "create"]}, {"apiGroups":["rbac.authorization.k8s.io"], "resources":["roles", "rolebindings"], "verbs":["get", "create", "update", "patch"]}]))
    for selected_namespace, permissions in assignments:
        metadata = {"name":"managed-ssh-controller", "namespace":selected_namespace, "labels":labels}
        resources.extend([{"apiVersion":"rbac.authorization.k8s.io/v1", "kind":"Role", "metadata":dict(metadata), "rules":permissions}, {"apiVersion":"rbac.authorization.k8s.io/v1", "kind":"RoleBinding", "metadata":dict(metadata), "roleRef":{"apiGroup":"rbac.authorization.k8s.io", "kind":"Role", "name":metadata["name"]}, "subjects":[{"kind":"ServiceAccount", "name":account, "namespace":controller_namespace}]}])
    publication_services={}
    for edge in options['required_edges']:
        if edge.get('binding_publication'):
            publication_services.setdefault(edge['namespace'],set()).add(edge['public_service'])
    for selected_namespace,names in sorted(publication_services.items()):
        metadata={'name':'managed-ssh-public-binding','namespace':selected_namespace,'labels':labels}
        rules=[{'apiGroups':[''],'resources':['services'],'resourceNames':sorted(names),'verbs':['get','patch']}]
        resources.extend([{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'Role','metadata':dict(metadata),'rules':rules},{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':dict(metadata),'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':metadata['name']},'subjects':[{'kind':'ServiceAccount','name':account,'namespace':controller_namespace}]}])
    return resources


def reconcile(config, receipt, kube=kubectl):
    validate(config, receipt)
    configuration_digest = digest(config)
    namespace, configmap = config["controllerNamespace"], config["runtimeConfigMap"]
    raw = kube(["get", "configmap", configmap, "-n", namespace, "--ignore-not-found", "-o", "json"])
    current = strict_json(raw) if raw.strip() else None
    if current:
        metadata = current.get("metadata", {})
        annotations = metadata.get("annotations", {})
        if metadata.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or metadata.get("deletionTimestamp"):
            raise ValueError("foreign runtime configuration")
        previous_declaration = strict_json(current.get("data",{}).get("declaration.json",""))
        if digest(previous_declaration) != annotations.get(DIGEST):
            raise ValueError("stored declaration failed integrity validation")
        for field in ["controllerNamespace","controllerAccount","runtimeConfigMap"]:
            if previous_declaration[field] != config[field]:
                raise ValueError("configuration update cannot transfer controller ownership")
        if previous_declaration["runtime"]["namespace"] != config["runtime"]["namespace"] or not set(previous_declaration["tenantNamespaces"]).issubset(config["tenantNamespaces"]):
            raise ValueError("namespace removal requires explicit authorization and serving retirement")
        if annotations.get(GENERATION) == str(config["generation"]):
            if annotations.get(DIGEST) != configuration_digest or current.get("data", {}).get("runtime.json") != canonical(config["runtime"]):
                raise ValueError("same-generation runtime substitution")
        elif annotations.get(GENERATION) != str(config["expectedPreviousGeneration"]) or annotations.get(DIGEST) != config["expectedPreviousDigest"]:
            raise ValueError("runtime configuration predecessor changed")
    elif config["expectedPreviousGeneration"] != 0:
        raise ValueError("existing runtime configuration missing; explicit restoration required")
    for tenant in config["tenantNamespaces"]:
        metadata = strict_json(kube(["get", "namespace", tenant, "-o", "json"])).get("metadata", {})
        if metadata.get("name") != tenant or metadata.get("deletionTimestamp"):
            raise ValueError("tenant execution namespace missing or terminating")
    planned = objects(config)
    for desired in planned:
        metadata = desired["metadata"]
        args = ["get", desired["kind"], metadata["name"], "--ignore-not-found", "-o", "json"]
        if metadata.get("namespace"):
            args.extend(["-n", metadata["namespace"]])
        existing = kube(args)
        if existing.strip():
            previous = strict_json(existing).get("metadata", {})
            if previous.get("labels", {}).get("app.kubernetes.io/managed-by") != MANAGER or previous.get("deletionTimestamp"):
                raise ValueError("runtime resource ownership differs from reviewed declaration")
            desired["metadata"]["resourceVersion"] = previous["resourceVersion"]
    for desired in planned:
        try:
            kube(["apply", "--server-side", "--field-manager="+MANAGER, "-f", "-"], desired)
        except Exception:
            metadata=desired['metadata']
            raise RuntimeError("runtime publication failed for "+desired['kind']+" "+metadata.get('namespace','')+"/"+metadata['name']+" at expected resourceVersion "+metadata.get('resourceVersion','absent')) from None
    desired = {"apiVersion":"v1", "kind":"ConfigMap", "metadata":{"name":configmap,"namespace":namespace,"labels":{"app.kubernetes.io/managed-by":MANAGER},"annotations":{GENERATION:str(config["generation"]),DIGEST:configuration_digest}}, "data":{"runtime.json":canonical(config["runtime"]),"declaration.json":canonical(config)}}
    if current:
        desired["metadata"]["resourceVersion"] = current["metadata"]["resourceVersion"]
        desired["metadata"]["uid"] = current["metadata"]["uid"]
    kube(["replace" if current else "create", "-f", "-"], desired)
    actual = strict_json(kube(["get", "configmap", configmap, "-n", namespace, "-o", "json"]))
    if actual.get("data") != desired["data"] or actual.get("metadata", {}).get("annotations", {}).get(DIGEST) != configuration_digest:
        raise ValueError("published runtime configuration did not verify")
    return {"verified":True,"generation":config["generation"],"digest":configuration_digest,"image":receipt["image"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--receipt")
    source.add_argument("--verify-artifact-run", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()
    config = strict_json(Path(args.declaration).read_bytes())
    if args.verify_artifact_run:
        print(canonical(verify_artifact_run(config, os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GH_TOKEN"))))
        raise SystemExit(0)
    receipt = strict_json(Path(args.receipt).read_bytes())
    validate(config, receipt)
    print(canonical({"valid":True,"generation":config["generation"]} if args.validate_only else reconcile(config, receipt)))
