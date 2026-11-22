#!/usr/bin/env python3
"""Read-only observation of an authoritative DNS backend that reports unready."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path

from scripts import reconcile_dns_transport as transport
from scripts import recover_dns_transport as recovery
from scripts.capture_dns_observation_policy import capture, unchanged_backend


def observe_failure_samples(report, address, query):
    samples = report.get("observations", [])
    if (report.get("passed") is not False or report.get("authorizes_traffic") is not False or
            len(samples) != 2 or {sample.get("transport") for sample in samples} != {"udp", "tcp"} or
            any(sample.get("source") != "authoritative" or sample.get("server") != address or
                sample.get("hostname") != query["hostname"] or sample.get("type") != query["type"] or
                sample.get("authoritative") is not True or sample.get("passed") is not False or
                sample.get("response_code") != 2 or sample.get("reason") != "non_authoritative_or_failed_answer" or
                sample.get("expectation_source") != "verified_serving_policy" or not sample.get("policy_digests")
                for sample in samples)):
        raise ValueError("both exact authoritative UDP and TCP queries must return SERVFAIL")
    return [{key: sample[key] for key in (
        "transport", "hostname", "type", "response_code", "started_at", "finished_at", "policy_digests")}
        for sample in samples]


def verify_unready_backend(config, listener, backend, verifier=None, *, expired=False):
    pod = backend["pod"]
    images = [c["image"] for c in pod["spec"]["containers"] if
              all(any(p.get("containerPort") == listener["targetPort"] and p.get("protocol", "TCP") == protocol
                      for p in c.get("ports", [])) for protocol in ("UDP", "TCP"))]
    if len(images) != 1 or not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", images[0]):
        raise ValueError("immutable predecessor image required")
    binding = dict(config, recovery={"candidate_image": images[0], "candidate_pod_uid": pod["metadata"]["uid"]})
    if expired:
        recovery.verify_expired_predecessor(backend, binding, verifier)
    else:
        recovery.verify_unready_predecessor(backend, binding, verifier)
    return {key: backend["snapshot"][key] for key in ("assignment", "parent_digest", "plan_digest")}


def inspect(config, listener_name, policy_path, hostname, record_type, verifier, observer):
    if not all(os.path.isabs(path) for path in (verifier, observer)):
        raise ValueError("absolute DNS verifier and observer paths required")
    listeners = [item for item in config["listeners"] if item["name"] == listener_name and item["port"] == 53]
    if len(listeners) != 1:
        raise ValueError("a declared public DNS listener is required")
    listener = listeners[0]
    address, namespace = listener["address"], config["namespace"]
    declaration = transport.read_observation_declaration(policy_path)
    queries = [query for query in declaration["queries"] if query["hostname"] == hostname and
               query["type"] == record_type and query.get("answer_source") == "verified_serving_policy" and
               query.get("minimum_answers", 0) > 0]
    if len(queries) != 1 or address not in declaration["servers"]:
        raise ValueError("a declared positive signed address query is required")
    query = queries[0]
    declaration.update(servers=[address], authority_port=53, queries=queries)
    declaration.pop("recursive_servers", None)
    raw = json.dumps(declaration, sort_keys=True).encode()

    current = transport.read_json("get", "service", listener_name, "-n", namespace, "-o", "json")
    transport.validate_existing(current, transport.service(config, listener))
    if current["spec"]["externalIPs"] != [address] or transport.current_target_port(current) != listener["targetPort"]:
        raise ValueError("declared socket differs from the current public listener")
    owner = transport.listener_node(listener)["metadata"]
    backend = transport.handoff_backend(namespace, current["spec"]["selector"], owner["name"], listener["targetPort"], read_runtime=False)
    pod = backend["pod"]
    def check():
        slices = transport.read_json("get", "endpointslices", "-n", namespace, "-l",
                                     "kubernetes.io/service-name=" + listener_name, "-o", "json")["items"]
        transport.validate_handoff_endpoints(current, pod, slices, listener["targetPort"])
        assignment = verify_unready_backend(config, listener, backend, verifier)
        unchanged_backend(namespace, current, pod)
        if transport.listener_node(listener)["metadata"]["uid"] != owner["uid"]:
            raise ValueError("DNS predecessor node changed during observation")
        return assignment

    before = check()
    expected = capture(declaration, raw, config, verifier)
    if len(expected["servers"]) != 1:
        raise ValueError("exactly one public DNS expectation is required")
    policy = expected["servers"][0]
    records = policy["policy"]["records"]
    if (policy["server"] != address or policy["pod_uid"] != pod["metadata"]["uid"] or
            policy["service_uid"] != current["metadata"]["uid"] or
            any(policy["policy"][k] != before[k] for k in ("assignment", "parent_digest")) or
            len(records) != 1 or records[0]["hostname"] != query["hostname"] or
            records[0]["type"] != query["type"] or not records[0]["allowed_answers"]):
        raise ValueError("wire observation does not match the authenticated predecessor")
    status, observed = transport.observe_dns_socket(raw, expected, observer)
    if status != 1 or observed.get("expectations") != expected:
        raise ValueError("explicit DNS observation failure result required")
    samples = observe_failure_samples(observed, address, query)
    if check() != before:
        raise ValueError("DNS predecessor assignment changed during wire observation")
    return {"schema": "fugue.dns-unready-predecessor-observation/v1", "authorizes_traffic": False,
            "service_uid": current["metadata"]["uid"], "node_uid": owner["uid"], "pod_uid": pod["metadata"]["uid"],
            "service_resource_version": current["metadata"]["resourceVersion"],
            "declaration_digest": "sha256:" + hashlib.sha256(raw).hexdigest(), **before, "samples": samples}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("transport", type=Path)
    parser.add_argument("policy", type=Path)
    parser.add_argument("--listener", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--type", choices=["A", "AAAA"], required=True)
    parser.add_argument("--verifier", required=True)
    parser.add_argument("--observer", required=True)
    args = parser.parse_args()
    result = inspect(transport.load_config(args.transport), args.listener, args.policy, args.host, args.type,
                     args.verifier, args.observer)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
