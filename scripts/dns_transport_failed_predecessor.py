"""Explicit recovery from a signed, wire-confirmed DNS data plane failure."""
import datetime
import re

from scripts import reconcile_dns_transport as transport
from scripts.dns_transport_rollback_authority import require_current_rollback, utc_timestamp, validate_reference
from scripts.recover_dns_transport import DNSObservationRejected, validate_api_origin


def validate_stage(stage):
    recovery = stage["failed_predecessor"]
    identities = {"node_uid", "public_service_uid", "diagnostic_service_uid", "pod_uid", "candidate_pod_uid"}
    fields = identities | {"release_id", "parent_digest", "fencing_token", "query", "api", "rollback"}
    if "recovery_fallback" not in stage or not isinstance(recovery, dict) or set(recovery) != fields:
        raise ValueError("DNS failed predecessor recovery requires full identity and an independent fallback")
    for key in identities:
        if not isinstance(recovery[key], str) or not re.fullmatch(r"[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}", recovery[key]):
            raise ValueError("DNS failed predecessor recovery requires exact Kubernetes identities")
    if (recovery["public_service_uid"] == recovery["diagnostic_service_uid"] or
            len({recovery["pod_uid"], recovery["candidate_pod_uid"], stage["recovery_fallback"]["pod_uid"]}) != 3):
        raise ValueError("DNS failed predecessor recovery requires distinct listeners and backends")
    if (not isinstance(recovery["release_id"], str) or not re.fullmatch(r"[a-z0-9_]{1,128}", recovery["release_id"]) or
            not isinstance(recovery["parent_digest"], str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", recovery["parent_digest"]) or
            type(recovery["fencing_token"]) is not int or recovery["fencing_token"] < 1):
        raise ValueError("DNS failed predecessor requires an exact signed release")
    query = recovery["query"]
    label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if (not isinstance(query, dict) or set(query) != {"hostname", "type"} or query["type"] not in ("A", "AAAA") or
            not isinstance(query["hostname"], str) or len(query["hostname"]) > 253 or
            not re.fullmatch(r"(?:%s\.)+%s" % (label, label), query["hostname"])):
        raise ValueError("DNS failed predecessor requires a canonical positive address query")
    if not isinstance(recovery["api"], dict) or set(recovery["api"]) != {"origin", "api_address"}:
        raise ValueError("DNS failed predecessor requires a pinned API origin")
    validate_api_origin(recovery["api"])
    validate_reference(recovery["rollback"])


def require_listener_identity(listener, current, owner, failure):
    uid_key = "public_service_uid" if listener["port"] == 53 else "diagnostic_service_uid"
    if (current["metadata"]["uid"] != failure[uid_key] or owner["uid"] != failure["node_uid"] or
            transport.current_target_port(current) != listener["targetPort"]):
        raise ValueError("DNS failed predecessor Service, socket or node differs from the declaration")


def witness(config, stage, listener, current, owner, old, candidate, fallback):
    from scripts.inspect_dns_predecessor import verify_unready_backend

    failure = stage["failed_predecessor"]
    require_listener_identity(listener, current, owner, failure)
    if (old["pod"]["metadata"]["uid"] != failure["pod_uid"] or
            candidate["pod"]["metadata"]["uid"] != failure["candidate_pod_uid"]):
        raise ValueError("DNS failed predecessor or candidate Pod differs from the declaration")
    try:
        predecessor = verify_unready_backend(config, listener, old)
    except DNSObservationRejected as error:
        error.backend_role = "predecessor"
        raise
    assignment = predecessor["assignment"]
    if (predecessor["parent_digest"] != failure["parent_digest"] or
            assignment["artifact_release_id"] != failure["release_id"] or assignment["fencing_token"] != failure["fencing_token"]):
        raise ValueError("DNS failed predecessor no longer serves the reviewed signed assignment")
    snapshot = candidate["snapshot"]
    transport.validate_handoff_snapshots(snapshot, fallback["snapshot"], owner["name"])
    require_current_rollback(snapshot, failure)
    return predecessor


def observe_failed_predecessor(config, stage, listener, current, *, policy_path, observer, verifier):
    from scripts.inspect_dns_predecessor import inspect

    predecessor, _ = transport.predecessor_listener_config(config, listener, current, config["generation"])
    query = stage["failed_predecessor"]["query"]
    return inspect(predecessor, listener["name"], policy_path, query["hostname"], query["type"], verifier, observer)


def validate_failure_observation(report, stage, current, witness, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    query = stage["failed_predecessor"]["query"]
    if (report.get("schema") != "fugue.dns-unready-predecessor-observation/v1" or
            report.get("authorizes_traffic") is not False or report.get("node_uid") != witness["node_uid"] or
            report.get("pod_uid") != witness["old_uid"] or report.get("service_uid") != current["metadata"]["uid"] or
            report.get("service_resource_version") != current["metadata"]["resourceVersion"] or
            any(report.get(key) != value for key, value in witness["failed_predecessor"].items())):
        raise ValueError("DNS wire failure proof does not match the selector CAS witness")
    samples = report.get("samples", [])
    if len(samples) != 2 or {sample.get("transport") for sample in samples} != {"udp", "tcp"}:
        raise ValueError("DNS wire failure proof requires both UDP and TCP")
    for sample in samples:
        started, finished = utc_timestamp(sample.get("started_at")), utc_timestamp(sample.get("finished_at"))
        if (any(sample.get(key) != value for key, value in query.items()) or sample.get("response_code") != 2 or
                not sample.get("policy_digests") or not now - datetime.timedelta(seconds=60) <= started <= finished <= now):
            raise ValueError("DNS wire failure proof is stale, changed or not SERVFAIL")
