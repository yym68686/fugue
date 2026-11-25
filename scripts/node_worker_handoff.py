"""Hold and verify an exact public TLS handoff for a claimed node Worker."""
import datetime
import http.client
import ipaddress
import ssl
import time
from contextlib import ExitStack, closing

from scripts import front_connection_witness as connection
from scripts import observe_front_candidate as front


def wire_time(value):
    return value.astimezone(datetime.timezone.utc).isoformat().replace("+00:00", "Z")


def connection_ref(fact):
    return {"front_uid": fact["pod_uid"], "id": fact["connection_id"], "started_at": fact["started_at"],
            "slot": fact["slot"], "target": fact["target"]}


def socket_target(address, port):
    host = ipaddress.ip_address(address)
    return f"[{host}]:{port}" if host.version == 6 else f"{host}:{port}"


def slot_phase(journal, observed):
    """Recognize only the exact durable predecessor, candidate, or compensation."""
    before, candidate = journal["expected"], journal["candidate"]
    if observed == before:
        return "pending"
    if front.timestamp(observed["updated_at"]) < front.timestamp(journal["created_at"]):
        raise ValueError("activation changed before this claimed journal")
    state = dict(before)
    state.pop("rollback_of_generation", None)
    state["updated_at"] = observed["updated_at"]
    promoted = dict(state, generation=before["generation"] + 1, previous_slot=before["active_slot"],
                    active_slot=candidate["Slot"], bundle_generation=candidate["BundleGeneration"],
                    worker_source_commit=candidate["WorkerSourceCommit"], worker_image_digest=candidate["WorkerImageDigest"],
                    operation="promote", reason="advance node Worker using journal " + journal["digest"])
    reverted = dict(state, generation=before["generation"] + 2, previous_slot=candidate["Slot"],
                    operation="rollback", rollback_of_generation=before["generation"] + 1,
                    reason="restore node Worker using journal " + journal["digest"])
    if observed == promoted:
        return "activated"
    if observed == reverted:
        return "reverted"
    raise ValueError("public Front durable activation changed outside this journal")


def require_fact(ref, journal, slot, target):
    if (ref["front_uid"] != journal["recipient"]["pod_uid"] or not ref["id"] or len(ref["id"]) > 128
            or ref["slot"] != slot or ref["target"] != target):
        raise ValueError("held connection did not reach the exact Front and Worker")
    return ref


def require_verified(result, journal, state, witnessed):
    if (result.get("schema") != "edge-node-worker-result/v1" or result.get("journal_digest") != journal["digest"]
            or result.get("outcome") != "verified" or result.get("activation") != state
            or slot_phase(journal, state) != "activated"):
        raise ValueError("Guardian did not record the exact verified node handoff")
    w = result.get("witness") or {}
    if set(w) != set(witnessed) or any(w.get(k) != witnessed[k] for k in ("schema", "journal_digest")):
        raise ValueError("Guardian receipt did not bind the offered connection witness")
    if front.timestamp(w["observed_at"]) != front.timestamp(witnessed["observed_at"]):
        raise ValueError("Guardian receipt did not bind the exact witness observation")
    if "probe_fresh" in witnessed:
        if ("drain_start" not in result or front.timestamp(result["drain_start"]) < front.timestamp(witnessed["observed_at"])):
            raise ValueError("Guardian did not durably establish this verified drain")
    for k in ("held", "fresh", *(("probe_fresh",) if "probe_fresh" in witnessed else ())):
        actual, expected = w[k], witnessed[k]
        if (set(actual) != set(expected) or any(actual[field] != expected[field] for field in expected if field != "started_at")
                or front.timestamp(actual["started_at"]) != front.timestamp(expected["started_at"])):
            raise ValueError("Guardian receipt did not retain both exact connection identities")


def verify_drain(adapter, journal, result, state, old, check_old, check_new, now, pause, record):
    """Keep the same real connection until the exact Front reports forcing it."""
    started = front.timestamp(result["drain_start"])
    deadline = started + datetime.timedelta(seconds=600)
    seen = None
    last_alive = None
    detached = None
    while now() < deadline + datetime.timedelta(seconds=90):
        if adapter.selected_activation() != state:
            raise ValueError("activation changed during verification of the bounded drain")
        if detached is None:
            sample = now()
            try:
                check_old()
                last_alive = sample
            except (http.client.RemoteDisconnected, ConnectionResetError, BrokenPipeError, ssl.SSLEOFError):
                detached = now()
                if detached < deadline or not seen or not last_alive or last_alive < deadline - datetime.timedelta(seconds=15):
                    raise ValueError("the witnessed old connection was lost without a verified near-deadline drain")
        check_new()
        grant = adapter.drain_grant()
        if grant is not None:
            healthy = front.timestamp(grant["healthy_at"])
            if (set(grant) != {"schema", "front_uid", "node", "previous", "serving", "start", "deadline", "healthy_at"}
                    or grant["schema"] != "edge-worker-bounded-drain/v1" or grant["front_uid"] != old["front_uid"]
                    or grant["node"] != journal["recipient"]["node"] or grant["previous"] != journal["expected"]
                    or grant["serving"] != state or front.timestamp(grant["start"]) != started
                    or front.timestamp(grant["deadline"]) != deadline or healthy < started
                    or seen is not None and healthy < front.timestamp(seen["healthy_at"])):
                raise ValueError("Guardian drain grant did not bind the immutable cutover and both Worker releases")
            metrics = adapter.drain_metrics()
            if metrics.get("capability") != 1:
                raise ValueError("selected Front did not advertise bounded Worker drain capability")
            if metrics.get("last_generation") == state["generation"]:
                if metrics.get("last_deadline_seconds") != int(deadline.timestamp()):
                    raise ValueError("Front reported a different Worker drain deadline")
                if now() < deadline and metrics.get("last_forced") != 0:
                    raise ValueError("Front reported a forced old-Worker closure before the deadline")
                if seen is None and detached is None and metrics.get("last_remaining", 0) > 0 and now() < deadline:
                    record(drain_started={"grant": grant, "metrics": metrics, "held": old,
                                          "remaining_seconds": int((deadline - now()).total_seconds())})
                    seen = grant
                elif seen is not None:
                    seen = grant
                if detached and metrics.get("last_forced", 0) > 0 and metrics.get("last_remaining") == 0 and metrics.get("last_completed") == 1:
                    event = adapter.forced_connection(old["id"], state["generation"], deadline)
                    if event is not None:
                        if not deadline <= event <= detached + datetime.timedelta(seconds=5):
                            raise ValueError("Front's exact force-close receipt falls outside the observed window")
                        record(drain_completed={"last_alive_at": wire_time(last_alive), "peer_closed_at": wire_time(detached),
                                                "front_forced_at": wire_time(event), "metrics": metrics, "grant": grant})
                        return
        pause(2 if now() >= deadline - datetime.timedelta(seconds=15) else 5)
    raise ValueError("no witnessed old socket force-close and exact Front evidence by the bounded drain deadline")


def handoff(adapter, profile, selected_front, address, probe, journal, *, probe_port=None,
            now=front.now, pause=time.sleep, open_tls=connection.HeldTLS, drain_record=None):
    """Adapter supplies disk readback and immutable arm/witness/result, not cutover."""
    if slot_phase(journal, adapter.selected_activation()) != "pending":
        raise ValueError("handoff must begin on the exact retained Worker")
    before, target = journal["expected"]["active_slot"], journal["candidate"]["Slot"]
    retained_port = 18443 if before == "a" else 28443
    candidate_port = (18443 if target == "a" else 28443) if journal.get("fixed_transport") else 38443
    retained_addr, candidate_addr = socket_target(address, retained_port), socket_target(address, candidate_port)
    if bool(journal.get("probe_service_uid")) != (probe_port is not None):
        raise ValueError("handoff must use the probe transport named in the journal")
    host, path = probe["host"], probe["path"]
    canary_digest = probe.get("bodyDigest")

    def proof(sock):
        value = sock.proof(path)
        if value["edge"] != profile["node"] or value["group"] != profile["group"]:
            raise ValueError("held public TLS did not serve the declared node and authority")
        return value

    def fact(sock, slot, target_addr):
        return require_fact(connection_ref(adapter.connection_fact(profile, selected_front, sock)), journal, slot, target_addr)

    def retained_serves(sock):
        # An inactive Worker may keep serving existing sessions but cannot
        # emit positive signed routing authority for new sessions.
        if canary_digest:
            sock.serving(path, canary_digest)
        else:
            proof(sock)

    def common_route(reference, retained, fresh, probe_fresh, deadline):
        # Compare concurrent new public/probe signed publications, and bind
        # their stable route digest to the predecessor's pre-cutover proof.
        samples = {}
        for _ in range(4):
            if now() >= deadline:
                break
            samples = {"public": proof(fresh)}
            if not canary_digest:
                samples["held"] = proof(retained)
            if probe_fresh is not None:
                samples["probe"] = proof(probe_fresh)
            samples["public_after"] = proof(fresh)
            if (samples["public"].get("digest") == reference.get("digest")
                    and all(sample == samples["public"] for sample in samples.values())):
                return
            pause(2)
        summary = {endpoint: {"version": sample.get("version"), "digest": sample.get("digest"),
                              "release_id": sample.get("traffic", {}).get("release_id")}
                   for endpoint, sample in samples.items()}
        raise ValueError("same-socket public and probe route evidence did not converge: " + front.canonical(summary))

    with closing(open_tls(address, host)) as retained:
        reference = proof(retained)
        retained_serves(retained)
        old = fact(retained, before, retained_addr)
        opened = now()
        deadline = opened + datetime.timedelta(seconds=150)
        adapter.arm({"schema": "edge-node-worker-arm/v1", "journal_digest": journal["digest"], "held": old,
                     "opened_at": wire_time(opened), "deadline": wire_time(deadline)})
        while now() < deadline:
            state = adapter.selected_activation()
            phase = slot_phase(journal, state)
            if phase == "reverted":
                raise ValueError("Guardian reverted the node Worker; no handoff verified")
            retained_serves(retained)
            if fact(retained, before, retained_addr) != old:
                raise ValueError("original TLS connection changed during node Worker handoff")
            if phase == "pending":
                pause(3)
                continue
            # Connection attempts are allowed to converge while the original
            # socket remains open. Never retry or reconnect the original.
            with ExitStack() as sockets:
                try:
                    fresh = sockets.enter_context(closing(open_tls(address, host)))
                    newest = fact(fresh, target, candidate_addr)
                    probe_fresh = sockets.enter_context(closing(open_tls(address, host, probe_port))) if probe_port else None
                    probe_ref = fact(probe_fresh, target, candidate_addr) if probe_fresh is not None else None
                except (OSError, ValueError):
                    pause(2)
                    continue
                if (newest["id"] == old["id"] or front.timestamp(newest["started_at"]) < front.timestamp(state["updated_at"])
                        or fact(retained, before, retained_addr) != old):
                    raise ValueError("public TLS handoff did not prove two distinct Worker connections after activation")
                if probe_ref is not None and (probe_ref["id"] in (old["id"], newest["id"])
                        or front.timestamp(probe_ref["started_at"]) < front.timestamp(state["updated_at"])):
                    raise ValueError("probe TLS did not independently reach the newly selected Worker after activation")
                common_route(reference, retained, fresh, probe_fresh, deadline)
                if canary_digest:
                    fresh.serving(path, canary_digest)
                    if probe_fresh is not None:
                        probe_fresh.serving(path, canary_digest)
                witness = {"schema": "edge-node-worker-connection-witness/v1", "journal_digest": journal["digest"],
                           "held": old, "fresh": newest, "observed_at": wire_time(now())}
                if probe_ref is not None:
                    witness["probe_fresh"] = probe_ref
                if now() >= deadline:
                    break
                adapter.witness(witness)
                while now() < deadline + datetime.timedelta(seconds=15):
                    result = adapter.result()
                    if probe_ref is not None:
                        proof(probe_fresh)
                        if fact(probe_fresh, target, candidate_addr) != probe_ref:
                            raise ValueError("fresh probe connection changed before Guardian acknowledgement")
                    retained_serves(retained)
                    proof(fresh)
                    if (adapter.selected_activation() != state or fact(retained, before, retained_addr) != old
                            or fact(fresh, target, candidate_addr) != newest):
                        raise ValueError("a witnessed connection or activation changed before Guardian receipt")
                    if result is not None:
                        require_verified(result, journal, state, witness)
                        if drain_record is not None:
                            if probe_fresh is None:
                                raise ValueError("drain verification requires both public and probe sockets")
                            drain_record(guardian_result=result)

                            def check_old():
                                # A routine signed route refresh can change the
                                # proof version. The socket and Worker identity
                                # must remain exact throughout the drain.
                                retained_serves(retained)
                                if fact(retained, before, retained_addr) != old:
                                    raise ValueError("draining socket no longer reaches its exact old Worker")

                            def check_new():
                                for sock, ref in ((fresh, newest), (probe_fresh, probe_ref)):
                                    proof(sock)
                                    if fact(sock, target, candidate_addr) != ref:
                                        raise ValueError("new Worker socket changed during the old-Worker drain")

                            verify_drain(adapter, journal, result, state, old, check_old, check_new, now, pause, drain_record)
                        return result
                    pause(3)
                break
        # No direct write. Loss of witness/expiry makes the Guardian retain or
        # restore the exact old Worker; this process cannot bypass the gate.
        raise ValueError("no verified Guardian node handoff before the bounded witness deadline")
