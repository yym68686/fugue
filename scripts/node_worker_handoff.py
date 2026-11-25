"""Hold and verify an exact public TLS handoff for a claimed node Worker."""
import datetime
import ipaddress
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
                    active_slot=candidate["slot"], bundle_generation=candidate["bundle_generation"],
                    worker_source_commit=candidate["worker_source_commit"], worker_image_digest=candidate["worker_image_digest"],
                    operation="promote", reason="advance node Worker using journal " + journal["digest"])
    reverted = dict(state, generation=before["generation"] + 2, previous_slot=candidate["slot"],
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


def handoff(adapter, profile, selected_front, address, probe, journal, *, probe_port=None,
            now=front.now, pause=time.sleep, open_tls=connection.HeldTLS):
    """Adapter supplies disk readback and immutable arm/witness/result, not cutover."""
    if slot_phase(journal, adapter.selected_activation()) != "pending":
        raise ValueError("handoff must begin on the exact retained Worker")
    before, target = journal["expected"]["active_slot"], journal["candidate"]["slot"]
    retained_port = 18443 if before == "a" else 28443
    candidate_port = (18443 if target == "a" else 28443) if journal.get("fixed_transport") else 38443
    retained_addr, candidate_addr = socket_target(address, retained_port), socket_target(address, candidate_port)
    if bool(journal.get("probe_service_uid")) != (probe_port is not None):
        raise ValueError("handoff must use the probe transport named in the journal")
    host, path = probe["host"], probe["path"]

    def proof(sock):
        value = sock.proof(path)
        if value["edge"] != profile["node"] or value["group"] != profile["group"]:
            raise ValueError("held public TLS did not serve the declared node and authority")
        return value

    def fact(sock, slot, target_addr):
        return require_fact(connection_ref(adapter.connection_fact(profile, selected_front, sock)), journal, slot, target_addr)

    with closing(open_tls(address, host)) as retained:
        reference = proof(retained)
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
            if proof(retained) != reference or fact(retained, before, retained_addr) != old:
                raise ValueError("original TLS connection changed during node Worker handoff")
            if phase == "pending":
                pause(3)
                continue
            # Connection attempts are allowed to converge while the original
            # socket remains open. Never retry or reconnect the original.
            with ExitStack() as sockets:
                try:
                    fresh = sockets.enter_context(closing(open_tls(address, host)))
                    new_proof = proof(fresh)
                    newest = fact(fresh, target, candidate_addr)
                    probe_fresh = sockets.enter_context(closing(open_tls(address, host, probe_port))) if probe_port else None
                    probe_ref = fact(probe_fresh, target, candidate_addr) if probe_fresh is not None else None
                except (OSError, ValueError):
                    pause(2)
                    continue
                if (newest["id"] == old["id"] or front.timestamp(newest["started_at"]) < front.timestamp(state["updated_at"])
                        or new_proof != reference or proof(retained) != reference or fact(retained, before, retained_addr) != old):
                    raise ValueError("public TLS handoff did not demonstrate unchanged route and two distinct Worker processes")
                if probe_ref is not None and (probe_ref["id"] in (old["id"], newest["id"])
                        or front.timestamp(probe_ref["started_at"]) < front.timestamp(state["updated_at"])
                        or proof(probe_fresh) != reference):
                    raise ValueError("probe TLS did not independently reach the newly selected Worker with the same route")
                witness = {"schema": "edge-node-worker-connection-witness/v1", "journal_digest": journal["digest"],
                           "held": old, "fresh": newest, "observed_at": wire_time(now())}
                if probe_ref is not None:
                    witness["probe_fresh"] = probe_ref
                if now() >= deadline:
                    break
                adapter.witness(witness)
                while now() < deadline + datetime.timedelta(seconds=15):
                    result = adapter.result()
                    if probe_ref is not None and (proof(probe_fresh) != reference or fact(probe_fresh, target, candidate_addr) != probe_ref):
                        raise ValueError("fresh probe connection changed before Guardian acknowledgement")
                    if result is not None:
                        require_verified(result, journal, state, witness)
                        if (adapter.selected_activation() != state or proof(retained) != reference or proof(fresh) != reference
                                or fact(retained, before, retained_addr) != old or fact(fresh, target, candidate_addr) != newest):
                            raise ValueError("a witnessed connection or activation vanished during Guardian readback")
                        return result
                    if (adapter.selected_activation() != state or proof(retained) != reference or proof(fresh) != reference
                            or fact(retained, before, retained_addr) != old or fact(fresh, target, candidate_addr) != newest):
                        raise ValueError("a witnessed connection or activation changed before Guardian receipt")
                    pause(3)
                break
        # No direct write. Loss of witness/expiry makes the Guardian retain or
        # restore the exact old Worker; this process cannot bypass the gate.
        raise ValueError("no verified Guardian node handoff before the bounded witness deadline")
