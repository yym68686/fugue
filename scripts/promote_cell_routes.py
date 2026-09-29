#!/usr/bin/env python3
"""Verify an activated private Cell's route/TLS LKG and full publication.

No public transport, DNS publication, activation file or code image is changed.
Each publication requires a new live observation window for its exact fence.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import time

from scripts import bootstrap_cell_inventory as cell
from scripts.bootstrap_cell_producer import digest, selected
from scripts.observe_front_candidate import now, timestamp
from scripts.publish_agent_edge_shadow import API, canonical


def validate(c):
    if set(c) != {"schema", "generation", "enrollment", "enrollment_digest", "gray_release", "observation"} or c["schema"] != "fugue.private-cell-route-promotion/v1" or c["generation"] != 1 or type(c["generation"]) is not int:
        raise ValueError("explicit initial private Cell promotion required")
    path = Path(c["enrollment"])
    if path.parent != Path("deploy/environments/production/cell-inventory") or not re.fullmatch(r"cell-[a-z0-9-]+\.json", path.name) or not re.fullmatch(r"sha256:[a-f0-9]{64}", c["enrollment_digest"]):
        raise ValueError("exact enrollment declaration reference required")
    r, o = c["gray_release"], c["observation"]
    if set(r) != {"id", "fencing_token"} or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,256}", r["id"]) or type(r["fencing_token"]) is not int or r["fencing_token"] < 1:
        raise ValueError("exact predecessor gray publication required")
    if set(o) != {"samples", "interval_seconds", "timeout_seconds"} or any(type(o[k]) is not int for k in o) or not 3 <= o["samples"] <= 6 or not 15 <= o["interval_seconds"] <= 30 or not 180 <= o["timeout_seconds"] <= 600:
        raise ValueError("bounded fresh publication observation required")
    return c


def enrollment(c):
    e = cell.validate(json.loads(Path(c["enrollment"]).read_text()))
    if digest(e) != c["enrollment_digest"]:
        raise ValueError("enrollment declaration differs from promotion pin")
    return e


def publication(e, api, channel):
    state = selected(api, "authority-cell:" + e["authority_cell_id"], channel, "release_set")
    a, r = state.get("artifact"), state.get("release")
    if not a and not r:
        return None
    if a["id"] != e["release_set"]["id"] or a["content_hash"] != e["release_set"]["digest"] or r.get("status") != "active" or r.get("fencing_token", 0) <= 0 or channel == "full" and r.get("canary_rule_ref") or channel == "gray" and r.get("canary_rule_ref") != "cohort=" + e["cohort"]:
        raise ValueError("private Cell publication differs from declared parent")
    return r


def active_publication(e, api, release):
    current = publication(e, api, release["release_channel"])
    if current is None or (current["id"], current["fencing_token"]) != (release["id"], release["fencing_token"]):
        raise ValueError("private Cell publication changed")
    if release["release_channel"] == "gray" and publication(e, api, "full") is not None:
        raise ValueError("full authority superseded initial gray observation")


def lkg(e, api, accepted_releases):
    value = api("GET", "/v1/admin/artifacts/" + e["release_set"]["id"] + "/lkg").get("lkg")
    if value is None:
        return None
    if value.get("artifact_id") != e["release_set"]["id"] or value.get("content_hash") != e["release_set"]["digest"] or value.get("artifact_kind") != "release_set" or value.get("scope_key") != "authority-cell:" + e["authority_cell_id"] or value.get("verified_by_release_id") not in accepted_releases or not value.get("verification_evidence_hash") or timestamp(value["expires_at"]) <= now():
        raise ValueError("private Cell LKG is foreign, expired or unverified")
    return value


def observe(c, e, api, release, baseline):
    deadline, samples, last_error = time.monotonic() + c["observation"]["timeout_seconds"], [], "awaiting serving convergence"
    while time.monotonic() < deadline:
        cell.isolated_worker(e)
        if cell.activation(e) != baseline:
            raise ValueError("private Cell activation changed during observation")
        active_publication(e, api, release)
        try:
            h = cell.health(e, release)
            cell.convergence(e, api, release)
            if timestamp(h["inventory_heartbeat_at"]) <= timestamp(baseline["updated_at"]):
                raise ValueError("inventory does not follow real activation")
            samples.append({"at": now().isoformat(), "bundle_version": h["bundle_version"], "inventory_heartbeat_at": h["inventory_heartbeat_at"], "verified_at": h["platform_serving"]["verified_at"], "route_probes": h["platform_serving"]["route_probes"], "tls_probes": h["platform_serving"]["tls_probes"]})
        except ValueError as error:
            samples, last_error = [], str(error)
        if len(samples) >= c["observation"]["samples"]:
            if timestamp(samples[-1]["inventory_heartbeat_at"]) <= timestamp(samples[0]["inventory_heartbeat_at"]) or timestamp(samples[-1]["verified_at"]) <= timestamp(samples[0]["verified_at"]):
                raise ValueError("publication observation did not advance real probes and inventory")
            return {"declaration_digest": digest(c), "enrollment_digest": digest(e), "release_id": release["id"], "fencing_token": release["fencing_token"], "release_channel": release["release_channel"], "activation": baseline, "worker_uid": e["worker"]["instance_uid"], "observations": samples, "completed_at": now().isoformat()}
        time.sleep(c["observation"]["interval_seconds"])
    raise ValueError(last_error)


def verify(c, e, api, release, witness, baseline, initial):
    if witness.get("declaration_digest") != digest(c) or witness.get("enrollment_digest") != digest(e) or witness.get("release_id") != release["id"] or witness.get("fencing_token") != release["fencing_token"] or witness.get("activation") != baseline or witness.get("worker_uid") != e["worker"]["instance_uid"] or len(witness.get("observations", [])) != c["observation"]["samples"] or not datetime.timedelta(0) <= now() - timestamp(witness["completed_at"]) < datetime.timedelta(seconds=90):
        raise ValueError("private Cell verification witness is stale or unbound")
    cell.isolated_worker(e)
    if cell.activation(e) != baseline:
        raise ValueError("private Cell activation changed before LKG verification")
    active_publication(e, api, release)
    cell.health(e, release)
    cell.convergence(e, api, release)
    api("POST", "/v1/admin/artifact-releases/" + release["id"] + "/verify-lkg", {"fencing_token": release["fencing_token"], "allow_initial_lkg": initial, "reason": "verified private Cell route and TLS proofs with stable activation and renewed inventory", "evidence": {"consumer_convergence": True, "local_probe": True, "platform_evidence": True, "watch_window": True, "baseline_monotonic": True, "database_rollback_compatible": True, "evidence_refs": [digest(witness), e["release_set"]["id"], release["id"]]}})
    result = lkg(e, api, [release["id"]])
    if result is None:
        raise ValueError("verified private Cell LKG was not retained")
    return result


def promote(c, e, api, save):
    validate(c)
    cell.parent(e, api)
    cell.isolated_worker(e)
    baseline = cell.activation(e)
    cell.check_activation(e, baseline)
    gray = publication(e, api, "gray")
    if gray is None or {"id": gray["id"], "fencing_token": gray["fencing_token"]} != c["gray_release"]:
        raise ValueError("initial gray predecessor differs")
    full = publication(e, api, "full")
    if full and full.get("idempotency_key") != "git-cell-full/" + digest(c):
        raise ValueError("full publication is not this declared initial promotion")
    existing = lkg(e, api, [gray["id"]] + ([full["id"]] if full else []))
    if full and existing is None:
        raise ValueError("private full publication has no positive recovery baseline")
    evidence = {"schema": "fugue.private-cell-route-promotion-result/v1", "declaration_digest": digest(c), "public_transport_changed": False, "dns_published": False, "activation": baseline}
    save(evidence)
    if full is None:
        witness = observe(c, e, api, gray, baseline)
        evidence["gray_witness"] = witness
        save(evidence)
        # A verified release retains the original evidence hash. Fresh retry
        # observations authorize the next step without rewriting that receipt.
        evidence["gray_lkg"] = existing if existing is not None else verify(c, e, api, gray, witness, baseline, True)
        save(evidence)
        active_publication(e, api, gray)
        if cell.activation(e) != baseline:
            raise ValueError("private Cell activation changed before full publication")
        api("POST", "/v1/admin/artifacts/" + e["release_set"]["id"] + "/release", {"release_channel": "full", "idempotency_key": "git-cell-full/" + digest(c), "reason": "initial private Cell full publication after verified route and TLS gray LKG"})
        full = publication(e, api, "full")
        if full is None or full.get("idempotency_key") != "git-cell-full/" + digest(c):
            raise ValueError("declared private full publication is not selected")
    evidence["full_release_id"] = full["id"]
    save(evidence)
    api("POST", "/v1/admin/platform-config/release-set/prepare-consumers", {"release_set_id": e["release_set"]["id"], "artifact_release_id": full["id"]})
    witness = observe(c, e, api, full, baseline)
    evidence["full_witness"] = witness
    save(evidence)
    current_lkg = lkg(e, api, [gray["id"], full["id"]])
    evidence["full_lkg"] = current_lkg if current_lkg and current_lkg["verified_by_release_id"] == full["id"] else verify(c, e, api, full, witness, baseline, False)
    evidence["completed_at"] = now().isoformat()
    save(evidence)
    return evidence


def main():
    p = argparse.ArgumentParser()
    p.add_argument("declaration")
    p.add_argument("--validate-only", action="store_true")
    p.add_argument("--evidence")
    args = p.parse_args()
    c = validate(json.loads(Path(args.declaration).read_text()))
    e = enrollment(c)
    if args.validate_only:
        print(canonical({"valid": True, "declaration_digest": digest(c)}))
        return
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token or not args.evidence:
        raise ValueError("configuration credential and retained evidence path required")
    latest = {"schema": "fugue.private-cell-route-promotion-result/v1", "declaration_digest": digest(c)}
    def save(value):
        latest.clear()
        latest.update(value)
        Path(args.evidence).write_text(canonical(value) + "\n")
    try:
        result = promote(c, e, API(e["origin"], token), save)
    except Exception as error:
        latest.update(failure=str(error), failed_at=now().isoformat())
        Path(args.evidence).write_text(canonical(latest) + "\n")
        raise
    print(canonical(result))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("private Cell promotion stopped: " + str(error)) from None
