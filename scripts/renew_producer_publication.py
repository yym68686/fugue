#!/usr/bin/env python3
"""Explicitly renew an unchanged producer policy after verified recovery.

Uses the existing rollback API in the shadow control lane. Policy bytes and
DNS source authorizations stay unchanged; the new fence requires fresh traffic
publications and evidence. No runtime facts or failed-source records are edited.
"""
import argparse
import json
import os
from pathlib import Path
import re
import urllib.parse

from scripts.bootstrap_cell_producer import authority_identity, digest, selected
from scripts.observe_front_candidate import now, timestamp
from scripts.publish_agent_edge_shadow import API, canonical
from scripts.reconfigure_cell_producer import validate_ref


def validate(config):
    if set(config) != {"schema", "generation", "origin", "producers"} or config["schema"] != "fugue.producer-publication-renewal/v1" or type(config["generation"]) is not int or config["generation"] < 1:
        raise ValueError("explicit producer renewal declaration required")
    origin = urllib.parse.urlsplit(config["origin"])
    if origin.scheme != "https" or config["origin"] != "https://" + str(origin.hostname):
        raise ValueError("canonical HTTPS API origin required")
    rows = config["producers"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= 17:
        raise ValueError("bounded producer set required")
    scopes = []
    for row in rows:
        if set(row) != {"scope", "policy", "full", "verification_evidence_hash", "failed_release_id"}:
            raise ValueError("exact policy, recovery baseline and failed release required")
        if row["scope"] != "global" and not re.fullmatch(r"authority-cell:cell-[a-z0-9]+(?:-[a-z0-9]+)*", row["scope"]):
            raise ValueError("invalid producer scope")
        for key in ["policy", "full"]:
            validate_ref(row[key])
        if not re.fullmatch(r"sha256:[a-f0-9]{64}", row["verification_evidence_hash"]) or not re.fullmatch(r"[a-zA-Z0-9_.-]{1,256}", row["failed_release_id"]):
            raise ValueError("invalid recovery evidence identity")
        scopes.append(row["scope"])
    if scopes != sorted(set(scopes)):
        raise ValueError("unique sorted producer scopes required")
    return config


def policy_scope(row):
    return "platform-config-producer" + ("" if row["scope"] == "global" else ":" + row["scope"].split(":", 1)[1])


def observe(config, row, api):
    current = selected(api, policy_scope(row), "shadow")
    artifact, release = current.get("artifact", {}), current.get("release", {})
    pin = row["policy"]
    policy = artifact.get("content", {})
    reason = "declared unchanged producer renewal " + digest(config)
    if artifact.get("id") != pin["artifact_id"] or artifact.get("content_hash") != pin["content_hash"] or digest(policy) != pin["content_hash"] or artifact.get("status") != "validated" or release.get("status") != "active" or policy.get("target_scope") != row["scope"] or policy.get("mode") != "serving" or policy.get("serving", {}).get("single_publication", False) is not False:
        raise ValueError("current producer differs from unchanged continuous policy")
    if release.get("reason") == reason and release.get("fencing_token") == pin["fencing_token"] + 1 and release.get("id") != pin["release_id"]:
        return current, True
    if authority_identity(current) != pin:
        raise ValueError("producer publication changed")
    full = selected(api, row["scope"], "full", "release_set")
    a, r = full.get("artifact", {}), full.get("release", {})
    lkg = full.get("lkg") or {}
    if authority_identity(full) != row["full"] or r.get("verification_state") != "verified" or r.get("verified_lkg_generation") != a.get("generation") or a.get("metadata", {}).get("producer_policy_release_id") != pin["release_id"] or lkg.get("artifact_id") != a.get("id") or lkg.get("content_hash") != a.get("content_hash") or lkg.get("verified_by_release_id") != r.get("id") or lkg.get("verification_evidence_hash") != row["verification_evidence_hash"] or timestamp(lkg["expires_at"]) <= now():
        raise ValueError("exact verified recovery baseline unavailable")
    gray = selected(api, row["scope"], "gray", "release_set").get("release", {})
    evidence = gray.get("verification_evidence", {})
    if gray.get("id") != row["failed_release_id"] or gray.get("verification_state") != "failed" or evidence.get("producer_policy_release_id") != pin["release_id"] or not evidence.get("failed_source_digest") or evidence.get("recovered_by_release_id") != r.get("id"):
        raise ValueError("failed source is not recovered by the pinned full")
    return current, False


def renew(config, api, save):
    validate(config)
    evidence = {"schema": "fugue.producer-publication-renewal-result/v1", "declaration_digest": digest(config), "results": []}
    for row in config["producers"]:
        observe(config, row, api)
    for row in config["producers"]:
        current, done = observe(config, row, api)
        if not done:
            # No blind retry of this mutation. A later workflow retry accepts
            # only the exact next fence bearing this declaration's reason.
            api("POST", "/v1/admin/artifacts/" + row["policy"]["artifact_id"] + "/rollback", {"release_channel": "shadow", "to_generation": current["artifact"]["generation"], "reason": "declared unchanged producer renewal " + digest(config)})
        after, done = observe(config, row, api)
        if not done:
            raise ValueError("renewed publication not observed")
        evidence["results"].append({"scope": row["scope"], "authority": authority_identity(after)})
        save(evidence)
    evidence["completed_at"] = now().isoformat()
    save(evidence)
    return evidence


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("declaration")
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    raw = Path(args.declaration).read_bytes()
    if len(raw) > 65536:
        raise ValueError("declaration exceeds bound")
    config = validate(json.loads(raw))
    token = os.environ.pop("FUGUE_API_KEY", "")
    if not token:
        raise ValueError("independent configuration credential required")
    def save(value):
        Path(args.evidence).write_text(canonical(value) + "\n")
    print(canonical(renew(config, API(config["origin"], token), save)))


if __name__ == "__main__":
    main()
