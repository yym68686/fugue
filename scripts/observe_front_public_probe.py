#!/usr/bin/env python3
"""Verify Front probe ingress from a runner outside the production cluster."""
import argparse
import json
from pathlib import Path
import time

try:
    from . import observe_front_candidate as front
    from . import reconcile_front_probe_transport as transport
except ImportError:
    import observe_front_candidate as front
    import reconcile_front_probe_transport as transport


def observe(config, profiles, samples=5, interval=15):
    observations = []
    for index in range(samples):
        for listener in config["listeners"]:
            profile = profiles[listener["name"]]
            proofs = []
            for route in profile["probes"]:
                current = front.proof(listener["address"], route["host"], route["path"])
                candidate = front.proof(listener["address"], route["host"], route["path"], listener["port"])
                if current != candidate or candidate["edge"] != profile["node"] or candidate["group"] != profile["group"]:
                    differences = sorted(key for key in set(current) | set(candidate) if current.get(key) != candidate.get(key))
                    summary = {"listener": listener["name"], "host": route["host"], "differences": differences,
                               "public_edge": current.get("edge"), "probe_edge": candidate.get("edge"),
                               "public_version": current.get("version"), "probe_version": candidate.get("version"),
                               "public_release": current.get("traffic", {}).get("release_id"), "probe_release": candidate.get("traffic", {}).get("release_id")}
                    raise ValueError("external candidate route differs from the current public authority: " + front.canonical(summary))
                proofs.append({"host": route["host"], "path": route["path"], "proof_digest": transport.digest(candidate)})
            observations.append({"at": front.now().isoformat(), "listener": listener["name"], "address": listener["address"], "port": listener["port"], "profile_digest": transport.digest(profile), "proofs": proofs})
        if index + 1 < samples:
            time.sleep(interval)
    return {"schema": "fugue.front-external-probe-evidence/v1", "authorizes_traffic": False, "declaration_digest": transport.digest(config), "observations": observations}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("config")
    parser.add_argument("--evidence", required=True)
    args = parser.parse_args()
    config = transport.validate(json.loads(Path(args.config).read_text()))
    profiles = {item["name"]: front.validate(json.loads(Path(item["observation"]).read_text())) for item in config["listeners"]}
    try:
        evidence = observe(config, profiles)
    except Exception as error:
        failure = {"schema": "fugue.front-external-probe-failure/v1", "authorizes_traffic": False, "at": front.now().isoformat(), "declaration_digest": transport.digest(config), "error_type": type(error).__name__, "error": str(error)[:2048]}
        Path(args.evidence).write_text(front.canonical(failure) + "\n")
        raise
    Path(args.evidence).write_text(front.canonical(evidence) + "\n")
    print(front.canonical({"external_front_verified": True, "authorizes_traffic": False, "samples": len(evidence["observations"])}))


if __name__ == "__main__":
    main()
