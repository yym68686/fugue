#!/usr/bin/env python3
"""Exercise the independently built probe ABI without access to any host data."""
import json
import subprocess
import sys

request = {
    "probe_image":sys.argv[1], "protocol": "fugue.diagnostics/v1", "session_id": "diagnostic-smoke", "probe_id": "package-smoke",
    "probe_digest": "sha256:" + "a" * 64, "catalog_digest": "sha256:" + "b" * 64,
    "target": {"type": "node", "node": "sandbox-test"}, "duration_seconds": 5,
    "sample_interval_milliseconds": 1000, "max_output_bytes": 65536, "parameters": {},
    "config": {"collectors": [{"name": "host-observation", "kind": "node-snapshot"}]},
}
result = subprocess.run([
    "docker", "run", "--rm", "--network=none", "--read-only", "--cap-drop=ALL", "--user=65532:65532",
    "--memory=256m", "--cpus=0.25", "--tmpfs", "/tmp:rw,size=33554432,noexec",
    "--env", "FUGUE_DIAGNOSTIC_REQUEST=" + json.dumps(request), sys.argv[1],
], capture_output=True, check=True, timeout=90)
if len(result.stdout) > request["max_output_bytes"]:
    raise RuntimeError("probe exceeded its output budget")
report = json.loads(result.stdout)
for key in ["probe_image", "session_id", "probe_id", "probe_digest", "catalog_digest", "target"]:
    if report.get(key) != request[key]:
        raise RuntimeError("probe did not preserve its admitted identity")
if report.get("schema") != "fugue.diagnostic.probe_report.v1" or report["quality"]["status"] != "degraded":
    raise RuntimeError("probe reported missing host evidence as complete")
if not report["quality"]["gaps"] or not report["evidence"]:
    raise RuntimeError("probe did not explain missing capabilities")
print("independent probe ABI and missing-capability reporting passed")
