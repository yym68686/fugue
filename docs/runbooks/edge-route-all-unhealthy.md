# Edge route all unhealthy

Use this runbook when route inventory, DNS answer generation, or edge health would leave a hostname or edge group without an eligible edge.

## Expected system behavior

- Blast-radius cap preserves last known good eligible edges when a new gate would remove every healthy edge group.
- Release guard blocks promotion of the gate that exceeded the cap.
- DNS answers should keep serving LKG rather than returning an empty answer set for platform hostnames.

## Triage

- Run `fugue admin traffic-safety explain <hostname>`.
- Run `fugue admin edge route-check <hostname>` if available in the deployed CLI.
- Run direct edge probes with `curl --resolve <hostname>:443:<edge-ip> https://<hostname>/healthz`.
- Check `fugue admin gate show edge.route_inventory_quarantine`.

## Recovery

- Disable or demote the offending gate: `fugue admin gate promote <gate-id> --mode shadow --reason <reason>`.
- Restore excluded edges only after confirming the edge is route-ready, TLS-ready, and not draining.
- Do not remove the final eligible edge for a hostname unless the service is intentionally disabled.
