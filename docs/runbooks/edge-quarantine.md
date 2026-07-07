# Edge Quarantine Runbook

## Trigger

Use this runbook when traffic safety or edge ranking excludes an edge because
it is unhealthy, draining, route-not-ready, TLS-not-ready, or node-quarantined.

## Read-Only Diagnosis

```bash
fugue admin edge nodes --json
fugue admin edge quality-rank <hostname> --json
fugue admin traffic-safety explain <hostname> --json
fugue admin robustness status --json
```

Confirm the `edge_id`, `edge_group_id`, route generation, TLS state, Caddy route
count, and quarantine reason.

## Safety Rules

- Do not manually add a quarantined edge back to DNS answers.
- Use service-scoped exclusion before broad edge-group changes.
- If only one service is affected, keep the blast radius scoped to that
  hostname.

## Recovery

1. If the edge is draining or in maintenance, wait for the planned window or
   explicitly undrain after validation.
2. If route or TLS state is stale, reload LKG or desired state:

   ```bash
   fugue admin node-updater tasks create --node <node-name> --type reload-lkg-bundle
   fugue admin node-updater tasks create --node <node-name> --type refresh-desired-state
   ```

3. If node deep health is failing, follow the node DNS or stale iptables
   runbook.

## Verification

- `traffic-safety explain` shows the edge group as healthy.
- `quality-rank` no longer marks the edge as excluded for quarantine.
- DNS answers contain only healthy, route-ready, TLS-ready edges.
