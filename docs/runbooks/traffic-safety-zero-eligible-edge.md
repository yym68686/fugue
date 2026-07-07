# Traffic Safety Zero Eligible Edge Runbook

## Trigger

Use this when `service.healthy_edge_count_zero` fires or
`traffic-safety explain` reports no healthy non-excluded edge for a hostname.

## Read-Only Diagnosis

```bash
fugue admin traffic-safety explain <hostname> --json
fugue admin edge quality-rank <hostname> --json
fugue admin edge nodes --json
```

Check service-level exclusions, edge group health, node quarantine, route
generation, TLS readiness, and gray release scope.

## Safety Rules

- A release or exclusion update that leaves zero eligible edges must fail
  closed.
- Do not bypass by publishing an empty DNS answer set.
- If one edge remains, strict protection requires operator awareness before
  reducing it further.

## Recovery

1. Remove or narrow the service-level exclusion if it is the only blocker.
2. Restore one edge by fixing route/TLS/node health.
3. If a gray release caused the failure, abort the gray artifact and keep full
   channel LKG active.
4. Re-run:

   ```bash
   fugue admin traffic-safety explain <hostname>
   fugue admin robustness status
   ```

## Verification

- `healthy_edge_count >= min_healthy_edge_count`.
- `blockers` is empty for the hostname.
- DNS answers include at least one non-excluded healthy edge.
