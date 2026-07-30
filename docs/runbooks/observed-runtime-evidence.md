# Observed Runtime Evidence Runbook

## Trigger

Use this when an application's stored or desired state disagrees with its
current Kubernetes runtime, when a route is unavailable, or when a migration
cutover is blocked.

## Read-Only Diagnosis

Inspect the application status and operation evidence first:

```bash
fugue app status <app-name-or-id> --json
fugue operation show <operation-id> --json
fugue admin robustness status --json
```

The observed status is authoritative for readiness. Verify `observed_at`,
`cluster_id`, `generation`, `evidence_source`, namespace/ManagedApp identity,
physical replicas, image, and Endpoint readiness. A failed Kubernetes query is
`unknown`; it is not proof that the ManagedApp is absent.

## Safety Rules

- Never restore `deployed` from a historical database replica count after a
  failed operation.
- Do not cut over or retire old migration artifacts until the per-app ledger
  records verified image replication, runtime object creation, and Endpoint
  readiness for the current cluster and generation.
- Keep old artifacts protected while the migration ledger is pending, blocked,
  failed, or unknown.

## Verification

- A complete successful Kubernetes absence query yields `unavailable` with
  `ready=0`.
- A Kubernetes query error yields `unknown` and preserves the error evidence.
- Green UI/CLI/Edge readiness requires fresh observed evidence, ready replicas,
  a present Endpoint, and no invariant violations.
- Migration evidence is retained for at least 90 days and includes the
  operation and operator identifiers.
