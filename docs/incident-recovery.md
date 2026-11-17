# Incident recovery with retained evidence

`fugue app recovery` starts an explicitly authorized replacement of a single
replica on an internal shared managed runtime. It uses the original container's
observed immutable image digest, current resources and configured placement.
It is an operator workflow, not automatic failover.

The replacement has its own Deployment, Service, container filesystem and fresh
PVC. Existing live files are never copied or mounted by both instances.
Declarative file seeds are reapplied. This is appropriate only when the
application can start against fresh local state; it does not clone databases or
recover durable files. Attached backing databases retain their existing identity.
Replicated volumes and shared project RWX storage are rejected.

## Start and inspect

Save a request such as the following to a private local JSON file. The probe must
exercise a real business request with a harmless payload and an expected response
body. A readiness endpoint alone is insufficient evidence. Probe credentials and
payloads are stored privately; recovery API responses omit them.

```json
{
  "authorize_overlap": true,
  "storage_strategy": "fresh",
  "retention_seconds": 86400,
  "timeout_seconds": 300,
  "ephemeral_paths": ["/tmp/request-spool"],
  "probes": [{
    "name": "business-request",
    "kind": "http",
    "method": "POST",
    "path": "/business/check",
    "body": "{\"dry_run\":true}",
    "expected_status": 200,
    "expected_body_contains": "accepted",
    "timeout_ms": 10000
  }]
}
```

The example route and payload must be replaced with the application's actual
business request. Probes require HTTP success and a body assertion in the first
4 KiB; redirects and stream-event assertions are not accepted. Requests can have
business effects. `ephemeral_paths` is optional and must not overlap protected
system paths, declarative files or persistent mounts. Each path gets an isolated
per-Pod `emptyDir`; it is not guaranteed space independent of the node disk.

```bash
fugue app recovery start <app-id> --file recovery.json
fugue app recovery list <app-id>
fugue app recovery status <app-id> <recovery-id>
fugue app recovery inspect <app-id> <recovery-id>
```

The CLI prints the exact original Pod UID, node, container IDs and restart counts,
plus log and platform-admin execution commands. Access requires the usual app
authorization; starting or releasing additionally requires `app.deploy`.

## Traffic and evidence lifecycle

Before creating the replacement, Fugue positively verifies the source node and
captures resource UIDs. It pauses the old Deployment, orphan-deletes its captured
ReplicaSets, then scales the empty paused Deployment to zero. The original Pod
remains running and cannot be replaced by its former Deployment. Traffic still
selects the original until the replacement passes readiness, exact image and
endpoint checks, and every explicit business probe.

Fugue then commits the replacement identity and traffic policy, redirects internal
Service aliases, and confirms public edge route publication where applicable.
Existing requests may finish on the original. The old Pod leaves new service
traffic and stays available for investigation. The workflow is durable and
resumes across controller leadership changes. Concurrent deploy, scale, restart,
delete, release and traffic mutations are blocked while the hold is active.
Retirement and stale-object cleanup honor the hold.

`retention_seconds` is mandatory: `0` means manual release only; a positive value
(up to 30 days) starts after confirmed cutover. A pre-cutover failure keeps
original traffic selected and requires explicit release. The status reports
`pending`, `preparing`, `probing`, `publishing`, `retained`, `failed`, `releasing`
or `released`. Controller diagnostics include app/recovery/phase identifiers for
blocked steps. `publishing` means cutover intent is committed but confirmation
is still pending; it must not be reported as completed traffic recovery.

```bash
fugue app recovery release <app-id> <recovery-id>
```

Before cutover, release cancels the replacement and restores ordinary source
reconciliation. After cutover, release checks the healthy replacement, deletes
only captured original Pod/Deployment UIDs, and resumes normal reconciliation.
Original PVCs are retained for explicit storage administration; release never
destroys their data. Compatibility Services continue to point at the replacement.

Retention protects against Fugue's automatic replacement and cleanup. It cannot
prevent the application exiting, kubelet restarting its container, node failure,
disk loss or an administrator deleting resources outside this workflow. Evidence
status explicitly reports `intact`, `changed` or `lost`; changed container IDs are
never presented as the original process.

## Capacity and billing

Starting reserves enough tenant capacity for the original plus one independent
replacement, including app storage; backing databases are not duplicated. The
request is rejected if that overlap exceeds the saved capacity envelope or the
existing balance restriction. Fugue does not raise limits, change placement or
purchase capacity. Other admission decisions include this reservation.

Billing responses expose `managed_recovery_reserved` separately from
`managed_committed`, and subtract both from `managed_available`. Reservations
alone are not evidence of consumed resources and do not activate ledger charges.
Existing shared-managed envelope billing remains in effect; this feature adds no
new per-replica tariff. Release removes the recovery reservation.
