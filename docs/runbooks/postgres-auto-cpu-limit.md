# Automatic PostgreSQL CPU limit upscale

This policy is independent of CPU request downscale. It is opt-in for an
app-owned, single-instance managed PostgreSQL database with automatic
right-sizing allowed. Enroll the exact service, namespace, CNPG Cluster name
and UID, PostgreSQL system ID, and authorized CPU ceiling:

```json
[
  {
    "service_id": "example-service-id",
    "namespace": "example-namespace",
    "cluster": "example-cluster",
    "cluster_uid": "example-uid",
    "system_id": "123",
    "max_limit_millicores": 500
  }
]
```

Set `controller.managedPostgresInPlaceResize.autoCPULimitUpscaleTargets` in
Helm, or `FUGUE_MANAGED_POSTGRES_AUTO_CPU_LIMIT_UPSCALE_TARGETS_JSON` in the
declarative controller release. Both the global in-place resize switch and the
CPU limit upscale gate must be on. An empty enrollment disables automation;
removing an entry does not undo a resize that already completed.

The newest two adjacent five-minute samples (3–8 minutes apart, newest no
more than 8 minutes old) must reach at least 90% of the current finite limit.
The exact PostgreSQL container must still show at least 50% throttled CFS
periods over a fresh cgroup v2 window. The controller repeats the throttling
check immediately before applying the resize. Missing telemetry, a quota
mismatch, an unhealthy primary or changed process identity prevents mutation.
Each successful step at most doubles the limit and never exceeds the enrolled
ceiling. There is a 10-minute success cooldown and a 30-minute retry backoff
after failed or canceled attempts. CPU request, memory, replica count and
placement do not change; normal actual CPU metering still applies.

The controller uses a fenced `database-resize` operation, verifies a local
temporary write and rollback before and after the Kubernetes Pod `/resize`,
and checks resource convergence, Pod UID and container restart invariants.
It does not widen tenant network access or fall back to a restart. This
adjusts the running Pod, not the CNPG bootstrap template. A replacement Pod
can start at the bootstrap limit. Existing runtime-resource drift protection
requires operator review before applying historical limits to a replacement,
and prevents this automation from silently overwriting that stored target.

For rollout verification, check the exact controller revision and enrollment
metric, then look for a completed `database-resize` from
`fugue-controller/postgres-cpu-limit-upscale/` with verified evidence and
compare live desired/actual Pod CPU limits, UID and restart count. If the
controller is live but the pressure criteria have not been met, do not report
the database limit as adjusted.
