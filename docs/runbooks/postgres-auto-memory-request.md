# Automatic PostgreSQL memory request resize

Managed PostgreSQL databases default to automatic **memory request only**
in-place resize. This controller policy is independent of app right-sizing mode
and the CPU enrollment list. The global in-place resize gate and each memory
request direction gate must still be open. The Helm default is enabled; the
kill switch is `controller.managedPostgresInPlaceResize.autoMemoryRequestResizeEnabled`
or `FUGUE_MANAGED_POSTGRES_AUTO_MEMORY_REQUEST_RESIZE_ENABLED=false`.

The initial scope is single-instance app-owned databases and single-instance
standalone databases exclusively bound to one app. Multi-instance databases
are excluded: the existing executor resizes the current primary, not every
replica, so this policy must not pretend a replica-wide target has converged.
Existing database lifecycle admission still refuses shared,
unbound, suspended, or concurrently mutated databases. No synthetic consumer,
new replica, replacement Pod, storage change, or maintenance rollout is created.

## Decision policy

- Use per-replica memory usage samples from the preceding 168 hours, at least
  12 distinct observations, and a latest observation no older than 15 minutes.
- Discard negative, missing, future, and previous-process samples. The current
  primary must have an exact Ready Cluster, Pod and running process identity.
- Reuse PostgreSQL's existing P99 x 1.5 recommendation, 16Mi rounding and 256Mi
  workload floor, plus a peak x 1.2 safety guard. There is no extra 512Mi floor.
- Changes of at least 5% are eligible. A downscale releases at most 25% per step,
  requires 48 hours of continuous process uptime (covering OOM restarts), and
  has a one-hour cooldown after a verified automatic memory resize. Upscales
  are not delayed by that downscale cooldown. Failed memory operations back off
  for six hours.
- Preserve the exact live CPU request, CPU limit (including unlimited), and
  memory limit. A recommendation above the memory limit is blocked and logged;
  it never silently authorizes a higher limit. Existing billing and resource
  reservation admission applies to request increases.

## Execution and evidence

The scheduler only reads Kubernetes and queues a `database_resize` operation.
The system actor `fugue-controller/postgres-memory-request/<identity-hash>/<source-MiB>`
binds the intent to the Cluster UID, PostgreSQL system ID, Pod UID, process start
and restart count, namespace, and initial request. Every executor observation
revalidates this identity and the unchanged CPU/limit envelope. Unapplied
recommendations expire after 15 minutes. A non-Ready node blocks execution;
node MemoryPressure blocks downscale.

The existing `/resize` executor verifies discovery/RBAC, no-restart resize
policy, unchanged process/Pod identity, SQL probes, resource convergence and
effective Pod requests. An init-container or Pod-level request that prevents
real scheduling-capacity recovery remains a visible blocked operation. Kubernetes
QoS restrictions are not bypassed. Failure never falls back to Pod deletion,
restart, eviction or a CNPG template update. Persisted `runtime_resources` are
updated only after verified completion; bootstrap `resources` stay unchanged.

Verify completed operations and their `postgres_resize_*` evidence, desired
and actual Pod resources, UID, restart count and container start time. Merely
seeing the enabled environment flag is not proof that a database was resized.
Controller logs identify queued targets and pre-queue blockers, including missing
identity or recommendations that require explicit limit authorization.
