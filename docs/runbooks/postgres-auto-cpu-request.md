# Automatic PostgreSQL CPU request downscale

The controller queues `database_resize` operations for explicitly enrolled
databases during ManagedApp reconciliation. It uses the existing PostgreSQL
request recommendation (P75 over the configured right-sizing window, default
168 hours / 12 samples, rounded up to 5m with a 1m floor). The last CPU sample
must be at most 15 minutes old. The app must allow automatic right-sizing.
Changes below 5m or 5% are skipped; failed automatic operations back off for
six hours. The operation preserves the live memory requests and limits and CPU
limit, including when bootstrap resources differ from live Pod resources.

Configure `controller.managedPostgresInPlaceResize.autoCPURequestDownscaleTargets`
in Helm, or `FUGUE_MANAGED_POSTGRES_AUTO_CPU_REQUEST_DOWNSCALE_TARGETS_JSON` in
the controller Deployment. Each JSON entry requires `service_id`, `namespace`,
`cluster`, `cluster_uid`, and `system_id`. Empty or invalid enrollment disables
automatic downscale. Enrollment grants the CPU request downscale direction to
that exact database; the global direction gate can remain closed. Production
enrollment is stored in `deploy/releases/controller/resources.json` and must
be changed through the declarative CI release path.

Enrollment is checked before queueing and on every execution observation.
The queued system actor includes the Cluster UID and PostgreSQL system ID,
so reusing a name or changing enrollment cannot apply an old recommendation
to a replacement database. Non-CPU envelope drift blocks automatic operations.

App-owned databases and standalone databases exclusively bound to one app are
supported. Shared databases and databases owned by a different app are refused.
Store lifecycle locking and backup/import/restore admission remain in force.
Execution uses the existing `/resize` operation with SQL probes and Pod UID,
container start time, restart, resource convergence, and init-floor checks.
There is no delete, restart, or rolling replacement fallback. Runtime resource
targets persist only after verified completion; bootstrap resources stay intact.

Verify the system actor and completed `database_resize` operation, then compare
Pod desired and actual requests, UID, restart count, and SQL probe evidence.
Do not infer a successful adjustment merely from the enrollment environment.
