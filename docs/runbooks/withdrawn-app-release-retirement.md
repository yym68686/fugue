# Withdrawn application revision retirement

Failed independent candidates remain owned release resources until traffic
withdrawal, retention, canonical Service references and drain evidence permit
retirement. Background retirement does not re-run the serving application's
business health gate: that gate still applies to foreground promotion and
canonical replacement. A degraded stable must not indefinitely retain revisions
that no longer receive traffic.

The active leader runs an independent retirement worker every five seconds;
ticks coalesce during a sweep. Apps and the three resource classes retain fair
resume positions. Scheduled workloads receive four queue entries per one pending
cancellation and one retired-resource cleanup, so resource recovery has priority
without starving cancellation or garbage collection. Each app has at most 100
in-flight retirement attempts, a 50-second deadline per attempt and a 60-second
sweep budget. Shutdown cancels and joins all attempts before leadership handoff.
Ordinary ManagedApp reconciliation does not synchronously wait for these sweeps.

Discovery batches the initial unfiltered Pod/ReplicaSet ownership inventory and
indexes owner UIDs. All observations after drain read fresh, unfiltered ownership
metadata; only related Pods need their full executable state fetched, fenced by
the metadata UID/resourceVersion. Incomplete or duplicate inventories are not
drain evidence. Already absent retired workloads do not consume attempt slots.
Background traffic checks inspect the complete current edge publication proof
once and retry later when it is unavailable; foreground promotion still waits
for publication convergence. No positive proof is inferred from a timeout.

Application readiness is required for promotion and canonical traffic targets,
not for draining an already withdrawn revision. An unhealthy withdrawn app can
retire only when its complete replica/container identity remains unchanged and
a Ready drain agent positively observes the full zero-connection quiet window.
Missing container identity, agent failure, restarts, Pod replacement and ownership
changes retain the workload. Failed running/stopped/unstarted observations retain
their individual reasons in diagnostics rather than replacing the first error
with the final fallback error.

For a failed candidate whose Pods were never scheduled, the controller verifies
the complete owned Pod set and immutable Deployment/Service identity twice, then
sets only that Deployment's replica count to zero with UID/resourceVersion
preconditions. Conflicts require a fresh observation. Retirement waits for
Deployment and ReplicaSet zero-replica convergence and the absence of owned Pods;
it does not fabricate container runtime identities for unscheduled Pods.

Automatic image tracking waits for failed or retired revision resources to be
removed before queuing another import, including when a new digest appears.
Repeated failures of the same digest use exponential retry delay from five
minutes up to one hour, measured after the latest terminal failure. Successful
deployment breaks the consecutive failure sequence. Existing previous rollback
targets and the serving stable do not block tracking.

Aborting an independent, unpromoted bound candidate restores traffic without
reapplying a canonical spec the candidate never changed. When desired projections
advanced past a retained canonical workload, recovery requires the controller's
recorded ready key, the live template key and an exact same-runtime historical
snapshot. Previously ready and promoted retired snapshots remain usable for this
exact baseline proof without reactivating their release rows. Recovery only
establishes a baseline; readiness, storage, capacity and
canonical drain checks continue to apply.

Older canonical Service selectors can include presentation and project labels.
The handoff accepts them only when all stable app/tenant/workload isolation labels
are present and every selector term matches the verified source template. The
same UID/resourceVersion selector CAS, endpoint convergence and positive source
drain observations apply before the canonical template can change.

Retirement keeps 100 task slots while limiting heavy namespace/publication read
phases to four at a time. Each read remains independent and fresh; this is
backpressure, not shared deletion evidence. The controller database pool permits
16 physical connections, with excess queries waiting under their own deadlines.
Absent an explicit `GOMEMLIMIT`, the controller uses a 512 MiB Go memory target
to trigger collection before its 768 MiB container limit. Observe connection
errors, memory and restarts as well as the task concurrency count.

After a controller release, verify:

- The running controller source commit and image match the Actions artifact.
- Withdrawn Deployments decrease and their release rows become retired.
- Unscheduled candidates converge to zero replicas before deletion.
- Image tracking records `retry_suppressed` while cleanup/backoff is pending.
- Current stable traffic remains authoritative; names containing `candidate`
  can identify the current stable and must never be bulk-deleted by name.

Paused retirement due to active connections, retained rollback windows, changed
resource identities, unconfirmed publication or API failures remains expected.
Inspect audit `observer_error` and the current canonical Service selector before
changing intent. An application 5xx regression is investigated separately from
whether withdrawn resources can safely retire.
