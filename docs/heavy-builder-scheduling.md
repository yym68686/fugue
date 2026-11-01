# Heavy builder scheduling

`heavy` selects the eligible build node with the most available memory. It does
not require six GiB of free memory. The initial admission minimum is 512 MiB,
independent of the legacy default container limit. `HeavyMemoryMinimum` can raise
this floor; normalization never lowers it below 512 MiB.

For each resource the scheduler calculates:

```text
physical headroom = allocatable - measured usage - safety buffer
Kubernetes headroom = allocatable - existing Pod requests
available = min(physical headroom, Kubernetes headroom) - active reservations
```

Temporary storage is also constrained by filesystem free space. Pod requests
include nonterminal assigned Pods, init containers, restartable init sidecars,
and Pod overhead. Physical safety and Kubernetes admission are separate ceilings;
the safety buffer is not deducted again from Pod request headroom.

After filtering node permissions, readiness, taints, required labels and resource
minimums, heavy candidates are ordered by available memory. The scheduler locks
a candidate, refreshes statistics and reservations, and checks capacity again.
If the lock is busy or refreshed capacity no longer fits, another eligible
candidate can be tried. The exact selected node is reserved and pinned in Pod
affinity.

The build receives `min(available memory, HeavyMemoryLimitCeiling)`, rounded down
to whole MiB. The default ceiling remains 16 GiB. Its memory request and limit
both equal the reservation, including source preparation init containers. CPU
and temporary storage retain their existing budgets. A confirmed OOM retries
with a minimum above the previous actual allocation; an OOM at the policy ceiling
is terminal. An admission failure never claims a build Pod ran.

Operation diagnosis distinguishes insufficient resources, lock contention,
unavailable statistics, and node eligibility failures. Per-node reasons include
explicit build-disable labels, and inspection exposes existing Pod requests,
available resources, and the prospective allocation. `observed_at` identifies a
live inspection; it does not reconstruct historical failure state. Inspections
do not create reservations. Failed builds preserve the currently serving app.
