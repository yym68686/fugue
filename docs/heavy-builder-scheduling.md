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

Confirmed OOM limits are persisted per tenant, app and complete build input
fingerprint, across operations and controller restarts. The fingerprint includes
the resolved commit or archive hash, build settings, environment and injected
files; credentials, output image metadata and resource policy are excluded.
Changing the policy alone cannot erase a previous OOM.

Let `M` be the highest confirmed OOM container limit for those inputs and `A` the
final reserved Pod limit after MiB rounding. Admission requires `A > M`; equal or
smaller allocations are rejected with `builder_memory_not_increased`. The next
minimum is the first whole MiB above `M`, without fixed 8/12/16 GiB retry tiers.
The scheduler rechecks the durable limit while holding the node reservation
lock, and continues to enforce the existing tenant memory ceiling.

GitHub sync checks this rule before creating another import operation. It reports
the blocked source-sync status, periodically rechecks capacity, and can resume
when capacity strictly increases. Manual rebuilds use the same execution guard.
A new commit, archive or other build input receives a separate history. On first
use after upgrade, retained structured build-attempt evidence seeds the history.
New OOM facts require the Kubernetes `OOMKilled` termination reason and the
executed allocation; exit 137 or application log text alone is insufficient.
History read/write failures stop the build instead of bypassing admission.
The resolved light/heavy profile is saved with the OOM fact, so `auto` builds
check the same resource envelope when capacity recovers. Legacy `auto` records
without a profile may queue one operation to resolve it from source; the profile
is persisted before admission and the historical memory guard still applies.

Operation diagnosis distinguishes insufficient resources, lock contention,
unavailable statistics, and node eligibility failures. Per-node reasons include
explicit build-disable labels, and inspection exposes existing Pod requests,
available resources, and the prospective allocation. Temporary capacity shortages
wait within the existing selection window (600 seconds by default, polling every
5 seconds), including after an OOM or storage retry. Only an incompatible policy
ceiling is immediately terminal; the existence of OOM history alone cannot
classify a CPU, storage or placement shortage as a memory policy refusal. An
exhausted selection window does not start another job retry window.

Pre-Pod placement failures persist the effective retry policy, strict OOM floor,
storage demand and the node inputs actually evaluated. Operation diagnosis uses
the latest attempt's persisted snapshot and its `observed_at`, without replacing
it with current state or a guessed `auto` profile. Older records without that
snapshot explicitly report missing evidence. Diagnostic policy and raw build
details remain restricted to platform administrators. Standalone live inspections
do not create reservations. Failed builds preserve the currently serving app.
