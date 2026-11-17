# Build memory history

Heavy Kubernetes builders (Dockerfile, generated static/Workers Dockerfiles,
Nixpacks and Buildpacks) learn memory from completed builds. Lightweight and
controller-local imports retain their existing allocation policy.

The first compatible build uses the existing policy: reserve live node headroom
within the authorized memory ceiling. Later compatible builds reserve the recent
successful peak plus 30% and 128 MiB, rounded up to MiB. The existing builder
minimum and configured ceiling still apply. Requests and limits equal the actual
reservation; the scheduler waits when that budget is unavailable rather than
silently shrinking it or consuming all remaining memory.

Each build stage records its cgroup high-water mark before exiting, including
short init stages, descendants and file cache. Sequential init stages use their
maximum; concurrent ordinary containers use the sum of their peaks as a
conservative bound. Buildpacks nests Docker containers under the builder's
verified memory controller. Unsupported controllers, missing stages, restarts,
OOMs, failed builds, reused/stale Pods and duplicate Pods cannot establish a
successful peak. Missing data does not mean zero memory.

History is tenant/app scoped and keyed by source configuration and executed
toolchain/generated configuration. Ordinary commits, upload hashes, destination
images and credentials are excluded. A changed explicit Dockerfile recipe is a
new profile. Each app retains at most 16 profiles, each with the last 8 successful
Pod observations within 30 days. The largest retained peak determines the budget.
UID deduplication and checkpoint transactions handle concurrent controller
replicas. Read/write failures are best effort and do not fail otherwise valid
builds. Unavailable history falls back to the first-build allocation policy.

Existing durable OOM admission still requires a strictly larger actual limit
after a confirmed OOM. Underestimated predictions grow by approximately 50% per
retry; the last existing bounded OOM retry uses the original live-headroom policy
to recover from a large source change. Neither history nor retries authorize a
higher ceiling, new nodes or additional serving replicas.

Build-attempt evidence exposes `peak_memory_bytes`, `memory_peak_complete` and
`memory_allocation_source` (`cold_start`, `history`, `oom_retry`). Missing peak
evidence is explicitly reported as `build_memory_peak`. Existing logs and
operation evidence remain available; this optimization does not change billing.

The CI prepush lane verifies real container peaks, Kaniko's shell, nonzero exits,
OOM attribution and nested Docker containment on its disposable Docker host.
No production workload rebuild or restart is needed to enable this feature.
