# Control-plane storage lifecycle: audit rollout

This is stage one of lifecycle governance, not an enabled FIFO collector.
`storage-lifecycle.json` is independently published alongside the existing
storage policy. V1 supports only `disabled` and `audit`; **enforce is rejected**.
No business API contract, deletion capability, deployment admission, original
signature, input identity, reference graph or existing retention rule changes.

## Proposed contract, not disposal authority

The versioned proposal records an effective timestamp, a minimum seven-day
history window and three distinct verified successful versions per native
kind/scope, 24 hours for never-used temporary uploads, and 30 days after the
last retention obligation ends for previously used uploads. These are policy
inputs for later implementation, not applied expiry deadlines in this release.
Current/LKG, original TTL, publications, consumers, tasks, pins, promised
rollback, external/offline readers and their complete dependency closures take
precedence. Old data is not retrospectively authorized for deletion.

## What runs

Each API replica takes one bounded sample every five minutes. The transaction
is Repeatable Read and READ ONLY, with SQL2s, lock100ms, zero temporary files,
1MiB work memory, no parallel workers/JIT and a six-second operation deadline.
It reads database allocation, heap/table/index/total allocations of ten fixed
relations, and one primary-key page of at most128 upload metadata rows. The
production page is64. Apps, operations and import-job reference metadata are
independently primary-key paged, each limited to the same page size plus one.
This is only a bounded positive-reference sample: reference pages need not
overlap a source page, so non-matches stay unknown, not unreferenced.
The existing verified fixed zero-temp guard works with ordinary database roles;
missing/changed guards fail closed without granting broader SET authority.
No archive/artifact body, token, filename, SQL error or tenant/upload ID is
exported. The private keyset cursor advances only after the entire sample
succeeds, resets after the last page, and resets when policy changes.

Known positive references from apps, operations and import jobs are reported
as observed references. Everything else stays **unknown**, including old and
apparently unreferenced uploads. External, rollback, session and other
authorities are not yet fully covered. `references_complete`,
`retirement_authorized` and `execution_supported` are false; eligible rows are
zero. Do not interpret a completed page sweep as a single-snapshot inventory.
An oversized/slow query fails closed and retries later without widening limits.

## Observing and validating the rollout

`/metrics` exports cached `fugue_storage_relation_allocated_bytes`, database
allocation, bounded source-page classifications, sample age, failures and
policy status. Scraping and the local runtime snapshot never query PostgreSQL.
Prometheus retains the allocation time series using its existing retention.
Use **max across API replicas**, not sum; do not sum heap/table/total kinds:
table already includes TOAST and total already includes indexes.

The registered `runtime-storage-budget` diagnostic now includes
`storage_lifecycle` under `/v1/snapshots/storage`. Missing/invalid policy disables
the audit immediately; invalid reads are visible as policy errors. Failed reads
retain their previous sample with its original timestamp and policy hash, so
stale information cannot be relabeled as a new successful measurement.

The existing observation cleaner exposes enabled/last-attempt/error metrics;
an attempt is not proof of successful cleanup. The audit does not widen its
closed table allowlist or alter signed heartbeat anchors.

## Gates before enforcement

Run audit for at least72 hours and accumulate7–14 days of comparable-load
allocation measurements. Then implement and validate typed upload obligations,
read leases, reference writer fencing, legally retired operation/consumer roots,
explicit history retirement, full external classification, reactivation
admission and idempotent dependency-ordered physical collection. Current
inventory diagnostics and their digests must remain non-executable.

Unknown references, missing dependencies, budget exhaustion and source errors
must not become disposal authority. Test concurrent publish/read/pin/upload,
cross-scope sharing, stale clients, multiple workers, crash recovery and restore
on an isolated same-version environment before enabling any deletion. Classified
old stock requires separate disposition. Future enforcement needs its own
reviewed policy version; changing the mode in this version cannot enable it.

Free-space thresholds20/15/10 percent are proposed alert inputs, not a hard
byte cap or a silent license to shorten protected history. Per-class byte caps
remain unset until physical accounting and protected-set measurements exist.
Normal DELETE/VACUUM aims at reusable steady-state space, not daily shrinking.

The existing independent observability configuration lane validates and loads
seven owned Prometheus alert rules without replacing Prometheus or its TSDB:
invalid lifecycle policy, failed/stale audits, existing observation-cleanup
failures, and warning/pressure/critical volume headroom. Volume alerts cover all
measured PVCs in a namespace with an active audit policy; they are not falsely
attributed exclusively to the control-plane database. These alerts do not
implement automatic throttling or deletion. Unrelated rule files are preserved,
and disabling the alert setting removes only this publisher's owned rule file.
