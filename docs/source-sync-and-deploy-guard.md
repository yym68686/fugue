# Source checks and deployment history guards

GitHub source checks run in one serial worker per controller leadership epoch.
The worker starts before the full ManagedApp reconciliation, runs immediately,
and then uses the existing GitHub sync interval (one minute by default). Slow
reconciliation no longer delays that timer. Checks never overlap, ticks coalesce,
and leadership cancellation joins the worker. Per-repository and database
requests keep their existing time bounds. A slow repository can still lengthen
one source-check pass; the interval is not a per-app latency guarantee.

Every completed repository check records its actual completion time in the
existing `source_sync.last_checked_at`; success also advances `last_success_at`.
A failure retains the preceding successful timestamp. The check writes only
`status_json.source_sync`, without rewriting app configuration, source ownership,
or the app's configuration update timestamp. Comparing the source and previous
check state rejects responses from before a source rebind, manual resume, or
newer observation. Cancellation is not a repository failure. A new HEAD uses a
fresh configuration snapshot before queuing its build.

The timestamp is a persisted runtime fact, not proof that a check is currently
running. A suspended repository, active app operation, stopped controller, or
slow repository check can leave it old; callers should interpret its age along
with phase and operation state. No new API fields or schema migration are needed.

The stale-deployment guard retains the same protection against applying an old
snapshot after a newer completed deployment. It reads only the same tenant/app's
completed deployments after the operation's creation time, ordered newest first,
in pages of 32. It loads just ID, desired state, sources, and completion time;
configuration-base snapshots and unrelated historical operation details are not
transferred. Pagination uses `(completed_at, id)` so ties and matching candidates
beyond the first page are preserved. The first matching candidate is the newest.
An empty candidate result requires no historical configuration transfer.

A page read gets one retry after 250ms for a timeout or broken connection.
Permanent errors fail immediately, and parent cancellation stops retries. An
unreadable guard never grants permission to deploy. Existing indexes support the
filtered lookup; this change requires no live index build or table migration.

Regression coverage includes healthy timestamp advancement, stale source facts,
source rebind during a check, source checks during a blocked full reconciliation,
worker cancellation/non-overlap, transient versus permanent query failures,
cross-page matching, equal completion timestamps, and real PostgreSQL reads and
state-only updates. Shared store code is consumed by the explicitly selected
controller release; serving configuration stays independently recoverable.
