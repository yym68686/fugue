# Finite producer input deferral

The optional `fugue.platform.producer.input-deferral/v1` control defers new
producer input attempts while preserving the current signed producer policies.
Without a selected control, it is off. Deploying this implementation does not
select a control, install the separate capture-ticket schema or enable capture,
migration, routing, fencing or retirement.

Publish a validated, signed `policy_snapshot` in the exact
`platform-config-input-deferral` scope through the ordinary shadow artifact
release API. The OpenAPI schema defines its immutable request and derived
idempotency key. Publication requires platform administration and explicit
`producer_capture.control` authority. It binds at most 32 exact current signed
producer policy/release/hash/fencing tuples. No override, changed request,
unexpired replacement or rollback publication can extend the operation. A
matching signed `release` action is terminal and removes this restriction even
if an original policy has since changed.

Every new-input decision reads the durable selection with bounded indexed
queries under a shared transaction barrier; publication takes that same barrier
exclusively. The read uses SQL5s, lock250ms, workmem1MiB and the existing fixed
zero-temp function, including for ordinary database roles. No read allocates a
ticket or release. A failed or unknown read does not admit a new attempt.
The barrier ends when the admission read commits: work admitted before selection
may finish later. This is not fleet coverage, cancellation or drain evidence.

Pending release recovery, timeout/rollback handling and quality observation run
before the new-input boundary. While deferred, quality observation still runs;
neither freshness nor the original serving policy is silently renewed. Expiry
permits the ordinary next attempt, subject to all existing resource and history
guards. Other writers and legacy fallback paths are not fenced by this control.

The signed interval is at most 1800 seconds and uses database wall time. Restart
and replay cannot renew its absolute timestamps. This protocol establishes no
clock synchronization or persistent clock high-water mark: arbitrary clock
rollback can invalidate a strict elapsed-time availability bound. A terminal
release stays off during clock uncertainty. Production selection therefore
requires a concrete availability decision, a reviewed clock assumption and a
fresh full resource budget. Implementation publication alone supplies none of
those authorities. The private Unix producer-runtime observation advertises
`input_deferral_protocol`; it does not assert that a control is selected or that
all workers drained.

The control retains original policy artifact/release references for audit and
retention. Those references do not become runtime historical-input requirements;
the live gate validates its signed exact tuples directly. Existing H/C/D/U,
restore/SHA/pin, reader/writer isolation and physical retirement protections
remain independent.

Tests exercise real HTTP validation/publication, pending safety recovery,
already-admitted work, ordinary PostgreSQL roles, full ordinary PostgreSQL
publication, publication/read contention and fsync-enabled crash restart. The
PostgreSQL fixtures create only private loopback synthetic databases. Their WAL
and timing samples are observations, not production or maximum-cost budgets.
