# Isolated compiler input copy

This lane copies one explicitly authorized, exact v1/v2 input into the platform
archive. It does not change the source manifest, source chunks or business read
route, and grants no routing, hot-copy, pin, GC, fencing or retirement authority.
It does not need the complete hot dependency closure. Copying does not reclaim
PostgreSQL relation or filesystem space.

Ship the native `archive-copy` command first through the normal API release.
After verifying its actual serving source and immutable image, commit a separate
`operation.json` matching those identities. The sole production CI validates and
previews that declaration before reserving an immutable, declaration-keyed
attempt receipt. Existing receipts or archive ledger records forbid replay,
including ambiguous, failed or partial attempts. There is no automatic retry.

The worker uses one existing node, one CPU, 512MiB memory, 256MiB ephemeral storage,
one database connection and a 600-second Job deadline. It runs at low priority,
without a service-account token or platform encryption/signing keys. The trusted
runner supplies only the existing application database session and separate
archive reader/writer sessions through an owned, temporary immutable Secret.
Only the contract file is mounted; credentials are not arguments or log output.

The declaration pins the source digest, format, raw length, backend fingerprint,
serving source/image, node UID and primary/cluster UIDs. It caps PUT and GET
payload bytes and client operation counts, aggregate source/PUT/GET payload rate,
and native duration. The fresh-copy request counts must be exactly one PUT and
two GETs per segment. All SDK operations use a single attempt. Payload accounting
does not claim to measure network headers or physical storage-device IO. Source
reads use one chunk per short transaction; SQL and lock waits are bounded to five
seconds and 250ms, respectively. The existing original source SHA and both remote
verification passes remain mandatory.

Before and during execution, the runner checks actual serving identities and
endpoints, public readiness, node CPU/memory/filesystem reserves, primary health
and restarts, free-space loss, cluster WAL growth, database concurrency and API
writer-error deltas against an explicit baseline. Existing deadline errors are
reported separately from the declared incremental tolerance. A new category,
fallback, exceeded bound or uncertain observation stops only this worker. The
512MiB hard stop/1GiB warning remains; this lane requires at least 3GiB free and
the node floor remains 25% plus 16GiB plus 192MiB.

The result artifact contains exact execution identities, native payload counts,
verified original bytes, elapsed time, durable ledger confirmation, source/route
invariants, bounded impact samples and cleanup receipts. Only exact owned Job,
Pod, Secret and PriorityClass identities are cleaned up; the immutable attempt
receipt, original rows, R2 objects and archive ledger remain. A successful pilot
supports a separately reviewed next-batch budget, not an unlimited migration or
an assertion of reclaimed capacity.
