# Unavailable dynamic DNS selections

A declared A/AAAA record can satisfy its route-proof quorum while every ready
candidate is excluded by its signed selector. For example, the allocated edge
can temporarily lose its proof while the remaining measured edge is stopped by
the business policy. Previously this path returned NOERROR with an empty answer
and an SOA, representing NODATA despite the declared record still existing. It
also skipped the existing verified per-record transition, which runs on SERVFAIL.

The authoritative platform answer now returns SERVFAIL when dynamic candidate
selection produces no wire records. It preserves the current selection audit,
original proof/lease deadlines, stopped candidates and signed allocation. An
already-admitted equivalent per-record successor may still answer through the
existing transition path; an unauthorized or expired successor cannot. Missing
record types, expired static TXT values, NXDOMAIN and static answers retain
their established semantics.

Synthetic regression tests first reproduce empty NOERROR and skipped successor
recovery. They cover failure receipts and exact replay, successful verified
transition, missing transition authority, expired successor, policy immutability
and real NODATA. Historical receipts must be replayed with their original code
release, as documented in dns-decision-receipts.md; their recorded answers are
never rewritten to match corrected behavior.

Deployment first updates the inactive DNS E slots using exact live predecessor
fences. Public Service selectors remain on the positive F slots until the new
processes are ready and direct observations pass. Transport handoff is a later
independent declaration and does not require another code build.
