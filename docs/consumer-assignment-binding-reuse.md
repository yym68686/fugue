# Request-scoped consumer binding validation

An assignment request with `serving_only=true` can examine route and TLS
assignments from both full and gray publications. Each selection previously
constructed the complete currently selected route projection, even when all
four candidates referred to the same current source. Selection uses its binding;
it does not return that route table.

Production allocation profiles from two API processes, sampled 60 seconds apart,
reported approximately 2,943 MB and 3,701 MB of allocations. Assignment requests
accounted for about 45% and 51% of those totals, and `ProjectRelease` for about
35% and 34%. Allocation samples are not CPU profiles and do not prove the cause
of an individual request's scheduling delay.

`NewRequestBindingReader` retains one successful binding for one sequential
request. It still builds and validates the full projection on the first matching
input. Subsequent identical inputs return an owned copy of the small binding.
Full route download and execution paths continue to construct complete snapshots.

Every lookup fingerprints both actual content trees with type tags and hashes
all artifact metadata, assignment fields, release fields and current signing
keys. Claimed content hashes, artifact IDs and map pointer identity are
insufficient. Unsupported content types bypass reuse. Failures are not cached;
the reader is never shared across requests. Returned canary group slices are
copied so a caller cannot modify later results.

The API continues to resolve active full/gray lanes, read the latest expected
membership, verify artifact integrity and enforce canary membership on every
selection. It also retains the final publication/fence reread and bounded retry
when publication changes during observation. No SQL authorization read,
signature requirement, group check, network freshness check or positive LKG
behavior is removed. No HTTP contract or serving configuration changes.

The optimization reduces complete projection executions from four to one for
identical inputs. Overall work remains proportional to the content size on each
lookup because actual typed content is fingerprinted each time; this is a
constant-factor reduction, not an O(1) authorization lookup.

Tests cover in-place body and metadata changes, signature/status changes,
assignment/fence/lane/cohort changes, signing-key rotation and in-place
revocation, custom values with identical JSON but different Go semantics,
result ownership, errors and request boundaries. API integration tests remove
and restore topology, revoke trust, and publish a new release while returning
a reused binding; stale assignments remain rejected.

In a synthetic four-check benchmark on macOS arm64, three 300 ms repetitions
with 256 routes gave median time 11.096 ms → 3.166 ms, allocated bytes
9,233,913 → 2,415,414, and allocations 41,449 → 11,598. The benchmark includes
fingerprinting and the first full projection. These results are not a guarantee
of production CPU or end-to-end latency improvement.
