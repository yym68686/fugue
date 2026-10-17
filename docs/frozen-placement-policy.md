# Reuse a complete policy within a fixed DNS compilation phase

DNS query construction resolves routes for each hostname. It previously
normalized the entire intent and policy, encoded and decoded the full policy,
and validated that policy again for every hostname. Production allocation
profiles showed this work under `captureDirectDNSQueriesWithNodes`, including
`placementRecordRoutes` and `placementHostnameRoutes`. Those profiles measure
allocations, not CPU or the cause of any particular request delay.

The placement reader now normalizes its fixed inputs once, on the first
hostname that needs compilation. `NewFrozenPolicyRouteProjector` owns a JSON
snapshot of the complete policy. It decodes and validates that snapshot once,
at the same point in projection as the ordinary path. The prepared state only
retains the minimum healthy edge count and traffic constraints used by route
projection; the large policy encoding is released after decoding.

This is phase-local computation reuse. It does not cache runtime evidence,
route outputs, authorization, or a serving configuration. The projector rejects
an artifact containing its own policy, preventing accidental policy replacement.
Every route, cache policy and TLS entry is still decoded and validated for each
projection, with the same ordering and output semantics. Invalid fields elsewhere
in the complete policy still reject the projection, even if unrelated to the
hostname being compiled.

Only fixed phases use the placement reader: ordered DNS query construction,
dynamic quality job discovery, one dynamic-quality context reader, and the
ownership check phase of a physical DNS selection update. Each constructs a
fresh reader. Dynamic quality rebuilds the context reader after each hostname's
policy publication, as before. Network observations, evidence validity times,
receipt replay, ownership checks, policy digests and publication gates retain
their existing behavior. The ordinary public route artifact projection remains
available and validates the artifact's own policy on every call.

For H hostnames and a policy of size P, repeated policy decoding/validation
changes from approximately O(H × P) to O(P) within that fixed phase. Normalizing
the full intent also happens once. Hostname route filtering and runtime/owner
checks still run per hostname; overall route selection is not O(1).

Tests compare the new reader with the ordinary path for outputs and error order,
including aliases, shared hostnames, disabled routes, missing/stale origins,
foreign owners, invalid unrelated policy fields, invalid DNS policy and missing
hostnames. Concurrent calls, mutable returned results, detached policy snapshots,
custom JSON, mixed policies and policy/time boundaries are covered. A regression
test first reproduced eight complete policy decodes for eight callers and then
verified one decode. Another regression test preserves the ordinary entry point's
policy lookup after route encoding, including a custom route encoder that adds,
replaces or removes the policy field. This compatibility case was caught and
corrected before release.

A synthetic macOS arm64 benchmark with 64 hostnames and 1,024 DNS policy rules
reported median 238.488 ms → 4.714 ms and 126,223,579 → 2,835,382 allocated bytes
per phase across three one-second repetitions. The benchmark includes initial
normalization, serialization, full policy validation and per-hostname work.
Production workload, publication boundaries and background ownership differ, so
these figures are not an end-to-end latency or CPU guarantee.
