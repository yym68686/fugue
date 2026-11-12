# Recovery and quality continuity

`edge-capacity-recovery-continuity-v2` separates a completed recovery transition
from the freshness of its latest capacity review. A complete stable success
streak remains admitted for the explicitly declared `retain_admission_seconds`
(at most 86400 seconds). An unavailable reference or collector neither clears
that transition nor renews its last success. A new subject failure or identity
conflict retracts admission. Partial streaks, changed declarations and changed
code identities must complete recovery again. Current route/TLS readiness is
independently required for serving. `review_overdue` exposes an overdue review;
`retention_expired` exposes exhaustion of the declared continuity bound.

`physical-network-paired-continuity-v7` obtains exact domain/path/TLS witnesses
for measurement candidates independently of the current DNS answer set. Current
inventory and node capacity still constrain candidates. Old policy versions
retain their original receipt replay semantics.

A quality switch requires complete authenticated same-round results from the
same attested public peer network. Explicit connection and incomplete-download
failures remain evidence; absent RTT and optional packet counters remain
unknown. Backend TCP measurements alone cannot establish a client winner.
Each sufficiently sampled cohort must support the candidate, including three
completed five-minute windows under the production policy and its existing
advantage and cooldown requirements. The observation endpoints explain missing
metrics, original metric times, raw score contributions, comparison exclusions,
uncertainty and sustained-window counts.

When comparable observations become unavailable, `retain_order_seconds`
explicitly bounds retaining the previous verified order (at most 86400 seconds).
The original `quality_observed_at` survives subsequent captures, signed fallback
publications and process restarts. Current topology and route readiness still
filter the order. Retention never authorizes a new quality promotion. States
`measured`, `retained` and `learning` distinguish fresh comparison, historical
continuity and an order without sufficient comparable quality evidence.

For bounded active acceptance from the network being optimized, use:

```sh
fugue admin edge quality-measure app.example.test \
  --traffic-class streaming --observer operator-network --rounds 25 --interval 1m
```

The CLI measures identical signed payloads concurrently across eligible edges,
retains complete outcomes, and respects cancellation. It does not publish DNS.
With the production minimum of three records per completed window, one round
every five minutes is insufficient. Slow failed rounds can also reduce window
coverage; inspect the recorded counts before claiming a winner. A network's
result cannot be generalized to every client's network.

The separate resident recovery scheduler and browser measurement page are not
part of these versioned semantics. Code is deployed before separate signed
configuration declarations activate V2/V7; legacy declarations are unchanged.
