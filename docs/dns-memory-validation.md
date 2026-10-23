# DNS configuration validation memory

Live Diagnostics heap and allocation exports from private DNS executors identify
configuration synchronization as the main allocation path. The captured heap
contained 73.22 MiB of sampled live allocations; generic JSON object decoding
accounted for 62.16%. Cumulative allocations attributed 51.41% to JSON marshaling
and 28.95% to DNS plan validation. These categories overlap. Cumulative allocation
is not retained heap or peak RSS.

Two deterministic validators now keep bounded caches of successful input
fingerprints: DNS Cell plan replay and predecessor composition validation. Each
cache holds at most 64 SHA-256 keys and no artifact, parsed object or readiness
result. Every lookup traverses the complete actual JSON content and complete
artifact envelope, including metadata. Predecessor keys include all four exact
artifacts and the reference. The cache never trusts a claimed content hash alone.
Custom marshalers and non-JSON typed trees use the original validator without a
cache lookup; errors are not cached. Validation order on a miss is unchanged.
Signature checks, key revocation, selected releases, monotonic fences, LKG expiry,
post-probe ledger rechecks, physical observations and quorum remain live checks.
The memoized functions have no clock, network or mutable trust inputs.

After bindings have been derived, the serving payload drops its second decoded
copy of embedded predecessor and Cell publications. The authenticated checkpoint
retains the complete originals. A digest of the complete discarded inputs stays
in the payload so exact release-bridge comparisons remain sensitive to changes
beyond the derived bindings. DNS intents, plan, policy and source approvals stay
available to serving and transition checks.

The synthetic transition benchmark adds 128 static records to neutral routing
fixtures. On an Apple M1 Pro, warm repeated validation previously allocated a
median 2,017,873 B/op and took 5.85 ms. After memoization it allocates 2,608 B/op
and takes 0.282 ms. Cold validation still performs full replay. This local result
does not establish an online RSS reduction. Compare bounded Live Diagnostics
process-memory profiles before and after deployment under the same workload.
The first measured baseline peak was 210.9 MiB RSS; a tenfold target is 21.1 MiB.
Do not claim this target from allocation benchmarks alone.


Initial production comparison after the first optimization used the same private
US DNS executor role, signed configuration and 60-second Live Diagnostics probe.
RSS peak decreased from 218.8 MiB to 148.0 MiB (32.3%); sampled cumulative allocation
in the window decreased from 1,493.39 MiB to 476.08 MiB (68.1%). Neither window had
an OOM. The newer window began about 75 seconds after process start, whereas the
baseline process had been running longer; a later steady-state capture is still
needed. This does not establish a tenfold reduction. The remaining sampled live
heap is about 49.6 MiB and still dominated by decoded JSON object trees.
