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


A later window during private promotion verification measured 178.2 MiB RSS and
796.51 MiB of allocations in 60 seconds, with no OOM. Observation load is higher
during promotion, so 148 MiB must not be treated as the stable peak bound.

The next optimization reuses immutable artifact content storage across retained
and newly downloaded states only after complete deep equality. It does not skip
any download, input validation or trust check. Fresh envelopes, signatures,
release metadata, timestamps and selections remain distinct. A 4,096-record
synthetic benchmark retained about 1.98 MB for the duplicate graph before reuse
and under 1 KB after; the equality walk adds CPU work and download allocations
still occur. This is a retained-heap result, not an online RSS guarantee.


Candidate validation now returns its strictly decoded payload to the enclosing
release validation call, eliminating another complete decode in that same call.
No result crosses a request boundary; unknown nested fields remain rejected. A
synthetic 512-record full release verification benchmark decreased from median
13,167,888 to 10,799,736 allocated bytes per operation (about 18%). This is an
allocation benchmark; online RSS must still be measured after release.


The retained DNS artifact's decoded JSON tree now shares identical compound
values after full signature and semantic validation and before publication to
serving readers. The operation walks children once, hashes typed boundaries and
sorted keys, and checks complete value equality before sharing. A per-call index
has a hard 65,536-entry bound and is discarded afterward. It retains no authority
or runtime facts. Unsupported typed values bypass sharing; normal JSON encoding
and artifact digests remain byte-for-byte unchanged. Already shared serving
content is never mutated during a refresh. New content compacts once; unchanged
refreshes reuse the prior immutable graph.

A synthetic 4,096-record fixture retained 5.19 MB without compaction versus
1.75 MB after. Compaction adds about 11 ms and 2.1 MB temporary allocations to
initial decoding in that fixture. It is not an RSS result. This targets retained
heap rather than claiming to eliminate the transport/decode allocation peak.


Production soft-budget comparison on the same private German DNS role, with
unchanged signed full configuration and 256 MiB container hard limit:

| Go soft budget | RSS peak | CPU core-seconds / 60 seconds | OOM |
| --- | ---: | ---: | ---: |
| Default | 119.9 MiB | 8.60 | 0 |
| 48 MiB | 62.4 MiB | 11.63 | 0 |
| 32 MiB | 56.2 MiB | 56.31 | 0 |

32 MiB saved only another 6.2 MiB but increased CPU about 4.8 times relative to
48 MiB. The selected budget is 48 MiB for both private readers. The kernel hard
limit remains 256 MiB; a soft budget can be exceeded during required work. These
are bounded captures, not a guarantee of the all-time peak. A tenfold RSS target
of about 21 MiB has not been achieved and must not be claimed from heap sizes.

Final 48 MiB rollout measurements on both private readers used complete 60-second
Live Diagnostics captures. The German reader peaked at 61.4 MiB RSS and the US
reader at 63.3 MiB; CPU consumption was 17.27 and 27.82 core-seconds respectively.
Both had zero OOM events in the window and zero container restarts at readback.
Both returned a positive answer for the affected hostname after startup recovery.
Relative to the 218.8 MiB baseline, these bounded peaks are about 71–72% lower.
The public DNS transport still selects the legacy readers; this measurement does
not establish public recovery or a tenfold reduction.
