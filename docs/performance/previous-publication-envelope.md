# Previous publication decoding

`DecodePreviousTrafficPublication` reads `previous_traffic_publication` and
`cell_dns_source`. It previously encoded and decoded the entire DNS artifact,
including unrelated records, readiness plans and embedded Cell publications.
Passive production goroutine samples captured that complete encoder inside
`VerifyDNSArtifact` while DNS publication admission held its transaction locks.
This proves unnecessary work in that path, not a common cause for every Agent
failure, DNS preflight rejection or log collection error.

For native JSON trees, inspect at most 262,144 values and 256 levels before
encoding only the two decoder fields. The inspection does not scan string
bodies. Include all case aliases so the standard decoder retains sorted-key
merge and null behavior. Custom encoders, unsupported values, nonfinite numbers,
cycles and over-budget trees retain the original complete encoder, including
errors and effects in ignored fields. The predecessor still receives the same
content, composition, lineage and reference validation. Returned data remains
independent of the input. Signature checks and mutable release, fence and trust
checks remain separate.

The decoder's work changes from O(complete encoded bytes) to
O(native tree values + selected encoded bytes), with bounded inspection and the
original fallback. This does not change the cost of hashing the complete
artifact for integrity elsewhere.

`BenchmarkPreviousPublicationEnvelope` compares the original decoder against
the optimized path using synthetic signed publications. One Apple M1 Pro run
with a retained predecessor and 2 MiB of ignored content measured 9.66 ms and
about 4.37 MB allocated for the original, versus 0.82 ms and about 0.30 MB for
the field view. These are local measurements, not production latency promises.

Compatibility tests compare complete returned values and errors for valid and
malformed publications, aliases, nulls, ignored invalid values and custom JSON
encoders. Mutation tests check ownership; bounded fuzzing compares native trees
against the independent complete-envelope decoder.
