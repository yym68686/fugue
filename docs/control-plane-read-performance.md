# Control-plane artifact read allocation

Consumer assignment, discovery and DNS observation paths repeatedly validate
large signed artifacts. The policy projection previously serialized the whole
child merely to read its policy, then decoded that policy again for cohort
validation. The projection now reads only the policy and shares that typed
value between membership and cohort validation. Its work depends on policy
size rather than route, DNS proof or certificate payload size.

PostgreSQL artifact scans use a process-local cache of decoded JSON content.
The cache is keyed by SHA-256 of the bytes actually returned by the database,
not the artifact's claimed hash. Each read still fetches the current database
row, including status, metadata and provenance. Existing integrity, trust,
fence, active publication and expected-consumer checks remain in their original
paths. No authorization decision or failed decode is cached.

The cache retains at most 64 entries and 64 MiB of conservatively accounted
decoded content. Documents smaller than 1 KiB or larger than 8 MiB bypass it.
Concurrent misses for identical bytes share a decode. Every caller receives
independent nested maps and slices, including on a miss; only immutable scalar
values are shared. Eviction needs no invalidation signal and cannot change a
caller's already returned content. Database I/O and full signature/hash
verification still cost work on every request.

Serving-only assignment resolution also reuses its existing request-scoped
artifact reader during route projection. Active release and expected membership
are still reread. The reader is not used across requests or a network probe.

The local Live Diagnostics operations provider now exposes fixed stage counts,
summed durations and maxima for assignment, artifact, route source, discovery,
DNS observation, node policy and Agent selection. Fixed failure counters
distinguish cell authorization failure, observation failure, changed authority,
expired evidence and unmet grant constraints. It does not record identities or
credentials. Nested stage durations overlap and are not CPU percentages.

## Validation

`make test` is the full repository check. Focused race checks cover cache
ownership, byte/entry bounds, current database status, unchanged claimed hashes
with changed content, failed decoding, request scope and concurrent diagnostic
counters:

```
go test -race ./internal/store ./internal/platformconfig ./internal/api \
  -run 'Test(PlatformContentCache|PlatformArtifactScanDoes|TrafficProjection|OperationObservations|ConsumerArtifact)'
go test ./internal/platformconfig ./internal/store -run '^$' \
  -bench 'Benchmark(TrafficCohortProjection|PlatformContentDecode)$' -benchmem
```

On an Apple M1 Pro, three synthetic benchmark runs with a 4 MiB payload gave
median policy projection time of 16.52 ms before and 0.020 ms after; median
allocation fell from 7.31 MB to 10.4 KB. The payload is deliberately outside
the policy to verify that unrelated payload size no longer dominates this
projection. A 4 MiB string document decoded in 17.13 ms without caching and
2.01 ms with a warm cache. Actual nested production documents have different
clone costs, so these figures are not an end-to-end speedup or a promise of
90% lower CPU.

Production acceptance must compare request volume, CPU, allocation rate,
latency, 5xx, restarts and signed consumer convergence after the declarative
release. Short diagnostic windows and rollout intervals must be identified
separately from steady traffic. Configuration artifacts and positive LKG are
independent of this code release.
