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

## Canonical hash reuse

After removing full-child policy decoding, production CPU sampling still
attributed about 24% of samples to artifact integrity, dominated by canonical
JSON encoding. The canonical hash implementation now memoizes successful
hashes for up to 256 complete content fingerprints. It stores only fingerprints
and digest strings, not content or integrity decisions.

Each call still walks and hashes every actual JSON value. A domain-separated,
length-delimited encoding with distinct type tags, sorted object keys and
ordered arrays fingerprints JSON decoder values without reflection or string
escaping. A mutation anywhere in the tree changes this key. The returned
artifact hash always comes from the original `encoding/json` representation;
unsupported/custom Go types, deep structures and small contents use that
implementation directly. Failed encoding is not cached. Signature, schema,
generation, current key revocation and release status checks still run.

Tests cover warm-cache content mutation, array order, claimed hash changes,
schema/signature changes, key revocation, draft status, concurrent calls,
eviction, nil containers, invalid numbers, escaping, deep values and cycles.
A 15-second fuzz run compared 1.49 million inputs against canonical JSON.
Synthetic structured-content hashing fell from 4.75 ms to 1.75 ms, with
allocation falling from roughly 3.6 MB to 128 KB per operation. Small-content
fallback adds about 0.1 microseconds in the benchmark; production measurements
remain the acceptance criterion.

Agent and DNS source preparation now share artifact reads only within their
pre-probe request phase. No reader survives the network probe. Additional fixed
diagnostic counters separate capacity identity, sample age/clock, pressure,
route transport, route binding and minimum evidence lease failures without
changing the grant API or weakening acceptance rules.

Large policy projection checks also memoize up to 256 successful results.
Keys include the actual policy JSON, complete parent content and both artifact
envelopes, so changing policy, topology, cohorts, scopes, generation or metadata
forces validation again. Errors and small policies bypass the cache. This is a
pure projection cache, not a release or authorization cache. A synthetic policy
with 64 cohorts of 64 groups took 9–11 ms without memoization and 0.86 ms with
a warm cache; allocation fell from about 4.15 MB to 0.47 MB. Tests warm the cache
before substituting each binding input and verify rejection and boundedness.

## Narrow DNS transport observations

Public DNS backend observations list Pods by both the authenticated node and
ServiceAccount. Pod UID, namespace, service account, identity policy, readiness,
Service selector, EndpointSlice ownership and the final resource-version
recheck are unchanged. Private candidate validation retains the complete node
Pod list because it must also examine the public backend under another account.
The live public node queries fell from 15 Pods / about 389 KB to 1 Pod / about
19 KB. This changes query volume, not the freshness or authority of the result.
