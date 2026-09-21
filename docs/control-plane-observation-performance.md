# Control-plane observation performance

Rollout watches start from the resourceVersion returned by the preceding Pod
LIST, including revision and database Pod lists. A missing/zero cursor falls
back to the bounded polling timer. Expired or disconnected watches also wait
for that timer before relisting. Existing Pods therefore do not replay ADDED
events and continually cancel the Deployment/ManagedApp watches. Real ADDED,
MODIFIED and DELETED events still wake the rollout evaluation.

API runtime observations decode minimal typed Kubernetes views directly. They
preserve namespace, generation, container image, endpoint readiness and complete
pagination checks without reconstructing the full object as map -> JSON ->
struct. A successful HTTP status does not mask an empty, truncated, oversized or
trailing response. Kubernetes and durable evidence queries share the caller's
refresh budget. Relevant image observations are fetched in one exact scoped
query and indexed by tenant/app/runtime/image/status; missing/pulling/failed
evidence and original timestamps are retained.

The background metrics and resumable R2 inventory collectors introduced in
`aa09ad20` remain independent of the scrape request. Runtime observation changes
do not renew signed configuration, serving authority, or LKG freshness.

Release history collection retains the transitive closure of immutable execution,
Guardian rollback, monitor and route dependencies. It reads a paginated snapshot
of both managers together, defers while a component is not settled or the
publication inventory changes, and deletes with UID/resourceVersion preconditions.
Canary and monitor health refresh timestamps do not change the inventory's
reference fingerprint; changes to their target record or rollback state still do.
Retention is configured in the Guardian deployment data, independently of
serving configuration: keep at least eight records per kind/scope and every
record younger than six hours, then delete at most 32 unreferenced objects per
five-minute pass. Current/previous authorities, candidates, transition journals,
DesiredRelease, status/LKG and their dependencies are protected irrespective of
age. The protected recovery closure may exceed the history floor; safety is not
traded for a hard object cap. No serving artifact or pointer is rewritten by GC.

Validation includes watch replay/empty-list/410 tests, JSON transport failures,
database cancellation, transitive retention/cycle/concurrent-publication tests,
and the existing rollout, runtime evidence and recovery suites. The synthetic
300-object decode benchmark on arm64 reduced allocated bytes from ~4.55 MB to
~156 KB and time from ~15.9 ms to ~6.3 ms per iteration. These are path-level
measurements, not a promise of the same end-to-end improvement.

```
go test ./internal/controller -run 'Test(PodRolloutWatch|UnboundOrFailedWatch)'
go test ./internal/api -run 'Test(RuntimeObservation|TypedObservation)' -bench BenchmarkRuntimeObservationDecode -benchmem
go test ./internal/store -run TestObservationQueriesHonorCallerBudget
go test ./internal/releaseguardian -run TestArtifact
make test
```

After declarative CI deployment, verify exact component image receipts, Ready
replicas, scrape success/latency, fresh metric snapshot timestamps, API allocation
profiles and request rate trends. Traffic and concurrent releases affect those
rates. Observe retention logs for protected/deleted/deferred counts; do not force
cleanup during a failed or unfinished release to meet a size target.

ReplicaSet revision-name observations and drain ownership enumeration request
`PartialObjectMetadataList`. They preserve the original selector (including an
unfiltered namespace list for drain ownership), latest-read semantics, owner
references, revision ordering and lifecycle checks. A 406 retries the identical
read using full JSON. Operation evidence that consumes ReplicaSet status still
uses the full representation. This reduces serialization and response bytes
without weakening the set of Pods checked before draining a release.

Route source and convergence binding reads reuse the existing invocation-local
artifact reader. Reference validation and child selection share each immutable
artifact load; lane selection, fences, topology, signatures and the final route
publication recheck still execute. There is no cross-request cache. Regression
tests count one child load and compare the resulting projection/binding, while
the existing revoked-key, removed-topology and superseded-publication tests
remain mandatory. The three-read synthetic 1 MiB benchmark measured 22.2 ms /
3.15 MB without reuse and 7.88 ms / 1.05 MB with reuse on arm64; this is not an
end-to-end production latency measurement.

Telemetry keeps its 16 MiB retained-payload admission limit, 128-event export
batch, eight source workers and 1,000-line per-source chunk. Its bounded queue
and cycle ceilings now permit the declared defaults of 32,768 slots and 20,000
lines, rather than silently reducing them to 4,096 and 4,000. The queue slot
array adds at most 1 MiB; payload bytes and critical-event reserves remain
independently enforced. A six-thousand-event regression checks a configured
burst fits while larger payloads still hit the unchanged byte limit. Cursor
rejection, catch-up deadline, fairness and source coverage tests remain required.

The production source snapshot established a cycle-budget stop at four edge
sources with no read errors and about 0.8 MiB / 1,207 events queued. This identifies
the count ceiling as a throughput constraint; it does not attribute every delay
to that ceiling. Verify backlog, read errors, cursor gaps, export drops, CPU and
memory after deployment, including log bursts and source rotations.
