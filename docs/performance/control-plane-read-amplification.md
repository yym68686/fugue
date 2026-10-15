# Control-plane read amplification

The serving poll path previously returned complete artifact JSON on every
metadata/authority lookup. Its decoded-content cache only avoided JSON decoding
after PostgreSQL had already retransmitted the bytes. The host journald scheduler
also read every historical node task, including logs and payloads, before
filtering its own task type. Route compilation read complete historical release
specs even though release readiness uses only image/runtime identity.

## Changes and invariants

- A single PostgreSQL statement reads fresh artifact metadata and computes SHA-256
  over the actual stored JSON representation. It omits the body only when the
  reader holds matching immutable decoded content. Claimed content hashes,
  timestamps, authority status, signatures and release membership are not cache
  validators. Mutations and transactional reads retain their existing paths.
- The existing 64 MiB/64-entry decoding cache bounds retained content. Store-local
  lookup hints are capped at 128 entries. In-flight readers pin immutable maps;
  callers receive private mutable containers. Cache eviction causes a full read.
- Journald scheduling reads the latest task per updater for its exact task type,
  projecting only policy hash and scheduling fields. The same query independently
  checks all pending/running tasks of that type, including older tasks for removed
  updaters. No global history limit can conceal an in-flight host mutation.
- An additive index is created concurrently through the independent schema lane,
  before the API/controller lanes. Interrupted builds are repaired, unexpected
  definitions are rejected, and normal task writes remain available.
- Route snapshots retain all release rows, workload bindings, and all metadata.
  Spec bodies are projected to image/runtime identity, preserving nil versus
  present snapshots and the repeatable-read transaction boundary. File-backed
  snapshots use the same projection. Readiness, provenance, fences, membership,
  post-probe authority checks and positive LKG behavior remain authoritative.

## Verification

Run `make test`. Disposable loopback PostgreSQL integration checks additionally
exercise actual-body changes with an unchanged claimed hash, fresh metadata and
signature observations, aliases, eviction, deletion, concurrent mutable callers,
non-ASCII JSON, 200,000 unrelated historical tasks and projected release proofs:

```
FUGUE_TEST_DATABASE_URL=postgres://127.0.0.1/fugue_test_readamp go test -race ./internal/store -run 'ConditionalArtifactRead|HostJournaldState|RouteBusinessReleaseProjection|PlatformContent' -count=1 -v
FUGUE_TASK_INDEX_TEST_DATABASE_URL=postgres://127.0.0.1/fugue_test_task_index go test ./internal/schemamigrate -run TestNodeUpdateTaskSchedulerIndexPostgres -count=1 -v
```

The tests enforce loopback hosts and explicitly named disposable databases. The index test exercises cancellation,
concurrent writes, retry, idempotency, query planning and wrong-definition refusal.

Local PostgreSQL 14 measurements: thirty reads of a 1 MiB artifact transferred
1,048,591 body bytes rather than approximately 31,457,700 (96.7% reduction).
200,004 task rows produced two scheduler rows in approximately 0.8 ms. These are
synthetic local checks, not production performance claims.

## Production acceptance

Use the declarative GitHub Actions schema/API/controller lanes; never replace
images or run migration DDL manually. Compare equivalent traffic windows after
rollout and warmup: API receive bytes, database transmit bytes, database CPU,
request latency/errors, consumer ACK/convergence, and retained serving LKG.
Targets are at least 90% less repeated artifact body traffic and 60% less total
DB-to-API traffic without correctness or latency regression.

`fugue_store_reads_total`, `fugue_store_read_rows_total`,
`fugue_store_read_errors_total` and `fugue_store_read_duration_seconds_total`
use only the fixed families `platform_artifact` and `host_journald_state`.
`fugue_store_artifact_body_bytes_total` measures body bytes actually received;
`fugue_store_artifact_body_elisions_total` counts fresh database digest matches.
These counters omit protocol/metadata bytes and must not be interpreted as total
network throughput. No tenant IDs, SQL text, artifact content or credentials are
included in metrics.
