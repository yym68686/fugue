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

## Compiler-input verification and selective recovery

Chunked compiler-input writes verify the actual stored bytes before committing
without returning the full input body. The canonical input stream determines
the whole digest and independent per-chunk hashes; PostgreSQL returns each stored
chunk's actual SHA-256, ordinal and length for comparison. Existing-input retries
retain the same checks. Legacy inputs and unusually small historical chunks keep
their bounded full-read compatibility path.

DNS selection recovery uses a separate eight-entry/4 MiB index of hashes, byte
ranges and summaries. It returns bodies only for needed observations while
checking every current stored chunk, including unselected chunks. No receipt
body, authority decision or freshness result is cached. Cold readers retain the
full verified path; selected observations still undergo normal receipt checks.
See [the storage invariants](../control-plane-memory-optimization.md).

Regression coverage includes real PostgreSQL checks for altered stored bytes on
new and retried writes, corrupt unselected chunks, selected/full-read equivalence
and result isolation. Additional checks cover cache bounds, legacy storage and
noncanonical/invalid-UTF-8 fallback. The live body-elision metrics for
`platform_artifact` do not measure this compiler-input path, and neither metric
establishes that database hashing CPU or I/O is zero.

## Bounded hostname lineage diagnostics

Post-rollout verification found an adjacent read amplification bug: hostname
lineage loaded up to 600 complete historical artifact bodies and repeatedly read
LKG per artifact. The endpoint now defaults to metadata only and 20 recent
candidates per kind. PostgreSQL performs hostname matching inside the bounded
candidate set; each kind/scope LKG is read once per request. Responses explicitly
report candidate coverage and content inclusion. `limit` supports 1–200;
`include_content=true` defaults to five candidates per kind and rejects larger
explicit limits. Individual artifact retrieval remains available. These are
OpenAPI-declared diagnostic semantics, not changes to serving authorization.


## Bounded network-observation writes

Post-rollout verification found a 65-witness persistence deadline on the existing
one-second write budget. `RecordEdgeNetworkSamples` previously executed one INSERT
per sample (up to 128), in addition to retention and transaction round trips.
It now sends one parameterized, bounded multi-row INSERT. The original transaction,
retention delete, `ON CONFLICT DO NOTHING`, validation, witness namespace and
128-sample cap are preserved. Serialization happens before opening the transaction.
No timeout is extended and no runtime evidence is cached or synthesized.

Real PostgreSQL tests cover a full 128-entry batch, duplicate keys within and across
batches, parity with file storage, 65 route witnesses, collection isolation, empty
batch pruning, constraint-failure rollback of both inserts and pruning, cancellation,
and rejection above the cap. A separate SQL expectation checks the single-INSERT
round-trip contract. Long-term deadline rates remain part of rollout monitoring.

## Agent recovery follow-up

Post-release observation found a pre-existing recovery delay in the runtime
Agent. An authenticated negative route proof immediately removes local
eligibility, but the scheduler previously waited its generic five-second tick
before requesting a new signed grant. Production canary logs showed short
missing-primary intervals and failed heartbeat/operation requests; the same
pattern was present before the read optimizations.

The Agent now schedules one prompt acquisition after a newly observed negative
proof. An attempted acquisition clears that urgency; if it fails, the ordinary
five-second retry deadline remains in effect. The acquisition, signature,
publication watermarks, fresh measurements, capacity requirements and absolute
expiry checks are unchanged. Negative evidence never grants permission to reuse
an ineligible route, and this scheduling fix does not claim to eliminate genuine
capacity or publication-convergence failures.

The regression test demonstrates the old five-second delay, prompt recovery
only after a fresh signed and measured grant, and bounded retry without extending
the previous permission when the authority is unavailable. The declarative
runtime-agent-canary lane publishes the change for live verification.


A follow-up sample still showed one heartbeat arriving while fresh authorization
was being acquired. The selected transport now allows an unsent request to join
one acquisition already in progress, within its existing client/context timeout.
It then re-reads trust, permission, expiry and fresh measurements before making
its only network attempt. Requests never start acquisitions or extend old grants;
failed, unmeasured, expired or revoked replacements remain unavailable. A request
already sent to an Edge is never replayed. Acquisition requests bypass this wait
to prevent waiting on their own completion, and valid selected requests retain
the immediate path. This removes a local pre-send race while preserving genuine
authority failures; it cannot guarantee availability when no eligible Edge exists.
