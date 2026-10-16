# Bounded Control-Plane Storage

Compiler inputs used to be encoded as a complete JSON value, parsed by
PostgreSQL as JSONB, returned as a complete value, decoded and hashed again.
Live Diagnostics correlated this operation with hundreds of MiB of backend
anonymous memory. Inventory checkpoints also rewrote several MiB per page.

## Compatibility And Activation

The schema migration adds four tables and does not rewrite existing data:

- `fugue_platform_input_manifests` and `fugue_platform_input_chunks` hold v1
  canonical JSON bytes in chunks of at most 256 KiB. Each chunk has a SHA256;
  the manifest has the original canonical digest, byte length and chunk count.
- `fugue_observation_checkpoint_heads` and
  `fugue_observation_checkpoint_entries` hold checkpoint metadata and bounded
  pages. A cursor and its pages commit in the same transaction.

Release the dual-format reader first with both new writers disabled. Establish
that version as the previous healthy code release by completing a subsequent
API release before enabling the formats. Only then publish the independent
storage policy. Rolling back to that reader can still read every new input and
checkpoint. Code older than the compatibility reader is not a valid rollback
target after activation. Legacy rows remain available and are not overwritten
or deleted by the new schema. Existing LKG artifacts and their lineage digests
remain unchanged; no traffic or DNS selection is part of this migration.

`deploy/environments/production/storage-policy/policy.json` is published by
the independent `storage_policy` CI job. It does not depend on code tests,
builds or component rollouts. The API reads the projected file every five
seconds (plus Kubernetes projection latency); invalid/unavailable updates
retain the last accepted in-process policy. A restarted API defaults to
legacy writes until it reads a valid policy. `runtime-storage-budget` exposes
the accepted policy, last load time, read failure and database pool counters.

`compiler_input_chunks` controls new compiler-input writes. All readers always
support both formats. `checkpoint_pages` controls migration of legacy
checkpoints: an existing paged checkpoint continues in that format even if
the switch is disabled, preserving its current cursor instead of restarting
from an old legacy copy. This compatibility behavior also applies to a code
rollback to the dual-format reader.

## Bounds And Integrity

One cluster-wide advisory transaction lock bounds concurrent compiler-input
database writers. Each SQL parameter carries one chunk, never a whole-object
batch or aggregation. Chunks and the manifest become visible atomically after
the canonical input stream's whole digest and every stored chunk's sequence,
length and actual SHA-256 have been verified. PostgreSQL computes hashes from
the stored bytes; the writer compares them with independent hashes from the
input stream. This preserves read-after-write verification without returning
the complete stored body or decoding it again. Retried writes verify existing
bytes too; corruption is reported rather than silently replaced. The maximum
document is 1 GiB, each chunk is 256 KiB, and the operation has a 30-second
database deadline. These limits do not grant release authority or bypass
signatures.

Canonical hashing/encoding streams array elements and preserves the existing
v1 byte representation. Decoding buffers one array element rather than the
entire JSON document. The typed runtime snapshot remains O(N), and individual
large values (including `facts` entries and physical evidence) still need
their own encoding buffers. This does not claim constant total API memory.

DNS selection recovery can read only the chunks containing observations the
caller still needs. A store-local cache retains at most eight entries within
4 MiB: chunk hashes, byte ranges and selection summaries, never receipt bodies
or validation results. A cache hit still reads the current manifest and checks
every stored chunk's actual SHA-256, ordinal and length, including chunks whose
bodies are omitted. Returned chunks are hashed again before selected observations
are decoded. The caller still validates receipt authority, policy, freshness and
context. Cold or legacy inputs, incompatible manifests and unsupported indexes
retain the full verified read path. These optimizations reduce bytes transferred
to the API; they do not eliminate PostgreSQL's work to read and hash stored data.

Inventory maps use stable hash partitions, each at most 64 KiB. The metadata
contains page digests and references the scan generation. Completing a scan
reuses that generation's pages. Only changed pages are written; obsolete
checkpoint pages are removed in the same transaction. Total checkpoint size
retains the existing 16 MiB limit. Replicas share the original advisory lock,
and reads use a repeatable-read transaction. Failed/canceled pages cannot
advance the cursor or replace the last complete scan. Page loading still
depends on retained reference count; this change reduces write amplification,
not the total size of the logical inventory.

Consumer assignment reuses artifact observations within one request while
reading each active release, fence, membership and trust state as before.
Repeated pure reference validation is reused only for that request's same
artifact. The policy-projection cache hashes actual tree values before any
full encoding, never trusting a claimed content digest as its cache key.
The cache remains bounded to 256 identities. Capability checks decode content
once instead of six times; authority and freshness are not cached.

## Verification

Focused tests cover v1 byte/hash equivalence, strict streamed decoding,
corrupted/missing chunks, duplicate writers, wrong-digest rollback, legacy
content, readers with writers disabled, checkpoint cancellation, replica
locking, unchanged-page writes and missing/corrupt pages. Fuzzing compares
the streaming encoder against the previous encoder. Existing tamper,
cross-scope, lifecycle and consumer-assignment tests remain required.

The opt-in integration tests use only a disposable loopback PostgreSQL:

```sh
FUGUE_MEMORY_TEST_DATABASE_URL='postgres://postgres@127.0.0.1:55439/fugue_test_memory?sslmode=disable' \
go test ./internal/store -run 'TestChunkedCompilerInputPostgres|TestObservationPagesPostgres' -count=1
```

The opt-in memory experiment additionally requires a local Docker container
whose name starts with `fugue-memory-`. It uses synthetic 26/60/100/150 MiB input,
the real store/pgx implementation and 20 ms process sampling. Results are
observed peak lower bounds, not proof of identical production performance.
It is never run against production. Full API changes require `make test` and
`make prepush` before normal declarative release.

After activation, collect `postgres-memory-window`, `runtime-database-queries`
and `runtime-storage-budget` concurrently. Confirm bounded chunk/checkpoint
parameters, current formats, memory categories, pool occupancy/wait deltas,
Pod readiness/restarts and public health. Set connection budgets only after
observing occupancy and long-held leadership connections; retain headroom
for those connections and foreground requests. A zero max-open value restores
the previous unbounded database/sql open-connection policy, while idle count
and idle timeout remain explicit. Rollout safety is distinct from OOM cause:
historical OOM without SQL evidence is not retrospectively attributed.
