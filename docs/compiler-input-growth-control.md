# Bounded compiler-input storage growth

## Artifact bodies

The public artifact, canonical hash, generation, signature, validation and
rollback contracts are unchanged. PostgreSQL supports two explicit physical
representations:

- Legacy: `artifacts.content_json` contains the original body.
- Reference: `content_json IS NULL`, `content_ref_hash=content_hash`; the body
  lives only in `artifact_contents`, typed as `platform-artifact-v1`.

`fugue_platform_artifact_body` resolves the latter and rejects missing/wrong-kind
content. An explicit check and foreign key protect reference integrity. Inline
content is never silently replaced by CAS content: tampering remains visible.
Metadata-only reads still omit bodies. Conditional caches hash resolved current
bytes. The old tuple-only dependency index/cache is not trusted for reference
rows; those take the bounded live projection until a future index version binds
both tuples.

Deployment is reader-first. The independent `artifactContentReferences` policy
must stay disabled until **all registered readers AND their automatic rollback
revisions** contain the new reads. The publisher performs that gate before
activation; merely installing the schema is insufficient. Disabling new writes
does not disable reference reads or reverse the physical migration.
The declarative component lane also gates every later store-reader candidate,
even with the writer disabled, so a subsequent release cannot silently remove
the reference reader required by already migrated history.

After activation, each storage-policy tick may convert at most four historical
bodies/8MiB in a five-second transaction. A database advisory lock and durable
keyset cursor coordinate replicas. Each conversion verifies the artifact's
signature and complete CAS equality before clearing only the duplicate body;
timestamps and signed metadata are unchanged. Untyped legacy CAS content can
be classified only after full comparison; body/timestamps are not rewritten.
Invalid/oversized rows remain intact, advance the scan and report failure rather
than blocking every later identity. No artifact, generation or CAS body is
deleted. Normal vacuum reuses freed pages; this is not an immediate filesystem
shrink and does not justify `VACUUM FULL`.

## Lower object/index overhead

`compilerInputBlocks` is a physical writer profile, independent of digest and
format admission. Supported minima are 256/2048/4096/8192 bytes; the maximum
remains 32768 bytes. `boundary_bits=0` preserves the existing first-record-end
splitter. Values 3/4 select record-content-anchored boundaries, averaging about
8/16 records between selected boundaries (subject to minimum/maximum/chunk
limits). The noncryptographic anchor chooses cuts only: SHA-256, full readback,
stored checksums, pack references and bounds remain authoritative.

The existing v3/v4 recipes/readers need no new format. Inserting/removing a
record can resynchronize at the next selected record, unlike positional
coalescing which can change every later group. Missing/invalid profile files
restore the old writer profile; existing bytes remain readable.

Reproduce the explicit synthetic offline comparison with an isolated loopback
`fugue_test` PostgreSQL database:

```
FUGUE_TEST_DATABASE_URL=... FUGUE_INPUT_BLOCK_COST_PROFILE=1 \
  go test ./internal/store -run '^TestCompilerInputBlockTotalCost$' -count=1 -v
```

Each case writes six 2,000-record snapshots, verifies exact reconstruction,
and measures payload, heap/TOAST, indexes, WAL and recipe bytes. A local
PostgreSQL 14 run (bytes, allocation includes fixed per-relation overhead):

| Workload | Old relation+WAL | Anchored bits=4 relation+WAL | Old/new objects |
| --- | ---: | ---: | ---: |
| Stable records | 1,928,648 | 748,960 | 2,014 / 139 |
| Sliding window | 2,065,752 | 911,320 | 2,205 / 200 |
| Every record changes | 8,822,208 | 2,133,640 | 12,009 / 839 |

Positional 4096-byte coalescing increased sliding-window relation+WAL to
2,650,392 bytes: fewer rows alone is not success. WAL includes full-page images
and should be compared across repeated runs; these are synthetic measurements,
not a promise of production savings. The algorithm remains linear in bytes;
the improvement is fewer persistent objects/index entries, not a new asymptotic
bound.

The test also estimates varint recipe size, explicitly **not** a measured new
codec/WAL. With anchored bits=4, current recipes are 9,468--10,248 bytes and
varints would be about 2,669--3,543 bytes in these cases. That small additional
gain does not yet justify a new format and reader/rollback migration. Original
fixed-width recipe decoding is retained.

## Operational inputs and audit originals

The existing signed `configuration-v1` contract selects a compact operational
input. Its full original remains independently stored under its original digest;
no truncated input is ever represented as satisfying the original digest.
`EnsurePlatformCompilerInputPairContext` verifies the deterministic compact
projection, persists both, then registers an immutable original/runtime binding
before publication. Replay does not renew the first registration timestamp.
Legacy inputs are not retrospectively guessed to be disposable audit material.
Runtime/audit digests in explicit task payloads are both retention dependencies.

`PlanCompilerInputAuditRetention` is a bounded read-only, single-snapshot
preview. Zero days means indefinite retention. Active publication, current LKG,
retained rollback generations, consumers, tasks, pins and their full dependency
closures continue to protect original evidence. An elapsed age with uncovered
retention authorities is **blocked**, not eligible for deletion. The preview
cannot grant execution authority and performs no deletes.

**Original-input retirement is not activated by this change.** The owner must
choose an audit retention period; complete external/offline rollback, quality
evidence and reactivation obligations must then be resolved before a bounded
destructive collector can be enabled. Runtime mode is not automatically changed
for existing policies, and this change does not remove historical transcripts
required by quality comparison or recovery. Native history inventory remains a
necessary safety check, not permission to delete its unknown complement.
