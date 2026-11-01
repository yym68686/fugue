# Shared compiler input storage — staged rollout

This is a physical representation, not a compiler or signature change. V1 and
v2 remain readable. V3 reconstructs the exact canonical JSON byte stream and
checks the original `input_snapshot_digest`; no timestamps, identities,
measurements, derived observations or binding delimiters are removed.

## Current release boundary

The first release installs compatible readers and additive empty tables only.
Production has **no `compilerInputShared` declaration**: shared writes are off.
There is no historical migration or deletion loop in this release. Reader
deployment alone reclaims no existing space. Protected dependency-graph GC,
writer activation and historical migration are separate, still pending stages;
they must not be described as completed by this compatibility release.

Enable a writer only in a later normal main-branch deployment, after verifying
every running reader and rollback target. Its independent storage declaration
is `compilerInputShared: {version: 1, enabled: true}`, published as
`compiler-input-shared.json`. Missing, invalid or disabled policy stops future
shared writes without changing reads. Keep the v2 declaration unchanged.
After any v3 input exists, allowed rollback code must support v3: switching the
writer off does **not** permit an older reader/migrator. Readers report accepted
formats through the private local storage observation provider.

## Representation and integrity

Canonical streaming JSON is split at array-object boundaries (256-byte minimum,
32768-byte maximum) or the existing 256KiB logical boundary. Every original byte
is retained, including punctuation and duplicates in their original positions.
SHA-256 identifies immutable objects shared across inputs. New objects, in first
appearance order, are compressed together into packs bounded by the existing v2
envelope; shared packs use standard zlib default compression, while the existing
v2 writer keeps BestSpeed. Incompressible packs retain raw fallback.

Object/pack identities are compact internal bigint IDs; hashes are 32-byte
binary values, not repeated 71-byte text keys. A recipe contains version 3, a
big-endian uint32 decoded length, then (uint32 size, uint64 object ID) pairs.
Recipe SHA-256 and ordered ordinals are checked. Object SHA-256, placement,
decoded pack SHA-256, physical pack SHA-256, sizes and the complete input digest
are independently checked on read. IDs are physical pointers, never authority.
The writer's read-after-write verifies actual reconstructed bytes before commit.
Transactions serialize with the existing input writer fence. Conflicts never
overwrite existing evidence or conceal corruption.

Each input has a unique pack reference set with foreign keys. Referenced packs
cannot be deleted. This is only a storage-level barrier, **not** a complete
release-dependency GC authorization. Do not run age-based DELETEs.

Decoded packs use a per-read 16-pack cache (<4MiB); the global DNS index remains
4MiB/8 entries and contains recipes, summaries, positions and a closure witness,
not evidence bodies or serving decisions. A warm selective read rechecks the
complete current object/pack metadata and hashes actual physical bytes of every
pack, including omitted evidence. Cold reads verify the complete input digest.
Metadata is bounded to 131072 unique objects and 8MiB recipes per input. Inputs
exceeding this budget atomically discard partial shared writes and use the
existing storage path. Database/corruption errors never trigger this fallback.

## Measured gate, not a capacity promise

Offline PostgreSQL 14, two adjacent approximately 95MiB canonical inputs:

| Metric | Existing v2 | Shared prototype |
| --- | ---: | ---: |
| Allocated tables/indexes/TOAST, both inputs | 23691264 bytes | 21807104 bytes |
| Second input full read | 4.21s | 3.93s |
| Second input write including verification | 2.65s | 4.40s |
| Warm all-omitted DNS read | 45.5ms | 80.4ms |

These are individual local measurements, not latency distributions or a claim
of production speedup. The second shared input added 8257536 allocated bytes;
the first paid the shared object index cost. Logical duplicate ratios do not
equal disk savings, and no 60% physical reduction or 1MiB input is promised.
Earlier prototypes were rejected because indexes exceeded the v2 baseline.
The opt-in profile test logs aggregates only and accepts guarded disposable
loopback databases or private stdin; never expose production bodies in output.

## Remaining activation and GC gates

Before writes: exact production reader/migrator and rollback compatibility,
bounded allocation, representative multi-input storage benefit, public/compiler
content and DNS replay equivalence, omitted-corruption tests, concurrent writes,
atomic failure and stable production health must pass. Use the standard GitHub
Actions release path only. No manual workload image patch or storage activation.

Before historical removal: implement and test a common fencing/pinning protocol
for input creation through artifact commit, reference creation, publication,
rollback and GC. Roots must cover current/full/gray/pending publications, LKG,
rollback pins, consumer desired/actual/candidate/LKG, ongoing operations and
retained/previous-publication transitive sources. Historical artifact rows are
not permanent roots. Unknown dependency forms fail closed. Preview first, then
small bounded batches with revalidation, short lock budgets and pause control.
Do not rewrite the complete historical store with low disk headroom. Deletion
only creates reusable PostgreSQL space; returning filesystem bytes is a separate
operation, not automatic or part of this compatibility release.
