# Configuration frames and shared evidence

`runtime-snapshot-separated-v4` is a physical representation of the existing
canonical runtime snapshot. It does not change the compiler, publication input
digest, signatures, artifact validation, route eligibility, freshness checks,
LKG selection, or historical replay contract.

The configuration/selection frame and each DNS selection's `physical_evidence`
body are stored as independently content-addressed components. A small ordered
layout reconstructs the **exact original byte stream**, including original
timestamps and evidence. Equal evidence bodies share a component. Components
reuse the v3 content-addressed object/pack store, so unchanged fact fragments
can also share packs across unequal evidence bodies and snapshots. Configuration
and evidence are separate records; a reference is never readiness authority.
Unrecognized JSON shapes remain literal. A bounded metadata overflow falls back
atomically to the existing writer; integrity and database errors never fall back.

Readers accept v1/v2/v3/v4 independently of writer policy. Full reads verify
component recipes, physical packs, object placements, original total length and
original snapshot digest. Selective DNS reads cache only bounded summaries and
byte ranges following a verified full read. Every subsequent use rechecks the
layout, all referenced recipes/placements and actual hashes of every distinct
physical pack, including omitted evidence. A changed witness requires a full
read. No cached result grants serving permission.

The writer stages up to sixteen chunk recipes (at most four MiB of fact data)
across component boundaries before querying/inserting shared objects and packs.
Evidence manifests are looked up/inserted in batches of at most sixteen bodies
or four MiB; an oversized body occupies its own bounded-input batch. New pack
payloads and object placements are also persisted in bounded bulk queries.
Its one final full read verifies both new and reused components before commit;
it does not separately read every body twice. Readers prefetch metadata for at
most sixteen components / four MiB of logical input and share a four MiB decoded
pack cache only inside the same read transaction. Pack reuse requires identical
hashes and sizes. Every component occurrence and the complete original input
are still hashed; cache state never crosses transactions. The private storage
observation reports bounded phase timings and sanitized failure categories.
Physical pack reads fetch at most sixteen packs / four MiB per query, with a
lookahead capped at sixteen recipes. Both physical hashes are checked before
any fetched pack enters the transaction cache. The most recent failed write's
phase and timings survive subsequent successful writes, so a small successful
input cannot erase a large input's deadline evidence. The original write
deadline is unchanged.

## Migration and collection

`fugue-input-storage` defaults to read-only inspection of one exact digest.
`--mode migrate` converts v1/v2/v3 to v4; `--mode rollback` restores v4/v3 to v2.
Mutations require `--apply` or the explicit `--rehearse` option. A rehearsal
**writes and rolls back**, consuming WAL and I/O; it is not a read-only dry-run.

Each migration holds the existing database writer advisory lock, verifies the
old complete snapshot, writes the new representation, validates the public
decoder and original digest again, compares the manifest with its expected
predecessor, and retires the replaced bytes in the same transaction. Creation
time and identity remain unchanged. Full/public/selective readers use one
read-only Repeatable Read transaction, which pins the old physical versions
until an already-started reader finishes. Current, pinned, historical, and LKG
references keep the same digest throughout migration.

`--mode collect` removes at most 16 unreferenced components and 16 unreferenced
packs per transaction. It checks both v3 and component references, verifies FK
barriers are present and enabled, and takes the common writer fence. It never
deletes input manifests, artifacts, publications, pins or LKG roots. Retiring
historical inputs is a separate complete dependency-graph GC problem.

The hard per-input maintenance limit is 128 MiB; statement/lock/overall budgets
are 5 seconds/250 ms/2 minutes. Errors abort the whole operation. Storage events
record committed representation changes. CLI output contains only aggregate
counts, digests and byte totals, never evidence or credentials. A lost commit
acknowledgment requires inspection before retrying.

Maintenance uses the ordinary database role. It sets `work_mem=1MB` and disables
parallel query workers, but does not set the superuser-only `temp_file_limit` or
grant additional privileges. This is not a hard zero-temporary-file guarantee;
bounded inputs/batches and the filesystem reserve below remain mandatory.

## Production sequence

1. Ship the additive schema and compatible readers via main → the existing CI
   component lanes with the writer disabled (default).
2. Establish compatible **automatic rollback** versions too. The policy publisher
   checks the registry's Store/schema readers, stable pod identity/readiness,
   immutable forward/LKG manifests, their digests and source compatibility. A
   fresh reader rollout by itself does not make an older LKG compatible.
3. Publish `compilerInputSeparated: {"version":1,"enabled":true}` using the
   independent storage policy lane. Disabling it stops only new v4 writes; it
   does not convert existing v4 and does not permit an incompatible rollback.
4. Review a single bounded `operation.json` under the storage-policy directory
   and push main. The same CI entrypoint executes it only when that file changes.
   The executor revision is pinned; current and LKG readers are checked again,
   and the CNPG primary must be Ready and healthy. Before either apply or
   rehearsal, free space must exceed **1 GiB + 8× the logical input budget**.
   The operation defaults to inspection and never picks a digest implicitly.
5. Inspect durable outcome, new-format writes, sharing, public health, restart
   counters and bounded errors. Measure canary migration peak WAL and storage
   before increasing the pace. There is no unbounded background migration.

An operation declaration has exactly these fields:

```json
{"version":1,"mode":"inspect","digest":"sha256:<64 lowercase hex>","expectedFormat":"","maxInputBytes":134217728,"apply":false,"expectedExecutorRevision":"<40 lowercase hex>"}
```

Inspection/rehearsal reports are payload byte counts, not promises about relation
allocation or filesystem recovery. Deleted versions become reusable after
readers finish and PostgreSQL vacuums them. Ordinary VACUUM generally retains
relation files, and freed pages in the old chunk relation do not automatically
become space for new component relations. No VACUUM FULL, global rewrite,
historical deletion or claim of reducing tens of GiB to hundreds of MiB is part
of this representation change. Production savings require actual measurement.
# Compiler input external archive (staged)

Status as of 2026-11-04: code is staged in the isolated `compiler-input-retention`
worktree; no production archive bucket is provisioned or validated, no archive
migration has been run, no read route has been committed, and no production
input rows have been deleted or truncated. The production chunk relation was
68,116,963,328 bytes at 2026-11-03 17:29:45Z and 17:56:40Z. Database size rose
from 155,488,016,063 to 155,510,519,487 bytes over that interval; this is a
sampled database delta, not a sustained growth-rate estimate. At 17:51Z the
volume had 1,499,280 KiB available (reported 100% used). Do not start bulk
copying until a dedicated target, measured transfer capacity, safe runway, and
backpressure policy are confirmed.

The archive migration mode is an opt-in copy/verification tool, not a retention
or deletion tool. It copies one existing v1/v2 chunked input into immutable,
compressed object segments, reads them back, and records `verified` only after
reconstructing the original SHA-256. `verified` does not change the reader
route and does not reclaim PostgreSQL space.

The object bucket must be separately provisioned and isolated from CNPG backups
and tenant buckets. Configuration uses `FUGUE_COMPILER_INPUT_ARCHIVE_*`; the
read route is installed only when that backend is configured. Migrations also
require the explicit `FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED=true`
opt-in. No archive bucket is inferred or borrowed from backup configuration.

Inspect one source manifest without copying. This metadata-only dry-run checks
the registered format, length, and chunk count; it does not read or hash the
source chunk bodies and therefore is not evidence that the whole input is
intact:

```sh
FUGUE_DATABASE_URL=... fugue-input-storage --mode archive --digest sha256:...
```

After explicitly enabling the dedicated archive backend and running a verified
canary, apply one exact input:

```sh
FUGUE_COMPILER_INPUT_ARCHIVE_MIGRATION_ENABLED=true \
FUGUE_DATABASE_URL=... fugue-input-storage --mode archive --digest sha256:... --apply
```

This stage has no bulk-scan, delete, or `TRUNCATE` mode. The reader accepts
only a routed row whose full digest and every segment receipt validate. A
separate operator route operation can switch exactly one already-verified
digest without deleting its original rows. Routing still requires cold/hot
dependency proof and all serving/rollback readers to support the archive.
Physical retirement additionally requires stopping old writers, proving the
full old-relation copy set, draining readers with a release/rollback
compatibility floor, and passing a separate bounded maintenance gate. Do not
use existing backup prefixes or retention policy for compiler-input archives.

After the per-input canary, operators may explicitly route one verified digest
with `fugue-input-storage --mode archive-route --digest sha256:... --apply`.
This requires `FUGUE_COMPILER_INPUT_ARCHIVE_ROUTING_ENABLED=true` and the same
dedicated archive configuration. The operation re-verifies the full external
object and retained legacy chunks before a short compare-and-swap; it does not
remove the old chunks. To restore the legacy reader for one digest, use
`--mode archive-unroute --digest sha256:... --apply` with the routing opt-in;
unroute verifies the retained old bytes first and does not require object-store
availability. Both operations affect exactly one digest, and failed validation
leaves the current route unchanged. Neither command authorizes bulk retirement
or `TRUNCATE`.
