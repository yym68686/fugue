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
Its one final full read verifies both new and reused components before commit;
it does not separately read every body twice. Readers prefetch metadata for at
most sixteen components / four MiB of logical input and share a four MiB decoded
pack cache only inside the same read transaction. Pack reuse requires identical
hashes and sizes. Every component occurrence and the complete original input
are still hashed; cache state never crosses transactions. The private storage
observation reports bounded phase timings and sanitized failure categories.

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
