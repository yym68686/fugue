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

### External compiler-input archive

`--mode archive` is a separate, one-digest v1/v2 export path. It streams the
original canonical bytes through immutable gzip segments, with a 1 MiB default
segment and a 1 GiB per-input maximum. Each uploaded segment is fetched back and
checked before its durable receipt is recorded; the complete reconstructed
input is checked against its original digest before the archive becomes
`verified`. This supports inputs larger than 128 MiB without raising the
in-database representation migrator's 128 MiB limit or buffering a whole input
in memory.

`verified` means an equivalent external copy exists; it does not change the
read route or permit retirement. `--mode archive-route` revalidates the
external copy and the source, then routes one exact digest with compare-and-
swap. The old source rows remain available for explicit `archive-unroute` until
the separate old-relation retirement gate is implemented and passed. The
archive reader fails closed on missing or invalid receipts and verifies the
complete byte stream on use. This path currently accepts only v1/v2 chunked
inputs; v3/v4 inputs do not reside in the legacy chunks relation and are not
part of its retirement coverage.

`--mode archive-read-check --digest sha256:… --read-check-max-bytes N` is a
bounded, read-only throughput/integrity measurement for exactly one already
verified archive. It uses the platform-owned archive reader and the same
segment, stored-byte, raw-byte, and full-input digest checks as normal archive
verification. The explicit raw-byte ceiling is mandatory (maximum 1 GiB), the
source representation must be v1/v2, and no archive row, object, route, lease,
or source chunk is written. The result explicitly grants no retirement
authority. It is not a substitute for the complete retirement plan or its
independent retention, hot-copy, and fencing gates.

`--mode collect` removes at most 16 unreferenced components and 16 unreferenced
packs per transaction. It checks both v3 and component references, verifies FK
barriers are present and enabled, and takes the common writer fence. It never
deletes input manifests, artifacts, publications, pins or LKG roots. Retiring
historical inputs is a separate complete dependency-graph GC problem.

The in-database representation migrator's hard per-input limit is 128 MiB;
statement/lock/overall budgets are 5 seconds/250 ms/2 minutes. The separate
external archive stream has a 1 GiB hard input limit and bounded segments.
Errors abort the current operation. Storage events record committed
representation changes. CLI output contains only aggregate counts, digests and
byte totals, never evidence or credentials. A lost commit acknowledgment
requires inspection before retrying.

The CI archive runner never replays a failed whole-input command based only on
SQLSTATE `55P03`. That error can occur after an earlier transaction committed
the input's owner lease or a segment receipt. Only the executor's individual
lock acquisition can retry, after its transaction has confirmed rollback and
within its bounded acquisition budget. A stopped command retains resumable
receipts; inspect them and wait for any owner lease to expire naturally before
declaring a new bounded operation. A later active-owner conflict is not evidence
that the original operation made no progress.

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
# Compiler input external archive

Platform administrators can provision a dedicated control-plane R2 backend with
`fugue admin storage archive enable --execute` and inspect it with
`fugue admin storage archive status`. Without `--execute`, enable is a read-only
preview. Provisioning reuses platform R2 management, never an application's
bucket, and returns no S3 secrets. Credentials are encrypted in the existing
platform object storage configuration: a bucket-scoped reader and a separate
writer. Upgraded readers discover activation from the shared configuration;
no live Deployment patch or plaintext environment injection is needed.

The bucket has an independently verified indefinite object lock and no object
expiration or storage-class lifecycle rules (R2's incomplete multipart abort
rule is harmless and preserved). The management API has no archive disable/delete operation.
Enabling is not migration or payment/backup success: verify real S3 uploads and
full digest readbacks separately. All current AND rollback reader releases must
support managed configuration before publishing routes or retiring old chunks.
Explicit archive environment configuration remains supported for standalone
operators; when present, it takes precedence and must bind the same backend.

The archive writer, reader, hot-chunk write fence, and two-phase retirement
commands are separate explicit operations. Deploying or enabling the backend
does not run any of them. Before production copying, verify the dedicated
target, transfer capacity, free-space reserve, and stop conditions. Record live
migration receipts and health separately from code availability.

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

The reader accepts only a routed row whose full digest and every segment
receipt validate. The single-input route operation can switch one verified
digest without deleting its original rows. Routing still requires cold/hot
dependency proof and all serving/rollback readers to support the archive.

`archive-batch --archive-limit N --apply` copies up to N legacy inputs, and
`archive-route-batch --archive-limit N --apply` switches up to N fully verified,
unrouted receipts. N is bounded to 1–1000. Both use one worker and the same
advisory lock; a failure stops the batch and committed receipts are skipped on
resume. Every route re-verifies both the full external input and the retained
source before compare-and-swap. Neither batch fences writers or deletes rows.
Production operations are declared in `storage-policy/archive-operation.json`
and run through the main-only CI lane, which checks the healthy primary and a
minimum 512 MiB free-space floor between each input, with an 80-minute batch
budget. The floor is a stop condition, not a guarantee of sufficient runway.
Route preflight checks the actual current and immutable LKG manifests and their
effective credential encryption keys against the API executor, including empty
environment fallback; a safe HEAD template does not establish rollback safety.
Only a maintenance barrier acquisition that fails with SQLSTATE `55P03` and
whose transaction rollback has been confirmed is retried outside transactions,
with 100ms backoff for at most five seconds. Each acquisition retains the
250ms lock timeout. The CI runner never retries the whole command: earlier
page/receipt transactions may have committed before a later failure. Unknown
failures, unconfirmed rollbacks and active-lease conflicts stop immediately;
exact retries require inspecting prior receipts and natural lease expiry.

`archive-hot-copy` is a separately declared, exact-digest promotion for a
critical v1/v2 input that already has a complete verified R2 receipt. It first
re-verifies the entire immutable R2 object, then copies source chunks into the
hot relation in 16-row transactions. The legacy source and archive receipt
remain intact. Existing readers continue to use the old source until every hot
chunk has been read back, checked against its stored hash, decoded, and checked
against the original full-input digest; only then does a manifest CAS select
the hot location. A partial copy is never visible to readers and can be resumed
idempotently; conflicting existing hot bytes fail closed. Current and immutable
rollback readers must explicitly prefer `storage_location=hot` and support the
hot relation before an operation is accepted. The runner requires a declared
stored-byte bound and a free-space reserve of at least 512 MiB plus four times
that bound. This is a conservative admission floor, not proof of actual WAL or
Longhorn amplification; begin with a small canary and measure PostgreSQL/WAL
and filesystem deltas before selecting a larger bound. Hot promotion duplicates
bytes while source rows remain and therefore is not a space-reclamation step.

The separate `archive-hot-restore` operation can rebuild one routed v1/v2 input
directly from its R2 archive before or after retirement. Unlike
`archive-hot-copy`, it does not read the legacy relation. Preview checks only
metadata and reports `worst_case_stored_bytes` (raw bytes plus the v2 envelope
ceiling), leaving measured `stored_bytes` at zero. Apply requires the migration
opt-in, an exact digest, the same explicit stored-byte budget and free-space/WAL
reserve as hot copy, and an executor revision that supports this operation.
Current and rollback readers must support local hot locations.

The restore reads bounded archive segments outside database transactions and
stages at most 16 chunks per transaction. Each page rechecks the maintenance
epoch, original manifest, routed receipt and complete segment set, and absence
of an active archive owner. It never writes archive leases or receipts, which
remain immutable after retirement. Full archive EOF validates the original
digest; a separate complete local readback must then validate it again before
the manifest CAS publishes the hot location. Format, original digest, logical
length and creation time remain unchanged. Interrupted operations retain bounded
staging progress. An exact retry accepts an existing differently compressed
chunk only when its stored hash, decoding and original bytes all match; it
never overwrites conflicting bytes. The budget accounts the bytes actually
retained, including valid pre-existing compression. An already-hot input is
verified locally and does not require a successful R2 GET.

Restoration is an explicit maintenance action, not automatic publication
admission or proof that every running and rollback dependency is local. A full
hot dependency plan, missing-root resolution, reactivation/pin admission, and
the independent cutover gates remain required before retiring the legacy
relation. Shipping this command does not authorize a production restore or
retirement.

Physical retirement has an explicit two-phase maintenance path. The
read-only `archive-retirement-plan` inventories every digest with rows in the
legacy relation, checks ordinal coverage and orphan rows, fully reads and
hash-verifies each external routed object (or the verified hot copy), and
reports the relation's exact heap/index/total bytes separately from each
input's logical bytes. It does not mutate production data. Its full readback can
be I/O intensive and should only be run after a dedicated archive target is
available.

`archive-hot-inventory` is a separate read-only metadata inventory. It collects
database roots and follows declared artifact, release, expectation, scoped
generation and input identities in one read-only Repeatable Read transaction.
It includes active publication and rollback generations, current and unexpired
LKG, at least three distinct verified historical generations per scope,
consumer/shadow identities, active task references and archive owners. Repeated
LKG verification events and legacy zero sequence values do not replace distinct
rollback generations. Typed DNS dependencies retain the previous complete
publication and its producer policies. An embedded artifact contributes an
expected hash; it does not replace the immutable ledger row.

The private operator report contains ledger identities and each input's retained
roots, not payloads, database URLs, credentials or arbitrary driver errors.
Shared descendants are resolved once and retain every incoming root. Declared
hash conflicts and unresolved native execution generations remain blockers;
generation labels are never parsed into input digests. Bounds are 16,384 roots,
32,768 nodes, 262,144 edges and 524,288 root/node bindings, with a one-minute
overall deadline, five-second statement budget and 250ms lock budget. Exceeding
a bound refuses the inventory rather than returning a truncated proof. This
uses an ordinary read-only database role and does not require setting the
superuser-only temporary-file limit.

`archive-hot-roots` uses the same coherent bounded root collector and returns
every root plus counts per source, without traversing its dependencies. This
separates sizing the actual retained-root inventory from verifying a graph.
It leaves `traversal_performed`, dependency completeness, byte verification and
retirement authority false and emits no reusable graph hash. In either mode,
queries for separate root sources share the same transaction snapshot and each
has the same five-second statement limit; no global sort over combined history
is required. Active expectations use the existing release/revision index.
Fixed failure-stage diagnostics contain no database or driver error text.
The inventory also records aggregate traversal phase, resolved/unresolved
identities, edges, root bindings and resolver calls. These diagnostics are
excluded from the deterministic graph hash. On refusal the CLI exits nonzero
and emits a separate `fugue.compiler-input-hot-inventory-failure/v1` object
with a fixed reason/category and these counts. Its completeness/verification/
retirement flags remain false and its plan hash is empty; it contains neither
an arbitrary error nor a partial graph that could be mistaken for authority.
Successful and failed inventories also include bounded aggregate `cost` counters:
root-inventory, traversal and resolver wall time, plus query count, returned rows,
query-return/row-fetch/decode microseconds for artifact, input and ledger batches.
Query return and row fetch can both include database work and transport; they
are not server execution profiles. Artifact source bytes count original JSON
text, while transported bytes count projected content and metadata payloads,
excluding protocol and identity fields. No SQL, connection, content or identity
is logged. Timings are excluded from the graph hash and cannot grant authority.
Artifact and input metadata is fetched in batches of at most 32 unique
identities; artifact text is capped at 8MiB per artifact and 32MiB per batch.
Missing identities remain explicit blockers, while failed database statements
abort the inventory instead of masquerading as missing dependencies. Batched
and individual reads use the same typed dependency and manifest validation.
Generation and release identities are also fetched in bounded batches, using
the same node builders as individual lookups. A generation always matches its
exact kind/scope/generation tuple; every candidate, serving, LKG, pinned and
rollback slot is retained, including repeated generation edges. The batch
uses the caller's existing read-only Repeatable Read transaction. Integration
checks compare all 32 individual lookups against the two equivalent ledger
queries, including concurrent replacements, missing identities and SQL failure.
For DNS artifacts the database projects only the lineage, policy, current and
previous publication envelopes, and declared DNS source. Answer/query/readiness
views are not dependency identities and are not transferred to the inspector.
The full original JSON text size still counts against the unchanged artifact
and batch limits; projection cannot admit an otherwise oversized source.
The projection decodes the five dependency fields together with `jsonb_to_record`
instead of extracting each field separately. A key-only presence set distinguishes
missing keys from JSON null, which the record decoder alone would collapse into
SQL NULL. It does not expand unrelated answer values with `jsonb_each`, whose
set materialization can spill to temporary storage before filtering. Missing
keys remain absent, JSON null remains present, and the same typed dependency
validation still runs. Complete source-size computation remains mandatory.
Typed publication validation shares one immutable JSON envelope rather than
serializing the entire artifact for each dependency view. PostgreSQL integration
checks compare projected and full reads, including transitions, conflicting
lineage and oversized omitted bodies. This is a transport/decoding optimization,
not proof that the production graph fits the existing deadline.

This inventory deliberately does not grant retirement authority. Its
`metadata_complete` field describes only the graph reachable from the collected
database roots. The report separately lists uncovered sources and leaves
`body_verification`, `reactivation_protected`, `retirement_authorized` and local
byte verification false. Native external generation ledgers, a complete pin
registry, artifact signatures, independent original-byte checks and persistent
reactivation admission still require their own proofs. Its deterministic graph
hash is not an approved retirement plan or a publication barrier. Deploying the
command does not run it or change any production read/write route.

`archive-retire --apply` requires the exact plan hash, current and rollback
reader release IDs, a committed write fence and database trigger, no active
archive leases, a matching database-side recheck, and a drained session-level
reader barrier. It also checks all database transactions twice with refreshed
PostgreSQL statistics: pre-existing transactions, any session retaining a
snapshot, and prepared transactions block retirement. Full transaction
visibility (superuser or `pg_read_all_stats`) is required. A blocked maintenance
attempt exits without cancelling sessions; operators must let the transactions
finish before retrying. This protects old snapshots that never took the input
reader lease. The reader barrier still prevents new supported input readers
from taking a snapshot before the cutover commits. The executor sets short
lock/statement timeouts and executes only
`TRUNCATE TABLE fugue_platform_input_chunks RESTRICT`; it never uses CASCADE or
terminates application sessions. In the same transaction it installs enabled
row and statement guards on the archive control, archive, segment, and lease
tables; after the retirement marker commits those ledgers cannot be changed or
truncated through ordinary SQL. API startup then requires the archive backend,
matching routed backend fingerprints, complete segment receipts, and all
retirement guards. It also requires a SHA-256 identifier for the
independently reviewed archive retention policy and stores that value in the
retirement audit row. This value is an operator attestation; Fugue cannot prove
from an S3-compatible endpoint that external lifecycle rules will not delete
the objects. Database guards cannot prevent an object-store administrator from
deleting an archived object; reads still validate the complete original digest
and fail closed if the object is missing. A reader-drain timeout, changed plan,
FK, or table lock aborts the transaction without removing the source rows. Run
the live preflight immediately before authorizing retirement:

```sh
python3 -m scripts.compiler_input_storage --mode check-archive-readers
```

It binds the active workloads, pods, immutable current/LKG records and their
source revisions, and fails on an unstable rollout or unresolved release
monitor state. Every API candidate is separately source-checked in the normal
GitHub Actions deploy lane for the routed reader, session lease and startup
guard. The release IDs passed to `archive-retire` are operator-supplied audit
values; the binary does not independently query Kubernetes, so they are not
proof that the live preflight was run. Do not execute retirement unless that
preflight has just passed and the independent object-retention policy has been
reviewed. Do not use existing backup prefixes or retention policy for
compiler-input archives.

The relation-byte figure is the exact PostgreSQL heap/index/TOAST relation
size observed in the plan; PostgreSQL freeing it does not guarantee the same
amount of Longhorn backing-disk reduction while snapshots retain blocks. Per
input logical bytes must not be presented as an exact allocation of physical
pages.

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

## Capacity guardrails and bounded automatic diagnostics

Automatic image-cache orphan pruning now keeps only a bounded plan receipt in
the control-plane history: counts, policy summaries, plan hash, limits, and
status. The controller still holds the complete classification in memory while
it evaluates every safety gate, and a scheduled task retains its bounded target
payload plus the original plan hash. Automatic summaries use a stable identity
derived from node/runtime/mode and the full plan hash, so repeated scheduler
passes for the same observation update one row instead of appending per tick.
Explicit administrator previews continue
to support full detail when requested. This prevents repeated scheduler passes
that cannot run (cooldown, active task, missing capability, or global limit)
from appending large per-image JSON arrays indefinitely; it does not authorize
deletion, change image-cache retention, or claim physical space recovery.
