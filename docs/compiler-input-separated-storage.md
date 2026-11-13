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
For separated-input writer deadlines it also retains a bounded per-attempt
profile: snapshot/evidence/component/pack/frame durations plus byte and item
counts. These nested timings overlap: `encode_components` includes snapshot
serialization and may include PostgreSQL queries/writes while evidence
components are staged; it is not a CPU-only measurement. `pack_encode_ms`
isolates the pack encoder's wall time, while evidence and frame stage timings
include their storage work. No snapshot bytes, identities, SQL text, or evidence
content are exposed.
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

### Retained historical archive compatibility

Historical compiler inputs may still have routed, verified external copies or
protected local hot copies. The reader, full-byte integrity checks, leases,
backend identity validation and local pins remain supported during this
compatibility window. A manifest's `storage_location=legacy` does not imply
that it reads the legacy chunks table: v4 uses shared components.

The one-off full-history copy/read-only/preparation CI coordinators and the
relation retirement planner/executor have been removed. There is no supported
`archive-retirement-plan` or `archive-retire` mode. Changing the production
workflow must not replay an old copy operation or recreate an incident worker.
Durable execution receipts are historical records, not new execution requests.

Normal collection remains bounded and protects referenced shared/v4 objects.
Existing routed inputs, verified copies and local pins are not deleted or
rerouted by removing the migration tools. An input with permanently missing
original bytes must not be represented as successfully restored or replaced by
new observations under the original digest.

Current configuration runtime input has an independently signed digest; see
[the runtime contract](compiler-input-runtime-contract.md).
Offline encoded-input and release-ledger backup tools retain their separate
integrity and authorization contracts; see
[encoded backup](compiler-input-encoded-backup.md) and
[release backup](platform-release-backup.md).

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
