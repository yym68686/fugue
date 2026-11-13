# Bounded compiler-input dependency index

Complete retention inventory resolves large DNS artifact bodies even when only
their dependency fields are needed. This optional derived index moves that
decoding into bounded preparation pages. It does not change retention roots,
typed dependency validation, input representation, reader routing, or retirement
authority. The additive schema release creates an empty table; it does not scan
or rewrite the historical artifact table.

The index trigger derives every entry from the source inside its transaction.
Caller-supplied dependency text, metadata, sizes and version bindings are replaced.
The inventory verifies the exact derivation function, trigger, identity key and
required column types before using entries. Each entry binds source xmin and
ctid, including same-transaction updates, with a 64-bit transaction horizon to
reject reused transaction IDs. Source updates incur no index-maintenance work;
missing or stale entries use the original bounded source projection. Reader and
source row access use the same read-only Repeatable Read snapshot.

`archive-hot-index` processes one explicit keyset page of DNS artifacts, defaults
to one entry, and refuses limits above 32 entries. It preserves the 8MiB original
artifact bound and refuses a page above 32MiB of source plus derived text. All
page writes roll back together on refusal. The mode requires explicit `-apply`
or `-rehearse`; rehearsal rolls back but can still generate WAL and I/O. It
records source/index bytes and concurrent WAL distance, refusing above 256MiB
WAL. These counters do not attribute concurrent WAL to the index operation.

The incident-only trusted-runner orchestration has been removed. The derived
index is not a production background backfill service. Its native page command
does not authorize a full-store loop or bypass runtime, space, WAL and ownership
checks. Any remaining maintenance must use a separately reviewed bounded
operation in the normal declarative release workflow.

The cursor is preparation progress, not a coherent
retention proof; the subsequent complete inventory must still run within one
60-second snapshot with all existing graph, statement and byte bounds.

Real local PostgreSQL regression tests compare indexed and source dependency
results, missing versus JSON null, concurrent and same-transaction updates,
untrusted writes, disabled derivation, oversized sources and whole-page rollback.
The synthetic 8 x 2MiB measurement is a local constant-cost comparison, not a
production full-graph completion or same-version recovery claim.
