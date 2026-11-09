# Compiler input retention audit

`fugue-input-storage -mode=archive-retention-inventory` produces one bounded,
read-only audit of the native retained dependency closure and its complement in
the input manifest and inline input content tables. It accepts no apply,
rehearsal, routing, contract or budget override flags. It performs no object
requests, original input reads, pin creation, phase transition or deletion.

The universe and closure share one Repeatable Read snapshot, database and
schema. Digest primary-key scans include a refusal sentinel beyond 32768
identities. Manifest and inline copies of the same digest count once; conflicting
logical sizes refuse the audit. The original 60-second transaction, 5-second
statements, 250ms locks, fixed zero-temp guard and 1MiB work memory remain.
Native traversal keeps its existing node, root, edge, binding and body bounds.
No pages or graphs from separate snapshots are combined.

Unexpired LKG verification events stream from that same snapshot. Repeated
events deduplicate by both artifact identity and expected content hash in a
root-bounded map. Different hashes remain distinct bindings, so a conflicting
event cannot disappear. This avoids a server aggregate spilling to temporary
storage before its output limit; it does not reduce retained history, stop
after a row prefix, or extend statement and transaction deadlines. Duplicate
events still consume read time and metadata transport within those deadlines.

Uncached DNS dependency fields use scalar JSON key extraction. A wide retained
policy or lineage must not spill merely because a record-expansion function
materializes it. Missing keys and explicit JSON null remain distinct; the full
source size still owns the 8MiB artifact and 32MiB batch limits. The existing v1
index derivation and tuple-version checks are unchanged. This addresses a
locally reproduced refusal; a production SQLSTATE alone does not prove which
expression or input caused that refusal.

Each entry contains its exact digest, logical size, format, storage location,
local representation and all native root bindings. Native retained inputs are
conservatively H, without asserting that their bytes or signatures have been
verified. The complement is U. C and D remain empty while independent cold
retention and discard dispositions are unavailable. Expiry, a stale consumer,
absence from the native closure or a verified/routed archive label cannot make
an input discardable. Existing TTL, recent-three-generation and pin protections
are unchanged. A v4 input with `storage_location=legacy` still uses separated
components; this field alone does not make it an old-chunk dependency.

`universe_complete` covers only the named `universe_sources`. It does not prove
all external references, physical orphan coverage, original SHA, remote GET
reconstruction, current/promised rollback or offline client compatibility.
`classification_complete` and `execution_authorized` remain false, with the
uncovered authorities listed explicitly. Logical byte totals are neither
physical allocations nor migration budgets. The sorted audit digest binds the
exact entries, native graph hash, snapshot, database/schema and uncovered
sources; it excludes diagnostic timing noise and is **not** an executable plan
hash. No mutation API accepts it.

On incomplete traversal, a missing dependency, inconsistent representation,
overflow or query failure, entries, totals and audit digest are withheld.
The failure receipt includes only an operator stage and a bounded category
(a validated SQLSTATE when available), never driver messages or SQL text.
Partial native diagnostics cannot establish an unreferenced complement. This
audit supplies the exact inventory needed by the remaining classification and
formal enforcement coordinator; it does not replace those steps or the isolated
same-version production restore and physical retirement proofs.

## Exact root obligations

`fugue-input-storage -mode=archive-root-obligations -root-obligation-targets='[{"kind":"artifact","id":"artifact-example"}]'`
adds a separate read-only diagnostic for 1–32 exact references. Generations
require their original kind and scope. Unknown fields, duplicate identities,
trailing JSON, write flags and unrelated options are refused before opening the
database. Use private operator storage for this identity-bearing report.

The native root predicates and same-RR resolver are unchanged. Each root now
has its original message release/expiry, current-LKG expiry, registered consumer
identity/heartbeat, task/pin state or latest original expiry across duplicate
LKG events when applicable. Current-LKG, latest-three-generation and consumer
roles have independent state protection: an expired timestamp or stale heartbeat
does not release them. An unexpired root's timestamp is not a disposal deadline
for the identity; all other roots and external obligations still apply.

The output preserves already-observed parent edges if a later resolver query,
graph bound or deadline refuses. Where time and binding/output bounds allow,
it includes one shortest observed path from every root to each selected target,
including unresolved targets. `declared_hashes_match` concerns only that witness
path, not every alternate edge, original bytes or signatures. The paths refer
to zero-based indices in `roots`. Raw observed edges remain available for a
bounded offline diagnostic if cancellation prevented path construction.

`observed_obligations` counts witnessed roots and reports state protection and
the latest witnessed time-root expiry. `strongest_root_known` remains false
while external sources and missing graph edges may impose stronger obligations.
No matching observed path is not evidence of no root. `observed_paths_complete`
only covers the already-observed graph; `traversal_complete` does not cover the
external generation ledger, original signatures/bytes or reactivation admission.
These sources remain explicit. `all_sources_complete`, `body_verification`,
`retirement_authorized` and `plan_sha256` are always false/empty.

The 60-second RR, 5-second statement, 250ms lock, 1MiB work memory, zero-temp,
16384-root/32768-node/262144-edge/524288-binding limits remain. Diagnostic output
has an independent 32MiB ceiling. Errors return exit 1 with sanitized categories
and the bounded observations, never a truncated proof, unreferenced complement,
disposable list, executable hash, SQL or driver text. This mode performs no
original input reads, object requests, pin changes, admission, routing or deletion.
