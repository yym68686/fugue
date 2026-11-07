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
Partial native diagnostics cannot establish an unreferenced complement. This
audit supplies the exact inventory needed by the remaining classification and
formal enforcement coordinator; it does not replace those steps or the isolated
same-version production restore and physical retirement proofs.
