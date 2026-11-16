# Signed configuration runtime input

## v38: complete invocation input is not retained

The API's explicit `FUGUE_COMPILER_INPUT_OMIT_ORIGINAL=true` persistence policy
selects `platform-config-compiler/v38` for **new** compilations, including manual
and producer compilations. The default remains false for reader-first rollout.
Compilation still validates complete, fresh evidence before projecting inputs.
Signed lineage preserves the complete invocation's `input_snapshot_digest` as a
commitment and binds the independently verified compact `runtime_input_digest`.
Only the latter is persisted in compiler-input storage. A digest commitment is
not original readback or exact full-input replay. No v36/v37 artifact or retained
body is rewritten or deleted, and no legacy audit binding is created for v38.

The v38 artifact's **signed metadata** additionally binds
`quality_evidence_digest`. Its separate object contains only typed DNS selection
records with physical/current or retained comparison evidence and their
original clocks; arbitrary Facts, route inventory and full snapshots are absent.
This pool is optional, not a serving, activation or rollback dependency. Its
payload budget is 1 GiB, at most 4096 objects, and at most 64 MiB per object.
Expiry is at most 14 days from the original input capture (not a replayed insert
clock). Oversized, empty or already expired objects are skipped; inserts delete
expired objects first and then FIFO by immutable first insertion time and hash.
Concurrent PostgreSQL writers share a transaction-scoped advisory lock. A
minute loop removes expired objects even without new writes. PostgreSQL vacuum
and file reuse determine physical allocation; the payload cap is not a promise
that relation files shrink or occupy exactly 1 GiB. Metrics expose budget,
payload, count, cleanup failure, deletions, skips and unavailable reads.

Quality restore/comparison reads verify the selected artifact signature, exact
evidence digest and intent/policy identity, with the existing LKG, receipt,
candidate, policy and freshness checks. Missing evidence never clears independent
live observations, grants health, renews observation clocks or lends an expired
serving lease. Fresh probes remain independent. Ordinary cold reads, DNS source
capture, current/LKG reads, publication admission and configuration rollback use
the retained operational closure, including exact intent/policy sources; the
raw invocation is not a promised recovery dependency of this version. Explicit
tasks that separately name a full input digest are still required to resolve it;
v38 does not silently rewrite or exempt old task obligations.

Administrative full/default input reads return HTTP 410 with an explicit
not-retained message for v38. `view=dns_configuration` still verifies and returns
the frozen operational projection. It never reconstructs an alleged original
from live state. Full v36/v37 queries preserve their old behavior.

Roll out readers and additive schema first with the writer flag false. Confirm
both API replicas and schema are healthy; then enable the writer in a separate
declarative API release, whose previous Git LKG already supports v38. Existing
consumer payload and Lineage fields do not change: the new version and evidence
metadata are signed strings, not unknown nested schema fields. This change does
not implement collection of old compiler inputs/artifact bodies, a 5 GB budget
for protected configuration history, or source-upload deletion.

## v36/v37: existing full-history contracts

An explicit `PolicySnapshot.runtime_input_mode: configuration-v1` creates a new
compiler generation (`platform-config-compiler/v37`). The resulting signed
lineage binds both `input_snapshot_digest` and `runtime_input_digest`.
Policies without this option retain the existing v36 behavior. This option must
not be enabled until every relevant reader and promised rollback binary can
consume it.

The runtime snapshot retains the typed configuration observations, identities,
selection records and original timestamps/expiry. It omits arbitrary replay
`facts` and raw DNS physical-evidence transcripts. The new digest authenticates
the entire independently persisted runtime snapshot. It is never a partial
verification of the original snapshot. It does not provide missing physical
quality evidence or renew a stale observation; operations requiring fresh
quality observations must still obtain and verify them.

DNS source capture, retained traffic partitions and the administrative
`dns_configuration` view read this exact signed operational input. A missing,
corrupt or inconsistent runtime input refuses the operation instead of reading
the original history or live business data. The administrative full view still
reads and verifies the original full snapshot and its input-pin admission.
Dynamic quality restoration may inspect only the signed operational selection;
omitted physical evidence is unavailable, never treated as verified evidence.

Operational admission follows the new runtime dependency. Retention inventory
and explicit full-history reads retain **both** input dependencies. Existing
full-input provenance, pins, rollback and history obligations are not released
by opting in, and no old artifact is rewritten. The migration-only public
retirement path and unused full-history enabling coordinator have been removed.

Full retention also resolves both original compiler source generations (intent
and policy) by their exact kind and scope. Missing or conflicting declared
lineage makes the inventory incomplete. Typed lineage digests differ from
artifact content hashes; metadata traversal does not authenticate the original
sources or replace full local readback and authority verification.

Local coverage includes persisted file-store cold reads after removing only a
synthetic original, retained-route reuse after a fresh Store instance, an actual
disposable PostgreSQL18 separated-input read without original chunks, and a
PostgreSQL retention census that keeps both digests H. Corrupt operational
input refuses even when the complete original is present. Existing v4 complete
SHA/corruption refusals remain unchanged.

Full reads reconstruct and verify the entire selected input in one database
snapshot. They do not synchronously build the optional DNS selection index;
the selective reader prepares that bounded index on its own cold read and
continues to verify physical witnesses on warm reads. Administrative full and
DNS-projection requests, DNS producer capture and retained traffic reads pass
their caller cancellation/deadline through the input reader, including the
legacy content fallback. Complete reads have a two-minute maximum, matching the
bounded writer/full-readback budget; a shorter caller deadline still wins.
Selective DNS reads retain their separate 30-second budget. A small projection
response still requires complete input verification. Configuration clients must
allow the complete-read budget when projecting large legacy inputs, and source
selection, LKG and observation freshness are rechecked after reading. Extending
the work budget never renews evidence or changes integrity/admission rules.

These tests do not prove a real scope's complete minimal recovery set. Before
production policy activation, complete the effective scope/consumer/task and
promised rollback inventory with actual stored bytes; prove compatible readers;
and exercise the real same-version current, DNS, publication, cold start,
rollback and task paths in an admitted confidential environment with old chunks
and cold archive inaccessible and caches cleared. Measure the added storage,
peak WAL and growth against the original resource gates. New writers no longer
write legacy JSONB/chunks, but that alone does not prove all old writers have
drained. Historical disposition, complete reader/rollback coverage and schema
retirement still require their own evidence; no public retirement operation is
being completed as part of runtime activation.
