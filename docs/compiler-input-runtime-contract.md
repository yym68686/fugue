# Signed configuration runtime input

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
