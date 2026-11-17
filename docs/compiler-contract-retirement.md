# Compiler contract retirement

The production API always emits `platform-config-compiler/v38`. The old
`FUGUE_COMPILER_INPUT_OMIT_ORIGINAL=false` deployment setting cannot restore the
v36/v37 writer. The pure compiler retains its historical modes for compatibility
fixtures; they are not production writer selection controls.

## Durable, staged admission

The additive schema lane installs `fugue_platform_compiler_contract` in **observe**
mode. Installing or restarting this migration never enables enforcement, changes
an existing signed generation, releases a pin, deletes history, or rewrites a
consumer's actual/LKG identity.

An enforced contract authorizes exactly v38, not an arbitrary numeric version
floor. Ordinary publication, rollback, LKG, task, consumer and selected-input
admission must then use the existing complete signed graph, local readback pins
and final tuple witnesses even if full-history admission is still in capability
mode. The final transaction locks and compares the contract epoch; an observation
prepared before the transition cannot commit afterward. Database root triggers
also reject direct legacy artifact/release/LKG/consumer writes from old binaries.
The ledger cannot be reverted, deleted or truncated by normal writes.

These database triggers are a direct-writer backstop, not a complete graph proof.
Do not enable the ledger with an ad-hoc SQL UPDATE. In particular, installing
this first-stage mechanism does **not** certify old controller/rollback binaries,
archive restores, offline clients or embedded DNS references as retired.

## Read-only readiness

Run the installed API-role tool:

```
fugue-input-storage --mode compiler-contract-readiness
```

It accepts no mutation flags and uses a bounded, coherent read-only repeatable-read
snapshot. It enumerates the existing retained graph, reports every non-v38
compiler artifact and its retaining roots, and always reports
`deletion_authorized=false`. An incomplete graph or exceeded budget is a failure,
not an empty dependency set. It follows the v1/v2/v3/v4 manifest readers and the
existing signed-source dependency decoders without fetching raw history.

## Remaining release gates

1. Replace every active/shadow/gray/full legacy scope through fresh capture and
   normal convergence/LKG verification. A recovery fence is not new evidence.
2. Preserve three genuine verified rollback generations per scope and resolve
   the transitive graph, including embedded prior DNS publications.
3. Explicitly reconcile or retire offline consumers, shadow receipts, pending
   tasks, pins, archive leases and external owners. An old heartbeat is not a
   release of retention or proof of stopped execution.
4. Verify current **and rollback** code, cold starts with empty caches, reconnect,
   import/restore and concurrent activation before a reviewed enabling transition.
5. Only then authorize tombstones and bounded collection. Shared v3 packs and v4
   components must remain until all references are gone. Relation allocation is
   not an estimate of deletable bytes; no `VACUUM FULL` is implied.

The first-stage release intentionally supplies no destructive enabling or
collection command. Until the gates are fulfilled, existing v37 runtime reads
and full-history retention remain available and the retirement is incomplete.
