# Kernel Break-Glass Runbook

## Purpose

Use kernel break-glass only when the ordinary validated shadow, gray, full, and
explicit rollback paths cannot restore Fugue platform state. It is not a
general override switch and does not remain enabled after the request.

The operation may bypass only:

- `artifact.validated`
- `generation.monotonic`
- `full.pinned_rollback`

It never bypasses artifact schema, canonical content hash, provenance
signature, canary scope isolation, blast-radius caps, kill-switch precedence,
release fencing, LKG integrity/signature, or verification evidence.

## Preconditions

1. Confirm that the target artifact content hash and provenance signature are
   valid.
2. Confirm the exact artifact id or generation.
3. Record why normal shadow/LKG/rollback recovery is unavailable.
4. Use a credential with both `platform.admin` and the explicit
   `artifact.kernel_break_glass` scope. `platform.admin` alone does not inherit
   this authority. The bootstrap recovery credential is the root emergency
   identity.
5. Choose the shortest practical TTL. The maximum accepted value is 15
   minutes.
6. For a gray release, provide one bounded canary selector. Break-glass cannot
   make a global or wildcard canary valid.

## Release

```bash
fugue admin artifact release <artifact-id-or-generation> \
  --channel <shadow|gray|full> \
  --kernel-break-glass \
  --break-glass-ttl 5m \
  --confirm-kernel-bypass BYPASS_PLATFORM_SAFETY_KERNEL \
  --confirm-target <same-artifact-id-or-generation> \
  --reason "<incident, failed normal recovery, and evidence>"
```

For gray, also pass:

```bash
--canary-rule-ref edge=<edge-id>
```

The CLI rejects a missing or incorrect confirmation, a target mismatch, a
non-positive TTL, or a TTL above 15 minutes before sending the request. The API
and Store repeat all authorization checks inside the release transaction.

## Rollback

```bash
fugue admin artifact rollback <current-artifact-id-or-generation> \
  --channel <shadow|gray|full> \
  --to-generation <target-generation> \
  --kernel-break-glass \
  --break-glass-ttl 5m \
  --confirm-kernel-bypass BYPASS_PLATFORM_SAFETY_KERNEL \
  --confirm-target <same-target-generation> \
  --reason "<incident and rollback evidence>"
```

Prefer ordinary explicit rollback whenever it works. Kernel break-glass is only
for the exceptional case where the rollback target cannot satisfy the narrowly
bypassable release invariants.

## Immediate Verification

1. Inspect the release ledger:

   ```bash
   fugue admin artifact show <artifact-id-or-generation>
   fugue admin artifact lkg <artifact-id-or-generation>
   fugue admin release guard status --json
   fugue admin robustness status --json
   ```

2. Confirm:

   - `override_mode=kernel_break_glass`
   - `override_expires_at` matches the requested short window
   - `bypassed_invariants` contains only the expected allowlisted ids
   - fencing token advanced once
   - no unrelated release lane changed

3. Inspect `fugue ops audit --json` and locate the
   `chain_id=platform-safety` event whose target is the new release id.
4. Confirm the audit metadata contains the actor, artifact, scope, generation,
   release channel, reason, expiry, fencing token, and bypassed invariants.
5. Confirm public synthetics, direct edge probes, consumer convergence, and the
   required watch window before promoting the release to verified LKG.

## Automatic Restoration

There is no persistent break-glass mode to turn off. The authorization is
embedded in one request, validated against its expiry, and consumed by one
release or rollback transaction. Every later operation is evaluated with
default protection unless it carries a new independently valid authorization.

The release ledger keeps the expiry only as evidence. Expiry does not weaken,
extend, or revoke an already committed release; use normal release/rollback and
verification mechanisms to change serving state.

## Failure Behavior

- Missing explicit permission returns forbidden at the API boundary.
- Missing reason, invalid confirmation, target mismatch, expired authorization,
  or TTL above 15 minutes is rejected.
- Any non-allowlisted Safety Kernel violation rejects the operation.
- Audit signing, audit insertion, or audit-chain state update failure rolls
  back the release transaction.
- Failed requests do not advance the release lane, replace active state, or
  append partial audit records.

## Audit Integrity

Platform safety override events are assigned a monotonic chain sequence. Each
event hashes its canonical actor, tenant, action, target, metadata, previous
hash, sequence, and timestamp, then signs the event hash and provenance.

Ordinary audit append code cannot set chain fields. Retain the complete chain
and the verification keys needed for its full retention period. A gap,
rewritten field, broken previous hash, unknown/revoked signing key, or invalid
signature is a security incident; stop further break-glass use until the chain
state is reconciled from trusted evidence.
