# Platform Artifact Release Runbook

## Trigger

Use this for edge route bundle, DNS answer bundle, Caddy route config, node
desired state, runtime placement, ranking policy, traffic safety policy, and
failure contract releases.

## Release Flow

1. Create draft artifact.
2. The store assigns a positive monotonic generation sequence, canonical
   SHA-256 content hash, supported schema version, and trusted provenance
   signature. Artifact creation fails closed if no signing key is available.
3. Validate schema, invariant, compatibility, and secret safety.
4. Release to shadow.
5. For the first generation in a scope, explicitly verify the shadow release and
   seed the initial verified LKG.
6. Release later generations to gray or full with the current verified LKG
   pinned as the rollback target.
7. Verify consumer convergence, local probes, public synthetics, watch window,
   baseline monotonicity, database rollback compatibility, and fencing token.
8. Only the explicit verification step promotes the generation to verified LKG
   and signs a new LKG snapshot.

## Commands

```bash
fugue admin artifact create --kind <kind> --file artifact.json
fugue admin artifact validate <artifact-id> --dry-run=false
fugue admin artifact release <artifact-id> --channel shadow --idempotency-key <key>
fugue admin artifact verify-lkg <release-id> \
  --fencing-token <token> \
  --allow-initial-lkg \
  --consumer-convergence \
  --local-probe \
  --public-synthetic \
  --watch-window \
  --baseline-monotonic \
  --database-rollback-compatible \
  --evidence-ref <ref> \
  --reason "<verified evidence>"
fugue admin artifact release <artifact-id> --channel gray --canary-rule-ref edge=<edge-id>
fugue admin release guard status
fugue admin artifact release <artifact-id> --channel full --idempotency-key <key>
```

## Safety Rules

- Full release is blocked unless a non-expired verified LKG and its exact
  artifact/hash/signatures remain readable and trusted.
- Artifact schema, canonical hash, generation sequence, and provenance
  signature are rechecked at release time, not trusted only because draft
  validation previously passed.
- Generation sequences are allocated monotonically per artifact kind and
  normalized scope. Ordinary publication must move forward within each
  release-channel lane.
- Publishing an older generation is only allowed through the explicit rollback
  operation. The deprecated `force_publish` flag maps to `soft_override` and is
  not a rollback mechanism.
- Full release enters `serving_unverified`; it does not overwrite verified LKG.
- Shadow and gray releases never set the production
  `serving_unverified_generation`.
- Gray requires one bounded selector. Global, wildcard, and multi-target
  selectors are rejected by the Safety Kernel.
- The release lane permits one active release and allocates a monotonic fencing
  token.
- Key rotation accepts a configured current or previous signing key; revoked
  key ids are always rejected.
- Secret-like content is rejected by validation.
- `soft_override` may bypass only ordinary artifact validation status and
  requires a reason. It cannot bypass schema, canonical content hash,
  signature, generation monotonicity, canary isolation, fencing, or pinned
  rollback requirements.
- `kernel_break_glass` is a separate, explicit, non-inherited permission. It
  can bypass only validation status, generation monotonicity, and a missing
  pinned rollback target for one transaction with dual confirmation and a
  maximum 15-minute authorization window.
- Neither override can bypass schema/hash/signature integrity, gray canary
  isolation, blast-radius caps, kill-switch precedence, fencing, LKG
  integrity/signature, or verification evidence.
- Override release and rollback records persist `override_mode`,
  `override_expires_at`, and `bypassed_invariants`; the same transaction appends
  a signed hash-chain security audit event.
- Only assert evidence flags after checking the referenced evidence. The
  verification request and evidence hash are audited.

## Verification

- Active release generation matches the expected artifact.
- Consumers report desired and actual generation convergence.
- Release state is `verified`, not `serving_unverified`.
- LKG points to the verified release, contains a verification evidence hash,
  matches the exact signed artifact, has a trusted snapshot signature, and has
  not expired.
- Repeating the same release idempotency key or verification evidence is
  idempotent.
- A normal `platform.admin` key cannot perform kernel break-glass unless the key
  also carries the explicit `artifact.kernel_break_glass` scope.
- A failed or expired break-glass request must leave the release lane, active
  generation, LKG, and security audit chain unchanged.
