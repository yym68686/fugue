# Platform Artifact Release Runbook

## Trigger

Use this for edge route bundle, DNS answer bundle, Caddy route config, node
desired state, runtime placement, ranking policy, traffic safety policy, and
failure contract releases.

## Release Flow

1. Create draft artifact.
2. Validate schema, invariant, compatibility, and secret safety.
3. Release to shadow.
4. For the first generation in a scope, explicitly verify the shadow release and
   seed the initial verified LKG.
5. Release later generations to gray or full with the current verified LKG
   pinned as the rollback target.
6. Verify consumer convergence, local probes, public synthetics, watch window,
   baseline monotonicity, database rollback compatibility, and fencing token.
7. Only the explicit verification step promotes the generation to verified LKG.

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
  artifact/hash remain readable.
- Full release enters `serving_unverified`; it does not overwrite verified LKG.
- Shadow and gray releases never set the production
  `serving_unverified_generation`.
- Gray requires one bounded selector. Global, wildcard, and multi-target
  selectors are rejected by the Safety Kernel.
- The release lane permits one active release and allocates a monotonic fencing
  token.
- Secret-like content is rejected by validation.
- `force_publish` cannot bypass validation, canonical content hash, fencing, or
  pinned rollback requirements.
- Only assert evidence flags after checking the referenced evidence. The
  verification request and evidence hash are audited.

## Verification

- Active release generation matches the expected artifact.
- Consumers report desired and actual generation convergence.
- Release state is `verified`, not `serving_unverified`.
- LKG points to the verified release and contains a verification evidence hash.
- Repeating the same release idempotency key or verification evidence is
  idempotent.
