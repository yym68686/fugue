# Platform Artifact Release Runbook

## Trigger

Use this for edge route bundle, DNS answer bundle, Caddy route config, node
desired state, runtime placement, ranking policy, traffic safety policy, and
failure contract releases.

## Release Flow

1. Create draft artifact.
2. Validate schema, invariant, compatibility, and secret safety.
3. Release to shadow.
4. Release to gray with a scoped canary rule.
5. Release to full only after release guard passes.

## Commands

```bash
fugue admin artifact create --kind <kind> --file artifact.json
fugue admin artifact validate <artifact-id> --dry-run
fugue admin artifact release <artifact-id> --channel shadow
fugue admin artifact release <artifact-id> --channel gray --canary-rule <rule>
fugue admin release guard status
fugue admin artifact release <artifact-id> --channel full
```

## Safety Rules

- Full release is blocked by invalid invariants, consumer drift, or expired LKG.
- Secret-like content is rejected by validation.
- Force publish requires `artifact.force_publish` and a reason.

## Verification

- Active release generation matches the expected artifact.
- Consumers report desired and actual generation convergence.
- LKG snapshot exists for full release.
