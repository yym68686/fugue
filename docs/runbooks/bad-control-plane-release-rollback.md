# Bad control-plane release rollback

Use this runbook when a Fugue control-plane release introduces a platform-level block signal.

## Preconditions

- Confirm the failure is platform-owned, not tenant workload continuity unless the workload is an explicit release signal.
- Prefer the GitHub Actions control-plane release path. Use direct cluster actions only for emergency recovery.
- Capture `fugue admin release guard status --json` and public synthetic probe evidence before rollback when possible.

## Rollback steps

- Stop expansion of the current release.
- Roll back the control-plane Helm revision to the last known good revision.
- Restore or freeze public data-plane slot, route/DNS/Caddy generations, and node-updater desired generation if they changed in the release.
- Run `fugue admin release guard status`.
- Run platform representative public probes and direct edge `--resolve` probes.
- Keep any new gate in `shadow` or `disabled` until a soak window passes.

## Verification

- `release guard` returns `pass=true block_rollout=false`.
- Platform representative URLs return non-503 responses.
- No active edge group is reduced to zero healthy eligible nodes.
- Quarantine table has no release-introduced blast-radius violation.
