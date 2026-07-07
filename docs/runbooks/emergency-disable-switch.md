# Emergency Disable Switch Runbook

## Trigger

Use this when an automated guardian, repair, ranking, exploration, or release
path is suspected to amplify an outage.

## Available Switches

- `FUGUE_ROBUSTNESS_REPAIR_DISABLED=true`: disables automatic repair execution.
- Release guard block: stop normal rollout until blockers clear.
- Edge draining or service edge exclusion: remove a bad edge from a scoped
  hostname or edge group.
- Artifact rollback: revert bad desired state through the release system.

## Procedure

1. Capture current evidence:

   ```bash
   fugue admin robustness status --json
   fugue admin release guard status --json
   ```

2. Choose the narrowest disable path.
3. Prefer rollback or scoped exclusion over manual live mutation.
4. Record expected blast radius and removal time.

## Verification

- The automated path stops mutating state.
- Existing LKG or last known good route continues serving.
- A follow-up issue or task records the permanent fix.
