# Legacy release-policy recovery retirement

The Git-ref rollback recovery lane is retired. The default branch must not
contain any of these executable entrypoints:

- `.github/workflows/recover-control-plane-release-policy.yml`
- `.github/workflows/watch-control-plane-release-policy-recovery.yml`
- `scripts/recover_control_plane_release_baseline.py`

`scripts/test_release_policy_recovery_workflow.sh` is the permanent tombstone
contract. The ordinary deploy workflow runs it and fails closed if a retired
entrypoint or an executable reference to one returns.

## Retirement evidence

Before source retirement, the rollback writer was `disabled_manually`; its
watcher remained active only to freeze a failed recovery run and could not
start the disabled writer. The harmless, GitHub-hosted, permissions-empty
probe at commit
`3e0f148908b500d81134cd86d8169578488fd342` completed as run `29609856678`
(job `87981724149`, attempt 1). After the probe workflow was disabled, GitHub
rejected its historical rerun request and the run remained at attempt 1. Five
post-disable API-health and DNS samples then passed over the complete
observation window.

This establishes the prerequisite used for retirement without attempting to
rerun either historical privileged workflow.

## Checkpoint boundary

This retirement is policy-only and performs no Fugue, Kubernetes, workload,
node, DNS, network, image, or runtime-baseline write. It does not migrate the
legacy mutable baseline tag, create a forward-only baseline branch, register a
runtime v2 lane, add terminal state, or implement production rollback. Those
are separate checkpoints and must not be folded into this one.

GitHub Actions deploys exact code SHAs only. It must never execute or
orchestrate production rollback. After a future Fugue runtime write, rollback
means Fugue itself detects the bad update and restores the previous verified
runtime state through its fenced control logic. A policy-only failure before
runtime mutation is repaired with a new forward commit; branch, tag, and
commit history are never moved backward.
