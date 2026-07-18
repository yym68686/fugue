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

## Retirement checkpoint boundary

The retirement checkpoint was policy-only and performed no Fugue, Kubernetes,
workload, node, DNS, network, image, or runtime-baseline write. RP0 follows as
its own independently tested and released checkpoint; terminal state,
watchdogs, ref protection, runner cutover, lane promotion, Fugue settlement,
and production rollback remain excluded from RP0.

## RP0 forward-only baseline transport

RP0 replaces every executable use of the mutable baseline tag with
`refs/heads/fugue-control-plane-release-baseline`. The legacy tag may remain as
inert historical evidence, but no workflow or executable script reads, writes,
or rolls it backward. `scripts/test_release_policy_recovery_workflow.sh` fails
closed if that tag path reappears in executable source.

The last verified runtime baseline is commit
`92805aab5209348932b2c1db060e5c3c56ce4a2c`, deployed by run `29380409275`.
Artifact `8329699987` has digest
`sha256:4ff05d34019da02bc10dd8f465acb9166fb280334717d9f349851ff3bd5001bf`.
Its attribution, successful Helm revision 717-to-718 transition, central
CoreDNS gate, and 180-second observation are checked before migration. A later
failed candidate created revision 719 and the formal release script restored
revision 718 as revision 720. Read-only production inspection confirmed that
revisions 718 and 720 have identical manifest and complete-values hashes.

`.github/workflows/migrate-control-plane-release-baseline-rp0.yml` is hosted on
GitHub and receives no cluster credentials. Repository variables provide the
four evidence values; the workflow independently re-reads and verifies the
immutable run, artifact, deploy log, observation samples, and unchanged API
health before any repository write.

Run `29625628436` proved all evidence and health gates but failed before any ref
write because the Actions installation token cannot invoke GraphQL
`updateRefs`; intent artifact `8423746274` preserves that failure. Forward
candidate `1ce1b814c73b3c6bc823e527684495d7ec741e99` then reached the same
boundary in run `29626881801`, where GitHub rejected REST create-reference
with HTTP 403; intent artifact `8424148249` preserves that failure. Both
candidates are superseded, the lane is disabled, and the absent-ref count
remains zero.

The next independent prerequisite does not create or update a ref. After the
same evidence and health gates, it creates a canonical metadata blob, a tree
containing only `fugue-runtime-baseline.json`, and an orphan commit with stable
runtime-evidence time and identity. Every object is read back exactly and the
baseline ref must remain absent. A pre-write intent artifact survives object
materialization failure; a separate result artifact records all three object
SHAs on success. Reader validation and absent-only ref creation remain later
checkpoints. No PAT, local OAuth credential, additional secret, force update,
or Git history rewrite is introduced.

The existing deploy workflow remains disabled until its later promotion
checkpoint. If it is eventually promoted, its resolver requires the branch to
exist and its hosted recorder advances the branch only with the exact observed
OID, forward ancestry, and `force: false`. `deploy-control-plane-v2` is
registered but intentionally fails on a GitHub-hosted runner before checkout,
self-hosted scheduling, or any cluster command.

GitHub Actions deploys exact code SHAs only. It must never execute or
orchestrate production rollback. After a future Fugue runtime write, rollback
means Fugue itself detects the bad update and restores the previous verified
runtime state through its fenced control logic. A policy-only failure before
runtime mutation is repaired with a new forward commit; branch, tag, and
commit history are never moved backward.
