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

Materialization run `29630134601` completed successfully at policy SHA
`7b3bf0507926934f102e8baabbaa376453407958`. Intent artifact `8425179886`
has digest `sha256:403958307c8ecb8441c112986fc426738b4fe3c9f204af6d179c7a967a237250`;
result artifact `8425189766` has digest
`sha256:f929c0c798f8380ed55100bfe86f3667cdc618f6d5b7fcbcce2b27cecb857484`.
The canonical object chain is blob `1ab84b0dc7783f6fbd5796ed477005ffa0ead963`,
tree `f5fbfb2758190fbf5fddab701e625ef9046bb812`, and root commit
`0aca9c8869d7ac064d22c9b1e5477f30de4813b4`; independent readback passed and
the baseline ref remained absent. The one-shot materialization lane was then
disabled.

`.github/workflows/validate-control-plane-release-baseline-rp0.yml` is the
following independent hosted, read-only checkpoint. It accepts the exact
materialization run, artifact, digest, and root commit, revalidates the entire
metadata result and object chain, rechecks the historical runtime run and its
attribution artifact, proves runtime ancestry and unchanged health, and again
requires the baseline ref to remain absent. Its evidence preserves both the
materialization-result and historical-runtime run/artifact identifiers and
digests. It cannot write Git objects or refs and has no cluster credentials.

Unpublished reader candidate `f97c3441d55fda5fe7c80c6b540307ed2ff92b53`
was invalidated after independent review found that it compared the metadata
result artifact identity with the distinct historical runtime artifact
identity. It was never pushed or dispatched. The replacement candidate was
rebuilt from the last verified parent and treats the two provenance layers
separately; the invalid candidate remains evidence only and must not be retried.
Replacement candidate `aee5a8e2b03c1273fd69be3f8435a6fe65f6defd` was
also invalidated before publication when real-evidence execution exposed a
remaining `mapfile`/process-substitution dependency in the exact-change check.
The next rebuild uses command substitution with explicit status propagation
and a portable read loop; `aee5a8e2b03c1273fd69be3f8435a6fe65f6defd` must
not be retried.
Candidate `8995cd3575d34c44ae14b8a78452febdb520b37c` passed the
complete test suite and real read-only provenance execution, but independent
review invalidated it before publication: five `read` here-strings could mask
a nonzero command-substitution status after valid-looking output. The next
rebuild captures and checks each command status before parsing fields and adds
an explicit valid-output-then-failure negative test. `8995cd3575d34c44ae14b8a78452febdb520b37c`
is evidence only and must not be retried.

Reader candidate `50907bef344ae84df036d5d669f01686f33fab25` completed
as run `29639478372` after CI run `29639138116` and build run `29639138115`
passed. Artifact `8428166438` has digest
`sha256:bb45baccf26cdf5ebaba2b11973825cf0129c6c585069ef3989f2c102273c139`;
independent download, JSON validation, root-object readback, baseline-absence,
and production-health checks passed. The one-shot reader lane was then
disabled, and the runtime baseline was not advanced.

`.github/workflows/create-control-plane-release-baseline-ref-rp0.yml` is the
next independent hosted checkpoint. It accepts only the exact successful
reader run, artifact, digest, and validated root commit, repeats the object and
runtime-provenance checks, uploads pre-write intent evidence, and completes a
five-sample health window while the ref remains absent. Its final semantic step
rechecks all three dormant lanes and uses the REST create-reference endpoint
once, without force, to bind
`refs/heads/fugue-control-plane-release-baseline` to root commit
`0aca9c8869d7ac064d22c9b1e5477f30de4813b4`. Earlier HTTP 403 evidence came
from attempting to point the installation token at a code commit containing
workflow files; the new target is a one-file metadata root and requires no
workflow-file update permission. A bounded readback is the sole settlement
authority after the one write attempt: the exact metadata root succeeds even
when the transport response is lost, while persistent absence, unreadability,
malformed state, or any other object fails closed. No PAT, local OAuth write,
ref deletion, history rewrite, self-hosted runner, or cluster credential is
introduced.

The existing deploy workflow remains disabled until its later promotion
checkpoint. Its next dormant, read-only compatibility checkpoint teaches the
resolver to distinguish the canonical parentless metadata bridge from a
normal code baseline: only an exact one-entry root tree and byte-exact compact
schema-1 payload with one final newline can supply the runtime ancestor, and
the metadata commit remains the separately observed ref object. The recorder
is not changed by that checkpoint. A later independent
checkpoint must make advancement compatible before promotion; the hosted
recorder will still require the exact observed OID, forward ancestry, and
`force: false`. Its independent ambiguous-response prerequisite permits only
one `updateRefs` attempt and treats bounded exact readback as the sole
settlement authority. An exact target ref settles a transport failure or wrong
response echo; absent, unreadable, or different state fails closed without a
second mutation. `deploy-control-plane-v2` is
registered but intentionally fails on a GitHub-hosted runner before checkout,
self-hosted scheduling, or any cluster command.

GitHub Actions deploys exact code SHAs only. It must never execute or
orchestrate production rollback. After a future Fugue runtime write, rollback
means Fugue itself detects the bad update and restores the previous verified
runtime state through its fenced control logic. A policy-only failure before
runtime mutation is repaired with a new forward commit; branch, tag, and
commit history are never moved backward.
