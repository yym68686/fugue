# Exploration release prerequisite: retired DaemonSet inventory

## Observed facts

Read-only snapshots at 2026-10-12 16:35–16:53 UTC preceded any release for
business exploration. US Guardian reported `recovery_required`; its local health
reason was `workload pod inventory is incomplete`, while dependency and route
health were positive. No new serving regression is inferred from that status.

The primary Front DaemonSet reported desired/updated/Ready/available counts of
two and an observed generation equal to its generation. Its exact Pod template
selector nevertheless returned three non-terminal Pods. Two were Ready on the
currently selected nodes. The third was the historical BWG Pod, with a deletion
timestamp and Ready=False. BWG's current `fugue.io/schedulable=false` label did
not match the unchanged DaemonSet selector requiring `true`. The same retained
out-of-placement objects existed for both worker slots.

`KubeStore.localHealth` counted that retained Running/Terminating object before
checking readiness, so three objects versus two desired produced exactly the
reported health reason. This reproduces independently in a neutral fake-client
test. It does not establish causes of unrelated past health failures.

The attempted recovery also reported a monitor-pointer CAS mismatch. That CAS
remains unchanged and must not be bypassed. Fixing desired-inventory accounting
is not a claim that every recovery-ledger problem has been resolved; subsequent
runtime health and monitor identity must be independently verified.

## Narrow correction

Only a terminating Pod controlled by the exact current DaemonSet UID may be
classified as retired, and only after a fresh identified Node read proves that
it no longer matches the DaemonSet's explicit node selector. Missing identity,
missing Node, foreign ownership, missing selector, and still-selected nodes
remain fail-closed. Reads are bounded to 32 distinct retirement nodes. The
health evidence digest binds the retired Pod UID, Node UID/resource version,
and selector separately from the desired inventory.

Required Ready/updated/available counts, workload generations, immutable image
identities, route health, release membership, signatures, publication fences,
monitor CAS and positive LKG are unchanged. Deployment health is unchanged.
No Pod, key, backup, node role or membership is deleted or modified.

Tests cover the retired case and required-node, missing-required-Pod, unknown
Node, foreign-owner, non-terminating and incomplete-rollout counterexamples.
All cluster interactions in the tests must remain reads.

## Publication

Publish this prerequisite separately through the normal Guardian component
intent and CI. Verify the actual Guardian image and subsequent component health
before attempting US worker publication. The business-exploration code and
traffic intents are separate, and no exploration is activated by this fix.
