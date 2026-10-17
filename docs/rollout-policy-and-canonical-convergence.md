# Rollout policy and canonical convergence

Changing an enabled safe rollout's gate, canary steps, rollback window or retire
grace changes constraints for subsequent decisions. It does not require a running
image, spare Pod capacity or a successful prior code release. The queued operation
rebases against current intent and commits only the policy with an operation and
configuration fence. It does not create a release, update image tracking's deployed
digest, fabricate runtime readiness or overwrite concurrent image/environment/
storage changes. Protection enablement, execution mode and strategy changes remain
on the ordinary workload reconciliation path.

After a safe release has passed its gates and the edges and canonical Services
serve its immutable revision, a redundant canonical replica can still be pending
for scheduling. The operation may defer that convergence only after fresh proof
of the same promoted revision, complete healthy endpoint membership, bound workload
identity, unchanged traffic policy and positive edge acknowledgment. It records
the pending scheduling reason and retains the independent serving revision.
Unhealthy, incomplete, conflicting or absent evidence still prevents completion.

Idle reconciliation checks retirement before canonical readiness. Retirement keeps
its existing rollback retention, grace period, traffic withdrawal, exact revision
binding, positive drain observations and UID/resource-version deletion guards.
Thus an unschedulable replacement cannot indefinitely prevent safely drained old
revisions from releasing capacity. No failed executable or configuration update
invalidates the serving artifact or grants forced retirement permission.
