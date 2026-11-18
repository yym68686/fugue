# Ordinary singleton rollout: capacity admission and connection drain

Capacity admission and traffic readiness are independent checks. The capacity
lock covers admitting the surge workload; it is not a signal that traffic has
switched. A deployment operation must still pass the normal traffic and durable
completion gates before the next operation for that same app can proceed.

For an ordinary stateless singleton public service, the controller can release
the capacity lock once the current deployment generation and ready Pod are
verified, the Pod is bound to a node, and its UID is present in the complete
cluster Pod inventory used by capacity preflight. The next deploy still runs
preflight; draining Pods continue to consume node resources. An inconclusive
inventory leaves the lock held, and a compensating rollout must reacquire it
after an early handoff. Migrations and independent Safe rollout revisions do
not use the early handoff.

For a canonical singleton Service using cluster-wide routing, the traffic
proof can ignore an older endpoint only when its Pod and ReplicaSet are proven
to belong to the same Deployment, its Pod is being deleted, and Kubernetes
explicitly reports the endpoint as not ready, terminating, and serving.
There must be a ready current endpoint for the same address family. A ready
old endpoint, an unknown condition, missing replacement, or ownership mismatch
still fails the proof. Nothing here shortens termination grace, deletes old
Pods, changes the routing configuration, or authorizes more capacity.

Attribute waiting using `rollout_capacity_lock` in the operation timing and
`rollout capacity lock released` (`surge_accounted` or `operation_exit`) in
controller logs. A long startup or genuine capacity shortage can still delay
admission; this change does not set a fixed latency target.
