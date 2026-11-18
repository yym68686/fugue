# Automatic participation of intermittent nodes

An authorized intermittent node retains its identity, desired workloads, storage
and resource policy during an outage. No replacement capacity is created.
Kubernetes Ready remains the actual observed state; this mechanism never sets it.

Rolling DaemonSets can declare `nodeParticipation: reachable` in the component
registry. Prepare captures a complete physical Node UID inventory from the
scheduled Pods, separates unavailable nodes, and binds that inventory into the
execution plan. Execute and terminal reconciliation restore the same inventory.
Forward, HTTP probes and rollback verify every participating Pod's owner, unique
physical node, readiness, source and running image. A participating node becoming
unavailable fails the operation; it cannot shrink the set. Missing inventories,
changed UIDs, extra nodes and an empty participating set fail closed. Deferred
nodes retain the shared desired template and Kubernetes catches them up after
reconnection. A later operation requires recovered nodes to prove current code.

`deploy/environments/production/node-participation/policy.json` independently
authorizes automatic Edge release participation for exact Node UIDs. The normal
CI entrypoint reconciles it every five minutes and after a successful push gate.
Both compatible code-release readers and complete baseline membership are
required before changing a release hold. Release and authority leases plus Node
UID/resourceVersion tests fence writes. Manual maintenance holds are preserved;
managing an existing network/quota hold requires explicit `adopt_hold` intent.

The state is `offline`, `recovering` or `active`. Repeated offline observations
perform no additional write. Recovery waits for the configured stable interval
and uses the existing member recovery primitive to prove current authority,
Worker executable, Front activation, fresh TLS and paired physical route proofs.
Only an inactive OnDelete Worker can be recreated. Same-slot stale code may
bridge through the retained previous authority, then recover to the current
authority; absent or conflicting history preserves the hold with a bounded
reason. Neither a new global authority nor a new serving bundle is invented.

Readmission additionally requires fresh heartbeat and a recent successful
recovery observation admitted by the existing signed DNS policy. The controller
rechecks physical identity, current code, authority and reader health under both
leases before removing its own hold. It never changes DNS, placement labels,
replicas, billing, volumes or manually maintained pauses. Each run uploads a
bounded evidence artifact; expected outages remain waiting states rather than
failed production releases. Recovery failures preserve the hold and retry on the
next scheduled run.

The historical one-shot hold and member recovery lanes now run only when their
declarations change. Updating shared executor code must not replay old Pod UIDs
or reinstall a superseded manual hold over the automatic controller.
