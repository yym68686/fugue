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
authorizes automatic Edge release participation for exact Node UIDs. The deployed
API runs the same policy reconciliation independently of GitHub's cadence.
Main → Actions installs the policy and controller; the scheduled CI entrypoint
also remains available as a fallback and uses the same leases.
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
next bounded reconciliation.

The historical one-shot hold and member recovery lanes now run only when their
declarations change. Updating shared executor code must not replay old Pod UIDs
or reinstall a superseded manual hold over the automatic controller.

## Node-local Worker and global DNS handoffs

The node-local Worker lane discovers physical peers by group and public Front
Services by selector. Legacy peer pins are accepted for transports without a
node selector. A permanently enrolled peer can be omitted from live checks
only under the current UID-bound participation policy, after Kubernetes and
Edge heartbeats show it unavailable, its public EndpointSlices offer no
serving backend, and the active verified full and signed DNS plan exclude it.
The latter proof comes from the single-node API observation, not from the
producer's planned suspension. A missing peer, timeout, conflicting hold,
unverified DNS candidate or changed node identity does not count. Forward
handoff and rollback use the same per-transaction peer witness.

Global hosted publication normally requires fresh predecessor receipts from
its full cohort. A newly effective hold already authorized by the same signed
producer may instead stage a contraction after verifying surviving consumers.
The signed predecessor, its immutable expectations and LKG are not rewritten.
The new release preserves route/TLS behavior and cannot add DNS addresses or
combine unrelated DNS edits with the contraction. The candidate still needs
its own ordinary green receipts and serving proof.

The API's capacity recovery verifier resolves a returned node's actual public
Front and Worker against the latest completed node-local Guardian journal
when one exists, rather than comparing it to the older group slot. A
node-local Worker already at the committed target can be readmitted without
recreating legacy workloads. If the node-local code differs from the committed
target, the hold remains; the legacy recovery primitive cannot write that
activation. The installed API's reconciliation uses its loopback observation
endpoint and existing Kubernetes release leases. Where configured, the external
capacity sampler still depends on its separately deployed observation path; a
missing external sample cannot be replaced with in-cluster proof.
Node-local handoffs do not need a CI tick to install a redundant hold when
all independent isolation checks already pass.
