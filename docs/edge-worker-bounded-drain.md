# Bounded drain for Edge Worker sessions

This is the Front-side runtime contract. Configuring it is not an
authorization to stop or replace any Worker or Front.

New TCP connections select the current Worker, even if the predecessor still
holds sessions. When enabled, the Front tracks the selected physical transport
and activation generation before dialing. An existing connection is never
migrated or replayed.

An opt-in Front requires both
`FUGUE_EDGE_FRONT_WORKER_DRAIN_FRONT_UID` (its Pod UID from the downward API)
and `FUGUE_EDGE_FRONT_WORKER_DRAIN_GRANT_PATH` (a dedicated read-only authority
projection), exact activation, and an immutable node-local Worker transport
declaration. Never mount or populate the grant from a user or tenant-writable
source. No HTTP endpoint accepts grants.

An existing two-slot Front may use the fixed transport declaration for the
exact two installed Worker releases. This does not authorize a new Worker
image or an overlap listener. Moving the Front's public Service does not
change the Worker activation and does not create a Worker drain grant. The
optional grant projection remains empty until the authority has actual
evidence of an eligible Worker transition.

The authority must persist the first established cutover time (not a timestamp
reconstructed from connection age), after verifying that every public/probe
ingress capable of sending new connections has moved and a real connection
through the served ingress hits the promoted Worker. Project a canonical
`WorkerDrainGrant` for the exact Front, node and adjacent activation records.
The deadline is 600 seconds after that time. Reconciliation and authority
restarts must not rewrite the cutoff. At and after the deadline only a fresh
serving witness (up to 30 seconds old at the Front) permits force-closing.
Revoking the activation by rollback invalidates the grant under the same
activation write lock, even when a stale projection is still mounted.

When selected old sessions and in-flight dials have been continuously absent
for two seconds, the Front reports local completion. At the deadline it can
close both sides of only its pinned predecessor connections and cancel their
in-flight dials. Logs and the `worker_drain_last_*` metrics describe the last
local observation; they are not a reusable-slot or live health witness. These
metrics do not include a session held by a legacy or different Front.

The production integration must separately wire the release authority's
persisted grant and fresh witness, inventory all old and new ingress owners,
and enforce the retirement/rollback contract. A previous version still needed
for immediate rollback may remain running after all its sessions have ended.
A Front which does not support this contract cannot close old sessions via it.
Do not replace such a Front in place while it is selected or holds sessions.
