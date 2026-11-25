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

For an opted-in node Worker journal, the Guardian records an immutable cutover
receipt only after observing connections on both the public and the
independent probe listener. It projects the same deadline across restarts and
refreshes only serving health. Other reachable Front host ports or Services
sharing the activation must be resolved before issuing the grant. An
unselected Front without an ingress may retain its established sockets; they
are outside the grant and do not by themselves prevent switching new traffic.

Without a retirement declaration, local completion releases the handoff fence
but leaves the old Worker installed. It never implicitly authorizes reclaiming
a Worker.

For fixed slots, a main-branch node Worker handoff may include `retirement`:
the exact old DaemonSet name, UID and generation and an `empty` or `deadline`
TLS witness. The witness selects how the verifier uses its socket; it does not
change the Guardian's retirement policy. Once the new Worker has verified
public and probe ingress, Guardian continues renewing health at the original
deadline. It retires as soon as both Front completion and the old Worker's
IPv4/IPv6 listener sockets are quiet, or at the deadline with the new Worker
still proven healthy. The old Worker's kernel inventory includes connections
arriving through a different Front.

Retirement changes only the journal node's eligibility for the exact old
OnDelete DaemonSet. It does not roll the other nodes. The same update persists
the retirement decision; Guardian can resume it after a lost update reply or
a restart. A terminal retirement receipt requires the absent old Pod, observed
controller generation, no replacement, and both former ports proven closed.
An unhealthy new Worker or a competing ingress withholds retirement. Do not
replace a Front in place while it is selected or holds sessions.

The next handoff may explicitly rearm a parked candidate from that terminal
receipt. This restores scheduling for the same node and pinned artifact
without changing the active slot. Changing the candidate artifact is a
separate release operation; rearm is not permission to install an unverified
image or overlap a third Worker.
