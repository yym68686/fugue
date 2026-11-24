# Independent hosted path execution

A preparation offer describes a change. Only a selected hostname and its
monotonic fence are a command. The private member directory is discovery,
never an acknowledgement. A selected command is bound to the verified parent,
child, exact enrollment and saved owner; it cannot change DNS address
declarations, TTL policy, certificate ownership or another hostname.

Only the serving Worker and authoritative DNS executor may advertise
`FUGUE_HOSTED_EXECUTOR_ADMISSION=true` via a **declarative component rollout**.
It defaults to false. Never enable it on behalf of an unnamed tenant, on
shadow or standby evidence, or as a substitute for a supported rollback.
Upgrade the control API and schema before enabling executors. Before the
first admission, establish the release set, expected members, durable cache
and an authorized change and withdrawal window. A missing required member
leaves an offer as an offer. Do not delete checkpoints to disable admission;
turning off *new* admission must not discard already owned hosts.

The executor fetches its own selected packet. A Worker reserves the hostname
across restarts and compares all routes against its applied group. A DNS
process keeps the host's zones and aliases together. Receipts are independent
POSTs: local UDP and TCP for each DNS zone/key, nonce-bound HTTPS for every
path, and a verified TLS handshake tied to the selected owner. The server
matches them against the authoritative command in a database transaction.
A failed batch cannot partially renew old successes. Receipt lifetimes end
at the earliest applicable proof, command or signed freshness limit. A
scoped receipt does not attest the entire global bundle; executor protocol
presence does not attest a host or its peers.

Acceptance requires the operator's two domains and their allowed changes.
Capture the original four path backends, B's independent target, both
authoritative transports, configured TTLs, and TLS owners. Verify B's new
target while A advances and again after A withdraws. Observe actual serving
endpoints in addition to per-host fences and receipts. DNS wire TTL may
fall with remaining proof lifetime; this is not a change to configured TTL.
Do not replace legacy Fronts with live connections or change their slots
as part of this test. A changed source authority or unsupported enrollment
must not be treated as permission to serve cached or historical commands.

### Reading the result

For an already desired plan, the platform-admin host-status endpoint reads its
own hostname decision before consulting any legacy global diagnostics. A
prepared selection is never a published forward. A partial forward or
withdrawal, expired evidence, or a changed parent, owner, business revision
or required member set cannot be verified with a green global baseline. The
optional `host_selection` describes one current fence; `release_id` is
reserved for the legacy global result. This is a read, not a new probe,
lease renewal or a substitute for an authorized public A/B exercise.

## Edge Worker release while the standby holds connections

Guardian reads the inactive Workers' own listener sockets before staging a new
candidate. When every remaining replacement is busy, the release stays in
`rollout_pending`. Its `workerDrainPlanDigest` identifies the admitted plan.
Guardian retains the desired record and serving LKG across restarts. The status
reason names the blocked nodes. The deployment job can time out while the
cluster continues watching the recorded release.

When a member becomes available, Guardian verifies the desired record and
serving declaration, prepares a fresh execution plan, compares the workload
identity and frozen membership, and checks again under the component lease.
Each Pod replacement still checks its activation and listener sockets.
An explicit successor can supersede the queued release against the same LKG.

### Retaining both physical listeners on a node

A logical A/B slot is not a TCP connection. A node may keep the old A
and B processes (and their existing streams) while a normal release stages
one replacement on a distinct, node-local listener. In an enrolled Front,
new connections resolve the local activation's **slot, Worker source and
immutable image** to a physical listener. Reusing the letter A for a new
image therefore does not require deleting the old A Pod. Rollback selects
the declared retained Worker for new connections; it does not close sockets
to the candidate. Each other node continues using its own activation.

The optional `FUGUE_EDGE_FRONT_WORKER_TRANSPORT` release input is a
canonical JSON object with fields, in order: `schema`
(`fugue.worker-transport/v1`), `group`, `node`, `retained` (exact
pins for A then B), `candidate`, `candidate_http_port` (38080) and
`candidate_https_port` (38443). Pins consist of `slot`, `source`
(full commit) and `image` (sha256 digest). The node and group must match
the configured process; all targets stay on the declared node IP. The
input does not admit a consumer, open a listener, create capacity, or write
activation. Without this input the existing A/B behavior is unchanged.

Do not enable overlap in a serving Front until the ordinary release owns the
exact replacement listener and immutable Worker on that node, verifies its
actual route/TLS readiness, binds the public Front and keeps both original
listeners. Missing state, an unregistered image or a wrong source refuses
new upstream connections; existing streams are unaffected. A watcher on
another node or a successful DNS lookup is not evidence of a handoff.
