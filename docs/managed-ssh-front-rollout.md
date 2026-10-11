# Managed SSH front rollout

The existing host-network SSH fronts serve active customer connections and must
remain running during replacement. The managed backend canary has passed public
client acceptance, but these older front binaries cannot emit the new exact-Pod
acknowledgements. This document describes the remaining ingress release work;
it does not authorize skipping its verification gates.

## Release and configuration separation

Build and verify a standalone SSH-front image through the existing `ci.yml`
entrypoint. The artifact receipt selects an immutable image and source; building
it does not select public serving configuration. An independent configuration
lane creates a new per-node candidate with its own durable route cache. Candidate
names bind image and node identity so an executable update cannot replace a
currently serving process in place.

Candidates use ordinary Pod networking and no host listening ports. The initial
enrollment declares only the two synthetic canary ports. A separately fenced
Service transport declaration can select a verified candidate on a node's exact
public address, with local endpoints. Current production HTTP/DNS transport
already uses this Kubernetes Service mechanism. Its connection-preserving
behavior for the SSH host-network predecessor must still be proved in an
isolated cluster before a canary public handoff. Existing front processes and
their established sockets remain alive. Removing our exact transport selection
restores the preceding path for new connections; compensation must refuse newer
ownership or generation changes.

Public handoff requires the candidate's exact Pod UID, node, image digest,
route-set digest, fresh persisted ACK, endpoint ownership, and successful host-key
probes. Test an established connection across selection and compensation while
new connections reach the selected process. Stage and observe each edge before
enrolling it as a required consumer for backend retirement. A backend's own
active-connection check remains required even when all new fronts have ACKed,
because retained legacy fronts can still hold older streams.

The transport declaration must support allocation changes without code changes.
Do not use a fixed single application, a hardcoded port, or a fixed active front
Deployment as the general publication design. A canary-only port declaration is
an explicit initial rollout scope, not a substitute for the platform allocation
contract. Workload control, traffic selection, credentials, and binary rollout
remain separately owned.

## Observed credential sources

Read-only checks on 2026-10-11 confirmed four current SSH-front nodes. Hashes of
their credential files and mounted Kubernetes Secrets were compared with the
control-plane node-token records in read-only PostgreSQL transactions. No token
was rotated, transmitted outside Fugue, or printed.

| Node | Group | Valid existing credential source |
| --- | --- | --- |
| `vps-591f4447` | `edge-group-country-us` | Secret `fugue-edge-token-vps-591f4447` |
| `vps-84c8f0a9` | `edge-group-country-de` | Secret `fugue-edge-token-vps-84c8f0a9` |
| `bwg` | `edge-group-country-us` | owner-only `/etc/fugue/edge-node.env` |
| `86eabf26-42a9-4f9d-998a-754115f8b829` | `edge-group-country-us` | owner-only `/etc/fugue/edge-node.env` |

The first two node-local files do not match current token records; their currently
mounted Secrets do. New candidates must use the explicitly reviewed source and
must not prefer a stale file over it. The two other files authenticate exactly
their respective nodes. The legacy US DaemonSet references one Secret across all
its replicas; do not weaken exact-node ACK authorization to accommodate that
older configuration. New candidates can reuse valid credentials without changing
any currently serving consumer.

## Remaining acceptance

- Isolated actual-image routing, durable ACK, and failed-publication retention.
- Isolated Service handoff and compensation with established TCP streams.
- Canary staging and public handoff on all four observed nodes.
- Same identity and current authorization after ingress and backend rollback.
- Fresh per-instance ACKs, backend active-session drain, and retained rollback
  material before retirement.
- Explicit new-binding transport reconciliation and stale/allocation rejection.
- A concrete Argus cutover and client-trust review after these prerequisites.

The original Argus port numbers remain unchanged. The explicitly approved
identity transition and this Mac's backed-up trust update are recorded in
`argus-managed-ssh-migration-review.md`. New managed fingerprints must never be
silently installed into other clients' trust files.

## Artifact and transport verification checkpoint

`verify_ssh_front_transport.py` creates its own randomly named kind cluster and
disposable TCP witnesses. It passed the real Service selection/compensation
sequence in 44.44 seconds: new connections moved from the host-network process
to the Pod-network candidate and back, while both held streams kept their
original executor and no Pod restarted. The test removed its cluster and client
containers. This is isolated transport evidence; production handoff still needs
its own canary observations.

The standalone `Dockerfile.ssh-front` and `managed_ssh_front_artifact` CI lane
build an immutable image without touching production. Image acceptance executes
the actual container and checks signed routes, an exact-instance persisted ACK,
the on-disk route cache, old-target connection accounting, route switching,
rejection of modified signatures, and continued LKG serving. It then runs the
isolated transport test. Only success produces a source/digest receipt; no
production configuration currently selects this artifact.

Local actual-image acceptance passed. The first attempt reached an ACK before
the local Docker host port was reachable; the test now separately waits up to
five seconds for its published TCP port and retains container diagnostics on
failure. It does not retry application payloads or weaken route verification.
Front requests now keep credentials in the Authorization header and remove
legacy `token` query parameters; both unit and actual-image tests enforce this.

The first front-artifact CI run passed prepush tests but stopped at declarative
release planning. The dependency expansion includes SSH-front code through
`internal/config` and the runtime package, including the SSH controller. The
retry enrolls the controller through its normal intent with the exact deployed
predecessor, while keeping front image selection independent. The release plan
must be checked before push; production traffic is not selected by this retry.

## Independent private candidate stage

The verified front artifact from commit
`6c63735c2ebc8c719ac4274d7178f8c2bd260a73` passed both actual-image and isolated
transport checks in CI run `38138692546`. `front-stage.json` selects that exact
image and receipt through the independent `managed_ssh_front_stage` configuration
lane. The lane verifies successful artifact-job provenance before creating new
versioned Deployments and narrowly scoped NetworkPolicies on the four declared
nodes. It does not create Services, bind host ports, modify existing DaemonSets,
rotate credentials, or select public traffic.

Each candidate has a separate durable cache, exact downward-API node and Pod
identity, an explicit valid credential source, and no Kubernetes API token.
The stage state records a generation and content digest; stale predecessors,
foreign resources, and changed executor identities are rejected. Previous slots
remain intact when a new generation is prepared. Live ACKs and host-key probes
remain required before a separate public transport declaration can select one.

Generation 1 staged successfully through CI run `38141035667`, job
`114474780170`. All four candidates became ready with zero restarts, and each
reported its own Pod UID and the same loaded route digest. Read-only database
checks independently confirmed fresh persisted acknowledgements for all four
UIDs. Authenticated SSH through each private candidate reached the unchanged
synthetic workload and matched its prepared host key. Public transport remains
on the existing front processes.

## Shutdown drain verification

A focused test reproduced an executor shutdown defect: `Run` returned on
cancellation while established proxy streams still existed. Returning lets the
binary exit, so preserving listeners during route changes alone was insufficient
for process retirement. The front now stops admission, marks readiness as
draining, and waits for held streams up to its configured shutdown timeout. At
the deadline it closes both sides of remaining streams; no new route activation
occurs during this phase. Normal rollout must still wait for zero connections
before deleting retained predecessors; the shutdown timeout is a bounded final
safeguard, not permission to interrupt active users.

The regression failed before the fix and passed afterwards, including the
forced-deadline case. Race tests passed. The locally built actual image also
preserved both old-target and new-target streams after SIGTERM and exited cleanly
after the clients finished. Future image receipts record
`graceful-shutdown-drain`; building that artifact does not replace the staged or
currently public processes.

CI run `38141570188`, artifact job `114476532682`, passed the real immutable
image and transport tests including graceful shutdown. Independent stage
generation 2 selects image
`ghcr.io/yym68686/fugue-ssh-front@sha256:3e9908984365189c0c7157fa4418f9ef2318f6aa01f7bf113ad0bfd04726649b`
and that exact receipt. It prepares new candidate slots and retains generation 1;
it still does not change public transport or restart either application.

## Public transport observation contract

`GET /v1/admin/ssh/front-acknowledgements/{edge_id}/{instance_uid}` exposes the
persisted acknowledgement to platform administrators. Reading never refreshes
the stored timestamps. The response separately evaluates whether the evidence
is fresh and whether it still matches the currently authorized route set. A
record from a different node/Pod, expired evidence, and a previously authorized
bundle cannot authorize selection. Ordinary tenant keys and edge tokens cannot
use this administrator inventory. Responses prohibit caching.

The transport gate must combine this API evidence with exact Kubernetes Pod,
node, image, Service, and EndpointSlice ownership. ACKs alone do not prove public
traffic reaches the process. `cmd/fugue-ssh-probe` accepts bounded JSON containing
an explicit address, authenticated public key, and expected fingerprint, then
verifies host-key possession with the existing SSH key-exchange probe. It stops
before user authentication and neither accepts user private keys nor modifies
client trust. This allows the CI transport verifier to probe both candidate and
public paths without deploying a probe binary on a production node.

## Fenced transport preparation

`transport-operation.json` selects one explicit stage, activate, or compensate
operation against a separate immutable selection declaration. The independent
`managed_ssh_transport` CI lane rejects superseded configuration, runs the
observation and selection tests, and compiles the read-only probe. The initial
declaration covers the two synthetic applications on the US primary edge and
only creates an internal Service. It has no external IP until a subsequent
explicit activation operation.

Activation requires the exact staged Service owner, UID, resource version,
selector, ports, candidate EndpointSlice, fresh persisted ACK, current managed
bindings, and candidate/public host-key probes. It rejects overlapping public
ownership by another Service. The JSON patch tests the entire previous Service
specification and ownership metadata. Authenticated synthetic SSH sessions span
the switch, and both pre-switch and post-switch connections must keep exchanging
data through the observation window. Verification failure compensates only the
same exact serving generation; a newer or foreign state is never overwritten.
Compensation changes only public selection and retains the old and new front
processes and existing streams. Test credentials and trust are temporary and do
not alter any user's SSH configuration.

This initial migration declaration is deliberately finite. Automatic management
of future platform-owned bindings and retirement validation against the actual
selected Service remain required before platform-wide completion.

The first internal transport run stopped before any Service creation. Its API
origin resolver incorrectly required Service type `ClusterIP`; the declared
API Service is type `NodePort` with a valid cluster address and port 80. The
regression reproduced this with a neutral NodePort fixture. The resolver now
accepts the standard cluster-address-bearing Service types while still selecting
only the declared ClusterIP and service port, rejecting ExternalName, missing
addresses, and mismatched ports. It does not use the external node port.

## Retirement follows actual public transport

Pod-network consumers require explicit `public_service`, `service_uid`, and
immutable `image` fields in the independent runtime declaration. Retirement
validates the serving Service owner, exact UID, declared selector, public node
address, local traffic policies, and the application's precise TCP port mapping.
Every EndpointSlice must belong to that Service and select exactly the ready
acknowledged Pod on the declared node. An unused private candidate's ACK cannot
satisfy these conditions. The transport is checked before and after the public
identity probe, and existing ACK freshness, old-stream accounting, retention,
signed backend drain, and zero-backend-connection checks still apply.

Tests reject private staging, compensated transport, another Service owner,
selector drift, missing or remapped ports, foreign EndpointSlices, old Pods,
wrong nodes, unready endpoints, and ambiguous endpoint sets. No production
consumer is enrolled until its public transport is selected and independently
verified; shipping this code does not itself authorize retirement.

## Automatic binding publication and remaining acceptance

The completed allocation API remains the authority for application public ports.
The reconciliation boundary is a separately declared set of serving ingress
Services: edge/node identity, Service UID, selected front image/selector, and an
allowed port range. The controller may add a port only for an enabled, committed
platform-managed binding whose exact target is verified and already appears in
the selected instance's fresh persisted ACK. It must never infer managed
ownership from a raw port number or import an application-native SSH endpoint.

Port publication must preserve every unrelated Service field and existing
binding, use resource-version and full-selection preconditions, and retry only
after rereading authoritative ownership. Configuration must grant update rights
only for explicitly enrolled Service names. Missing or stale front evidence
leaves the previous selection intact. Disabled/revoked access remains denied by
the signed backend authorization contract even while an old transport mapping
is retained for established streams. Safe transport removal and executor
replacement require their own drain and rollback observations.

Acceptance must include adding a new synthetic managed application after the
ingress policy is already active, proving its new public port appears without
changing code or the static migration declaration, and confirming that an
application-native endpoint is excluded. Repeat with stale ACK, replaced Service
UID, selector drift, a conflicting public allocation, and concurrent port adds.
This automatic publication boundary is separate from the finite migration
operation and production acceptance remains an explicit completion requirement.

The controller now implements opt-in `binding_publication` on each declared
Service-backed consumer. Its policy supplies an exact transport-intent digest
and permitted port range. Publication validates current backend evidence and
authorization, the exact node/Pod/image, persisted ACK, existing Service
selection, endpoint ownership, and conflicting Service addresses. A bounded
host-key exchange through the selected front's new listener must also match the
binding's authenticated public identity before the patch. It adds only
the new verified binding and records immutable logical ownership for that port.
Existing ports and unrelated Service fields remain unchanged. Replaying the
same binding is idempotent; reassigning a retained port to another binding is
rejected. Each Service currently supports up to 100 retained managed ports and
a bounded ownership annotation; exhaustion requires an explicitly designed
additional transport rather than silently discarding existing ownership.

The independent runtime publisher grants only `get` and `patch` on the named
Services; it does not grant arbitrary Service creation or deletion. JSON patches
test UID, resource version, full spec, annotations, and labels. Tests cover a
concurrent update between observation and patch, preserve the original port on
conflict, and reject stale ACKs, wrong identities/tenants, application-native
targets, revoked grants, absent routes, intent drift, and replaced Services.
The production runtime still has no enrolled consumers/publication policy, so
shipping this implementation cannot add production ports or retire old slots.

The first public canary selection attempt stopped during its initial read-only
observation because the active backend lacked fresh identity evidence; the
Service remained at staged generation 1 without external IPs. The observation
now distinguishes pending freshness from identity mismatch. A single bounded
three-minute deadline allows only those read-only observations to wait for fresh
controller evidence. Identity/ownership changes, API failures, and mutations are
not blindly retried, and the existing freshness thresholds remain unchanged.
The host runner also attempted to restore a Go module cache over existing files;
the transport job now leaves that existing cache alone by disabling setup-go's
archive restoration for this job. This does not change serving software.

The retry succeeded in CI run `38146166167`, job `114489863661`. The US primary
synthetic Service retained UID `2017d294-cfcf-468b-b646-79b90ef9f31d` and advanced
to serving generation 2 with public address `15.204.94.71`. Its scope remains
only ports 22004 and 22005. The operation verified candidate/public identities
and preserved both held authenticated sessions. Original Argus ports are not
selected by this Service. The next explicit operation exercises compensation to
private generation 3 while keeping the front processes and established streams.

Selection schema v2 can reactivate a compensated Service only when its exact
UID, previous intent digest, private selector/ports, and predecessor generation
still match. The new serving and compensation generations are explicit and
consecutive; an old operation cannot overwrite a newer selection. Repeating an
already selected operation performs current identity/endpoint/session checks
without another mutation.

Operation schema v2 declares an ordered, bounded list of node selections. All
declarations are validated for unique node and Service authority before any
write, then executed sequentially. Failure stops subsequent nodes and preserves
earlier successful selections. This supports a reviewable multi-node rollout
and safe retry after partial success. The first multi-node operation only stages
internal Services for the synthetic ports; activation remains a separate intent.

Production compensation passed in CI run `38146514520`, job `114490905008`.
The original Service UID remained unchanged, the generation advanced to 3, and
external IP selection was removed. Three observations at 14:24:22, 14:24:31,
and 14:24:41 UTC verified both candidate and public identities while both held
authenticated sessions continued exchanging data. Both executor generations
were retained. The four-node preparation now references this exact compensated
predecessor for reactivation and stages the other three internal Services.

An independent public-API observation found Python urllib receiving an
incomplete HTTP chunked response while curl HTTP/1.1 and HTTP/2 returned complete
ACK JSON. Matching curl's user agent, Accept-Encoding, and Connection headers
did not reproduce that truncation. Its exact source remains unproven, so serving
code was not changed on that hypothesis. Observation failures now report bounded
HTTP status, truncated byte count, or exception class without printing response
contents, credential-bearing URLs, or tokens. Internal API Service observations
used by the verified production transport operations succeeded.

## Four-node canary serving and runtime enrollment

CI run `38147871599`, job `114494918639`, successfully activated the two
synthetic ports on all four declared nodes. Each node retained its Service UID
and both front processes, checked candidate/public identities three times, and
preserved pre-switch and post-switch authenticated sessions. US primary resumed
at generation 4; the other three selected generation 2. The original Argus
ports were outside this migration scope.

Runtime generation 6 enrolls these exact Service UIDs, front image, node/Pod
selectors, public addresses, and transport-intent digests. Automatic publication
is initially limited to ports 22004-22100. Existing Argus ports remain outside
that policy. The rollback retention is explicitly one hour for this acceptance
stage; the only retained old backend belongs to the synthetic fixture and has
already been retained for more than three hours. An authenticated test session
is deliberately held on that old slot so signed drain and zero-connection
retirement can be verified without touching a customer's session. Runtime
publication remains independent of executable deployment and does not alter
application images or Deployments.

The transport CI budget is 40 minutes for up to eight sequential operations,
each with its own bounded three-minute observation deadline, probes, and
compensation. This accommodates the declared batch limit without changing any
evidence expiry or retrying mutations.

## Dynamic allocation and backend retirement acceptance

Runtime generation 6 deployed successfully. The retained synthetic backend
`ssh-8dc9fd07d4822467cd49a9d40b90f5b6` entered signed drain at 15:06:41 UTC.
Its health reported `draining=true`, one active connection, and one running
replica. That held authenticated session still exchanged data, while a new SSH
connection to the old slot was refused. After the test client closed normally,
the controller scaled it to zero, verified Pod disappearance, and recorded
retirement at 15:09:54 UTC. Its immutable identity Secret and bound state PVC
remain retained for recovery.

A third synthetic workload, `app_1791731173_3c446201c5fc`, was created after
ingress policy enrollment. It has no application SSH daemon and received a new
managed identity and public binding on port 22006. No code, runtime policy, or
static migration declaration changed to add that port. The controller added
22006 to all four existing Services and persisted its exact logical binding
ownership. All four public addresses authenticated to the unchanged target Pod.
At 15:15:26 UTC, four separately held public sessions were each visible in the
fresh ACK from their exact selected front Pod, proving the traffic reached the
new ingress rather than merely succeeding through a legacy listener.

The target Pod UID remained `277a9c42-fbc1-44e7-b46d-ba9d2a970014`, its container
ID remained unchanged, and restart count was zero. The updated protected
identity/anchor dump contains five identities; an isolated restore verified
every private/public pair and matched the new identity with authenticated API
inventory. Backup SHA-256 is
`2d2c36c1142396c12777d77ca9b24105f0506898e4bba12d1ef0bfbcdf545c59`.

Runtime generation 7 extends the same verified managed-binding policy to the
declared platform SSH range 22000-32000. Only applications with committed managed
identity, intent, route, and current verification may acquire a transport port;
application-native endpoints are not imported. Two read-only sessions opened
through the original poly/argus paths span this ingress change. Their already
approved host identities and application workloads remain unchanged. Existing
front processes remain running so their established streams are preserved.

Generation 7 deployed successfully through CI run `38150758680`, runtime job
`114503552188`. All four Services now contain managed ports 22001, 22002, 22004,
22005, and 22006 with their exact binding ownership; application-native port
22003 remains excluded. The poly and argus sessions opened before publication
continued exchanging data after the new ingress selection. Separately opened
authenticated sessions through every public edge were recorded by each selected
front's own ACK at 15:24:53 UTC. Both host fingerprints remained unchanged.
Original business Pod/container IDs and zero restart counts were reverified.
This ingress change did not touch the already enrolled client trust file.

An abrupt client-disconnect test used only the new synthetic workload. It
observed one temporary helper for its inspector, two while a test command was
held, and one again after force-killing only the local test SSH client. The
remote test process was absent afterwards. This verifies abnormal client
disconnect cleanup, not arbitrary host/process kill recovery. Additional race
tests verify connection/channel admission limits, release of capacity after
disconnect, natural authorization expiry closing active execution without a
configuration update, denial of subsequent expired-key login, and audit failure
preventing commands on an already authenticated transport.

The new synthetic workload was deliberately restarted through the normal app
reconcile API with an exact spec precondition. Its Pod UID changed from
`277a9c42-fbc1-44e7-b46d-ba9d2a970014` to
`4c502092-e3ac-4f94-87aa-a0963ac5a421`. The controller verified and selected new
slot `ssh-bba57410fb06e488c5e0ebe8952b888f` at route generation 2 while preserving
the logical host identity and port 22006. Native SSH through all four public
addresses reused the original pinned key and reached the new workload hostname.
The old synthetic slot remains retained under the declared rollback interval.
