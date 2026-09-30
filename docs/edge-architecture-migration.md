# Edge topology migration

The serving system still uses `edge_group_id` as its compatibility identity.
`deploy/edge/topology.json` is a separate placement intent. Its authority cell
maps each existing group to a neutral release boundary; a serving pool can span
multiple cells; an Edge has a stable ID and explicit risk dimensions. Country
and region are labels, not risk domains or release identities.
The `legacy_group_id` alias is transitional. Once a cell's runtime, release
ledger, inventory and LKG use the neutral cell ID, remove the alias in a
separate verified configuration step.

Neutral Worker convergence uses a distinct consumer identity
`edge-worker:<cell-id>:<edge-id>`. The stable physical Edge ID is unchanged.
The Kubernetes-bound credential exchanger requires the explicit `authority_id`
in the operator-managed consumer annotation to match the Pod authority label;
the signed claim, returned identity, immutable expected member and cohort must
all agree. Route/TLS assignments and positive or negative heartbeat cursors
cannot cross cells or overwrite the legacy `edge-worker:<edge-id>` record.
Expected sets for neutral Edge inventory use that cell namespace; legacy
sets and their topology digests retain their original representation. The
new identity never copies an old observation or promotes LKG. Deploying the
reader does not enroll a Pod, change inventory, prepare a new expected set,
or authorize a neutral publication.

DNS consumers use `dns-server:<cell-id>:<physical-node-id>` for an explicitly
declared neutral authority. Zone telemetry aliases collapse only within that
authority; legacy and neutral receipts on the same physical node keep separate
monotonic cursors. The original encoded identity must agree before projection
can fold an alias. Neutral DNS credentials require a Kubernetes Pod binding,
and the live Pod policy and authority label must still match when reporting
facts. Neither identity issuance nor a larger heartbeat sequence selects a
backend. Runtime-fact reads resolve each retained receipt's authority from its
immutable expected member, then require the uniquely selected public Service
and EndpointSlice Pod. They recheck publication and transport around the read
without copying observations or renewing deadlines. This API capability does
not enroll neutral DNS executors or prepare their traffic cohorts.

The neutral DNS client now asserts its configured cell during both shadow and
serving credential exchange, and uses the returned scoped consumer identity
for positive and negative receipts. Missing or foreign authority fails before
artifact download or durable cursor/checkpoint writes. A neutral DNS executor
cannot carry a legacy inventory token or send the flat inventory heartbeat;
its node ID remains the physical Kubernetes node. Existing positive checkpoints
remain bound to their original node and authority and are never relabeled.

The first neutral DNS candidate is an independent Deployment with explicit node
placement, a retained PVC and a dedicated Pod ServiceAccount. It exposes only
private process health to the API observer; it has no public Service address,
host port, host cache, legacy inventory credential or DNS ingress permission.
Both process probes use liveness while missing signed configuration leaves
serving health unavailable. Its initial Recreate lifecycle is for isolated
staging only and must be replaced by an independently verified serving lifecycle
before public transport can select it. New cohorts, proof egress and listener
selection require separate configuration and evidence.

Neutral Controls obtain their RouteIntent identity through a live Pod-bound
ServiceAccount exchange. The operator-managed annotation must name the exact
cell, configured artifact scope and only `edge_route_intent`; its authority must match the
Pod label. The API signs a two-minute credential with its independent
RouteIntent keyring without exporting the issuer key. The private TLS listener
admits only canonical GET RouteIntent and POST credential-exchange paths with
the exact Host and SNI. The scoped identity cannot read another cell or a legacy
group, while legacy issuers cannot read neutral cells. This identity is not
permission to publish: the requested cell still needs a prepared signed traffic
release with its own complete route/DNS/TLS cohort.

The isolated `edge-control-public-a` candidate uses an explicitly declared
physical node and its own retained PVC. It has no host ports, external Service
address, legacy activation directory or Core issuer-key mount. Projected Pod
credentials are exchanged over the same pinned private TLS origin for every
RouteIntent read; invalid exchanges cannot fall back to a local issuer.
Its four cell-specific keyring projections are optional during staging so
configuration absence does not prevent installing or recovering the process.
Missing signing, reader, inventory or recovery keys still fail closed at their
respective operations. Process readiness is not publication readiness: until
independent trust and a complete signed cell traffic release are provisioned,
the candidate must report no serving publication and cannot replace either
legacy Control's positive LKG. Staging does not remove a country alias or
change any Front, DNS listener, Agent policy or existing Worker.

Initial cell keyrings are independent configuration in
`deploy/environments/production/cell-trust`. Git records exact Secret names,
stable Edge membership and digests only; private material remains in the
encrypted production `FUGUE_EDGE_CELL_TRUST` configuration. The generic
`cell_trust` CI lane selects one changed declaration without a code-release
dependency, checks all material and existing resources before any create, then
verifies the result. It neither generates replacement keys nor adopts or
overwrites an existing Secret. Replaying the same generation restores missing
projections or resumes a partial initial installation using the same keys.
The bootstrap schema rejects key rotation; later rotation needs an explicit
overlap declaration that preserves the serving artifact's verification keys.
Installing trust does not seed inventory, publish a bundle or authorize public
transport.

The first neutral Worker is an isolated Deployment with a distinct retained
PVC and stable physical Edge identity. Its projected heartbeat fence is true,
and it mounts no legacy Edge token or host identity file. A missing legacy
token is accepted only with a canonical cell identity, complete Control bundle
and inventory sources, a Pod credential path and no dynamic/legacy desired-state
reader. Route/TLS consumer credentials and reports bind the cell explicitly.
It cannot write the compatibility heartbeat or inherit a legacy consumer's
receipt. The management-only Service and NetworkPolicy expose no public route
or host port. The staging readiness check is process liveness; `/readyz` still
reports unavailable while the independently authorized publication is absent.
The retained activation directory starts empty. Staging neither invents an
activation epoch nor seeds inventory, serving health, current or positive LKG.
No retained Front or existing Worker participates in this code release.

The topology file contains no endpoints, health, traffic weights, loaded bundle
digests, or code revision. Those are runtime facts or signed artifacts. A pool
membership is not route authorization. In particular, the current public
DiscoveryBundle lists globally healthy Edge nodes and does not grant any tenant
or hostname access. No consumer may expand a signed route's serving scope based
only on this file or on the discovery audit.

The same topology schema can now be carried in a signed static `PlatformIntent`
as `edge_topology`. The producer preserves it in the projected intent and its
lineage. The field is optional, and neither its presence nor a clean audit
changes route bundles, DNS answers, or current Edge selection. Serving
authorization still requires an explicit grant derived from that signed intent
and matching runtime facts.

`edgetopology.EligibleCandidates` is a non-serving compiler for a verified
tenant/hostname grant and authenticated runtime facts. Its grant must list every
path route digest required by the hostname for each authorized authority cell:
the existing route proof includes the serving group ID, so digests differ
between cells. Cells omitted from the grant are not candidates. Each Edge must have current proof
for all of them, the matching TLS hostname, health, capacity and pool/capability
membership; exclusions, optional residency and requested risk diversity are
hard gates. An optional `edge_selection_constraints` entry in the signed
PolicySnapshot now derives a hostname grant from the exact projected route
artifact, every path's cell-specific digest, topology, and existing DNS
placement constraints. The grant is embedded in that signed route artifact
only when explicitly configured. Compilation rejects an unknown tenant,
inactive path, missing DNS dependency, impossible candidate minimum, or an
unproved cell. It does not alter the current DNS answer or Edge bundle.

Platform services use explicit `owner_kind: platform` with an empty tenant ID.
Every path must be a `platform`, `platform-route`, or canonical
`control-plane-*` route with no application or tenant owner.
Omitted `owner_kind` retains the tenant grant contract and requires a tenant ID.
This distinction lets Agent control requests obtain a hostname grant without
inventing a tenant or admitting application routes into platform authority.

The caller must bind both the grant and authenticated facts to the current
signed TrafficReleaseSet before using any result for serving. Runtime fact
collection, shadow comparison with the old selection, and cutover gates are
still outstanding. The grant alone cannot affect DNS or Agent traffic.

Runtime Agent already initiates HTTP control requests through `ServerURL` in
`internal/runtime/agent_service.go`. This is a real selection surface even
without an Agent-to-Edge business tunnel. Dynamic control-request transport
must keep the configured API hostname and TLS identity, choose only fresh
authorized endpoints, and avoid automatically replaying mutating requests.
Bootstrap, enrollment, heartbeat, operation polling and completion must retain
their existing authentication and retry semantics.

The public DiscoveryBundle currently uses HMAC authentication. Its signing key
must not be distributed to Runtime Agents merely to let them verify discovery:
that would also let a verifier mint authority. Independent client validation
requires a public verification mechanism and separately managed trust anchors,
bound to exact publication, hostname, audience and absolute expiration. An
authenticated API response or a healthy TLS endpoint alone is not a replacement
for that candidate authorization.

The `agentedge` package supplies the optional AgentService control transport.
Ed25519 grants bind one runtime audience, canonical HTTPS
origin, each cell's independent traffic publication and route requirements, cell/host diversity,
signed selection policy and original evidence deadlines. Verification accepts
only independently provisioned public keys and rejects replay, equivocation,
unknown fields, duplicate JSON keys, revoked trust and expired observations.
Selection uses measured latency, independent standby cells, consecutive winning
rounds and cooldown; duplicate or mixed-grant observations do not advance it.
The selected HTTP client preserves Host, SNI and certificate verification,
rejects other origins, disables redirects and does not replay a mutating request
after a lost response. Requests and response reads cannot outlive the grant.
The API exposes runtime-authenticated `GET /v1/agent/edge-candidates`. Its
independent `agent-edge-control` policy publication pins a validated topology,
selection bounds, an explicit availability floor and a separate desired backup
count. It intersects existing route and DNS constraints; a hint can preserve an
already eligible Edge but cannot authorize it. Each candidate requires the
original Edge heartbeat, authenticated Kubernetes identity and capacity facts,
and HTTPS proofs for every path on the API hostname. The API rechecks policy
and cell publications before signing and never extends those evidence leases.
The admin preview performs the same observations but does not sign or authorize
traffic. The admin trust endpoint exports only public keys. A missing private
keyring leaves issuance unavailable; requests never generate a new trust root.

The Agent's availability floor is distinct from the quorum for publishing a
public DNS address set. Its signed constraint is intersected with explicit
per-route Edge selection constraints; the general DNS `minimum_healthy_edges`
and route address-publication minimum do not silently raise the control-client
floor. Both compilers retain the same ownership, route-policy, pool, residency,
capability, endpoint exclusion and route-digest checks. Public DNS compilation
continues to enforce its original quorum without modifying its policy or answers.

Private signing configuration and Agent public trust are separate bounded files
with explicit generations, key lifetimes and revocations. API signing reads
`FUGUE_AGENT_EDGE_SIGNING_KEY_FILE`. AgentService opts in only when
`FUGUE_AGENT_EDGE_TRUST_FILE` points to independently provisioned public trust.
`FUGUE_AGENT_EDGE_CHECKPOINT_FILE` defaults to `edge-checkpoint.json` in its work
directory and must be kept on persistent storage. AgentService first collects
shadow measurements while retaining its existing request path. Before the first
full Agent policy, the initial shadow publication can issue only shadow grants.
An active grant requires a full policy publication and latches the transport to
authorized candidates. Heartbeat, polling, image reports and operation results
then use that transport with their existing payloads and retry semantics.

The Agent refreshes grants separately through the configured HTTPS hostname,
keeps original deadlines during acquisition failures and persists accepted
policy/cell publication floors, activation state and the trust generation before
using new permissions. Restart never restores latency measurements. Old trust
cannot reverse a persisted revocation, and missing or corrupt checkpoints do not
silently reset an existing checkpoint to a valid native connection. Authenticated
negative route proofs disqualify a local candidate immediately; transient absence
uses the signed failure threshold. Grant expiration or an unmet hard floor blocks
control requests after activation instead of selecting an unauthorized endpoint.
Renewal probes the new signed candidates before installing them. The selector
atomically replaces the permission and its validated measurements only after
the new checkpoint is durable. Failed replacement probes retain the previous
positive grant to its original expiry; authenticated negative proofs still
disqualify the affected endpoint immediately. This avoids an unmeasured interval
between accepting a new route publication and the next periodic probe round.
Explicitly publishing a shadow policy after activation pauses selected requests;
it does not grant an implicit native bypass. Production Agent opt-in, real fleet
observations and active-policy publication remain rollout steps. Installing this
API code does not activate a policy or change existing Agent, DNS or business
traffic.

Agent trust is declared in
`deploy/environments/production/agent-edge-trust/package.json`. The independent
`agent_edge_trust` CI lane validates the encrypted production environment secret
`FUGUE_AGENT_EDGE_SIGNING_KEYRING` against that public declaration, then writes
the public ConfigMap and private Secret using generation and Kubernetes
UID/resourceVersion guards. It runs independently of component builds and does
not restart workloads or publish selection policy. A partial write can resume
at the same generation; drift, replay and replacement of foreign resources are
rejected. API mounts the Secret as an optional, read-only projected directory,
so missing trust cannot prevent the API from serving its existing configuration.

Generate a new root only through the explicit `fugue-agent-edge-keyring generate`
command, with an absolute private output path, key ID, generation and absolute
validity dates. It refuses to overwrite an existing private file and prints only
the public projection. Rotation first distributes a keyring containing both old
and new public keys, then publishes a new policy selecting the new key. Keep the
old public key until its grants have expired; removal or revocation is another
explicit higher-generation trust declaration. Neither API requests nor this CI
reconciler can manufacture a replacement root.

The initial observational policy is declared in
`deploy/environments/production/agent-edge-policy/shadow.json` and published by
the independent `agent_edge_shadow_policy` CI lane. The publisher validates the
immutable artifact through the API, checks its predecessor before publication,
uses an idempotency key and reads the resulting authority back. It accepts only
shadow policies and stops if full Agent authority exists. It never submits LKG
verification claims. Actual Agent observations and a separately reviewed full
policy publication are required before active control traffic can be accepted.

`runtime-agent-canary` is an isolated first Runtime Agent release lane. Its
private external runtime identity is declared separately and its credential
Secret is immutable: replay reuses that exact identity, while an existing
runtime with missing credentials blocks rather than rotating its key. The
canary has no Kubernetes service-account token and cannot apply workloads. Its
own persistent volume retains trust and publication floors across restarts and
rollback. It does not change any application's runtime assignment. This canary
provides real Agent heartbeat, operation polling, shadow measurement and later
selected-transport evidence; creating it is not evidence that the rest of the
fleet or authority cells have migrated.

`active.json` declares the initial active policy and the exact canary image
source used for acceptance. The independent `agent_edge_activation` lane first
collects a bounded window of the actual Pod UID/image, independently verified
checkpoint signatures, measured primary/standby diversity, renewed grants and
fresh Runtime Agent heartbeats. Recent permission gaps or control failures stop
the window. It retains that witness before seeding the observed shadow policy's
LKG and publishing a full active policy with identical constraints. A second
window verifies the selected transport before active LKG promotion. The lane
does not promote based only on a green Deployment or an unsigned preview.
An explicit observation bound can allow a recovered loss of the desired standby
while the signed hard floor and a live measured primary remain intact. It must
retain ordered timestamps, grant continuity, the recovery duration and any
acquisition failures in the witness. An acquisition 503 must be followed by a
different independently verified grant within the same bound. Unknown errors,
missing history, expired permissions, control failures and unrecovered loss
still reject the window. Every acceptance sample requires restored cell
diversity. The Agent accelerates candidate refresh to the signed probe cadence
while degraded; it never extends the old lease to make this check pass.

The control loop wakes at the signed probe/refresh deadlines instead of
rounding each one to a fixed five-second tick. Slow network work does not add
another full polling interval to the next degraded renewal. Trust checks and
overdue retries remain bounded; expired permissions cannot resume requests.
The independent acceptance observer can wait once for a server observation
at most one second ahead of its own clock to become past. It retains the
original heartbeat and cell timestamps, then rechecks freshness and the grant's
remaining lifetime. Clock rollback, a larger future timestamp, expired facts
or an expired grant still reject the window. This does not change signed policy
or extend any evidence lease.

The first sample anchors its window to an actual healthy measurement of the
independently verified current grant. The entire history from that timestamp
is retained for all subsequent samples; a moving log tail cannot discard the
baseline or hide an in-window failure. If a read catches an unfinished permitted
recovery, the observer rereads the same Pod, image and complete log interval
only until the original recovery deadline. A fresh signed grant, measured
diversity and current heartbeat must still be observed before returning a
positive sample. Neither a new grant nor another poll can reset that deadline.

Checkpoint and log reads are also aligned around renewal. If the durable
checkpoint changes while reading the fixed log interval, the observer rereads
the same Pod and image without moving the interval's start. The first race
sets a ten-second read deadline bounded by the original grant's expiry margin;
later renewals cannot extend it. A successful reread still validates the full
history, original recovery bounds and the independently signed current grant.
Its witness records snapshot retries. A successor's healthy log cannot make
an old checkpoint pass, and a retained control failure still rejects the window.

For a read-only consistency check against the currently visible Edge nodes:

```sh
go run ./cmd/fugue-edge-topology -discovery https://api.fugue.pro/v1/discovery/bundle
```

`unknown_edges` and `mismatched_groups` fail the audit. `unobserved_edges` are
reported separately because the public discovery endpoint omits unhealthy or
draining nodes. The command does not verify the DiscoveryBundle signature and
must never be used as a serving authorization gate. It writes no traffic, DNS,
release, or inventory state.

The migration proceeds under the existing signed traffic release and positive
LKG boundaries:

1. Keep the old group route and DNS outputs unchanged while checking every
   observed Edge against the new topology intent.
2. Compile per-tenant, per-hostname candidate authorization from signed route
   intent plus pool membership and current route/TLS/health facts. Compare its
   shadow result with the old decision before allowing any traffic.
3. Add measured sticky primary/standby selection only for control requests
   actually initiated by a Runtime Agent. Public clients continue to use the DNS/entry
   path; the independent DNS failover executor remains the sole writer for its
   declared records.
4. Migrate publication, inventory, Lease, and LKG one authority cell at a time.
   Retire the country-derived group identity only after both cells serve from
   neutral identities with verified rollback and public route probes.

Front code needs an independent executor before changing live authority IDs.
The original Front owns host ports 80/443, and deleting that Pod during a worker
rollout can interrupt both new and established connections. The
`edge-client-front-public-a` bootstrap lane creates one separate Pod-network
Front on the explicitly named Edge node. It has no host ports or API credential,
and reads the existing activation directory through a read-only mount. It can
be measured without changing the public listener or activation state. A later
independent transport declaration must prove equivalent routes and preserve
the old Front until its existing connections drain before retiring it. The
second independent lane targets the two explicit nodes in `cell-public-b`;
neither lane derives placement from country labels. The read-only
`front_observation` job compares immutable Pod identities, the durable
activation record and freshly authenticated HTTPS route proofs against the
existing Front over a bounded window. Its retained evidence explicitly grants
no serving authority. Public transport handoff and connection-drain evidence
remain separate prerequisites.

The independent `front_probe_transport` lane can create an isolated high-port
listener after retaining the Front observation witness. Its schema cannot name
80, 443 or other privileged ports. It checks declared Service/host-port owners
and the node's current TCP listeners, then uses generation and UID/resourceVersion
guards. After creation, the public probe port must return the same freshly
authenticated HTTPS route proof as the candidate Pod. This tests the transport
path without changing the production listeners or deleting either Front.
An independent hosted runner also probes the listener from outside the cluster;
cluster-local routing alone cannot establish external ingress. Before public
handoff, an internal-only Service stages ports 80/443 with no external address.
Its EndpointSlice must identify the exact Ready candidate Pod on the declared
node. A separate held-connection witness uses one verified TLS socket for
repeated nonce proofs and matches its exact client tuple to the Front's active
connection ID. It cannot reconnect after a closed socket. This establishes the
observation needed to test existing connection preservation during handoff.

The public Front handoff is a separate declaration with expected, selected and
compensation generations. It changes only the staged Service's external address
and traffic policy through a UID/resourceVersion/spec compare-and-swap. It keeps
one original verified TLS socket alive across the change and attributes new
connections to the candidate's exact runtime connection ID. For node-originated
Service traffic that is source-NATed, a bounded read-only kernel CT_GET must bind
the original socket tuple to the exact candidate Pod. An explicitly pinned,
existing node observer can execute that reader when the CI identity lacks kernel
observation rights; this installs nothing and never flushes or modifies a flow.
Both Fronts remain present. Failed local verification compensates only the
exact write made by this handoff. A separate hosted runner then verifies external
public ingress; failure invokes the same retained prewrite witness and monotonic
compensation generation. Neither passing these gates nor an empty old Front
alone retires a worker authority or country alias.

Before each cutover, verify the exact previous positive LKG, route and TLS
proof for the target hostname, DNS ownership, cell health, and the other cell's
unchanged state. A rejected or incomplete candidate preserves the current
serving artifact. A failed code release cannot revoke a serving configuration.

Production observation on 2026-09-27: during periodic producer refresh, DNS
candidate readiness can briefly be incomplete while Edge route proofs move to
the new release. The old positive checkpoint remains intact, but once its
route proofs expire, the authoritative DNS node may temporarily return no A
answer. The next release converged and both authoritative nodes returned the
same A answer, but a positive LKG alone did not guarantee answer continuity.
Before enabling dynamic placement or neutral-cell cutover, require an explicit
overlap/ordering acceptance check that proves public DNS answers remain present
through gray/full transitions and validates the exact current ReleaseSet on
both DNS consumers. Never extend a stale proof merely to keep an answer.

The DNS executor now has two staged mitigations. Temporary loss of a configured
dynamic address returns SERVFAIL instead of cacheable NOERROR/NODATA. A retained
positive checkpoint may consume fresh HTTPS observations from a verified newer
release when each retained record's route proof plan and hard policy agree. Per-record
query authorization and selection rules must still match; changes only to
candidate score, score breakdown, or explanatory reason may retain the old
selection. A changed address, Edge identity, owner, weight, quorum or record
rule is rejected. Neither the checkpoint's applied time nor a proof deadline is
extended. Cross-release observations are not reported as successful application
of the new release. These mitigations must pass isolated DNS observation before
public DNS rollout; they do not complete the architecture migration.

Application traffic constraints are compared only for records owned by that
application. Their complete values, including tenant identity and release
weights, still have to agree. An unrelated application's stable release change
must not discard the unchanged platform hostname's proof bridge. Common policy,
record ownership, endpoint sets and route proof requirements remain independent
mandatory checks; changed application routes cannot borrow unchanged proofs.

The producer's source digest excludes raw runtime scores, so a score change
alone does not immediately trigger publication. Periodic refresh still embeds
those observations in the DNS query view; a derived selection-mode change also
changes policy and can trigger publication. Future placement must keep those
ranking observations separate from endpoint authorization.

Each DNS location now has independent A/B execution slots, with separate Pod
selectors, identities and durable caches. Code updates target only an unselected
slot. A separate versioned transport declaration moves a public listener after
both backends prove the same current artifact assignment and record readiness.
The workload guard rejects replacement of a publicly selected DNS executor.

Serving refreshes retain an unexpired in-memory proof after transport timeout
or an unobserved requirement, without extending its original deadline or the
checkpoint authority. TLS, route-state, digest and identity errors remain
negative. A restarted process cannot reuse persisted transient readiness.
An assignment race rejects the whole observation and rereads authority at most
three times; both positive and negative observations must pass the assignment
check before replacing serving facts. A rejected candidate scan can supply its
original observations to the retained release only for identical requirements
and the existing release/compatible-successor authority checks. Changed,
missing, ambiguous or foreign-release requirements are independently reprobed.
This avoids a second full scan of unchanged requirements. Failure diagnostics
count bounded reason codes rather than exposing hostnames or raw probe errors.

Cutover prerequisites remain:

- Both worker slots and public DNS consumers must understand the explicit
  platform owner and platform route kinds before enabling signed grants.
- Route, TLS, health, drain/quarantine and capacity observations must be bound
  to the exact signed release; pool membership alone remains insufficient.
- Agent endpoint authorization needs a public verification key and independently
  managed trust anchors, followed by hostname-preserving transport selection.
- Public DNS continuity must be verified through real gray/full refresh cycles,
  including fail-closed behavior for changed route authorization.
- Neutral authority-cell identities must replace country-derived identities in
  release authority, inventory, leases and LKG before removing compatibility
  aliases. Country labels remain optional locality or residency inputs.

Public Front transitions select one declared probe listener and one internal
Service at a time. Completed handoffs remain in the declaration inventory, but
they are not restaged and their old-public connection assumptions are not
replayed. Only an explicit handoff declaration change selects a public address
CAS; changing executor code alone does not repeat a traffic mutation.

Legacy Front replacement now requires a complete live connection inventory
with an observed count of zero. Missing, malformed or unavailable observations
block deletion, as do retained connections. This is a deletion guard, not proof
that ingress is isolated: retiring a Front still requires the independent
transport handoff and drain acceptance before any worker maintenance.

An address-specific Front Service must select a single declared Edge node. A
shared DaemonSet selector across nodes was insufficient even with Local traffic
policies: in-cluster access to an externalIP selected another node while public
external probes still passed. Each multi-node cell therefore stages distinct
per-node Front executors with disjoint selectors and exact hostname placement.
They keep reading the existing authority without acquiring public ports. Probe
and serving Services bind only to that node's executor; country is not a
placement or selection input.

Forward Worker code transitions also check the complete legacy Front cohort
before applying shared resources or staging any Worker authority. A Front that
needs code replacement and still owns connections stops the transition before
traffic promotion. The exact connection inventory is checked again immediately
before deletion. Configuration-only LKG recovery can still switch authority
without waiting for code replacement; its retained executor remains protected
by the deletion gate.

A neutral identity change now has a bounded local planning command:

```sh
go run ./cmd/fugue-edge-topology -topology previous.json -next-topology next.json
```

The plan binds the complete previous and next topology digests, exactly one
cell's old and new authority IDs, and its unchanged Edge IDs. It rejects
concurrent pool, capability, risk, locality or placement changes and any
second cell transition. The output explicitly grants no traffic authority.
It is input preparation for the signed artifact and runtime migration, never
a replacement for that migration's evidence or a command to rewrite live state.

The remaining cutover must establish a neutral candidate without changing the
flat Edge inventory projection. Both new and retained authorities may observe
the same stable Edge ID while only the selected authority may publish its
serving projection. Neutral route, DNS and TLS cohorts must be explicitly bound
to the signed TrafficReleaseSet before the new control process may fetch route
intent. A label change or unsigned snapshot copy cannot provide this grant.
Public Front transport is now independent, but the Fronts still read the
legacy group activation. The target authority needs independently scoped keys,
leases, inventory, current/LKG artifacts and equivalent route proofs before
its transport can be selected. Source authority remains recoverable through
the overlap and retained connections.

Front externalIP handoff exposed a real restricted-egress regression: kube-proxy
can DNAT a public address to a Front Pod IP on the source application node
before kube-router evaluates its public-IP rule. Restricted applications then
reject the private destination despite allowing public internet. Host and
external-runner probes did not cover this path.

The Controller now reconciles a separate, ManagedApp-owned NetworkPolicy only
for applications explicitly using restricted egress with public internet
enabled. It reads public Front serving/probe Service declarations from the
configured control-plane namespace and verifies ownership, generation and
spec digest. Each peer contains both that namespace and the exact Service
pod selector, with only its published HTTP/TLS backend ports. Private CIDRs
and management ports remain excluded. Staged internal-only Services grant
nothing. Incomplete reads preserve the previous rule; revoking the application's
permission deletes only its exact owned policy with UID/resourceVersion
preconditions. Policy reconciliation is independent from application image
and storage rollout, and does not restart application Pods.

Every future Front handoff requires a read-only test from existing Ready
restricted application containers. A pinned existing host observer verifies
the exact CRI container/Pod identity, enters only its network namespace and
uses normal TLS verification with the configured hostname. Public and probe
paths must serve identical proofs; a host-reachable Front management port
must remain inaccessible from the application namespace. Source and observer
identities are rechecked before evidence retention. These gates complement
external ingress and held-connection tests; none can substitute for another.

The Front workflow now reads its one mutation target from
`deploy/environments/production/front-transport-transition/intent.json`. An
initial `observe` generation is inert. Each next generation selects exactly
one probe, internal stage or explicit public-handoff declaration. The resolver
checks namespace, resource membership, observation profile, address and stage
generation before exposing the target to CI. Changing an executor script or
adding an unrelated declaration cannot replay a completed handoff. New Edge
transition targets require configuration changes, not new workflow jobs or
hardcoded workflow arguments. Existing per-Service CAS and evidence checks
remain mandatory.

Neutral publication uses the explicit `authority-cell:<cell-id>` artifact scope
so preparation cannot displace the global producer's release lanes. The signed
PlatformIntent names `authority_cell_id`, one neutral topology without a legacy
alias, and its DNS process declarations. Compilation derives sorted physical
Edge and DNS membership; the paired policy must pin its exact
`consumer_topology_digest`. ReleaseSet carries `consumer_topology`, and every
child binds that digest through signed metadata and policy. Legacy global
artifacts retain their original representation.

Expected route, TLS and DNS consumers are prepared from this declaration.
Missing inventory, stale heartbeat, quarantine or a failed Pod cannot erase a
required member. API assignment and convergence readers check the exact scoped
identities, and the store repeats membership validation in the full-publication
transaction. Dropping a member, making it optional, moving authority or widening
freshness cannot promote a release. Preparing the topology does not copy old
facts, select public transport or establish positive LKG. Consumer deployment
scope, independent per-cell release selection, bootstrap activation, fresh
proofs and explicit transport cutover still need separate rollout steps.

Route source selection resolves neutral serving authorities only within their
`authority-cell:<id>` publication lanes. Control requests and consumer serving
assignment/fact reads instead use the exact signed credential scope, allowing
existing global credentials to finish their compatibility lifecycle without
silently changing authority. Neither path searches another scope after an
absent, invalid or unprepared publication. Agent grants retain the actual source
scope and verify that a cell publication matches the candidate authority;
mixed legacy/new-cell grants remain possible during an explicit transition.
This code support does not change any deployment's credential scope or activate
a new cell publication. Agent binaries must support scoped grants before the
Agent topology policy selects a neutral publication.

Executors select their publication scope with `FUGUE_PLATFORM_ARTIFACT_SCOPE`.
An omitted value preserves `global`; a cell scope must name the executor's
configured authority. The Pod identity annotation must declare the same scope.
Control checks both exchanged credentials and route-source provenance; Worker
and DNS check assignments and persisted positive state before using them.
Changing this setting cannot make a global checkpoint belong to a new cell.
The isolated public-a candidates enroll in their independent cell scope while
the Agent canary receives scoped-grant compatibility. This enrollment alone
creates no serving assignment, positive LKG or public transport selection.

DNS authority observations enumerate retained global and cell-scoped receipts.
The storage read is bounded to 64 receipts for one physical node and 4096 for
inventory; overflow rejects the observation instead of choosing from truncated
results. Scope and consumer ID jointly identify a receipt. Freshness and signed
membership remain mandatory, and the exact selected public Service backend
decides between old and new authorities. Missing cell observations cannot fall
back to historical inventory health. Reading facts does not modify receipts,
advance their cursors or transfer positive evidence between scopes.

Independent configuration producers use `platform-config-producer:<cell-id>`
policy lanes and `authority-cell:<cell-id>` output lanes. Their signed policy
pins a static intent and projection policy in that same authority, including
the complete consumer topology digest, application domains, route defaults,
query strategy and consumer readiness mode. Each producer captures a current
business snapshot; a cell never copies the current global traffic artifact.
Physical endpoint reads use exact declared Kubernetes node names and public
addresses only. Country inventory, historical health and loaded releases cannot
change declared membership or supply serving evidence. Missing endpoints keep
the previous publication. Scheduling and transactional policy locks are scoped
independently, with bounded discovery and cancellation on shutdown. First full
LKG and public handoff remain explicit verified steps; enrolling a shadow
producer authorizes neither.

Initial cell producer inputs are declared in
`deploy/environments/production/cell-producers`. The independent
`cell_producer_shadow` CI lane accepts one changed declaration, reuses only
exact immutable generations, validates all three signed inputs through the API
and previews the current business snapshot before publishing a shadow policy.
Preparation checks the declared predecessor and repeats authority checks before
publication. Retries reuse identical artifacts and the publication idempotency
key. Any existing gray/full authority blocks this bootstrap lane. It cannot
publish a traffic channel, verify LKG or select a public listener. The CI lane
is serialized; its predecessor reads are not a transactional compare-and-swap
against unrelated external writers. The generic artifact store still provides
transactional lane fencing and immutable generation checks.

The first cell retains the original API DNS minimum of two healthy Edges even
though its local executor membership contains one Edge. Independent per-cell
route preparation must precede a verified cross-cell DNS publication mechanism;
a single-cell shadow projection is not evidence that this global availability
constraint is satisfied. No bootstrap step lowers that constraint to permit
cutover.

An explicit `publication_role: cell-routes` now separates a neutral cell's
route/TLS release from DNS publication. Intent, producer policy, compiled policy,
parent and signed membership must agree on that role. Its immutable topology
contains every declared Edge and an empty DNS set; its release contains exactly
route and TLS artifacts. Ordinary traffic releases retain their three required
members. DNS configuration and runtime placement facts are rejected by the new
role, while route-level DNS restrictions and availability minima remain in the
signed route policy. The business projection still reads current application
ownership and origin evidence, and never copies another release's output.

Gray/full admission requires a fresh authenticated `cell_routes_v1` capability
from every required Worker. Each role publication still needs new expectations,
exact parent/child provenance, actual Caddy/HTTPS route and TLS evidence, and
the existing full/LKG transaction gates. Route-only verification cannot create
a DNS LKG; a failed candidate preserves the previous positive route/TLS LKG.
This capability does not rewrite an existing authority or select public traffic.
Cross-cell DNS publication remains a separate implementation and rollout step.

`FUGUE_EDGE_INVENTORY_BOOTSTRAP_FILE` optionally points to an independently
projected, non-secret initial inventory permission. It is used only when the
actual activation file is absent; malformed or unreadable activation never
falls back. The permission names the exact cell, physical Edge, Pod instance,
slot, executable source and signed prepared gray route-only parent. Its absolute
lease is at most fifteen minutes and is never renewed by a read or heartbeat.
The Worker verifies current scoped assignment and signed membership twice around
inventory preparation, and caps its signed heartbeat to the original deadline.
Bootstrap eligibility remains distinct from applied serving health.

This permission writes no Front activation, bundle, serving cache or LKG. The
first observed and verified group bundle is still required before the existing
activation CAS initializes a real serving record. Once a valid activation exists,
the producer uses that record independently of the bootstrap file, so missing or
expired bootstrap configuration cannot invalidate positive serving recovery.
The optional projection alone grants nothing; enrollment and private probes
remain separate configuration steps.

The independent `cell_inventory_enrollment` CI lane selects one explicit
`deploy/environments/production/cell-inventory` declaration. It binds the
already-deployed private Worker UID, source, image, cell, node, state PVC and
validated route-only ReleaseSet. The lane admits only an empty gray lane or the
same declared gray parent, prepares both required consumer sets, and creates a
fifteen-minute permission in a mutable Kubernetes projection. Retries reuse its original deadline.
No full publication, artifact LKG attestation or public transport is changed.

Initialization requires repeated actual Worker observations and authenticated
route/TLS convergence for that exact Pod and gray publication. A bounded Job
using the observed Worker image mounts only its isolated activation directory
and executes the existing create-only activation CAS with the real bundle
generation. The Worker retains its read-only activation mount. Three subsequent
observations require fresh inventory after initialization. Missing proofs,
replaced executors, expired permission, existing foreign activation or serving
transport stop this initial lane; an expired permission is never renewed by a
retry. Later full/LKG promotion and public handoff remain separate operations.

Initial inventory eligibility survives authenticated inventory aggregation:
the stored snapshot has a content digest, while each signed heartbeat has its
own producer generation. The local store supplies verified producer observations
in memory; compilation checks the aggregate digest and exact instance against
those observations. This provenance is never serialized into a heartbeat.
Restart reads preserve the original eligibility deadline, and eligibility still
stops after the first publication. Neither aggregated identity nor bootstrap
eligibility is serving health.

An operator can declare a new initial attempt after an expired permission by
pinning its ConfigMap UID, prior declaration digest, authorization digest and
generation in `previous_permission`. This applies only while activation remains
absent and the cell, physical node, slot and gray parent are unchanged. An
explicit new Worker Pod/source pin may recover initial enrollment after a code
rollout; every new identity/image check still applies. The lane replaces
only that expired authorization with Kubernetes UID/resourceVersion and exact
data preconditions, preserving the watched projection object. Ordinary retries never rotate it,
and an unexpired permission, replaced object or established activation stops
the operation. This configuration recovery does not rebuild either executable.

Bootstrap ConfigMaps must remain mutable so kubelet continues watching projected
authorization updates. An immutable Kubernetes object cannot be reused for this
configuration channel, even after deletion and recreation under the same name.
Replacing a previously immutable projection uses a new declared ConfigMap name
and the normal isolated Worker rollout; existing public workers are unaffected.
The authorization deadline itself remains fixed within each explicit attempt.

Initial Cell trust may name a dedicated production-environment encrypted package
in `materialSecret`, restricted to `FUGUE_EDGE_CELL_TRUST_` plus that exact Cell's
uppercase underscore identity. Existing declarations keep their original shared
package lookup. Private keys remain outside Git; declarations pin only content
digests, purpose and physical membership. Each Cell/purpose uses independent
material. A read-only change selector admits trust configuration to its queue
without depending on executable builds; unrelated pushes cannot cancel a
pending trust initialization with an empty job.

The second declared authority `cell-public-b` starts with a private Control and
one isolated Worker on physical Edge `vps-591f4447`, with its own state PVCs,
reader/inventory/signing/recovery credentials and `authority-cell:cell-public-b`
consumer scope. Its shadow route/TLS producer names only that initial executor;
provisioning an additional reader credential does not make another node required
or eligible until a signed membership update enrolls it. Country remains endpoint
metadata. Internal health Services and isolated NetworkPolicies grant no public
transport or legacy serving authority. The currently selected public frontends
and DNS listeners retain their existing service until explicit later handoff.

The read-only `cell_inventory_plan` job checks declaration changes before any
production concurrency slot is requested. Unrelated commits do not enqueue an
empty registration job that could cancel a waiting valid configuration run.
The writer rechecks the current declaration and script contents before mutation;
neither selection nor enrollment depends on a code build or deployment job.

After real activation, `cell_route_promotion` accepts one independently declared
private Cell promotion. It pins the enrollment digest and gray release, observes
fresh advancing inventory and route/TLS proofs before initial gray LKG, publishes
the same parent to full, prepares the full expectations, and requires another
fresh observation window before full LKG. Observation witnesses are retained
before verification requests; the API rechecks signed membership and facts in
its publication transaction. Retries preserve already verified receipts and
their evidence hashes. Foreign full authority, replaced activation, stale facts
or failed probes stop promotion while preserving prior positive recovery state.
This lane cannot write DNS, public transport, activation or executable images.

Cell Workers fetch existing custom-domain certificates through their scoped
component identity and exact current TLS assignment. The certificate endpoint
checks the signed route/TLS parent, serving gray/full fence, declared local route,
verified domain and matching tenant/app ownership before disclosure, then
rechecks current authorization. Shadow, other cells and historical assignments
cannot read private certificate material. The Worker verifies signed inputs and
the response owner, rechecks assignment, and uses the existing certificate
validator and private-file installer. Missing scoped credentials cannot fall
back to a legacy Edge token. Certificate distribution writes no readiness,
domain status, serving authorization or artifact LKG; live Caddy proofs remain
required. The existing legacy certificate path retains its prior behavior.
Shared SNI hostnames may use a platform TLS reference while also carrying an
application custom-domain path. Certificate ownership in that case comes from
the signed local custom-domain routes and their unique verified allowlist entry;
the platform path cannot supply or replace the tenant/app owner. API and Worker
use the same ownership check, including rejection of mixed custom-domain owners.
Cell TLS warmup performs its real local handshake but never calls the legacy
domain-status writer or exports private material through a global Edge token.
The existing assignment-bound serving observer owns Cell route/TLS facts.

A staged shadow observation can have a newer artifact sequence than the actual
selected serving publication. Transitioning its runtime cursor into the exact
current gray/full assignment checks the complete signed parent, immutable
membership, unfrozen lane, fence and cohort under the same transaction. The
global sequence, timestamp and nonce checks remain unchanged. This does not
authorize serving-to-serving generation regression without the existing explicit
rollback proof, and it cannot synthesize a positive heartbeat or verified LKG.


Independent DNS publications use the explicit `cell-dns` role in their own
neutral authority scope. That role owns exactly one DNS artifact and immutable
DNS process membership; it cannot enroll Workers, publish routes/TLS or write
another authority's LKG. The original complete traffic role and `cell-routes`
retain their composition. The business producer does not infer this new role.

DNS intent pins each routing Cell's parent and route/TLS member IDs and digests,
serving release ID/channel/fence and exact canary selection. The compiler accepts
these signed immutable inputs separately from runtime observations, retains them
inside the DNS artifact, and uses each Cell's existing route projection. It does
not recreate routes under DNS policy. Compilation verifies current selected
Cell authority, and serving publication/rollback/LKG transactions recheck it.
PostgreSQL acquires nonblocking shared dependency locks before reading routing
lanes; a concurrent routing mutation causes a bounded conflict and leaves the
previous positive DNS publication intact.

Every proof requirement binds a reference digest, physical Edge, Cell, address,
route digest, hostname/path and state. Original route ownership, exclusions,
pinning and default/per-route health floors remain constraints. DNS additionally
intersects pool/capability/residency requirements and enforces distinct Cells and
failure domains. IPv4/IPv6 addresses never count as separate physical Edges.
Readiness, answer generation and API observations share the same quorum and
freshness checks. Signed retained inputs let a restarted DNS process verify
configuration offline, but it must collect fresh HTTPS/TLS facts before answering.

The `cell_dns_v1` capability is required for every declared DNS member before a
serving publication. A shadow reference cannot authorize an answer. A proof for
the DNS parent cannot substitute for its referenced routing Cell. Old listeners,
public selectors and Agent grants remain governed by their separate transport
and configuration transitions. This protocol support alone does not complete
public migration: capability enrollment, independent DNS deployment, coordinated
route-reference/public handoff, Agent rebinding and connection draining still
require explicit declarations and observed production acceptance.


An explicit `cell-dns` member may establish local configuration receipts through
an independently declared private validation Service. The Service is unique for
its authority and physical node, has only ClusterIP transport and binds its
selector and UDP/TCP ports with `transport.fugue.dev/digest`. The digest covers
canonical JSON `{authority_id,node_id,spec}`, where spec contains type,
internalTrafficPolicy, publishNotReadyAddresses, selector and ports. The
`fugue-dns-validation` manager label, generation, authority-id and node-id
annotations are mandatory. The API verifies the signed parent/current publication,
exact expected member, isolated Pod identity, declared DNS socket owner and actual
EndpointSlices, then rechecks private/public selection and node/Pod resource
versions. Same-authority public selection takes precedence: a private standby
cannot overwrite a selected public instance. Public runtime-facts continue to use
only the selected public backend. A private receipt proves local configuration and
recovery, never public traffic selection.

Two independent DNS executors are declared for `cell-dns-public`, each with its
own state PVC, projected Pod identity, health Service and private validation
Service. They initially have no selected DNS configuration or public listener.
Their egress permits the API, cluster resolver and public HTTPS route probes;
observer ingress is limited to API Pods. Routing Cells keep their own scopes and
state. Initial DNS intent, immutable references, capability enrollment and public
handoff are later configuration steps, independent of these executable lanes.

Route-only producers may opt into a signed `route_placement_transition` before
neutral DNS configuration is compiled. The declaration contains complete old
and new topology intents, plus each affected route constraint and its canonical
source digest. The topology pair may only remove explicitly declared authority
aliases. Physical Edge membership, pools, capabilities, locality labels and
failure domains must remain identical; countries are never interpreted as
authority identifiers. Enrolled private Cell members must match the declared
neutral topology exactly.

Capture compares every affected constraint against its pinned source before
replacing aliases in DNS placement and group exclusions. Tenant/app ownership,
matching scope, physical exclusions, exclusion expiry/owner/generation/fence,
enablement and health minimums remain intact. Missing, changed and unlisted
affected constraints reject capture. The publication transaction independently
reconstructs each transformed constraint from the signed source and checks both
route and TLS children. Rollback retains its independent verified baseline.
The option is rejected for global and complete-traffic producers. It never
changes business rows, runtime identity, publication selection or LKG by itself;
activation and public handoff still require their own observed configuration
transitions.

Established Cell producer reconfiguration has its own configuration lane in
`ci.yml`. A declaration pins the previous producer publication, the current
route/TLS full publication, and the verification evidence hash of its positive
LKG. The artifact release request carries `producer_reconfiguration`; the store
checks these identities inside the file transaction or after PostgreSQL locks
the producer scope followed by the target scope. Expiry and signatures are
checked again before commit. The content-bound idempotency key permits a lost
response retry only while the same successor and serving baseline remain
current. Superseded successes and changed/frozen predecessors conflict.

The default `placement` operation replaces a shadow/paused route-only producer with shadow mode
and changes its generation and explicit route placement transition. Static and
projection input references, schedules, membership and promotion settings cannot
change through this operation. It neither mutates full traffic nor verifies or
renews LKG. The initial bootstrap lane still refuses established serving Cells.

The separate, explicit `activate_serving` operation keeps the reviewed placement
transition and all input references and schedules unchanged. It changes only
generation, mode and bounded serving settings, requiring `single_publication:
true`. The exact full baseline must already be verified and contain the declared
canary cohort. Automatic candidate admission still validates pinned inputs,
composition, real route/TLS capabilities and current publication fences; fresh
gray evidence gates full, and fresh full evidence gates LKG. No public transport
is selected by activation. Activate one Cell and observe its verified full before
proceeding to another Cell or publishing DNS references.

For route-only producers, `single_publication` stops capture after one verified
full from that exact producer policy publication. The durable publication ledger
also prevents a second automatic gray inside the transaction, including after
restart, LKG expiry or a subsequent operator publication. Pending recovery is
processed before this hold. The hold neither renews proof expiry nor grants
authority to another policy; resuming production requires a separate policy
publication. Omission retains continuous producer behavior.

When live business configuration advances during this hold, the explicit
`refresh_serving` operation authorizes a successor cycle. It changes only the
producer generation. The predecessor must already use single publication, and
the exact current verified full/LKG must belong to that predecessor policy
publication and have been published by the producer. Inputs, placement,
schedules and observation settings cannot change through refresh. Competing
successors use the same transaction locks and exact predecessor checks; a
changed, unverified or operator-owned baseline rejects without altering LKG.
Refresh still captures current business configuration and requires new gray and
full receipts. It does not attest route equivalence for a DNS handoff.

Independent DNS distinguishes missing dependencies from known inactive routes.
Every symbolic record must have complete signed route dependencies with matching
ownership. When those routes are inactive and policy says `omit`, compilation
keeps them out of both answers and readiness targets without blocking unrelated
DNS data. A missing dependency, orphan query rule or insufficient active quorum
still rejects the compilation. `serve_error_page` retains exact loaded-state and
TLS proofs. DNS consumers replay the signed Cell inputs to validate these
decisions, including after offline checkpoint recovery.

An established route Cell can stage one additional execution member through
`expand_membership`, using a declaration in
`deploy/environments/production/cell-membership-expansion`. The same independent
configuration lane admits one reconfiguration or membership declaration per
commit. The predecessor must be a completed serving producer with a positive
full/LKG. Its source metadata and signed membership must match the pinned static
input. An in-progress or frozen gray lane rejects expansion.

Expansion moves the producer to shadow and pins two new immutable inputs. The
static input may add exactly one Edge already declared identically in the signed
placement transition; all existing members, their labels and failure domains,
pools and serving intent stay identical. The projection policy only changes its
generation and matching topology digest. Source signatures, current publication
fences and positive LKG are checked under the producer/target transaction locks.
The previous full remains selected. The added process still needs independent
code deployment, fresh route/TLS evidence and inventory enrollment before a
separate serving activation. Expansion itself never grants public transport,
fabricates health, or initializes an activation file.
