# Managed application SSH

Status: identity, observation, managed sessions, independent intent, candidate
publication, explicit activation, and guarded route rollback are implemented
locally. Kubernetes candidate publication and execution passed isolated-cluster
acceptance. Instance-bound edge acknowledgement and guarded slot retirement now
have local and isolated-cluster coverage. Production release enrollment, enforcing
network-policy acceptance, and traffic migration remain in progress. The user authorized committing and pushing completed work,
following GitHub Actions deployment, and investigating unexpected behavior.
Initial code and independent keyring configuration have now been pushed.
Production traffic, application identity activation, Argus, and client trust
have not been changed. Component release monitoring remains in progress.

## Outcome

Fugue owns application SSH identity, authorization, serving configuration, and
session execution independently of application images and releases. Existing
applications must not need source changes or image rebuilds to migrate. The
initial compatibility step can configure an existing OpenSSH server, but does
not count as completion of platform-managed SSH.

This extends the scope of `native-app-ssh-plan.md`: the managed implementation
terminates standard SSH in a Fugue component and uses an explicitly authorized
runtime execution adapter. A client-side HTTP or WebSocket terminal is not a
substitute for standard SSH protocol support.

## Invariants

- One stable identity belongs to one tenant and logical application, not to a
  Pod, container, node, image, public port, or release.
- Normal reconciliation, rollout, migration, disable/enable, and rollback do
  not regenerate keys. Missing or corrupt existing key material is an error.
- Concurrent identity creation selects one durable identity. Keys already
  present in application images are never automatically adopted.
- Private keys do not appear in ordinary API responses, logs, diagnostic
  bundles, route artifacts, or source control. Their protected storage and
  backup policy are independent of executable releases.
- Intent, admission policy, immutable signed artifacts, observations, and
  executable components remain separate. Private material is referenced by
  artifacts, never embedded in publicly readable routing state.
- A failed candidate preserves the previous positive serving configuration.
  Expired or revoked authorization is not extended by LKG fallback.
- Existing applications and SSH routes are not silently switched to new host
  identities. Activation requires verified candidate evidence and an explicit
  migration operation with a recorded trust transition.
- SSH readiness is evidence with a scope and timestamp, not an inference from
  port allocation. Server readiness does not assert that every client trusts
  the server or possesses a valid user key.
- Operations never match a particular project, hostname, app name, or incident
  fixture. Compatibility depends on explicit runtime capabilities and schema.

## Phase 1: managed identity and evidence

- [ ] Define the identity, observation, migration, and public trust contracts.
- [ ] Implement durable, encrypted, independently recoverable host identities.
- [x] Verify idempotency, concurrent creation, corruption handling, and tenant
      isolation for persistent storage implementations.
- [x] Add a read-only, bounded SSH handshake probe for a selected expected host
      identity. It verifies possession during key exchange and stops before
      user authentication. Inventory across all supported algorithms remains
      part of observation integration.
- [x] Report route configuration, host identity, observation freshness, and
      client trust separately; remove configuration-only ready assertions.
- [ ] Render explicit host-key configuration for compatible existing servers,
      preserving application image, command, environment, and data.
- [ ] Add candidate validation and activation with retained serving state;
      avoid app restarts for access changes where the runtime supports it.
- [x] Expose authenticated fingerprints and a reviewable client trust update.
      Do not disable strict host checking or overwrite unrelated host entries.
- [ ] Synchronize API consumers, CLI diagnostics, and web status.
- [ ] Pass targeted tests, generated contract checks, and repository tests.
- [ ] Validate migration, reload, rollback, and active-session behavior in an
      isolated environment using synthetic applications.
- [ ] Review a concrete production migration with impact and rollback evidence.
- [ ] Execute the approved normal release and migration path and verify the
      affected endpoints without changes to application source or images.

## Phase 2: platform-managed sessions

- [x] Define and validate the managed SSH service/runtime executor boundary.
- [x] Implement standard SSH authentication and bind each connection to its
      tenant, application identity, authorized user, and exact workload target.
- [x] Implement interactive PTY, resize, stdin/stdout/stderr, exit status,
      cancellation, signal delivery, and bounded resource consumption.
- [ ] Implement SFTP and SCP compatibility in the target filesystem, without
      relying on an application-provided SSH daemon.
- [ ] Implement explicitly authorized forwarding needed by supported clients;
      prevent access to other tenants, node management, and cluster credentials.
- [ ] Ensure helper/runtime compatibility is explicit and generic. Unsupported
      images must report precise limitations rather than falsely claim success.
- [ ] Reconcile signed serving configuration and authorization separately from
      the SSH component executable; retain verified state on failed updates.
- [ ] Implement revocation, authorization expiry, session limits, and audit
      records without exposing command output or secrets by default.
- [ ] Ship deployment manifests, narrow runtime permissions, health checks,
      metrics, and component release integration.
- [ ] Verify native OpenSSH, file transfer, and the promised remote development
      client workflows with synthetic workloads.
- [ ] Verify tenant isolation, authorization races, restart/recovery, partial
      rollout, bad configuration, missing identity, and rollback behavior.
- [ ] Canary the platform-owned serving path before migrating existing routes;
      drain established connections rather than claim seamless process recovery.
- [ ] Complete reviewed production migration, trust enrollment, and runbook.

## Evidence and release boundaries

The reported incident establishes an identity mismatch and image-shared host
keys. It does not establish the complete historical sequence of earlier key
replacement. Current defects and acceptance tests must be grounded in observed
behavior rather than an invented historical sequence.

The compatibility path may use an existing globally included SSH configuration
file. Configuration rendering must verify the effective host-key list and must
not silently leave image-baked keys active. Secret propagation and successful
server reload require independent observations; changing a Secret is not proof
of either. Adding a mount or modifying a Pod template can require a workload
rollout and is not automatically a zero-disruption operation.

Changing the public host identity may require one reviewed client trust
transition. No attempt will be made to manufacture the old private key from a
saved public key or to preserve a known shared key as the managed identity.

All production component releases follow `AGENTS.md` and the declarative CI
pipeline. Local implementation and test success do not authorize manual live
patches, workload restarts, or client trust edits.

## Implemented identity foundation

`GET /v1/apps/{id}/ssh/identity` reads prepared public identity metadata.
`POST /v1/apps/{id}/ssh/identity` explicitly prepares one identity, idempotently,
without creating routes, changing app specs, or scheduling deployments. The CLI
exposes these as `fugue app ssh identity show` and `identity prepare`. Preparation
does not imply activation, observed readiness, or client trust.

The identity uses Ed25519. Private seed material is encrypted with AES-256-GCM;
associated data binds the ciphertext to all public identity metadata and its
storage encryption key identifier. Public API responses exclude ciphertext and
encryption key details. Corruption and unavailable encryption keys fail closed.

PostgreSQL stores identities in the independently migrated
`fugue_app_ssh_identities` table and retains public ownership in
`fugue_app_ssh_identity_anchors`. A missing encrypted row with an existing
ownership record requires restoration and cannot trigger key regeneration.
The schema migration is additive and does not cascade identity deletion from
compute deletion. The file backend stores identities in a separately locked,
atomically replaced `<store>.ssh-identities.json` file and public ownership in
its `.anchors` companion. An older release rewriting ordinary application
state cannot discard them. Restore both files together; losing both ownership
and encrypted records requires recovery from the independent backup.
Retained records are inaccessible through app APIs after compute deletion.
An explicit retention and destruction policy still needs implementation.

Configure `FUGUE_SSH_IDENTITY_KEYRING_FILE` with a protected JSON file readable
only by its owner. It has schema `fugue.ssh-identity-keyring/v1`, an
`active_key_id`, and a `keys` map of key identifier to base64-encoded 32-byte
random encryption key. No development fallback or release-signing key reuse is
allowed. The file is reread for private operations, so restoring configuration
does not require changing or restarting API code. Back up the encrypted identity
records and the encryption keyring independently. Safe storage-key rewrapping
is tested but is not yet exposed as an administrative operation.

Validation completed for this foundation: encryption roundtrip and metadata
tampering, missing keys, stable identity under encryption-key rotation,
cross-instance file-store concurrency, unchanged workload state, tenant
isolation, corruption/restoration, PostgreSQL concurrent preparation and
independent schema retry, API scope checks, CLI method selection, and bounded
SSH key-exchange probing without user authentication. Tests use synthetic
workloads and a disposable loopback PostgreSQL database.

The backend `make test` suite passed after the initial foundation. Subsequent
ownership-record changes passed focused PostgreSQL integration, race detection,
and generated contract checks. The frontend OpenAPI snapshot and generated
types are synchronized. Its full `contract:check` currently reaches
TypeScript errors in pre-existing source archives under
`docs/investigations/2026-10-11-server-feature-audit/historical`; no source archive
or compiler exclusion was changed for this work. A separate typecheck excluding
those historical files in memory passed, without changing `tsconfig.json`.

## Remaining migration design constraint

An existing included SSH configuration file is a compatibility entry point,
not proof that its ownership is independent from application reconciliation.
Before using it, verify that current and rollback renderers cannot overwrite
the managed host-key configuration or delete referenced key material. Adding
keys to an app-owned Secret without addressing that ownership would violate
the release-independence invariant. A separate serving component avoids this
coupling, but must have a verified runtime executor before public cutover.

## Observation and client trust

`POST /v1/apps/{id}/ssh/identity/observe` probes only the server-allocated public
endpoint, with an identity-ID precondition, a five-second deadline, concurrency
limits, and no user authentication. Results are recorded independently of
serving intent. The observation names its observer, target, identity, route
digest, result, and expiry. It represents that observer's path, not every edge
replica. A later failure replaces older positive evidence for readiness.

`ready` now requires fresh matching host identity evidence. `route_configured`
preserves the separate statement that a route is publishable. Existing CLI
connections can still invoke the native SSH client for configured routes so
that an API status correction does not itself prevent an already trusted
connection. The SSH client continues its own host and user authentication.

`fugue app ssh identity observe <app>` records evidence. `identity trust-plan`
requires a fresh verified observation, compares the identity with the selected
local known-hosts file, and displays matching line numbers, old fingerprints,
and a proposed entry. It does not edit the file, silently accept a new key, or
disable strict host checking.

## Managed execution implementation

`cmd/fugue-managed-ssh` runs a platform-owned SSH server outside the application.
Its signed serving artifact and host identity are separately mounted. Candidate
validation, monotonic generations, persisted verified state, explicit key
expiry, and revocation are independent of its binary. Invalid candidates retain
the prior state. Authorization expiry still closes sessions; retaining a config
does not extend grants. Revocation of an existing grant does not require a
successful application runtime probe. Shutdown stops acceptance and drains
existing connections for a bounded window before cancellation.

The Kubernetes executor binds tenant and app labels, namespace, Pod name and
UID, container name and container ID before and after opening the execution
transport. Runtime credentials have named-Pod `get` and `pods/exec` permission
only. Host namespaces and privileged containers are rejected. The current
adapter requires Linux, a compatible static helper architecture, the configured
shell, basic bootstrap utilities, and a writable executable temporary directory.
It runs as the target container's current user; changing to another Unix user is
not silently emulated.

The platform helper is streamed into a private temporary directory, checked
against its digest, executed for a session, and removed after normal completion.
It is never put in an application image or used to replace PID 1. Its framed
protocol carries stdin, stdout, stderr, EOF, terminal resize, and signals, with
bounded frames and input acknowledgement so blocked stdin cannot suppress a
signal. SFTP runs in the target filesystem. TCP forwarding is restricted by
explicit loopback port or port-range policy and originates in the target
container network namespace. The supplied process executor is for that helper;
it is not used to run tenant commands in the platform server process.

The deployment renderer emits a separate candidate Deployment, Service,
ServiceAccount, named-target Role/RoleBinding, and restricted NetworkPolicy.
Host keys, signed config, trust roots, and persisted serving/audit state are
separate inputs. Its single-instance Deployment is a candidate slot, not an
in-place zero-downtime upgrade mechanism: a second independently validated slot,
route activation, and old-session draining must be completed before production
rollout. Application Deployments are not emitted or edited by this renderer.

## Isolated acceptance evidence

The managed implementation passed race tests for authentication, rejected config
and identity changes, generation replay, persisted restart recovery, audit
failure, command exit codes, terminal resize, signals, active-session revocation,
forward authorization and revocation, and bounded runtime framing.

The opt-in `TestManagedSSHIsolatedKubernetes` runs against an explicit loopback
kind cluster whose context name must start with `kind-fugue-ssh-`. Its fixture is
an unchanged Alpine container without `sshd`, and it verifies:

- Execution in the target filesystem/user, real PTY size, and SFTP transfer.
- Native OpenSSH plus default native SCP upload/download using strict host-key
  checking and temporary synthetic credentials.
- Forwarding to a workload-local test service.
- A scoped ServiceAccount that cannot read another Pod or list Secrets.
- Unchanged Pod UID, container ID, image, and zero application restarts.
- No abandoned completed helper binary after sessions finish.

The native SCP test covers the modern SFTP transport. Legacy `scp -O`, real
remote-development client acceptance, abnormal-helper cleanup, restart races,
cross-edge migration, and production canary/rollback remain unverified.

The integration test intentionally requires `FUGUE_SSH_TEST_KUBECONFIG` and
`FUGUE_SSH_TEST_HELPER`; it never uses the default Kubernetes context. Static
helper and managed-server binaries have also been cross-compiled for Linux.

## Working checkout and release work still required

Implementation moved from an older local branch into the isolated
`fugue-managed-ssh` checkout based on `origin/main` at
`6daf35ea` after incorporating current main changes. Work before the latest
refresh is retained in stash `74e6315dfb80df202b8b1a5ba2c8e761effafaf1`; the
prior refresh is retained in stash `f35cae77ee783ad4ef9e05f9f09d221e33f9e1b1`; the
initial transfer is also retained in stash
`0f8d4866d47dc2c20e55712a9908725ac7015f42`. Initial code was pushed as
`46be3447`; `54a5cd76` enrolled independent keyring recovery and `8b938942`
fixed the bounded prepush scheduling failure and retried component deployment.

Before production release, enroll exact production edge consumers and tokens,
complete signing/identity key provisioning and backup, enforce network isolation,
finish image/release registry integration, and the production
rollback/monitoring runbook. Do not deploy the candidate controller as if these
remaining serving and release operations were already complete.

## Latest verification checkpoint

On the rebased checkout, the full `make test` command completed successfully,
including generated OpenAPI drift checks, all Go packages, static Caddy race
tests, and the Windows CLI cross-build. The SFTP dependency is v1.13.11, which
bounds extended-attribute preallocation; its required crypto dependency updates
are reflected in both root and static-Caddy module metadata. The static Caddy
source patch pins were not changed.

The isolated Kubernetes acceptance test also passed under the named-Pod-only
ServiceAccount, including native SSH/SCP and unchanged application execution.
PostgreSQL identity and observation roundtrips passed with race detection.
Focused tests verify that revocation still applies when runtime validation is
unavailable and that a bad candidate preserves prior serving state.

The frontend was regenerated from this checkout using
`FUGUE_OPENAPI_SOURCE=/Users/yanyuming/Downloads/GitHub/fugue-managed-ssh/openapi/openapi.yaml`.
Its snapshot/generation checks passed. Full TypeScript validation remains
blocked only by the historical source archives described earlier; an in-memory
exclusion check passed without editing the compiler configuration.

## Independent intent and candidate controller

`GET` and `PUT /v1/apps/{id}/ssh/managed` expose independent intent. Updates
require the exact prior generation; initial creation requires generation zero.
The API accepts existing tenant key IDs, a prepared identity ID, an explicit
application container, runtime user and directory, capability policy, and
bounded connection/channel limits. It does not edit the application spec,
allocate or switch a public route, or update client trust. The CLI provides
`app ssh managed show` and `managed apply --file ... --expected-generation ...`.

Intent, control state, and immutable signed artifacts have separate PostgreSQL
tables, outside the general application state writer. The file backend likewise
uses independently locked, atomic files. Publication checks both the intent
generation and control revision. Reusing an artifact generation with different
contents fails without changing the previous state. Identity replacement and
cross-tenant publication fail. The controller persists artifacts before applying
them to runtime resources.

`cmd/fugue-ssh-control` periodically reconciles configured intents. It reads
independent runtime and signing configuration files on reconciliation. It
compiles five-minute grants from the current key inventory, with no fallback
to the public keys copied into an old application spec. Deleted keys are omitted;
disabled intent emits no grants. A failed target lookup does not prevent
publishing revocation to an existing slot. Serving processes recheck loaded
authorization promptly; controller delivery additionally depends on the
reconciliation loop and Kubernetes Secret projection. The tested 100 ms server
revocation interval is not a claim about end-to-end control-plane propagation.
An unavailable control plane cannot extend grants beyond their signed expiry.

Target discovery uses the current application Service selector and positively
ready EndpointSlices owned by that Service UID. It verifies the endpoint's Pod
UID, tenant/app labels, Pod readiness, and the explicitly selected container ID.
It retains the previous selected replica while it remains a valid endpoint.
Host namespaces and privileged targets fail validation. Sidecars are never
selected by guessing a container name.

The Kubernetes publication adapter requires a dedicated namespace labeled
`fugue.pro/managed-ssh-control=true`. Each immutable executable/target combination
gets its own slot. Secrets, persistent LKG/audit state, narrow named-Pod roles,
and network policy precede the candidate Deployment. Application ownership labels
are deliberately absent from these resources, so application cleanup cannot
adopt them. SSA never forces another field manager's ownership, and existing
resources must carry the exact SSH slot and tenant labels. Identity Secrets are
immutable; artifact updates fence stale generations using resource versions.

Candidate observation binds the loaded digest, serving Pod UID, exact target
Pod/container, a fresh runtime capability probe, and actual SSH key exchanges
through both the Pod address and its Service. Real-cluster testing reproduced
the gap between Pod readiness and Service propagation; a Pod-only handshake is
therefore insufficient to admit traffic. Initial public promotion requires
explicit reviewed preconditions. Later verified candidates for an already
activated identity can promote automatically, unless a rollback hold is set.
Do not start this controller in production until exact edge enrollment,
network-policy enforcement, and release integration have passed acceptance.

## Additional verification and production findings

Focused race tests cover independent file and PostgreSQL intent CAS, tenant
isolation, immutable artifact history, failed activation, generation replay,
revocation during target-discovery failure, guarded Kubernetes Secret updates,
and preservation of application specs. The CLI requires an explicit generation
and preserves the reviewed JSON spec. Frontend generated types are synchronized.
The complete backend `make test` passed again after these additions. An initial
run hit the unchanged node-journal fixture's ten-second timeout; that fixture
passed in isolation, and the subsequent full suite passed. Full frontend
`contract:check` still reports the historical archive errors; a current-source
typecheck excluding that archive only in memory passed.

The edge SSH proxy now prepares new listeners before committing a route set.
A target change updates the existing listener, preserving established streams;
new connections use the new target. An occupied new port or a duplicate public
port rejects the candidate without disturbing prior listeners. Loopback tests
verify both old and new connections during the switch. Edge persistence failure,
loaded-route acknowledgement, and safe binary handoff still require acceptance.

Public endpoint reservation now has an independent persistent binding. Preparing
a binding preserves the reviewed legacy hostname/port and route digest, or
allocates a separate address for a new managed endpoint without enabling app SSH.
Application spec changes and legacy disable cannot destroy that reservation.
The PostgreSQL migration installs a bounded port-reservation guard that rejects
an older writer stealing a managed port or rotating its owner through the legacy
table. New native and managed allocators share the same transaction-scoped lock;
the file backend locks ordinary state before independent bindings. Concurrent
mixed allocation passed on both backends with no overlaps.

`managed/status`, `managed/binding`, `managed/activate`, and `managed/rollback`
expose review and mutation as distinct authenticated API operations. The CLI
submits reviewed request files without inventing preconditions or changing
client trust. Activation requires exact control revision, intent generation,
artifact digest, fingerprint, binding, and legacy route digest. The transaction
rechecks current intent and tenant authorization. Failed or stale requests cannot
publish a route. New native SSH mutations reject a managed binding.

Production public activation requires PostgreSQL. A separate
`fugue_managed_ssh_routes` record and immutable route history commit atomically
with serving state. The legacy endpoint table is a guarded compatibility
projection for older API readers, not the owner of serving configuration.
Triggers preserve the managed target and fingerprint across old application
updates, disable operations, and live-resource cleanup. Tests exercise old SQL
writes and the unchanged legacy bundle producer after activation. Public port
allocation and route projection remain independent from the application spec.
The file backend supports local identity/intent/candidate testing, but public
activation fails closed because an older file-backend binary cannot enforce
this projection ownership.

Rollback selects a retained, freshly verified slot and revalidates current
authorization; it does not restore revoked keys or expired grants. It retains
the same host identity and places automatic promotion on hold. A new candidate
can still be prepared, but explicit activation is needed to clear the hold.
PostgreSQL tests verify two activations, retained old slots, rejected revoked-key
rollback, successful reviewed rollback, and complete transactional route history.

`TestCandidatePublicationIsolatedKubernetes` additionally runs the real
Kubernetes publication adapter against a disposable kind cluster with the
managed SSH image pinned by its local manifest digest. It verifies idempotent
SSA publication, Secret projection, candidate startup under named-Pod RBAC,
Pod and Service handshakes, execution in the unchanged target filesystem,
authorization hot-reload, and zero application restarts or spec edits. The
separate native SSH/SCP/PTY/SFTP integration also passed on the refreshed helper.
Kind's default CNI does not prove NetworkPolicy enforcement; deny-path network
tests must run with an enforcing CNI before production acceptance.

The subsequent Calico v3.33.0 acceptance run enabled an enforcing CNI in a
task-owned kind cluster. Its network tests require positive controls first,
then verify that another namespace cannot reach SSH or health, the edge front
cannot reach controller health, and the managed server cannot connect to an
arbitrary tenant endpoint. The allowed API path, runtime helper execution,
revocation, and retirement are tested in the same run. The upstream Calico
manifest is content-hash pinned by the CI verifier; its Pod CIDR is explicitly
set to avoid the local Docker network. This verifies the rendered Kubernetes
policy semantics; production CNI admission still requires a production canary.

Read-only production checks confirmed the affected workload remains on its
original Pod UID and image, with zero container restarts. Its selected business
container supports the helper prerequisites, runs as root on x86_64, has writable
temporary storage, and uses `/workspace`. No helper was installed, no file was
changed, and no application process was restarted by this preflight.

The live SSH front consists of two DaemonSets with one German and two US replicas.
They are absent from the current declarative component registry, although their
binary is included in `Dockerfile.edge`. Building a new edge image therefore
does not establish that the live SSH fronts update. Add explicit release lanes
and verify session draining before upgrading these host-network listeners.

## Route publication verification checkpoint

After refreshing the backend checkout to `origin/main` at `779eb55f`, the full
`make test` suite passed with these route changes, including contract generation,
all Go packages, static-Caddy verification, and Windows CLI cross-compilation.
The real PostgreSQL integration passed activation/rollback, transactional audit,
old-producer projection, independent port reservation, and stale authorization
checks. Real candidate publication and native SSH/SCP acceptance passed in the
task-owned kind cluster; that cluster and the disposable PostgreSQL container
were removed after verification. No production resources were created or changed.

Frontend OpenAPI snapshots/types were regenerated from the refreshed backend.
Current application sources passed typechecking with the historical archive
excluded only in memory. The full frontend contract check still fails on the
pre-existing archive; no compiler configuration or archived source was altered.

## Edge acknowledgement and safe slot retirement

The SSH front now prepares all new listeners, atomically writes and fsyncs its
route cache, and only then commits the new listener targets. Failed persistence
retains existing listener targets and emits no successful acknowledgement.
An instance UID supplied through the Pod downward API is required for ACKs.
`POST /v1/edge/ssh/ack` requires the exact node-scoped token, not just membership
in the same edge group. The API checks the current authorized route-set digest
and version, then stores bounded evidence separately from serving intent.
Replayed timestamps cannot refresh old evidence. Front status exposes loaded
digest and ACK freshness separately from route synchronization.

ACKs include established connections grouped by their original target. Counts
start synchronously at acceptance, before dispatch to the connection goroutine;
an older stream is not misattributed to the new target after route activation.
These records are runtime observations, not permission to publish configuration.
The control plane derives the exact expected instances from declared edge Pod
selectors and node identity; absence or ambiguity prevents retirement. It also
probes each declared edge public address against the expected SSH host key,
requiring that address to belong to the declared Kubernetes node.

Retirement requires a fresh verified active slot, matching ACKs from every
declared consumer after activation, no old-target edge connections, and an
explicit rollback retention interval of at least one hour (default 24 hours).
It first publishes a signed draining configuration to the old SSH server.
Draining refuses new transports but retains existing authenticated sessions.
Only observed zero connections and the exact draining digest permit a
resource-version-fenced scale to zero. Pod disappearance is verified separately.
Subsequent reconciliation does not reset replicas to one. Secrets, roles, PVCs,
LKG, audit files, and retired metadata are retained; no storage destruction is
performed. Retiring slots cannot be selected for direct rollback, and reuse
requires a new signed generation plus candidate validation.

Unit/race tests cover failed route persistence, exact node identity, expired and
replayed ACKs, wrong route/instance, retained old streams, zero-connection drain,
and retirement metadata preservation. PostgreSQL integration verifies ACK and
retirement roundtrips. The real candidate-publication test now also hot-loads a
draining artifact, scales the candidate down, verifies Pod disappearance, and
checks that later SSA reconciliation does not restart it or modify the workload.
That isolated test passed, including the stricter Service EndpointSlice ownership
check. Secret projection took approximately one minute during this run; it is
not represented as instantaneous revocation or instantaneous retirement.

## Independent keyring recovery lane

The existing `ci.yml` entrypoint has a separate `managed_ssh_keyrings` job that
does not depend on code compilation or deployment. It is inert until an explicit
`deploy/environments/production/managed-ssh/keys.json` declaration exists.
The declaration contains generation/predecessor digests and Secret references,
never private material. Material is read from the independently retained
`FUGUE_MANAGED_SSH_KEYRINGS_JSON` CI secret and must match those digests.
Encryption and signing keys cannot reuse material. Updates retain old keys,
validate both resources before mutation, use resource-version preconditions,
and retry safely after a partial write. Missing existing material is never
regenerated; `restoreMissing: true` restores only the exact reviewed predecessor
keyring from backup and cannot simultaneously rotate it. Five local tests cover
replay, partial update, forbidden recreation, exact restoration, and key reuse.
The CI contract verifies current-main selection, production scope, explicit
declaration, and independence from code-release jobs.

The generation-one keyring declaration was pushed as `54a5cd76`. CI restored
both keyrings successfully; read-only Kubernetes metadata checks matched the
reviewed generations and digests. Private material is retained in the production
GitHub environment Secret and an owner-only local backup outside the repository.
No application identity has been prepared from these keys. Database/identity
backup restoration and storage-key rewrapping/retention procedures still need
acceptance before production identity preparation.

Read-only production inspection found the US SSH-front DaemonSet references the
same token Secret across multiple node instances. Its Secret name does not prove
the token's actual principal, so production enrollment must verify each node's
credential binding and provide distinct node-scoped credentials where needed,
rather than weaken the exact-node evidence check. Current fronts also lack the
instance-UID environment projection. Both gaps must be resolved through the
reviewed release path before retirement is enabled.

The final `make test` run after ACKs, retirement, and keyring recovery passed,
including the expanded declarative CI contract and the five keyring tests.
The disposable retirement-test kind cluster and PostgreSQL container were
removed. Frontend generated-contract checks passed; full frontend typechecking
still reports the existing historical archive errors. Current application
sources passed the in-memory archive-exclusion check. No production keyrings,
routes, application Pods, client trust, commits, or pushes were changed by this
verification checkpoint.

## Staged production release enrollment

The release registry now includes `ssh-control`, built by the existing
declarative component machinery. Its first Deployment is deliberately limited
to Kubernetes reads of namespaces, nodes, Pods, Services, and EndpointSlices.
It has no Secret-read, workload-write, RBAC-write, or exec authority. Optional
configuration/keyring mounts allow the observer to start before independent
configuration is provisioned. No managed intent exists merely because this
component is installed. Its health endpoint distinguishes loop counters from
individual reconciliation failures. Runtime mutation authority will be enrolled
separately with explicit namespace constraints after canary acceptance.

The API manifest independently mounts the optional SSH identity encryption
keyring file. Preparing keys remains unavailable until reviewed material is
provisioned. The schema release is additive and precedes API/controller rollout.
At this checkpoint the selected component plan is API, schema, and the new SSH
controller. Existing SSH fronts and application Deployments are not enrolled
for a code rollout by this first stage; no public endpoint activation is part
of the build or controller install.

The independent `managed_ssh_artifact` CI job builds the digest-addressed runtime
image, verifies its OCI source label and helper digest, and runs both real
candidate publication/retirement and native SSH/SCP acceptance. The verifier
also installs the pinned enforcing CNI and requires the network allow/deny
checks to run rather than skip. Its public receipt records the immutable image,
source commit, helper digest, architecture, and verification results. That
receipt is not serving intent: runtime configuration must explicitly select it.

The first release run stopped before deployment: affected tests and vet passed,
but warming all affected packages before compilation left only 10.5 seconds for
the release-tool suite within the unchanged 240-second CI budget. The fix warms
only the identified large API package; remaining packages run with compilation
under the same concurrency bound. No test or timeout gate was removed. Tests
verify overlap, exactly-once execution, and preservation of warm-phase failures.
The retry passed prepush and built API, schema, and SSH-control images; production
rollout and the separate immutable SSH-image acceptance job remain monitored.

Frontend generated contracts were pushed as `d58f3cce` after integrating current
frontend main; its GitHub contract-drift check passed. Existing historical source
archive errors still prevent an unfiltered local TypeScript check.

The independent runtime publisher is prepared for the next stage. It requires
the exact successful artifact job, source commit, receipt digest, immutable
image, and network-isolation evidence. Runtime configuration is published only
after explicit namespace-scoped authority and default-deny policy resources.
Tenant authority excludes application Deployment writes and Secret reads;
foreign resource ownership, stale predecessor versions, image substitution,
and unreviewed namespace changes fail closed. No production runtime declaration
or mutation-authority enrollment has been published yet.
