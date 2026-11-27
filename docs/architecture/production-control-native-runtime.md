# Native production release runtime: single Deployment

This is an implementation boundary, not proof of a production cutover. The
legacy workflow and runtime are still needed for resources that have not passed
the independent cutover checks.

## Global DNS offline policy

The shared journal resource for changes to the global producer is
`producer/global-policy`, not a resource per edge. The producer selection
fence and content digest are distinct from the serving Full release and its
verified LKG. All three must match the observed predecessor.

- `prepare-dns-policy HTTPS_ORIGIN READ_TOKEN_FILE DRAFT_TOKEN_FILE HOLD_CHANGE`
  accepts an exact edge ID, before hold (or null) and after hold (or null).
  It derives and validates the immutable DNS and producer policy artifacts
  with Go's existing typed single-hold validator. It may create or validate
  those drafts; it cannot select or release them. The draft account needs
  only artifact creation/validation, never publication authority. Retrying
  preparation reuses the same immutable generation for the same predecessor
  and change. An unknown or mismatched generation cannot be substituted.
- `shadow-dns-policy HTTPS_ORIGIN READ_TOKEN_FILE NEXT_PRODUCER_ARTIFACT_ID`
  is GET-only. It compares the two runtime summaries against the same
  business revision, active producer, verified Full and unexpired LKG, checks
  the intended membership and existing offline recovery conditions, and
  returns a content-addressed comparison. It does not prove DNS withdrawal,
  consumer convergence, or rollback.
- `draft-dns-policy JOURNAL_CONFIG HTTPS_ORIGIN READ_TOKEN_FILE NEXT_PRODUCER_ARTIFACT_ID REQUEST`
  repeats the comparison and additionally requires a settled journal head
  matching the previous producer fence and digest. The owner can inspect and
  sign the resulting exact intent. The shadow output itself is not approval
  or terminal evidence.
- `draft-dns-policy-rollback JOURNAL_CONFIG HTTPS_ORIGIN READ_TOKEN_FILE DRAFT_TOKEN_FILE ORIGINAL_INTENT_DIGEST REQUEST`
  accepts only the exact currently settled `applied` DNS intent. It constructs
  the inverse one-edge delta, prepares immutable drafts, and runs the same
  read-only shadow before returning a new intent. This is not an automatic
  revert: the inverse requires a fresh independent approval and the ordinary
  native dispatch/serving-settlement path. It refuses a pending, superseded,
  `not_applied`, or mismatched original operation.

These commands use dedicated credentials and an explicit HTTPS origin, do
not follow redirects, and never use a publication endpoint. A signed intent
is not permission to call the existing generic artifact release API. The
journal refuses DNS claims under per-edge identities or any other resource.

The platform-store schema now installs a dormant, one-way selection freeze
at the two tables used by the generic release and rollback transaction.
Mutations to the global producer policy selection take a shared lock on a
single durable gate row; the proposed freeze waits for those transactions
to finish and verifies the exact live selected release, fence and content
digest. Stale transactions cannot advance the selection after the freeze.
Schema reinstallation does not reopen it, and the serving Full/LKG and
unrelated lanes are not frozen by this selection gate. Tests use disposable
PostgreSQL, including an unprivileged writer and the actual Store release and
rollback paths.

The native provider authority is an append-only record, separate from the
freeze. Its creation requires the frozen predecessor to match the enrolled
journal baseline, no existing claim, and an independently provisioned SQL
login, not a group role or the schema owner. For this resource the journal and
provider tables must be in the **same PostgreSQL database**. Do not use two
separate connections to two databases to simulate a transactional fence.

`fugue-production-control observe-dns-cutover JOURNAL_CONFIG` reads the
selected producer, provider freeze, writer enrollment and current grants,
and journal head in one read-only PostgreSQL snapshot. Use its output to
distinguish a selected producer from the last verified serving version. This
command requires a read-only profile with access to both sets of tables.
External permission revocation and request drainage have separate evidence.

`fugue-release-guardian dns-policy-dispatch CONFIG INTENT_DIGEST APPROVAL_DIGEST`
is the native executor. Its independently mounted configuration contains
`journalFile`, `apiOrigin`, `readTokenFile`, and `artifactKeyFile`. The journal
file contains the hostname-verified TLS DSN for the dedicated writer login,
cluster, and owner public keys. The API token is GET-only. The artifact key
file contains `primaryKey`, `primaryKeyId`, optional `previousKey` and
`previousKeyId`, and optional `revokedKeyIds`; the HMAC verifier keys must
not be issued by GitHub or confused with the independent owner key. All
credential files must be private. The process does not start the general API
or legacy Guardian loops and does not have a generic publication route.

The executor repeats the paired DNS shadow against the exact approved change,
prepares the existing compiler admission proof, reserves its single durable
claim, and enters the existing Store publication transaction. In that one
transaction it holds the journal resource lock through commit, verifies
the original Ed25519 approval and pending fence again, resolves the exact
producer/Full/LKG and all signed source references (including the LKG ID),
and inserts a transaction-bound provider receipt. PostgreSQL independently
checks the enrolled writer login and claim and permits only the exact
predecessor supersession, incremented fence, and target selection. A deferred
constraint aborts a receipt without the matching selection. No receipt or
mutation from a different session can be replayed. Legacy generic releases
and rollbacks remain fenced after native authority is active. The Store
still runs all of its ordinary signed reconfiguration and retention checks.

A dispatch reports **policy selection**, not actual DNS serving. An
ambiguous commit or repeated request returns the existing claim and cannot
start another dispatch; the last verified journal head remains unchanged
until an independent observer proves the terminal result. The approved and
fresh shadow digests are recorded in the same provider receipt as the write.
Tests cover actual PostgreSQL triggers and a real signed Store producer
reconfiguration while preserving Full/LKG; their shadow and enrollment
evidence is synthetic. They do not prove production credential revocation,
input immutability, external request drainage or DNS consumer convergence.

The independent bootstrap profiles `dns-checkpoint`, `dns-anchor`, and
`dns-settle` accept `dnsOrigin`, `dnsReadTokenFile`, the corresponding journal
profile, and the observer public key and witness directory. Do not provide a
publication or Kubernetes credential. `dns-checkpoint [INTENT_DIGEST]` is
read-only. It reports the exact producer selection, Full/LKG and immutable DNS
member. It checks the member's SOA serial and primary and its probe record
answers on every declared public listener address over both UDP and TCP. It
rereads the checkpoint after the sweep and rejects a changed checkpoint or a
target selection whose Full still points at its predecessor.

For `dns-anchor` or `dns-settle`, the observer signs the ordinary production
execution witness. Its `observationDigest` identifies a canonical DNS
`ServingEvidence` record with `checkpointDigest` and `listenerCoverageDigest`
from the read-only command, and `authoritativeDnsDigest` from the separately
collected application DNS answer report. The witness also names separate
`authorityDigest` evidence. Mount the witness, observation record, canonical
`listenerCoverage` object, application DNS answer report and authority record
under their content digests. Settlement repeats the listener sweep and requires
an exact match. The signer verifies the affected application answers before
signing. The verifier checks the separate application report's SHA-256 binding.
A baseline witness must
also match the exact provider freeze before native activation. An `applied`
result needs its matching committed provider receipt; `not_applied` requires
the exact unchanged verified predecessor and no receipt. The result check
and insert hold the same journal resource lock as publication, and once the
terminal record commits, no late native write under that claim can commit.

There is intentionally no production freeze/activation command yet. Do not
manually edit the gate or native authority. The rollback draft above covers
only a settled, serving `applied` change; recovery from an ambiguous or
non-converging selected policy still needs an independently approved recovery
coordinator and must not bypass the claim fence. Complete the independently
approved cutover coordinator and actual DNS answer collection before freezing
the legacy lane or declaring it migrated. The
database principal for schema migration and authority activation must be
isolated from the old clients, and the dedicated runtime role must not have
authority-table, DDL, approval, or terminal-journal mutation privileges.
The database checks role memberships and forbidden grants at enrollment and
again when the writer inserts a receipt. On PostgreSQL installations where
`PUBLIC` can create objects in the provider schema, use a dedicated provider
schema or remove that shared grant before enrollment.
There is no production DNS cutover evidence yet.

## Node participation topology

An owner can migrate the existing participation policy without authorizing a
provider mutation. Run `migrate-journal` with the schema-owner identity, then
`draft-participation CLUSTER REQUEST POLICY` with the legacy policy as the
initial input. Inspect, sign, and record it with the standard approval flow.
`store-participation JOURNAL_CONFIG ACTOR INTENT_DIGEST APPROVAL_DIGEST`
commits only a topology revision and its audit event. It cannot reserve a Node
write. The catalog principal needs SELECT on intents, approvals and
participation topologies, and INSERT only on participation topologies; the
audit event is written by an owner-installed trigger. Readers need only SELECT
on those three tables and schema USAGE.

`read-participation JOURNAL_CONFIG [GENERATION]` authenticates the current or
historical revision. An approved revision remains readable after the signing
window closes. Subsequent drafts must name the exact previous generation and
digest. To restore older policy content, create a new, consecutive revision
based on that content and sign it against the current head; reading history
never changes the head. Do not put policy files or owner signing material back
in the program image. The legacy policy remains in use until shadow parity
and provider-side writer cutover have been demonstrated.

Live Kubernetes diagnostics require a separately approved
`fugue.automatic-node-participation/v2` revision. In the draft, add
`kubernetes.endpoint` (the exact HTTPS API origin), `kubernetes.ca_digest`
(SHA-256 of the DER bytes of the one trusted CA certificate, with a
`sha256:` prefix), and `kubernetes.namespace_uid` (the enrolled namespace
UID). Obtain these values from a trusted cluster inventory, not from the
context being checked. The client must present that origin and CA,
without a hostname override or proxy, and return the enrolled namespace's
UID before and after observation. Both PostgreSQL and Kubernetes must be
read-only to the observer; add only Namespace GET for the enrolled name
to the permissions below. Changing the approved cluster binding requires
a new owner-approved topology revision. Legacy v1 remains readable and
replayable, but cannot drive live diagnostics.

`preview-participation JOURNAL_CONFIG NODE SNAPSHOT` uses the journal's
read-only connection to load the current approved topology. The input JSON
uses schema `fugue.participation-snapshot-replay/v1` with `cluster`, `topology`
(an object with `generation` and `digest` copied from `read-participation`),
`observedAt` (RFC 3339), and `observations` with `node`, `inventory`,
`retained`, `validatedReaderBaseline`, and `transport` (or `null`). The
preview reports the observed
Node UID and resourceVersion plus any proposed hold annotation changes and
the exact prior annotation map. A `review_recovery` result identifies the
point at which legacy recovery begins its Front, Worker, and network checks.
The snapshot comes from the caller. Treat each report with
`kind=untrusted_snapshot_replay` and `executionEvidence=false` as a debugging
record only. Production shadow validation requires independent live
observations and comparison of the recovery and rollback preconditions.

`observe-participation-readers JOURNAL_CONFIG KUBECONFIG CONTEXT NODE` is a
separate diagnostic for those live checks. It takes the reader names from the
approved topology and reads the two reader Deployments, their selected Pods
and owning ReplicaSets over a GET-only, hostname-verified TLS transport. It
checks rollout convergence, capability annotations, exact runtime image
digests and the Guardian's declared group baseline, then repeats the read and
rejects changes to those object versions. Use a Kubernetes principal with
Deployment and ReplicaSet GET and Pod LIST in the topology namespace only.
The context is verified against the approved cluster binding. This report
alone is not independent cutover evidence and cannot authorize a write.

`observe-participation-member JOURNAL_CONFIG KUBECONFIG CONTEXT NODE` verifies
physical Node identity and a bounded group inventory. It checks the exact
version and legacy spec digest of the selected serving Front Service, or checks
the three retained group-owned OnDelete DaemonSets if none matched the bounded
manager-scoped list. It repeats those observations and rejects object drift.
The diagnostic requires Node GET/LIST, Service GET/LIST and DaemonSet GET,
with namespaced reads restricted to the topology namespace. It does not query
DNS, infer traffic withdrawal, approve recovery or authorize a write.

`shadow-participation-kubernetes JOURNAL_CONFIG KUBECONFIG CONTEXT NODE`
runs the native phase preview on observed Kubernetes objects rather than on
a supplied snapshot. It rechecks the approved topology after the reads and
uses the live reader baseline between two checks of member object versions.
If a Ready Node has a serving Service, it reports that DNS and heartbeat
evidence is required, with no proposed action. If stabilization has elapsed,
it still requires Front, Worker and network recovery checks before any
readmission can be considered. It is not a comparison against the legacy
runtime on production and emits no executable intent or cutover evidence.

`shadow-participation-dns JOURNAL_CONFIG KUBECONFIG CONTEXT NODE PRIVATE_TOKEN_FILE`
adds a GET to the exact HTTPS Fugue origin in the approved topology when a
Ready Node has a serving Service. It binds the returned node, group and
public address to that Service and compares two fresh observations around
the Kubernetes reads. A suspension without verified serving exclusion,
unavailable policy, expired heartbeat evidence or a change during the read
fails closed. A matching result is a phase preview only; it is not an
independent readback from the DNS provider and cannot authorize any write.
Stabilization still does not prove Front/Worker or network recovery.

The API serves this projection at
`GET /v1/edge/nodes/{edge_id}/participation-observation`. Leave it disabled
unless a dedicated seed is provisioned as
`FUGUE_PARTICIPATION_OBSERVER_SIGNING_KEY` and its key ID as
`FUGUE_PARTICIPATION_OBSERVER_SIGNING_KEY_ID`. Keep the seed out of the
shadow client, Actions, the repository and logs. On the trusted issuer,
`issue-participation-observer JOURNAL_CONFIG NODE KEY_ID PRIVATE_SEED_FILE`
reads the approved enrollment and outputs a node/group-scoped token valid
for two minutes. Redirect stdout straight to a new private file (use
`umask 077`) for the read-only observer; never print the token in logs.
These credentials do not grant admission, DNS publication or Kubernetes
permissions and cannot authenticate an admin or other component endpoint.

`draft-participation-hold JOURNAL_CONFIG KUBECONFIG CONTEXT NODE REQUEST
[PRIVATE_TOKEN_FILE]` takes a fresh read-only shadow and requires a settled
journal head for the enrolled Node. A Ready Node with a serving Service
requires the private observation token. The resulting review submission binds
the approved topology revision, physical UID, exact Kubernetes resourceVersion,
and before/after values of the two owned annotations. An absent value is
recorded as null so rollback is distinguishable from writing an empty value.
No unrelated annotations can be included in the proposed write. The current
state digest must match the last independently verified journal version.
`inspect SUBMISSION` displays the decoded hold and its exact rollback values
together with the digest, without accessing the cluster.
The draft does not anchor a baseline, reserve a claim, recognize readmission,
or authorize any Kubernetes action. The dedicated offline dispatcher below
does not support readmission or recovering holds.

`preflight-participation-hold JOURNAL_CONFIG KUBECONFIG CONTEXT SUBMISSION
[PRIVATE_TOKEN_FILE]` re-observes the Node identified by the submission.
It rejects drift in the approved topology and approval, journal predecessor,
physical Node and resourceVersion, or proposed annotations and phase. The
fresh read's preview timestamp is not the actual application time. A match
does not check production write authority, grant a claim, permit execution,
or establish that recovery is safe; the report has `executionEvidence=false`.

`render-participation-freeze NODE` prints a Kubernetes admission policy and
binding. It installs nothing and grants nobody a write path. For one Node
name it prevents admission of creates/deletes and changes to the automatic
phase, release hold, group, role, enrollment, scheduling eligibility, and
workload classification metadata. Ordinary Node status and unrelated labels
can still change. Use it only during a resource-scoped freeze; legitimate
changes to protected metadata must also wait. It does not fence recovery Pod
deletes, Pod exec, Leases, other provider writes, or requests already past
admission. Before handing off ownership, separately disable both the Actions
lane and the API's Python worker, revoke their provider authority, verify
every serving admission endpoint, and prove their in-flight operations have
drained. This policy is not authorization to enable a native writer.

`observe-participation-freeze JOURNAL_CONFIG KUBECONFIG CONTEXT NODE` reads
the exact admission policy and binding for an enrolled Node, on the
approved cluster. It requires GET for those two named objects. It rejects
drift from the rendered specification, reported expression warnings, and
revisions that change during the read. `currentTypeChecked: false` means the
API has not reported type checking of the current generation. No result
claims enforcement across endpoints, credential revocation, or drainage.

The offline hold guard uses the same policy and binding identity as the
freeze. It permits the dedicated writer to change only the two owned
annotations, from the exact prior values to the proposed values, on the
enrolled UID and pre-write resourceVersion. The writer cannot use this
guard for a recovery transition; its stability timer must start at actual
application, not draft time. The guard does not grant RBAC or prove that
the legacy writer has drained. The dispatcher verifies the stored guard
but cannot install or promote it.

The offline hold write primitive sends one JSON Patch over a fresh,
hostname-verified TLS connection, without a proxy, redirects or retries.
The patch tests the enrolled Node UID and resourceVersion before changing
the two owned annotations. Its response is not a settlement witness. It
is covered against an isolated Kubernetes API. Only the gated offline
dispatch path below invokes it in Guardian.

`observe-participation-offline-guard JOURNAL_CONFIG KUBECONFIG CONTEXT SUBMISSION WRITER`
reads the policy and binding twice on the approved cluster and compares
them to the exact proposed hold and named dedicated ServiceAccount. Like
freeze observation, it reports stored configuration only. It does not prove
enforcement across endpoints or authorize a write.

`observe-participation-offline-readback JOURNAL_CONFIG KUBECONFIG CONTEXT SUBMISSION`
uses a separate read-only client to read the enrolled Node twice, with
the approved namespace identity checked before and after. It reports:

- `target_visible`: the exact target metadata is visible on the enrolled
  Node at a version different from the reviewed predecessor.
- `predecessor_visible`: the reviewed resourceVersion and metadata are
  still present; an in-flight request may yet apply.
- `condition_changed`: the reviewed resourceVersion no longer matches,
  but the observed owned metadata is not the target.

Missing, replaced, changing or contradictory observations produce an
error, not a terminal receipt. Even target visibility does not prove
which identity applied the change, that old requests have drained, or
that the related DNS traffic has been withdrawn. The report has
`executionEvidence=false`; none of these observations authorizes a retry,
settlement or advancement of the last verified version on its own.

`observe-participation-pending JOURNAL_CONFIG KUBECONFIG CONTEXT INTENT_DIGEST`
is the restart/read-only form for unresolved dispatches. It loads the
stored submission by digest, checks the same pending fence, approval
reference, last verified version and approved topology before and after
provider readback, and refuses output if the claim settles or ownership
changes during observation. It never accepts a local replacement
submission, reserves another dispatch or settles the claim. This
command reports `executionEvidence=false` even when the target is visible.

The ledger serializes Node hold reservation with topology activation.
An offline hold can acquire a new fence only for the current approved
topology; an unsettled Node fence prevents a successor topology from
activating. Replaying the previous topology activation still returns the
original receipt. This serialization does not fence any provider writer.

Pre-dispatch authority for an offline hold is a separate, short-lived
provider attestation. It binds the reviewed intent, approved Kubernetes
identity and topology, dedicated writer, and exact type-checked policy and
binding revisions. It must reference distinct independently retained
records for admission across serving endpoints, old permission revocation,
old request drainage, and native writer identity. Neither an owner
approval nor the observation commands can supply that attestation. No
collector or production authority has been provisioned for this operation.

`fugue-release-guardian participation-offline-dispatch CONFIG INTENT_DIGEST
APPROVAL_DIGEST PROVIDER_ATTESTATION` runs one explicit offline annotation
transition or the exact approved inverse of a settled hold; it is not
triggered by GitHub, the normal Guardian reconcile loop,
or an owner approval alone. It loads the immutable submission from the journal,
verifies the dedicated provider writer, and repeats the live shadow and
authority checks after reserving the one-time fence. A lost response or crash
leaves the claim pending; invoke `observe-participation-pending` to check it,
not another writer. A successful PATCH response does not settle the claim.

The mounted settings are `journalFile`, `actor`, `writer` (a dedicated
ServiceAccount username), `writerKubeconfig`, `writerContext`, `readerKubeconfig`,
`readerContext`, `observerPublicKey` (base64 Ed25519), and optional
`dnsObserverTokenFile` for a Ready Node's Fugue observation. The journal
principal reads stored topology, baselines, approvals, claims and results,
and may append claims only; it cannot approve, activate topology, anchor,
or settle. The Kubernetes reader gets observation access only and the
writer gets SelfSubjectReview and the exact scoped Node patch permission,
not policy management or other Node/Pod actions. No role or attestation is
provisioned by running the command.

For initial enrollment and recovery after a pending dispatch, use a
dedicated API-independent bootstrap profile for the exact Node:

- `participation-checkpoint [INTENT_DIGEST]` is read-only. It hashes the enrolled
  physical Node state and closed, type-checked admission policy/binding
  revisions (and, when supplied, the unresolved claim). It does not sign a
  witness or establish that old or delayed writes have drained.
- `participation-anchor GENERATION VERSION_DIGEST WITNESS_DIGEST` requires the
  independent signed baseline witness and a fresh matching closed checkpoint.
  The ledger rejects a concurrent change of approved topology.
- `participation-settle INTENT_DIGEST OUTCOME GENERATION VERSION_DIGEST WITNESS_DIGEST`
  requires independent signed proof bound to the pending claim and matches
  a fresh closed checkpoint. `applied` requires the exact target at the
  next ledger generation. `not_applied` requires the exact predecessor
  with the original generation and evidence that the claim cannot write.
  Other observations leave the claim pending. A rollback settles as
  `applied` on its own claim (the prior digest at a new generation), not
  `restored` on the original claim.

These profiles mount `mode`, `node`, `journalFile`, `actor`,
`observerPublicKey` and `witnessDirectory`; they do not accept a Deployment
cutover or registry credentials. Kubernetes access is read only. Use
independent PostgreSQL roles for checkpoint, anchor and settlement.

For an offline Node hold rollback, first close the original guard and settle
the original hold as applied. `draft-participation-hold-rollback` uses the
read-only journal and Kubernetes context to verify the closed checkpoint
matches the current LKG in the original topology. The new intent references
the prior result and restores exactly its predecessor, including annotation
absence or empty strings; record and approve it independently. The journal
rejects attempts to reverse a different, unrelated or superseded operation
even if that request is owner-signed. Do not reopen the old writer.

`observe-participation-rollback-boundary` returns the exact open guard
revisions and the stable Node, inventory, and reader observation to which
the independent attestation must be bound. Readiness alone is insufficient:
the attestation separately requires approved network recovery after the
current serving activation, the exact Front/Worker and approved probes,
and a serving-authority fence that outlives the possible request until
the rollback guard is closed and the request drains. Expiration disables
dispatch; it does not discharge that fence. The dispatcher verifies the
per-member stability window, checks both authorization layers, writes at
most once, and leaves a missing result pending for the bootstrap. The
repository does not provision the live serving fence or collect and sign
these external records; production readmission cannot be inferred from
this procedure or from fixture signatures.

## Single-Deployment release draft

Before cutover, `guardian-native-shadow KUBECONFIG CONTEXT PLAN_DIR CLUSTER_ID REQUEST_ID`
compares legacy and native publication planning against the same candidate.
It reads Kubernetes without publishing, checks the live Deployment UID and
resourceVersion, and requires matching predecessor, candidate, health, and
rollback bindings. The report is not an approval or proof of a live rollout,
rollback, or old writer revocation.

`guardian-native-draft` accepts the same arguments and emits a portable
intent and its exact payload. The draft still uses the GET-only transport
and does not record an intent or submit a release. Use the standalone
`fugue-production-control` command to keep review and approval out of GitHub:

- `inspect SUBMISSION` prints the bound intent and its digest. Review the
  draft's bound plan and predecessor, not just a source revision.
- `sign SUBMISSION EXPECTED_DIGEST KEY_ID PRIVATE_PKCS8_PEM TTL` works without
  a network connection. It requires the reviewed digest and an owner-only
  Ed25519 PKCS#8 PEM key. The private signing key stays off the journal,
  provider, and CI hosts.
- `record-intent JOURNAL_CONFIG ACTOR SUBMISSION` and
  `record-approval JOURNAL_CONFIG APPROVAL` only append to PostgreSQL. Give
  the intake principal SELECT/INSERT on intents and approvals and INSERT
  on audit, not execution, Kubernetes, DNS or artifact write permissions.
  The caller label is an audit annotation, not an authorization credential.

## API-independent bootstrap

Build the standalone image with `Dockerfile.release-bootstrap`. It does not
contain the Fugue API, Python, or a provider administration client. Run it
outside the API Deployment's lifecycle. It connects directly to PostgreSQL;
publication and observation also require the dedicated in-cluster identity
and Kubernetes CA. It does not apply policy, grant access, or modify the
Deployment directly. Guardian remains the sole workload writer.

Mount one read-only bootstrap configuration per command. The fields are
`mode` (one of `publish`, `observe`, `anchor`, `settle`), `journalFile`,
`actor`, `cutover` (the exact native `KubeDeploymentCutover`), the
`dependencyService` used by Guardian, `observerPublicKey` (base64 Ed25519
public key), and `witnessDirectory` (read-only, with execution witness
files named by the hexadecimal part of their digest plus `.json`).
Only the publisher takes `registry` and optional `registryAuthFile`.
Keep identities separate in Kubernetes and PostgreSQL, not just in the
command profiles: publication cannot anchor or settle; observation only
reads; the anchoring and settlement principals have no provider mutations.
The profiles do not install roles or grant permissions.

- `publish INTENT_DIGEST APPROVAL_DIGEST ATTESTATION_FILE` requires a signed
  attestation covering the exact release boundary, including separately
  recorded endpoint admission, old credential revocation and request
  drainage, active native Guardian, node pull tests and registry retention.
  The observer can read the boundary with `guardian-native-boundary`
  (same arguments and GET-only transport as `guardian-native-draft`).
  The bootstrapper checks the signature, binding and freshness before the
  claim and before each publication write. It does not create or sign the
  supporting evidence.
- `observe INTENT_DIGEST` performs Kubernetes GETs and reads the journal.
  It reports the candidate or previously verified version actually
  serving; it does not conclude that this execution can no longer write.
- `anchor GENERATION VERSION_DIGEST WITNESS_DIGEST` and
  `settle INTENT_DIGEST OUTCOME GENERATION VERSION_DIGEST WITNESS_DIGEST`
  append only to PostgreSQL and require a signed execution witness using the
  `provider-observer` key ID. Settlement checks the original persisted claim.
  Neither operation needs a Kubernetes token.

Compensation still runs through Guardian and remains bound to the approved
rollback. A failed or lost submission must not be replayed as a new intent to
clear its fence. This bootstrapper is not authority to recover arbitrary
unhealthy or deleted workloads; such a recovery must have its own fenced
operation and proof before the API release path can be cut over.

## Guardian execution

- On an approved, marked DesiredRelease, Guardian compares the live pointer,
  candidate, and rollback monitor with the signed journal payload. It reserves
  an append-only launch entry under the pending resource fence and passes the
  exact pointer resourceVersion, candidate, LKG monitor, intent, and fence to
  its child. Publication and child launch consume different permissions. A
  recorded launch is never renewed or replayed, even if Guardian crashes before
  starting the child. Unknown outcomes require separate observation and recovery.
  A dedicated native Guardian must be configured with
  `FUGUE_RELEASE_NATIVE_REGISTRY` (explicit HTTPS registry host, optionally with
  port), and optionally `FUGUE_RELEASE_NATIVE_REGISTRY_AUTH_FILE` (mounted
  read-only JSON with `username` and `password`). Neither the mounted credential
  nor authorization to promote is provided by GitHub.
- `FUGUE_RELEASE_NATIVE_JOURNAL_FILE` names a read-only mounted JSON file
  containing `cluster`, `database` (one PostgreSQL endpoint with hostname-verified
  TLS), and `ownerPublicKeys` (key ID to base64-encoded Ed25519 public key).
  Provision the principal outside GitHub: `USAGE` on `fugue_control`, `SELECT`
  on intents, approvals, resource baselines, claims, results and launches, and
  `INSERT` only on `execution_launches`. It must not own the schema. It cannot
  add approval or terminal records or update and delete historical entries.
  Claim, launch and completion entries are written to the execution audit
  in the same SQL transaction. If the audit fails, no corresponding entry
  or authorization commits.
- The dedicated writer handles exactly one enrolled target. It does not start
  legacy candidate imports, authority activators, or the legacy artifact
  pruner, and it will not publish status or reconcile an unmarked pointer.
- Before leasing and writing, the child reads the immutable approval-bound
  execution snapshot and exact monitor record, verifies the full forward and
  rollback OCI images directly from the registry, and rechecks the pointers
  after acquiring the component Lease. Current-only repair does not walk or
  require the historical predecessor. A marked target cannot fall back to the
  Python verifier or the historical artifact shortcut. This adapter supports
  one existing Deployment and one approved image per manifest, including every
  declared sidecar and init container. It does not authorize deletion,
  additional image volumes, or delegated writes.
- Each native workload write is one UID/resourceVersion-bound server-side
  apply with no forced ownership transfer or client replay. A rejected CAS
  or unknown response cannot authorize compensation against a drifted
  Deployment, or be treated as a completed execution.
- A registry readback from Guardian does not prove that nodes can pull with
  GitHub disconnected, that registry retention pins both versions, or that old
  writers have been revoked and drained. Before switching a resource, prove all
  three, along with exact rollout/rollback and admission at the serving
  endpoints. An existing LKG pointing at GHCR cannot be adopted by merely
  copying its bytes to another registry: its sealed manifest uses the original
  immutable reference. The registry transition needs its own verified baseline
  and cutover procedure. Do not rewrite its historical monitor record to
  synthesize that evidence.
- `ObserveServingRelease` is read-only. It separately verifies the exact
  marked pointer, immutable monitor record and live Deployment, matches the
  ReplicaSet/Pod ownership chain and actual image IDs, and checks recent
  local, dependency, and route health. It never advances the PostgreSQL
  last-verified version: that additionally needs proof this claim cannot
  issue a delayed write. Unknown state is not proof of no write.
- `NewWitnessedExecutionJournal` authenticates content-addressed baseline
  and terminal attestations from a separately configured, resource-scoped
  provider observer, never from the owner approval key or publication input.
  An attestation binds two independently retained records: the positive
  readback and the proof of old-writer drainage or terminal write closure.
  Terminal attestations also bind the exact intent, approval, fence, and
  outcome. An unavailable, stale, or mismatched witness cannot advance the
  head. This verifier does not collect either record, provision signer keys,
  or establish that production drainage has occurred.
- Guardian status reports the serving record separately from the durable
  last-verified record. A healthy provider pointer with an unresolved journal
  claim remains `verifying`; a missing journal does not advance status.
