# Native production release runtime: single Deployment

This is an implementation boundary, not proof of a production cutover. The
legacy workflow and runtime are still needed for resources that have not passed
the independent cutover checks.

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
The operator-selected context is not a cryptographic cluster binding; this
report alone is not independent cutover evidence and cannot authorize a write.

`observe-participation-member JOURNAL_CONFIG KUBECONFIG CONTEXT NODE` verifies
physical Node identity and a bounded group inventory. It checks the exact
version and legacy spec digest of the selected serving Front Service, or checks
the three retained group-owned OnDelete DaemonSets if none matched the bounded
manager-scoped list. It repeats those observations and rejects object drift.
The diagnostic requires Node GET/LIST, Service GET/LIST and DaemonSet GET,
with namespaced reads restricted to the topology namespace. It does not query
DNS, infer traffic withdrawal, approve recovery or authorize a write.

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
