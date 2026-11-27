# Fugue microservice migration gates

The ownership manifest is a boundary contract, not proof that production has
already been split. Every later change must pass the gates below in order. A
failed gate freezes only the affected lane when that lane has its own release
coordinator; while `legacy-fugue-helm-release` is still shared, a failed gate
freezes the legacy release and no lane may mutate it independently.

## Gate A — inventory and shadow planning

- The ownership manifest validates in a clean checkout and every referenced
  source/artifact/state owner is unique or explicitly shared.
- A change classifier can produce a deterministic component/lane set and a
  shared-resource conflict set without performing a mutation.
- Existing production remains on the legacy `fugue` release. This gate must
  not change Helm values, Kubernetes objects, database schema, or image tags.

## Gate B — independent artifact

For one lane at a time:

- the lane has its own image/artifact digest, build cache, Helm values and
  release record;
- an unchanged lane produces byte-for-byte identical artifact digests;
- the lane owns its write path for spec/status/data, and all cross-lane access
  is through a versioned contract or a mediated adapter;
- a deployed component uses a dedicated least-privilege identity for that
  contract and neither imports another component's concrete store nor receives
  its database credential;
- the current and previous compatible API clients pass contract tests;
- the lane persists a last-known-good reference and can roll back without
  rewriting another lane's state.

## Gate C — cell and recovery proof

- A failed cell cannot write or restart objects in another cell.
- Reconciliation is idempotent: replaying the same `(component, cell,
  generation)` intent produces no additional mutation.
- Concurrent release-control workers replaying the same component-plan spec
  converge on one shadow release ID, fencing token, lane version, message, and
  status digest.
- A lane-local rollback or retry does not cancel or supersede an unrelated
  lane's release.
- Recovery is fail-closed when fencing, status handoff, evidence, or LKG is
  missing; it never falls back to a guessed live version.

## Gate D — production cutover

Production cutover requires all of the following evidence for the candidate
lane:

- two consecutive canary releases with zero cross-lane mutations and no
  unexpected rollback;
- at least 24 hours of healthy status convergence, including one injected
  dependency timeout and one component restart;
- API compatibility tests pass for the current and previous contract version;
- recovery completes within 5 minutes for a single-cell failure and within 10
  minutes for a lane rollback, or the lane remains frozen;
- the old global release path is no longer the serving owner for that lane.

Until Gate D is recorded, `ownershipMode` remains
`transitional-shared`. Production dispatch/enablement is additionally subject
to the active release-coordination freeze and must be performed by the unique
coordinator.

## Production control-plane inventory

`production-control-migration.tsv` is an exhaustive inventory of tracked
workflow jobs and schedules, production declarations, release lanes, local
Actions, and executable Python/shell/PowerShell scripts under the production
control directories. Check inventory drift with
`go run ./cmd/fugue-control-migration check .`; print the discovered surface
set with `scan`. Every row is a **proposed** disposition: `migrate` names a
native destination, while `retire` names the required prerequisite or
replacement. Neither value is evidence that production write permission has
been transferred or revoked. The scanner never connects to production and
cannot certify a cutover.

Guardian publication previews use the same candidate, live predecessor,
health and exact rollback-manifest checks as the writer. They expose the
current resource version and prewrite workload identity and use an HTTP
transport that refuses every request except an ordinary GET. A preview is
advisory only: it is not a signed approval, does not reserve or fence the
resource, and must be repeated by the authoritative writer immediately before
its compare-and-swap. A resource is not cut over until its legacy writers are
independently disabled and the new writer's production identity is verified.

For an already prepared bundle and an explicitly scoped reader:

```sh
go run ./cmd/fugue-declarative-release guardian-preview /path/to/read-only-kubeconfig context-name /path/to/prepared-bundle
```

Compare the proposed DesiredRelease, execution plan digest and exact rollback
manifest digest against the legacy publisher for the **same** prepared bundle.
Record both runs' current DesiredRelease identities; differing predecessors
invalidate the comparison. The preview checks publication eligibility only.
It does not verify actual workload CAS at apply time, perform an apply dry
run, prove registry availability, or run a rollback.

The current preview still consumes the existing declarative bundle. It does
not make the Git-backed planner, runner, or artifact store removable. Before
enabling a native writer, test it on an isolated target with independently
verified artifact availability, then observe denied writes from the old
identities at the Kubernetes/API, DNS provider, object store and runner/host
boundaries. Do not use an Actions run with production credentials as proof
that those identities have lost authority.

## Native intent journal (pre-cutover)

The native journal's schema is in `internal/productioncontrol/journal.sql`.
It is not applied by the existing production API or CI. Use a schema-owner
identity for migration only. Give an intent journal principal `USAGE` on
`fugue_control` and `SELECT, INSERT` only on `intents`, `approvals` and
`audit`. The execution journal also needs to read those records, and can
append to `resource_baselines`, `execution_claims` and `execution_results`
under a separate trusted identity. Neither journal identity needs DDL,
`UPDATE`, `DELETE` or provider permissions.

An intent binds the operation, immutable cluster/resource identity and
request ID, expected generation and live digest, exact payload digest,
target digest and rollback digest. Admission must derive that resource identity from the
actual provider write boundary. Reusing a request ID with different content
returns a conflict and creates no second operation. Each intent or approval and its
audit entry commit in one transaction. A component release draft can be
derived from Guardian's read-only preview for a healthy settled predecessor;
the exact prepared bundle is bound to it, not a Git lane or mutable tag.

Ed25519 approvals cover only the digest of that exact intent, under a
cluster-scoped owner key loaded from outside source and build systems. An
artifact receipt alone is not an approval. The executor must verify the
configured key, signature, expiration, predecessor and fence at execution
time. An approval row in the journal is not sufficient for execution.
The execution journal pins the imported last verified version until an
independent verifier can prove both the terminal version and that the old
claim cannot still issue a write. It permits at most one unresolved claim per
resource. A lost commit response, process restart, expired approval, or
timeout does not expire an existing claim or confer a second dispatch.
Recovery observes the existing claim rather than reissuing its operation.
For this first lane the rollback target must be its existing verified version.
The native Guardian publication adapter accepts only the digest-bound,
healthy-predecessor, single existing Deployment case, without delegated
runtime writes. It requires a trusted provider-admission dependency to check
exclusive authority, in-flight legacy fencing, and verified forward and
rollback artifacts available independently of GitHub. It checks the live
Deployment UID and resourceVersion before claiming execution, then rechecks
authority and the unexpired pending claim before each ConfigMap write. It
records the claim on the DesiredRelease CAS; that marker is a correlation
identifier, not an authorization or a substitute for provider enforcement.
The legacy publisher refuses a marked pointer before staging new artifacts.

A successful CAS submits the release to Guardian; it is not a rollout or
health receipt. A partial write or an unknown response keeps the claim
pending and never authorizes another submission. Observation may advance the
verified version only after independent terminal and fencing evidence.
The read-only publication observation can correlate the exact claim with a
candidate pointer or a rollback pointer even if the CAS response was lost.
It does not infer anything about actual serving health or permit a redispatch.
A second release keeps the already verified DesiredRelease digest as its
predecessor and rollback identity when it is a serving-equivalent alias of the
canonical monitor record. The rollback bytes must still match that monitor;
changing the alias does not relax manifest, image, or health-contract checks.

The Kubernetes-fenced publisher requires an explicitly configured independent
HTTPS registry. Before reserving an execution it reads the immutable forward
and rollback artifacts in their actual runtime repositories, including the
selected platform manifest, configuration and all layers. It verifies their
digests and sizes and checks the OCI revision against each receipt. It rejects
mutable references, out-of-origin authentication or redirects, and unavailable
or corrupt blobs. It rechecks cutover evidence and the live workload after
the streaming read and before issuing any claim. A successful read does not
grant authorization or prove retention or worker reachability. The mirror,
registry credentials, and existing rollout/repair consumers must still be
configured and tested independently of GitHub before cutover.

### Kubernetes admission barrier for an existing Deployment

The offline renderer produces three Kubernetes v1
ValidatingAdmissionPolicy/Binding pairs for the Deployment, release ConfigMaps
and component Lease. It takes no kubeconfig or production credentials:

```sh
go run ./cmd/fugue-declarative-release render-native-cutover-policy /path/to/cutover-spec.json
```

The off-repository input names the exact `key` (`component`, `group`),
`namespace`, `deployment`, `monitorComponent`, `componentLease`, `phase`,
and the authenticated `publisher`, `guardian`, `canaryProber`, and
`deploymentStatusWriter` usernames. The three Fugue writers must use
distinct, newly enrolled service accounts. Do not reuse identities available
to Actions, self-hosted runners, or legacy Guardian processes.

- `frozen` denies protected writes other than Deployment health/status
  writes from the exact configured Kubernetes controller identity.
- `native` keeps legacy writers denied and separates publication, execution,
  and canary evidence. The publisher can stage immutable records and advance
  the pointer's recorded fence; a Guardian pointer update retains the current
  claim binding. Scale changes and deletion of the protected existing
  Deployment are still denied. This is not a policy for targets with an
  autoscaler, delegated writes, or a separate actuator.

Neither phase grants RBAC. No production entrypoint automatically applies the
renderer output. The adapter re-reads the exact enforcing specs and checks its
actual Kubernetes identity using SelfSubjectReview; it still refuses to
publish without independent deployment cutover evidence. A matching object in
storage is not proof that admission is active on every API-server endpoint.
Verify enforcement per endpoint, remove legacy rights (including policy/RBAC
administration, impersonation, access to the new service account and indirect
host/node/provider write paths), and account for every old request **already
past admission** before granting native writes. Pause in the frozen phase
if any old action or artifact availability is unresolved.

An optional test starts a disposable loopback Kubernetes API server and etcd;
it uses no external cluster:

```sh
FUGUE_TEST_ENVTEST_ASSETS=/path/to/envtest go test ./internal/releaseguardian -run '^TestDeploymentCutover'
```

The isolated test demonstrates legacy writes denied before and after switching
the fence to native, including an old request prepared before freeze but sent
after it, and checks the exact provider-side identity and binding readback.
It is not evidence of a production runner revocation, that Guardian rolled out
a release, or that DNS, node participation, or maintenance has been migrated.
