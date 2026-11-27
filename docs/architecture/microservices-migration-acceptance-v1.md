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
