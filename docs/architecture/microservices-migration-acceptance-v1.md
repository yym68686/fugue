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
