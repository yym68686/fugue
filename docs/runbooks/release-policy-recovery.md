# Release-policy recovery lane

The ordinary control-plane release planner must reject changes to its own
workflow and reserved release entrypoints. A blocked ordinary release is not
authorization to edit a live runner or bypass the planner.

`recover-control-plane-release-policy.yml` is a no-cluster-mutation recovery
lane for a narrowly reviewed release-policy implementation fix. It is usable
only while the ordinary deploy workflow is disabled and has no active runs.
Before dispatch, repository operators must set both repository variables to
exact lowercase commit SHAs:

- `FUGUE_CONTROL_PLANE_RELEASE_POLICY_RECOVERY_BASE_SHA`: the current exact
  `fugue-control-plane-release-baseline` commit and direct parent of the fix.
- `FUGUE_CONTROL_PLANE_RELEASE_POLICY_RECOVERY_SHA`: the exact reviewed main
  commit containing only the statically enumerated recovery files.

The recovery workflow shares the ordinary deploy lane's production-mutation
concurrency group, reruns OpenAPI generation checks, all release-domain and
NodeLocal safety contracts, stale-recovery tests, and the complete Go suite.
The test job checks out without persisted Git credentials. The bounded writer
rechecks that deploy is disabled, that no deploy run is non-terminal, that main
still names the exact authorized target, and that duplicate recovery dispatches
have been cancelled before its first write. Because watchdog scheduling is
asynchronous, the same write boundary also queries every earlier workflow run
and rerun attempt for the exact authorized SHA. Any earlier attempt that is not
`completed/success` latches the SHA closed; recovery can continue only from a
newly reviewed exact commit.

The workflow performs no Helm, Kubernetes, node, image, or application
mutation. Its only production write is the dedicated release baseline tag. A
bounded, testable helper advances that tag with an exact expected-OID CAS,
rolls it back to the exact prior object, and re-advances it with another exact
CAS. The success artifact records the observed pre, forward, rollback, and
final OIDs, patch digest, both authorized SHAs, lane states, zero remaining
runs, the zero cluster-mutation assertion, and successful rollback
verification.

The production writer uses GitHub's `updateRefs` GraphQL mutation rather than
the Git smart-HTTP push path. Each mutation supplies the exact `beforeOid`, the
exact `afterOid`, and `force: true`; the server therefore performs the
non-fast-forward rollback as an atomic compare-and-swap. The recovery gate
introspects the live `RefUpdate` input and fails before any write unless
`beforeOid`, `afterOid`, `force`, and `name` are all present. REST ref updates
are not an acceptable fallback because their force update has no expected-OID
precondition. Local bare-repository tests keep the Git backend only as an
offline model and fault-inject the GraphQL backend separately.

Guard or test failure occurs before the tag writer and therefore goes directly
to bounded lane freeze and evidence; no tag-compensation artifact is expected
for that pre-write path. A tag transaction, writer lane-quiescence, writer
evidence, or other post-write failure invokes same-runner `always()`
compensation before the writer can finish, plus an independent compensation
job for job timeout or runner loss. Neither automatic compensation path depends
on a second production-environment approval. Each observes the exact remote
tag: target is CAS-restored to base, base is accepted as already rolled back,
and every other OID is preserved for investigation rather than overwritten.
Both lanes are then disabled with bounded retries, every other non-terminal run
is cancelled and rechecked with fail-fast pagination, and post-action evidence
is uploaded. Do not
re-enable either lane until the compensation/freeze artifacts and final remote
tag state have been independently reviewed. A successful recovery disables
its own lane and deliberately leaves the ordinary deploy lane disabled;
re-enable deploy only after confirming the recovery artifact and exact final
baseline tag, then resume with one normal single-domain checkpoint.

`watch-control-plane-release-policy-recovery.yml` independently converges a
failed or cancelled recovery to frozen lanes and durable evidence. It observes
completion of the exact recovery workflow name and only acts on a
non-successful `workflow_dispatch` from this repository's `main` branch at the
externally authorized recovery SHA. The
watchdog has no checkout, cluster credential, or content-write permission. It
independently disables both release lanes, cancels and rechecks other
non-terminal deploy/recovery runs, and records the triggering run, lane states,
main and baseline OIDs, pending-run inventory, and zero-cluster-mutation
assertion before uploading bounded evidence. The writer's historical failure
latch prevents a queued duplicate from crossing the tag-write boundary after
the original run is cancelled; the watchdog independently converges both lanes
to a frozen, evidenced state even when its runner starts later.
