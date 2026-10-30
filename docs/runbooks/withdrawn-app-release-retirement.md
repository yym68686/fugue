# Withdrawn application revision retirement

Failed independent candidates remain owned release resources until traffic
withdrawal, retention, canonical Service references and drain evidence permit
retirement. Background retirement does not re-run the serving application's
business health gate: that gate still applies to foreground promotion and
canonical replacement. A degraded stable must not indefinitely retain revisions
that no longer receive traffic.

For a failed candidate whose Pods were never scheduled, the controller verifies
the complete owned Pod set and immutable Deployment/Service identity twice, then
sets only that Deployment's replica count to zero with UID/resourceVersion
preconditions. Conflicts require a fresh observation. Retirement waits for
Deployment and ReplicaSet zero-replica convergence and the absence of owned Pods;
it does not fabricate container runtime identities for unscheduled Pods.

Automatic image tracking waits for failed or retired revision resources to be
removed before queuing another import, including when a new digest appears.
Repeated failures of the same digest use exponential retry delay from five
minutes up to one hour, measured after the latest terminal failure. Successful
deployment breaks the consecutive failure sequence. Existing previous rollback
targets and the serving stable do not block tracking.

Aborting an independent, unpromoted bound candidate restores traffic without
reapplying a canonical spec the candidate never changed. When desired projections
advanced past a retained canonical workload, recovery requires the controller's
recorded ready key, the live template key and an exact same-runtime historical
snapshot. Recovery only establishes a baseline; readiness, storage, capacity and
canonical drain checks continue to apply.

After a controller release, verify:

- The running controller source commit and image match the Actions artifact.
- Withdrawn Deployments decrease and their release rows become retired.
- Unscheduled candidates converge to zero replicas before deletion.
- Image tracking records `retry_suppressed` while cleanup/backoff is pending.
- Current stable traffic remains authoritative; names containing `candidate`
  can identify the current stable and must never be bulk-deleted by name.

Paused retirement due to active connections, retained rollback windows, changed
resource identities, unconfirmed publication or API failures remains expected.
Inspect audit `observer_error` and the current canonical Service selector before
changing intent. An application 5xx regression is investigated separately from
whether withdrawn resources can safely retire.
