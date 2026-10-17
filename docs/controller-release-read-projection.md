# Controller release observations

Controller reconciliation and rollout proof polling read release metadata without
fetching executable configuration that they do not use:

- `preserveActiveAppReleaseResources` protects canonical, revision and bound
  resource names using every matching release's status and immutable binding.
- `expectedAppTrafficDigest` reads the current policy and release targets on every
  observation. It does not cache authorization or change the exact route proof.
- `ListAppReleaseRetirementState` returns every matching row in the existing
  order, with complete `SpecSnapshot` only for `previous/draining` and
  `candidate/failed` releases. These are precisely `AppReleaseAwaitingDrain`.
  Already retired cleanup uses the retained immutable workload identity.

The PostgreSQL projection selects the necessary snapshot from the same row in
one query. Tenant filters, lifecycle facts, revision bindings, rollback targets
and retention timestamps are preserved. Required malformed drain configuration
and malformed workload bindings still fail decoding. Full release reads and all
persisted snapshots remain available for execution, rollback and audit.

The projected retirement view must not be written back as a complete release.
Current drain, traffic, retention, UID and compare-and-swap checks continue to
authorize retirement. No publication, serving configuration or positive LKG is
changed by these reads.

Tests exercise file/PostgreSQL parity, ownership filtering, cancellation,
nil/empty snapshots, complete drain configuration, invalid required fields, and
resource/proof behavior. A loopback PostgreSQL forwarding fixture counts actual
response bytes: large unrelated configuration must not cross the connection for
metadata-only controller observations, while required drain snapshots still do.
