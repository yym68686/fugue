# Domain ownership and verification rollout

The deployment stays a modular monolith by default. A Go package boundary does
not authorize another writer, add a release lane, or prove fault isolation.

## Ownership

| Domain | Command ownership | Persistence boundary | Verification |
| --- | --- | --- | --- |
| Backup usage | The backup-usage service builds the inventory and owns its tenant cache and locks. The API authorizes and adapts a read-only source. | No backup mutations are available through the usage source. | The usage package verifies cache behavior and its dependency boundary; API and Store tests cover the adapters. |
| Application image tracking | HTTP handles authorization, status translation, sanitization and audit. The image-tracking domain handles interactive commands, diagnosis and the Controller's leader-scoped polling, retry and metrics. The Controller still executes queued operations. | Store assembles the private tracking repository. Configuration upsert/check retention keep their original SQL transactions; successful deployment and JSON deletions join the caller's existing transaction. Evidence consumers receive a read-only view. | The image-tracking domain and repository run without importing the three composition roots. Root contract tests still exercise HTTP, authorization, leadership, and existing storage wiring. |

These are migrated boundaries, not a claim that the remaining API, Controller
and Store files are composition-only. App release and reconciliation, billing,
edge/DNS publication and operations still include root-owned logic. Migrate
them by writer and transaction, keeping auth, fencing and billing behavior.

## Shadow Go verification

`make prepush-shadow` reports the proposed Go test/build scope for the working
tree. Use `PREPUSH_BASE_REF=<commit> make prepush-shadow` to replay a committed
diff. It is separate from `make prepush` and cannot authorize a push.

- One package's runtime edit includes its transitive production importers and
  the tests that import any changed or affected package. Test-only changes do
  not rebuild downstream packages.
- Unknown inputs, missing graph data, deleted/renamed paths whose old owner
  cannot be proven, and multi-package changes select whole-module Go checks.
- Compare the report to `legacyTestCommands`; a missing suite forces full Go
  scope and is recorded in `omittedLegacyPackagesBeforeFallback`. This is a
  package-level comparison, not measured regression coverage. Non-Go,
  generation, lint, deploy and release checks are unchanged.
- The report describes its current Go environment. Before activation, compare
  target platforms/build tags and fixture/dependency inputs the Go graph does
  not describe. Record paired legacy/candidate runs per representative edit
  class, Go version, platform, cache state and machine. Include timed-out runs
  as failures, run the full test oracle periodically, and require zero known
  missed checks plus P95 ≤ 60 seconds for the agreed local edit classes before
  removing scheduling workarounds. CI continues to run the production gate.

## Production isolation decision

The declarative registry already defines separate API, Controller, and SSH
Control artifacts. That is evidence of independent packaging, not of Gate B:
API and Controller also share a Kubernetes ServiceAccount, and all three
deployments reference the same PostgreSQL credential. Image tracking also
updates deployed digests in the operation completion transaction. No separate
image-tracking release or production failure injection is justified by this
refactor.

For a future candidate, record why fault isolation or autonomous release is
needed and the concrete writes it would own, then use the
[migration gates](microservices-migration-acceptance-v1.md). Stop before cutover
if Gate B's credential/transaction isolation or Gate C's recovery proof is
missing. Separate release jobs and ordinary health checks never substitute for
the Gate D canary and rollback witnesses.

JSON retirement remains independent: file-backed Store initialization,
image-tracking transaction and deletion tests still use that backend. Its
consumer inventory, data compatibility and recovery path must be evaluated
before deprecation; neither backend nor schema is changed here.
