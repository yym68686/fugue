# Domain ownership and verification rollout

The deployment stays a modular monolith by default. A Go package boundary does
not authorize another writer, add a release lane, or prove fault isolation.

## Ownership

| Domain | Command ownership | Persistence boundary | Verification |
| --- | --- | --- | --- |
| Backup usage | The backup-usage service builds the inventory and owns its tenant cache and locks. The API authorizes and adapts a read-only source. | No backup mutations are available through the usage source. | The usage package verifies cache behavior and its dependency boundary; API and Store tests cover the adapters. |
| Application image tracking | HTTP handles authorization, status translation, sanitization and audit. The image-tracking domain handles interactive commands, diagnosis and the Controller's leader-scoped polling, retry and metrics. The Controller still executes queued operations. | Store assembles the private tracking repository. Configuration upsert/check retention keep their original SQL transactions; successful deployment and JSON deletions join the caller's existing transaction. Evidence consumers receive a read-only view. | The image-tracking domain and repository run without importing the three composition roots. Root contract tests still exercise HTTP, authorization, leadership, and existing storage wiring. |
| Application operations | The queue owns lane selection, claim order, dependency checks, retry delays, deletion precedence and the local claim/recovery reservation. Controller opens its existing leader scope and runs the operation. The operation view owns tenant/project read policy and projections; HTTP still owns request and response contracts. | Store exposes a separate read-only view and a queue transition port. The first-execution projection reads only its original release-step snapshot. Store applies queue writes through the original JSON or PostgreSQL transactions; the local reservation and response timestamps are not Store fences. | Queue and operation view tests run without API, Controller or Store. Root contract tests still exercise HTTP authorization, cancellation and stale execution behavior. |

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

On a disposable worktree, apply one representative edit and run
`python3 -m scripts.prepush_replay --case <edit-class> --base HEAD --full-oracle`.
The replay alternates execution order, records 20 measured pairs by default
after one warm-up pair (reported separately), and runs the uncached whole-module
Go test oracle once. The research run uses the same scheduler and non-selection
checks but bypasses Go scope workarounds to measure their proposed replacement.
Its output is deliberately not a prepush receipt. Compare separate edit cases
for domain source, test-only, transitive contracts and broad fallback; retain
failures and retain the separate warm-up times. Warm repeated-diff P95 does not
prove cold or real incremental performance, and an all-green oracle does not
prove that a failing cross-domain contract would be selected.

## Production isolation decision

The declarative registry already defines separate API, Controller, and SSH
Control artifacts. That is evidence of independent packaging, not of Gate B:
API and Controller also share a Kubernetes ServiceAccount, and all three
deployments reference the same PostgreSQL credential. Image tracking also
updates deployed digests in the operation completion transaction. No separate
image-tracking release or production failure injection is justified by this
refactor.

The application operation queue is also not a separate-release candidate.
The Controller reservation covers the interval between a Store claim and
worker registration; the durable lifecycle then meets app configuration,
runtime evidence, and finalization. Splitting queue arbitration from its
executor would introduce another recovery boundary without a measured need.
Edge quality rollup remains a separate candidate under the
[worker handoff](api-worker-handoff.md): an API rollout is not an ownership
handoff, and a standby worker is not a serving writer.

For a future candidate, record why fault isolation or autonomous release is
needed and the concrete writes it would own, then use the
[migration gates](microservices-migration-acceptance-v1.md). Stop before cutover
if Gate B's credential/transaction isolation or Gate C's recovery proof is
missing. Separate release jobs and ordinary health checks never substitute for
the Gate D canary and rollback witnesses.

JSON retirement remains independent. The production API, Controller and SSH
Control manifests require the configured PostgreSQL secret; standalone API and
Controller still fall back to `./data/store.json` when the DSN is absent, as
documented in both READMEs. File-backed initialization, image-tracking and
operation tests still exercise it. The agent's cell-local JSON state is not the
control-plane Store. Do not infer that there are no external standalone
consumers from managed production wiring. Verify the import and recovery path,
the standalone upgrade behavior and real PostgreSQL test coverage before
deprecation. Neither backend nor schema is changed here.
