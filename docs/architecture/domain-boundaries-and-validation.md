# Domain ownership and verification rollout

The deployment stays a modular monolith by default. A Go package boundary does
not authorize another writer, add a release lane, or prove fault isolation.

## Ownership

| Domain | Command ownership | Persistence boundary | Verification |
| --- | --- | --- | --- |
| Backup usage | The backup-usage service builds the inventory and owns its tenant cache and locks. The API authorizes and adapts a read-only source. | No backup mutations are available through the usage source. | The usage package verifies cache behavior and its dependency boundary; API and Store tests cover the adapters. |
| Application image tracking | HTTP handles authorization, status translation, sanitization and audit. The image-tracking domain handles interactive commands, diagnosis and the Controller's leader-scoped polling, retry and metrics. The Controller still executes queued operations. | Store assembles the private PostgreSQL tracking repository. Configuration upsert/check retention keep their original SQL transactions; successful deployment joins the caller's existing transaction and deletions use foreign-key cascades. Evidence consumers receive a read-only view. | The image-tracking domain and repository run without importing the three composition roots. Root contract tests still exercise HTTP, authorization, leadership, and existing storage wiring. |
| Application operations | The queue owns lane selection, claim order, dependency checks, retry delays, deletion precedence and the local claim/recovery reservation. Controller opens its existing leader scope and runs the operation. The operation view owns tenant/project read policy and projections; HTTP still owns request and response contracts. | Store exposes a separate read-only view and a queue transition port. The first-execution projection reads only its original release-step snapshot. Store applies queue writes through PostgreSQL transactions; the local reservation and response timestamps are not Store fences. | Queue and operation view logic runs without API, Controller or Store; queue integration tests also run against the real Store port. Root contract tests still exercise HTTP authorization, cancellation and stale execution behavior. |
| Node host policy (installer side) | The host policy module owns the Journald and ZRAM shell fragments; API embeds them in the existing updater and join contracts. | Host-side staging, verification and rollback remain in the original shell transaction. No Controller desired-state or Store mutation was moved by this extraction. | Shell behavior is verified from the host policy package; a small API contract test proves the installer renders the exact payload. |
| Managed PostgreSQL image migration | API authorizes, previews and submits the command; the image migration module owns the stated plan, durable witness, execution order, and settlement decision. Controller binds the module to one already claimed operation and provides Kubernetes and SQL observations and guarded mutations. | Store keeps admission, binding locks and the PostgreSQL completion transaction. Completion uses the domain's evidence check to update the backing service. | Domain tests verify loss of the PATCH response and operation ownership without importing Controller or Store. API, Controller and Store retain their HTTP, live-platform and transaction contract tests. |
| Registry-backed image storage billing | One domain owns registry measurement, deduplication and the API's per-tenant refresh scheduling. API and Controller keep their respective triggers and provide registry adapters. | The Store port is bound to one tenant, exposes the two inventory reads, and commits only measured image storage through the existing accrual transaction. It cannot change balances, caps, or operations. | Domain tests exercise measurement, failure handling and coalescing. An HTTP contract covers scheduled storage updates and Store tests cover ledger accrual. |
| Application release retirement | The retirement domain owns bounded per-app scheduling, cursor and retry state. Controller owns the leader lifecycle, verifies live traffic, runtime and workload evidence, and invokes guarded writes. | A tenant/app-scoped port serves the retirement projection and guarded writes. Store revalidates the same snapshots and keeps the retirement and historical binding changes in the existing transaction. | Domain tests independently verify fairness, exclusion, retry isolation and worker cancellation. A source audit limits acquisition of the write port to the Controller adapters. Controller and Store verify runtime evidence, tenant/app scope and concurrent transitions. |
| Application image registry and usage snapshots | The image domain owns registry transport, manifest traversal, blob accounting, distributed measurement and reference indexes, inspection cache and visibility-scoped snapshot refresh. HTTP authorizes and supplies read adapters. Controller maintenance verifies the current reference before deletion. | Registry deletion requires the observed digest. HTTP invalidates its observation after deletion or a confirmed absence. Usage snapshots detach their values and discard stale background observations. | Domain tests exercise registry behavior, scope, generation coherence and measurement conflicts. API tests cover first load, stale responses and tenant-specific observations. |

For an operation's pre-claim dependency failure, Store verifies the pending
snapshot under its existing write transaction. A stale dependency verdict
cannot overwrite a claimed or cancelled operation. Queue/Store contract tests
for Compose ordering, retry and deletion run outside the Controller root;
Controller retains worker execution and shutdown tests. Store imports the
operation read contract without importing either read or queue policy.

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
- Some Go tests read another package's source or execute a repository command
  without importing it. Declare those consumers as test-only edges (for
  example, the Prometheus source audit, OpenAPI generator contract and the SSH
  Kubernetes test that compiles another package's test binary).
  Run these consumers uncached: child-process inputs may not invalidate the
  test result cache. A missing declared consumer or an unowned fixture still
  selects uncached full Go tests. This is not an inventory of every implicit
  dependency. In narrower scopes, the tracked-source business-coupling
  and retired release mode audits also run uncached for both runtime and
  test-only edits. They scan the tracked tree regardless of which packages
  import the changed source. The image-tracking and release-retirement
  composition guards also run uncached for non-test Go files under `internal`
  and `cmd`. Acquiring a write port from Store need not import the owning
  domain's implementation.
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

Run `python3 -m scripts.prepush_shadow --base <base> --compare-target linux/amd64`
to compare the native graph with a CGO-disabled component build profile. Repeat
`--compare-target` for each required target, and run separate comparisons with
the actual build tags when they differ. The comparison binds the exact diff,
reports differences in selected packages and falls back to full verification
when profiles disagree or an input cannot be owned in one profile. It does not
execute a cross-platform test or supply release authorization.

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
The report's `warmReplayCriteriaMet` covers only this run on its recorded
host; `activationReadiness` remains `not-evaluated`. Target comparisons,
mutations that fail the intended contracts, and independent cache profiles
remain separate activation evidence.

For incremental timing, use `--incremental --case queue-test --base HEAD` in a
clean disposable checkout. Each policy starts with a separate baseline cache.
Each pair shares an edit that changes a Go test name or a runtime literal, so
the compiler sees a different input and the edit can propagate to importers.
The report records every input hash, the uncached Go oracle, and the native
and Linux artifact target plans for the first sample. The manual daily audit
exposes this profile through `shadow_incremental`. It offers operation queue,
backup usage, retirement, installer policy, and cross-domain edit cases.

For a source-owned writer capability or generator with an implicit downstream
consumer, use the manual `contract-tracking-scope`,
`contract-retirement-scope`, or `contract-generator-consumer` shadow case. The witness first requires the
exact named consumer test to pass, applies one synthetic source regression,
proves native and Linux artifact-target reports select that consumer uncached
with no legacy omission, then requires the exact test and package to fail with
the expected diagnostic. The temporary edit exists only in the disposable CI
checkout; its evidence never authorizes a production release. A passing suite
without this negative witness is not evidence that the contract edge is
selected.

First measured case: a test-only operation-queue edit on Darwin arm64
(8 logical CPUs, Go 1.25.7, `GOFLAGS=-p=2`), over 20 warm pairs: existing
prepush P95 13.082s, candidate 3.230s, zero timed-out or failed samples.
The uncached whole-module oracle exceeded 600s, so this evidence does not
qualify the candidate for activation. A separate uncached rerun finished with
133 passing test packages. For a queue source edit, the Store's
unnecessary import of the scheduling implementation increased the graph to
17 test packages and 15 build packages. Returning the restricted Store
adapter instead of importing the scheduler reduced both to 3 in the same
edit case. A single runtime-edit scout still exceeded the deadline during
the Controller tests, so neither the old priority rules nor the API warmup
is removed.

A separate test-only installer host-policy edit on the same machine and
toolchain recorded 20 warm pairs: existing P95 18.603s, candidate P95 2.207s,
no failed samples. This replay did not run the full oracle and does not qualify
the candidate for activation.

After the PostgreSQL-only change at `130737ba8`, a local operation-queue
runtime edit on the same host and toolchain recorded 20 warm pairs: existing
P95 23.149s, candidate P95 9.482s, no failed samples, and 137 packages with
executed tests in the passing uncached oracle. Native and both Linux target
graphs agreed. This is evidence for one local edit class, not activation. A
shared operation-read contract edit selected 21 test packages and 18 build
packages; the affected tests exceeded the default deadline without any old
suite omitted. A combined queue and operation-read change selected full scope
on all three profiles. Shared, cross-domain, and unclassified changes must
retain wider verification.

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

The control-plane Store and image-tracking repository require PostgreSQL.
Store integration tests use disposable PostgreSQL databases; use
`make test-postgres` to run them locally. The agent's cell-local JSON state
is not part of the control-plane Store.
