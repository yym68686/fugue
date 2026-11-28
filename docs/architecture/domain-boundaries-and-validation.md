# Domain ownership and verification rollout

The deployment stays a modular monolith by default. A Go package boundary does
not authorize another writer, add a release lane, or prove fault isolation.

## Ownership

| Domain | Command ownership | Persistence boundary | Verification |
| --- | --- | --- | --- |
| Backup inventory and usage | The backup domain owns inventory scheduling, pagination, physical reconciliation, directly owned object keys, and the tenant usage cache and locks. API authorizes reads and adapts the Store and object reader. The CLI depends only on the wire contract. | The Store port derives the existing key from the namespace digest and commits the cursor and pages under the API writer fence. The generic checkpoint writer cannot write an inventory checkpoint or its owner. Reconciliation receives a read view. | Domain tests cover old checkpoint resumption, page integrity, object ownership, tenant cache isolation and how the writer is wired. PostgreSQL tests verify that both checkpoint formats preserve the cursor when the owner changes and that an open page blocks an owner transfer. |
| Application image tracking | HTTP handles authorization, status translation, sanitization and audit. The image-tracking domain handles interactive commands, diagnosis and the Controller's leader-scoped polling, retry and metrics. The Controller still executes queued operations. | Store assembles the private PostgreSQL tracking repository. Configuration upsert/check retention keep their original SQL transactions; successful deployment joins the caller's existing transaction and deletions use foreign-key cascades. Evidence consumers receive a read-only view. | The image-tracking domain and repository run without importing the three composition roots. Root contract tests still exercise HTTP, authorization, leadership, and existing storage wiring. |
| Application operations | The queue owns lane selection, claim order, dependency checks, retry delays, deletion precedence and the local claim/recovery reservation. Controller opens its existing leader scope and runs the operation. The operation view owns tenant/project read policy and projections; HTTP still owns request and response contracts. | Store exposes a separate read-only view and a queue transition port. The first-execution projection reads only its original release-step snapshot. Store applies queue writes through PostgreSQL transactions; the local reservation and response timestamps are not Store fences. | Queue and operation view logic runs without API, Controller or Store; queue integration tests also run against the real Store port. Root contract tests still exercise HTTP authorization, cancellation and stale execution behavior. |
| Node host policy (installer side) | The host policy module owns the Journald and ZRAM shell fragments; API embeds them in the existing updater and join contracts. | Host-side staging, verification and rollback remain in the original shell transaction. No Controller desired-state or Store mutation was moved by this extraction. | Shell behavior is verified from the host policy package; a small API contract test proves the installer renders the exact payload. |
| LocalPV boot unit | The LocalPV host module owns the renderer and reconciliation fragment used by the standalone preparation script and the node updater. API retains the HTTP and assembled updater contracts. | Host image, loop device and unit changes still use the existing ordering and unit lock. The relocation does not change the generated updater or advance the node updater version. | Renderer and installer tests run outside API. API retains an embedding contract; prepush reruns host checks uncached for external script and Python inputs. Runtime renderer changes still require a node updater version bump. |
| Managed PostgreSQL image migration | API authorizes, previews and submits the command; the image migration module owns the stated plan, durable witness, execution order, and settlement decision. Controller binds the module to one already claimed operation and provides Kubernetes and SQL observations and guarded mutations. | Store keeps admission, binding locks and the PostgreSQL completion transaction. Completion uses the domain's evidence check to update the backing service. | Domain tests verify loss of the PATCH response and operation ownership without importing Controller or Store. API, Controller and Store retain their HTTP, live-platform and transaction contract tests. |
| Registry-backed image storage billing | One domain owns registry measurement, deduplication and the API's per-tenant refresh scheduling. API and Controller keep their respective triggers and provide registry adapters. | The Store port is bound to one tenant, exposes the two inventory reads, and commits only measured image storage through the existing accrual transaction. It cannot change balances, caps, or operations. | Domain tests exercise measurement, failure handling and coalescing. An HTTP contract covers scheduled storage updates and Store tests cover ledger accrual. |
| Application release retirement | The retirement domain owns bounded per-app scheduling, cursor and retry state. Controller owns the leader lifecycle, verifies live traffic, runtime and workload evidence, and invokes guarded writes. | A tenant/app-scoped port serves the retirement projection and guarded writes. Store revalidates the same snapshots and keeps the retirement and historical binding changes in the existing transaction. | Domain tests independently verify fairness, exclusion, retry isolation and worker cancellation. A source audit limits acquisition of the write port to the Controller adapters. Controller and Store verify runtime evidence, tenant/app scope and concurrent transitions. |
| Application image registry and usage snapshots | The image domain owns registry transport, manifest traversal, blob accounting, distributed measurement and reference indexes, inspection cache and visibility-scoped snapshot refresh. HTTP authorizes and supplies read adapters. Controller maintenance verifies the current reference before deletion. | Registry deletion requires the observed digest. HTTP invalidates its observation after deletion or a confirmed absence. Usage snapshots detach their values and discard stale background observations. | Domain tests exercise registry behavior, scope, generation coherence and measurement conflicts. API tests cover first load, stale responses and tenant-specific observations. |
| Builder admission | The domain owns build input identity, bounded memory history, permanent-failure reuse, OOM evidence and admission decisions. Controller supplies app-scoped evidence and coordinates build placement and source-sync status. | Only the Controller composition adapter acquires the builder checkpoint writer. The generic writer rejects the three historical namespaces, and there is no public generic page writer. The scoped adapter uses the original keys, PostgreSQL transaction and replica lock. | Domain tests run without API, Controller or Store. Store verifies namespace isolation; Controller integration verifies restart, concurrency, contention and old evidence replay. |
| Runtime OOM right-sizing | The domain owns the periodic pass, workload-event interpretation, local claims, tenant commitment projection and growth decisions. API adapts the namespace-scoped Kubernetes observer and assembles the command; Controller continues executing ordinary deploy operations. | The scoped admission command fixes the operation kind and system actor, and keeps the existing app-row lock and durable per-tenant event index. The generic operation writer cannot impersonate the OOM actor. | Domain tests cover event identity, growth, retry and claim expiry. API contracts exercise observer lifecycle, cap checks and durable cross-replica deduplication. |

For an operation's pre-claim dependency failure, Store verifies the pending
snapshot under its existing write transaction. A stale dependency verdict
cannot overwrite a claimed or cancelled operation. Queue/Store contract tests
for Compose ordering, retry and deletion run outside the Controller root;
Controller retains worker execution and shutdown tests. Store imports the
operation query data and exposes explicit read methods. The read domain
owns the interface consumed by API. Release planning binds queue
implementation to Controller, operation views to API, and shared query
data to the components that import Store.

Operation cancellation uses an actor-bound port for HTTP callers and a
deploy-only supersession port for Controller. PostgreSQL rechecks the
actor's tenant and project in the same transaction as cancellation and the
app status update. The cancellation domain verifies which operation states
can transition. The operation view owns audience-specific diagnostic
projections, evidence ranking and historical scheduling reconstruction.
API loads authorized evidence and serves the HTTP contract.
The operation execution domain owns stage timing and persistence retry
behavior. Controller supplies error classification and keeps guarded Store
calls and the leader context in its execution adapters.

These are migrated boundaries, not a claim that the remaining API, Controller
and Store files are composition-only. App release and reconciliation, billing,
edge/DNS publication and operations still include root-owned logic. Migrate
them by writer and transaction, keeping auth, fencing and billing behavior.

## Shadow Go verification

`make prepush-shadow` reports the proposed Go test/build scope for the working
tree. Use `PREPUSH_BASE_REF=<commit> make prepush-shadow` to replay a committed
diff. It is separate from `make prepush` and cannot authorize a push.

`make prepush-local` runs the proposed checks and does not produce a release
receipt. It includes the build-profile comparison, runs the selected build
before affected tests for runtime edits, and omits the legacy priority and
API warmup only on this local path. Profile disagreement or unclear ownership
keeps the full uncached Go gate. It rejects CI use and a changed worktree.
Keep using the unchanged `make prepush` for the production gate.

The local fallback has a separate ten-minute ceiling and does not count
towards the 60-second local-edit target. In the hosted cross-domain replay,
the selector widened on all profiles; all 20 pairs exceeded the candidate's
short timeout, while the separately run full oracle passed. A wider fallback
that still times out fails the command; it never retries with fewer checks.
On Darwin arm64 (Go 1.25.7, `GOFLAGS=-p=2`), the uncached whole-module Go
test command alone passed in 368.92 seconds on the cross-domain fixture at
`5544660e4`. The former six-minute ceiling could fail even before counting
selection and the other checks. The production timeout is unchanged.
Dispatch the manual audit with `shadow_case=cross-domain` and
`shadow_incremental=false` to exercise this actual full fallback budget once.
Its separate witness binds the native and artifact plans and executes the
whole-module build and uncached tests. It fails on a narrowed plan, timeout
or source drift and is not counted in the local-edit P95.

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
  import the changed source. The backup inventory, image-tracking,
  release-retirement, builder admission and runtime OOM composition guards
  also run uncached for non-test Go files under `internal` and `cmd`. Acquiring
  a write port from Store need not import the owning domain's implementation.
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

For runtime edits, `--build-first` tries the selected dependency build before
the selected tests so the two Go processes do not compete to compile the same
changed packages. `--verify-targets` includes native and both Linux artifact
profiles in every measured candidate run. In that mode, a profile mismatch
or incomplete graph executes the uncached whole-module Go checks rather than
a narrower test set. The manual daily audit exposes these switches as
`shadow_build_first` and `shadow_verify_targets`. Neither changes
`make prepush` or emits a production receipt.

For a source-owned writer capability or generator with an implicit downstream
consumer, use the manual `contract-backup-checkpoint`,
`contract-builder-checkpoint`, `contract-oom-admission`,
`contract-tracking-scope`, `contract-retirement-scope`, or
`contract-generator-consumer` shadow case. The witness first requires the
exact named consumer test to pass, applies one synthetic source regression,
proves native and Linux artifact-target reports select that consumer uncached
with no legacy omission, then requires the exact test and package to fail with
the expected diagnostic. The temporary edit exists only in the disposable CI
checkout; its evidence never authorizes a production release. A passing suite
without this negative witness is not evidence that the contract edge is
selected.

The `builder-runtime` and `builder-test` incremental cases measure one
changed domain source file and one changed placement test, respectively.
They measure local edits, not the shared checkpoint-contract or release
intent changes. Use the target comparisons inside every timed sample.
The `oom-runtime` and `oom-test` cases similarly measure an edit to the
runtime OOM sweep and an isolated claim-expiry test. They do not measure
edits to the shared operation transaction or an actual OOM-induced deploy.

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

The hosted backup runtime replay at `bc7a9f228` used 20 distinct edits
with separate caches (Linux amd64, 4 logical CPUs, Go 1.25.7, `GOFLAGS=-p=2`).
The build-first candidate recorded P95 50.377s with zero failures; the
legacy gate recorded P95 22.382s. Native and both artifact profiles selected
the same packages, omitted no legacy suite, and the uncached oracle ran
141 packages successfully. This demonstrates the candidate's budget for that
case while exercising more tests, not a speedup over the legacy gate. The
target comparison for this run was outside the candidate timing; use
`--verify-targets` to measure the complete conservative local path.

The follow-up at `4b4f4d739` included the target comparison in every
candidate sample on the same hosted profile: 20 incremental pairs, candidate
P95 43.003s (legacy 18.805s), zero failures, no known omitted legacy suite, and
142 packages in the passing uncached oracle. This validates the scheduling
approach for the backup runtime case, not every local runtime edit or a change
to production authorization.

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
