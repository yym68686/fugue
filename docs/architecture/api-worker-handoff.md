# API and background execution handoff

The API remains the HTTP authentication, validation, intent submission and
read boundary. An operation accepted through its existing route remains
queryable through that route independently of which process advances it.
Immediate, read-only cluster queries and API-local metrics, caches and
connection preparation are not durable background operations.

## Execution boundaries

| Work | Target executor | Durable state or retry invariant |
| --- | --- | --- |
| Billing query preparation | API database pool | PostgreSQL prepared statements belong to a connection. Preparing the same query in another process does not warm the API. Do not remove the current bounded warmer without measuring a replacement on the API pool. |
| Edge quality rollups | Observation worker | Commit rollups and completed window watermarks together. Retry uncommitted windows on the next pass. |
| Passive ASN, DNS policy verification, observation and quality-evidence retention | Observation worker | Reuse observation cursors or bounded retention passes. The current DNS loop also reconciles serving hosted-DNS flattened records; separate those writes from observation before moving it. Preserve the quality-evidence capacity guard on insert. |
| Storage policy and lifecycle | API-local pool and codec configuration; storage worker for audit and maintenance | Reload the local policy in each consumer; separate the artifact deduplication writes currently bundled in its reload loop. Give maintenance its own bounded work progress. |
| Backup execution, retirement, inventory | Backup execution and inventory workers | Preserve atomic backup claims, coordination leases and artifact identities. Inventory remains separate from execution and does not enqueue replacement backup effects. |
| Database imports and TLS issuance | Import and certificate workers | Preserve the durable jobs and replay rules. Require explicit treatment of ambiguous SQL or CA effects before any retry. |
| App mail and other application-resource reconciliation | Controller | Advance the existing app desired state and report status on persistent app resources. |
| Platform compile/publish | Guardian | Do not bypass existing publication fences, signing or consumer selection. Move read-only capture separately where necessary. |
| Automation shadow | Observation/automation worker | Reports are evidence only; do not inherit actuator credentials. |
| Node participation | Dedicated executor after read-only comparison | Preserve target identity, lease fencing and actual consumer evidence. Both the API and scheduled Actions currently invoke the legacy apply path; migrate their authority together. |

This is the target allocation, not a claim that these tasks have moved.

The PostgreSQL billing integration test uses two separate pools against the
same database. Warming the first pool prepares all four statements there and
none on the second pool. Warming the second pool directly prepares those same
four statements without executing billing. Keep the bounded warmer attached
to its API pool; it is connection lifecycle work, not a durable operation.

## First compatibility release: Edge quality rollups

The extraction leaves the deployed API as the writer and keeps its metrics
and persisted window format. It recognizes a persisted dedicated-worker owner
under the same named advisory lock used by the current deployment. No
component in this release changes that owner.

Before setting the owner, verify that every serving API revision and its
permitted rollback target honor the gate, including pods still terminating
after a rollout. The dedicated process must prove its readiness and use the
same fence before acquiring ownership. Do not reset the persisted owner on
API rollback. For future transfers, the owner check and the batch commit
must be fenced in the same PostgreSQL write transaction; the compatibility
read under a pinned session lock alone is not proof of that invariant
across loss of the lock connection. Releasing API compatibility is not
evidence that cross-process handoff or rollback has been exercised.

## Independent rollup worker

The observation worker opens only its own PostgreSQL pool. It must not use
the API bootstrap path, its Kubernetes service account token, or its admin
credentials. Both PostgreSQL writers take the same transaction-scoped
advisory lock on the transaction that commits rollups and watermarks. While
the persisted owner is absent the worker stays passive; a restart or
readiness probe never promotes it.

The next declarative rollout needs a separate database credential for the
worker, a Deployment with token automount disabled, and health/metric
collection. Grants must cover only sample reads, rollup and watermark reads
and writes, and reads from the single-row owner view. The worker must not
be granted direct access to the shared metadata table. Transfer is a
separate, privileged release action and must verify the API rollback floor and the standby worker before
changing the owner. Do not give the worker the credential used to transfer
ownership. A previous worker revision can keep running after worker
rollback if it understands the persisted owner and the same transaction
fence. A rollback must never reset the owner to let an old API run.

The release expects `fugue-observation-worker-database/database-url` in
`fugue-system`. The worker must reject the schema-owner credentials. Do
not work around missing credential provisioning by mounting the API
Secret. Provision a non-owner login through the database operator's
approved release path; verify that its credential can connect and that
it cannot transfer the owner. The observation-worker Actions lane
provisions the immutable credential and a restricted PostgreSQL login
after binding the production declaration to a ready primary and the live
PostgreSQL system ID. It does not transfer ownership.

Scrape the worker's own Pod endpoint independently of API metrics. The
configured target and a fresh rollup error-counter series are prerequisites
for cutover; those facts only prove observability while the worker is passive.
After cutover also require committed watermarks and a worker last-success
timestamp, rather than treating a Ready pod or a successful scrape as proof
of work.

An explicit `writer-handoff.json` change selects the forward-only Actions
step. It verifies the exact settled API and worker workloads, worker metrics
and fresh database watermarks before installing the fence. The transition
takes the existing writer lock and conflicting table locks without waiting;
if either is held, nothing changes and the same admitted release may retry.
It installs database triggers, changes the owner and captures the baseline
watermark in one transaction. The triggers reject actual changes from older
API binaries but still allow no-op startup SQL. A new watermark beyond that
transaction's baseline plus a new worker success sample is the completion
proof. A failed check after commit requires recovery on the worker side, not
resetting the owner to the API.

The 2026-11-27 production handoff (`55cfb6371`) switched the writer to the
independent observation worker. The `a1dc7cd9b` API rollout replaced both API
Pods while the same observation Pod stayed up and committed a new watermark.
The subsequent cleanup removes the API rollup executor and its process-local
writer metrics. API ingestion, quality reads and retired-ranking status remain
unchanged; use the `fugue-observation-worker` scrape job for writer metrics.

## Periodic quality-evidence retention: API compatibility

The API's evidence-admission transaction still enforces the existing expiry
and FIFO capacity rules when accepting a new object. Only the periodic expiry
timer may move. The compatibility build reads the owner while holding the
same transaction lock as evidence admission; an absent checkpoint leaves the
API timer active. Foreground admission identifies its own transaction for a
later database fence. This release does not create a checkpoint, enable an
independent timer, or transfer deletion authority.

Before transfer, verify the compatible API and rollback floor, then establish
the worker's standby capability and durable progress reporting. The cutover
must install a fence and switch the timer owner in one transaction. After
cutover, a pre-compatibility API binary is not a supported rollback: it did
not identify evidence-admission transactions, so the database will reject
its old delete path. A worker rollback may not re-enable the API timer.

The separately deployed standby worker only receives execute access to the
bounded expiry procedure and read access to its durable progress projection.
It cannot read the evidence table or modify the owner. PostgreSQL commits
successful batches with progress; interruptions roll back both and retries
never broaden the expiry predicate. The worker exposes these facts on its
own `/maintenance/metrics` endpoint, separate from the rollup scrape.

Keep the handoff declaration absent until both running images **and both
actual rollback artifacts** contain the compatibility boundary. The handoff
compares the pinned running workloads with the immutable production release
records and fails before changing PostgreSQL if an older rollback target
would remove admission or scheduling support. If an intervening refactor
changes the API sources, the declaration must name separately tested source
floors for the running API and its actual rollback artifact. The worker
must retain its scheduler and bounded database capability in both artifacts.
After commit, verification
requires a durable worker pass newer than the recorded transfer timestamp;
a timeout must not grant ownership back to the API.

The `acf5f7bd9` handoff switched the timer in production without redeploying
either process. The independent collector and the primary database both
reported `owner=worker`; PostgreSQL recorded batches after transfer and kept
the delete fence enabled. A zero deletion count with no eligible evidence
is a successful batch, not a missed poll. This is not yet a post-handoff API
rollout or worker-replacement test.

After every batch, verify the exact rollout and its consumer separately:
carry an in-flight operation across API rollout, restart the selected worker
from persisted state, observe one committed outcome with an old instance
present, and roll back a compatible binary without reopening authority.
Only then delete the retired startup path and unused grants, secrets and
image tools.
