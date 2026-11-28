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
| Passive ASN, DNS policy verification, signed heartbeat and observation sample retention | Observation worker | Persist classification and retention cursors with the last usable observation. Preserve signed heartbeat anchors and replay heads. Preserve the quality-evidence capacity guard on insert. |
| Hosted DNS flattened answers | Hosted DNS worker | Refresh derived records against the observed revision. Preserve user record intent across concurrent edits, retries and worker replacement. Keep policy verification separate from record writes. |
| Resource usage sampling and distributed project-image usage snapshots | Observation worker | Keep usage timestamps, source provenance, and the existing status projections. Rebuild incomplete snapshots without changing the most recent usable result. |
| Bootstrap machine inventory, shared-pool policy drift, legacy node labels and managed-shared location runtimes | Node or application reconciliation according to the resource | Persist per-node or per-runtime decisions. Keep cache refresh read-only. Move incidental mutations from stale GET refresh paths behind the same authority before revoking API credentials. |
| Automatic right-sizing and OOM right-sizing | Controller | Carry app and backing-service identity, resource revision, and deduplication of OOM evidence into the existing durable deploy-operation boundary. Preserve user-controlled policy modes. |
| Console and API metrics snapshots | API-local | Refresh only process-local read caches. Ownership of the source observation belongs to its executor. |
| Storage policy and lifecycle | API-local pool and codec configuration; storage worker for audit and maintenance | Reload the local policy in each consumer; separate the artifact deduplication writes currently bundled in its reload loop. Give maintenance its own bounded work progress. |
| Backup execution, retirement, inventory | Backup execution and inventory workers | Preserve atomic backup claims, coordination leases and artifact identities. Inventory remains separate from execution and does not enqueue replacement backup effects. |
| Database imports and TLS issuance | Import and certificate workers | Preserve the durable jobs and replay rules. Require explicit treatment of ambiguous SQL or CA effects before any retry. |
| App mail and other application-resource reconciliation | Controller | Advance the existing app desired state and report status on persistent app resources. |
| Platform compile/publish and offline Edge recovery | Guardian | Preserve per-authority publication fences, signed inputs, consumer selection, verified LKG, and the explicit offline recovery policy. Move read-only capture separately where needed. |
| Automation shadow | Observation/automation worker | Reports are evidence only; do not inherit actuator credentials. |
| Node participation | Dedicated executor after read-only comparison | Preserve target identity, lease fencing and actual consumer evidence. Both the API and scheduled Actions currently invoke the legacy apply path; migrate their authority together. |

Record production ownership against an exact release receipt for each transfer.

### Hosted DNS flattened answers

This handoff only moves the periodic refresh of derived hosted DNS answers.
The API still accepts DNS record intent and reads the stored result. Signed
platform capture and publication are a separate authority path and do not
consume the worker's cache as proof of a fresh observation.

First release the worker in standby. Its restricted database login can call
only the hosted DNS functions, and while the owner is `api` those functions
do not expose the work page or allow its completion. Next release the
compatible API, which checks the owner under a shared transaction lock before
committing a derived answer. It can only update the exact observed record
revision in an active zone; a concurrent intent change wins.

After ownership transfer, the worker reads at most eight records after the
durable cursor, resolves them outside the page transaction, and commits
derived results and the next cursor together. A failed or interrupted page
leaves the old cursor intact. The next worker resumes from that checkpoint.
The separate publication path retains its existing inputs and retry rules.

Do not transfer ownership on either compatibility release. First verify
independent monitoring and confirm that both live images and their actual
rollback targets respect the database owner. An initial worker's empty
rollback and an API rollback that predates the fence are not eligible.

### Storage lifecycle audit worker

The `fugue-storage-audit-worker` candidate requires its own database login.
That login can invoke a fixed sampler and read the redacted progress view;
it cannot read the underlying business tables, fetch archive bodies, edit
the policy, or alter the sample. The function runs as a separate NOLOGIN role
with only the required source metadata columns and write access to its own
progress row. The administrative helper sets a transaction-local no-spill
limit without granting the caller a PostgreSQL setting privilege.

A bounded page, its four private cursors, and its attempt result are committed
together. A failed read retains the last complete sample and cursor; a
changed policy starts a fresh sweep. PostgreSQL enforces the minimum period
and keeps a missed reference unknown; no audit outcome authorizes retirement.
Reinstalling the capability does not enable it or reset its owner. A rollback
must keep the worker owner after activation; the policy can independently
disable sampling without restoring the legacy executor.

The compatibility API used `FUGUE_STORAGE_LIFECYCLE_HANDOFF_ENABLED=true`
after installing the capability with `owner=api`. Its database role received
`USAGE` on `fugue_storage_audit` and `SELECT` on `lifecycle_status_v1` in
that schema. The compatibility API checked ownership
under a shared advisory lock on the same PostgreSQL session as its legacy
read-only scan. The SQL transfer refuses to change ownership while that lock
is held. Once an API process observes `owner=worker`, it does not resume that
scan even if the status view becomes temporarily unavailable. The existing
API-local policy refresh projects the last durable sample into the unchanged
diagnostic response; neither a diagnostic request nor a metrics scrape reads
PostgreSQL. The worker exclusively exports the lifecycle metrics after
activation. The cleanup release removes the API inventory scan and its
startup flag. The API keeps its existing diagnostic response using only the
redacted status view.

The standby installer verifies the CNPG cluster and database identity, binds
a new immutable Secret to a dedicated login, and only grants the API access
to the redacted status view. An installer retry cannot replace an active
owner or a policy with a different digest. Run it only from a declarative
release; it is not an additional production release path.

First release `storage-audit-worker` on its own component lane. It rehearses the
checked-in SQL and the actual worker's restart on the pinned production
PostgreSQL image, installs the standby capability, and retains a read-only
receipt for the exact Ready workload, matching persisted policy and active
Prometheus scrape. The initial receipt must report `owner=api` and zero
committed worker passes. No API rollout is required for this stage.

Then roll out the compatible API with its gate enabled. Do not
activate until the actual deployed API and its immutable rollback target
both obey the gate, the worker's scrape is present under the namespace used
by the existing capacity alerts, and API policy and worker policy digests
match. Transfer is a separate release step. Validate a new durable pass and
the existing thresholds after transfer; do not reset the owner to recover
from a failed check. The standby release does not enable the API flag or
transfer execution. Subsequent worker image releases reuse the installed
credential and SQL capability. The activation verifier checks every live
executor Pod and the immutable release records. An initial worker install can
declare an empty rollback, which means worker removal pauses sampling until
the worker is restored. The durable owner stays with the worker throughout.
After activation the verifier checks a committed sample under the declared
policy and verifies threshold series on the worker scrape. The storage policy
job can pause sampling through its existing policy update path.

The production transfer completed on November 28, 2026 (`6df331068`). The
database recorded a successful pass after transfer, and Prometheus observed
the same standby worker as the active owner. The API cleanup publishes before
and after observations to show that the worker Pod stays up and commits a
new durable pass while both API replicas are replaced.

Before enabling, exercise the SQL, worker restart and a dependency timeout
against a disposable database:

```sh
go build -o /tmp/fugue-storage-audit-worker ./cmd/fugue-storage-audit-worker
python3 -m scripts.rehearse_storage_lifecycle_audit --worker /tmp/fugue-storage-audit-worker
```

The rehearsal locks only the worker's disposable control table after the
worker is ready. The worker stays live but not ready, retains the last
committed page, and leaves the API fixture's business rows readable and
unchanged. Releasing the lock allows one new page to commit under the same
owner and policy. This is not a production canary or rollback witness.

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

## Sample retention: admit before transferring

There are two legacy deletion paths: the observation-retention timer and
age-based deletes performed in sample-ingestion transactions. The
latter currently prunes edge-network samples at one hour and edge-performance
and resource-usage samples at seven days. The next owner must preserve these
effective cutoffs as well as the independent scheduled policy. A transfer
must not give the worker tenant-state rights or the heartbeat audit keyring.

The compatibility API defaults to the old paths when the sample owner is
absent. Each ingestion delete takes a shared transaction lock; ingestion
still succeeds during lock contention. Once the worker owns deletion,
ingestion instead records a monotonic cleanup cutoff in the same transaction.
That demand does not advance while a producer is idle, and its rounding can
retain extra history, never make data expire earlier. The scheduled probe
policy remains independent of these ingestion-triggered cutoffs. The API
timer recognizes the sample owner under its existing session lock and still
handles signed heartbeats. Neither API path can create the owner checkpoint.

The standby capability processes one closed target per transaction and
commits progress with deletion. It cannot read private table contents or
change writer selection through its progress view or bounded procedure. A
full batch is retried sooner to drain backlogs; each target keeps its own
retry time and database advisory lock. The worker independently reports
policy validity, writer, durable pass count, last completion and deleted rows
on `/sample-retention/metrics`. The existing PostgreSQL schema bootstrap still
uses the API's ownership of these tables; revoking permanent DDL/DELETE
capability requires migrating that bootstrap authority, not just disabling
the API timer.

Do not set the checkpoint until a separate release has proved the worker's
readiness, its independent scrape and source floors for all serving and
actual rollback artifacts. The transfer must take both API locks and atomically
attach and enable the database fence with the ownership change. The fence
preserves legitimate tenant cascades and turns a legacy inline delete into a
no-op rather than aborting the associated insert. The worker refuses to
advance without that fence. A temporary lock conflict rolls back the whole
activation. Actions retries that conflict only after checking the exact
serving and rollback artifacts, primary, credential, policy and writer
state again. An ambiguous transport failure is not an automatic write retry.

After the transfer has committed and all three targets have new durable
progress, retire the API's scheduled sample delete. A release replacing the
compatibility API records before and after observations of both API replicas
and checks for a new committed pass on every sample target from the same
independent worker. Sample submissions still retain their foreground cutoff
contract. Signed heartbeat retention keeps the existing API timer and lock
until its key and database capabilities are moved separately.

## Application mail: provider boundary before executor handoff

The reusable mail provider protocol and one bounded reconciliation pass
live in `internal/appmail`. The API still owns the existing schedule,
cursor and lock until the production handoff. This preparation does not
change HTTP behavior, grant Controller access, or transfer a write.

The durable schedule records the desired binding generation in the same
transaction as the intent. Bounded seeding covers bindings created before
the schedule existed. The Controller owner gate defaults to closed. A
claim reserves one app with database-clock expiry and an unpredictable
lease ID. A newer generation becomes due immediately without stealing a
live lease; a stale completion cannot publish status for the new intent.
Failures back off with a fixed upper bound, and successful bindings get
periodic refreshes for DNS and provider readiness. Persist outcome codes,
not provider bodies or credentials.

The Controller leader can seed the queue while the API is still the
writer. The seed cursor and that page's work commit together. The leader
checks the enqueue trigger, HTTPS provider configuration and the same
application suffix as the API. At most once per minute in standby it
uses the provider's read-only GET for one existing active binding and
checks the exact identity and generation without persisting the response.
The metrics distinguish local configuration, authenticated connectivity
and write ownership; none exposes binding or provider details. Only the
persisted owner authorizes a claim; standby and startup never set it.

The handoff must include every binding reconciliation PUT, including the
preflight PUT in credential creation and login requests, not just the
scheduled sweep. Keep binding generation/CAS as the intent boundary and
report the outcome against that exact generation via the existing
`binding`, `reconciled`, `provider` and `error` response fields. A restart
must not lose a pending binding or reapply an older generation. Keep
credential issuance, login updates and credential revocation separate
from background binding retries; a one-time credential result must not
become part of persisted reconciliation status.

There are two distinct fences: the existing PostgreSQL CAS for desired
state and the Mailflare protocol for external effects. A canceled HTTP
request can already have reached Mailflare. Before takeover, prove that
app identity and generation fence delayed and duplicate provider PUTs,
and that both the running API and its actual rollback image refuse new
binding PUTs once Controller owns them. A green Controller readiness
check and the database cursor alone do not prove these properties.

The dedicated main-branch job requires a handoff-only commit and a
rehearsal on the pinned PostgreSQL base image. API and Controller
publication share its non-canceling queue; the running processes do
not share a lifecycle. Admission verifies both settled deployments,
their immutable monitored rollback targets, the current Controller
leader and fresh provider GET, and the declared PostgreSQL primary.
The transaction will refuse to proceed while any compatible API
reconciler is in flight or any current binding lacks a durable job.
After committing, only a fresh success for each still-active binding's
exact generation proves takeover. If confirmation fails, inspect the
database owner and provider result; repair forward. A release rollback
must not reset the database owner or restore a pre-fence image.
