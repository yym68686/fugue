# Metrics and inventory collection

API and Controller metric requests render process metrics and a previously
collected immutable byte snapshot. Scrapes never start an R2 listing, a
database aggregation, or a Kubernetes maintenance probe. Background refreshes
run every 15 seconds, with a 30-second context budget; refresh failure retains
the last complete body and its original observation time. The
`fugue_metrics_snapshot_*` metrics distinguish cold start, failed refresh and
stale observations. Component readiness is separate from inventory freshness.

Backup inventories use `observation/v1/` checkpoints in the existing metadata
store (a separate locked sidecar file for the file-backed Store). These are
rebuildable runtime facts, not serving configuration or artifact authority.
Each bounded transaction coordinates one LIST page and the corresponding
cursor across API replicas. A canceled transaction cannot advance the cursor.
The identity includes backend namespace, ownership and credentials; changing
that boundary cannot reuse another scope's observation. Credentials are never
persisted in a checkpoint or returned in scan errors.

Unreferenced objects are reduced to namespace/tenant counters. Only known
artifact references retain per-object metadata, keeping memory proportional
to known references and owners rather than bucket size. Pagination may span
many minutes. Completion publishes a new generation atomically; an unfinished
or failed scan never overwrites the previous complete generation. Scan start,
finish and per-page object observation times are preserved. LIST is not a
transactional snapshot, so metadata created after the scan boundary is not
evidence of a missing object. Inventory measurements do not authorize object
deletion, change the billing ledger, or change positive serving LKG.

The independently configurable background settings are:

| Environment variable | Default |
|---|---|
| `FUGUE_BACKUP_INVENTORY_REFRESH_INTERVAL` | `15m` |
| `FUGUE_BACKUP_INVENTORY_PAGE_TIMEOUT` | `10s` |
| `FUGUE_BACKUP_INVENTORY_RETRY_INTERVAL` | `30s` |
| `FUGUE_BACKUP_INVENTORY_MAX_SCAN_AGE` | `24h` |

The backup usage API preserves its original measurement timestamp while a
new generation scans. `refreshing`, `stale`, scan progress, last attempt and
last success describe the observation independently of its physical totals.
Unavailable physical measurements are not presented as zero. A stale
reconciliation cannot pass the robustness check.

Controller release metrics use a repeatable-read projection of the same
latest 500 attempts as before and one batch of their steps. This replaces two
500-query loops with two queries; no desired source or full step payload is
needed. The duration and safe-rollout aggregators share this projection.
Registry maintenance reports applicability and read success separately:
unknown or inapplicable maintenance is not represented as a missing CronJob.

Managed application observations query exact tenant/app/runtime/image
identities needed by the current managed object and serving release, including
source aliases. Present, pulling, missing and failed evidence keeps its age
and precedence. A refresh cannot substitute an unrelated historical image or
another migration runtime's positive evidence.

# Kubernetes log collection

The API-based collector reads forward from per-container-instance timestamps,
without `TailLines`. A bounded eight-worker pool uses the existing global
cycle budget and per-target line bound. An inclusive boundary and occurrence
counts preserve multiple identical records at the same timestamp. Queue
rejection leaves the cursor before the rejected record. Completed previous
container instances are also eligible. Cursor identity includes Pod UID and
container ID; current and previous views of the same instance share a cursor.

Cycle duration, backlog age and retention gaps are exposed. A truncation now
means a bounded read has more backlog to resume, not permission to skip that
backlog. Cursors live for the collector process; a new process replays the
latest five minutes. Queue admission is not an exactly-once exporter receipt:
process loss, source rotation, configured sampling and exporter exhaustion
remain explicit limits of this existing in-memory pipeline. The collector
does not claim that `dropped=0` proves end-to-end completeness.

# Scrape policy release

`deploy/environments/production/observability/scrape-policy.json` is the
explicit component/port contract for public system telemetry. It includes
the A/B workers and regional DNS, and excludes tenant application ports,
Caddy public listeners and DNS port 53. Helm rendering uses the same contract.
The `observability_configuration` lane in the single `ci.yml` entrypoint is
independent of code builds. It validates with the running `promtool`, updates
only the selected job with a resource-version precondition, waits for the
mounted ConfigMap, sends SIGHUP and verifies the loaded configuration. It does
not replace the Prometheus Pod or discard its existing TSDB.

Regression coverage includes canceled and failed refreshes, cross-replica
inventory resume, tenant separation, invalid pagination, two-query release
projection, negative image evidence, equal-timestamp log boundaries and queue
rejection. Production acceptance compares scrape latency and success,
inventory progress, target coverage, log backlog, CPU and allocation profiles
against the original live-diagnostics observations.

# Live acceptance follow-up

The first production inventory completed 97 pages / 96,866 objects. It also
exposed a pre-existing classification error: Longhorn owns a shared block
repository, so its blocks are not direct artifact orphans, and its newly
uploaded byte count is not the manifest object's size. Inventory derives
repository boundaries from durable snapshot metadata, reports their physical
bytes separately, and leaves internal block reachability to the engine.
Shared repository bytes are not assigned to an arbitrary tenant. This changes
neither billing nor deletion authorization. Checkpoint identity v3 requires a
fresh complete generation for the new classification. Completed scanners
sleep until their configured refresh deadline instead of decoding a large
checkpoint every second.

The Kubernetes client now uses explicit bounded rate settings:
`FUGUE_OBSERVABILITY_KUBERNETES_LOG_QPS` (40) and
`FUGUE_OBSERVABILITY_KUBERNETES_LOG_BURST` (80), with eight workers, a global 4,000-line cycle limit and the same memory
limits. The per-container ceiling is 1,000 lines; the previous 200-line
ceiling could not keep up with a measured 1,072-line/minute source on a
15-second interval. Busy sources borrow otherwise unused global capacity;
queue rejection still preserves the cursor. At about 400 container targets the implicit client-go
5 QPS setting alone imposed an 80-second cycle. Drained terminated instances
are no longer polled. Untimestamped kubelet unavailable-log responses are
counted once per instance and retried after five minutes; old terminated
instances outside the bootstrap window are not read. Source GC remains a real
limit, not evidence that previously unavailable history was recovered.

Live follow-up showed every 2,000-line cycle saturated while aggregate backlog
grew. The 4,000-line bound leaves catch-up capacity without changing the
15-second polling interval, Kubernetes QPS, queue size or memory bound.

The final log reader budgets each cycle against available ordinary queue slots,
leaving one export batch of spare capacity for concurrent ingress. The telemetry
export batch ceiling is 128 rather than 32 records, reducing sequential exporter
round trips. Queue capacity, its critical reserve, the 16 MiB queued-byte limit
and the 160 MiB Go memory target remain unchanged.

Backlog age is the oldest observed unread record timestamp, not the age of
an idle source cursor. It is published only after the complete collection
cycle, retaining known pending records from targets not visited this cycle.
`fugue_telemetry_pipeline_kubernetes_log_deferred_targets` makes incomplete
traversal visible; zero observed backlog alone is not a completeness claim.

Export batches also flush before their serialized event bytes exceed one quarter
of the queued-byte budget (4 MiB for this agent). A single larger event flushes
alone. This decouples catch-up throughput from the memory cost of large logs.

# Request and metrics allocation follow-up

Live Diagnostics identified the controller's 15-second metrics refresh as a
large reader of completed node maintenance history and detailed prune plans.
Metrics now use dedicated read-only projections: completed prune tasks retain
only their label/result fields, and the same most recent 200 plans retain only
counters and protection summaries. Task logs, image manifests and deletion
candidates are not transferred or decoded. Selection still chooses the latest
task per label set before testing its reason, so a later non-orphan task cannot
resurrect an older orphan result. The full diagnostic and execution APIs keep
all of their evidence. PostgreSQL integration tests compare these projections
against the previous full reads; neither pruning nor runtime policy is changed.

Consumer assignment and artifact download handlers share repeated artifact
reads only within one request. Release channels, active fences and expected
consumer revisions are still queried on every request, and signature, child
status, lineage and cohort checks still run. The reader is not a cross-request
cache and never participates in mutations. Missing or failed reads are not
retained. Supersession and consumer removal remain covered by the assignment
and download authorization tests.

Telemetry redaction keeps its original ordered regex rules. An ASCII-only
negative filter skips rules whose mandatory literal is absent, and strings
without either assignment separator skip all rules. Non-ASCII text falls back
to the existing Unicode case-folding behavior. Differential fuzzing includes
Unicode folds, malformed UTF-8, nested assignments and mixed case. This removes
unnecessary scans and temporary copies without sampling away or weakening
redaction of log data.
