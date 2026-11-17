# Live Diagnostics probe catalog

Live Diagnostics has two extension surfaces. Built-in probes (`cpu-profile`,
`memory-profile`, and `process-snapshot`) keep their existing API and target
restrictions. Registered probes use a signed catalog in the control namespace;
the catalog names a digest pinned OCI package, a capability profile, allowed
target classes/namespaces, parameter schema, duration budget, and opaque
collector configuration.

The catalog is runtime configuration. It is published by the independent
`scripts/publish_diagnostic_catalog.py` lane and is not part of a serving
release or workload Deployment. The publisher keeps the previous signed
catalog and uses the current Git revision as a monotonic publication guard.
An invalid update falls back only to a signature verified previous catalog;
revoked signing keys invalidate both current and previous envelopes.

A registered session freezes the probe digest, catalog digest, runner image,
Pod UID/container ID or node identity, parameters, and target image. The API
and direct Kubernetes CLI path share the same admission lease and target
budget. The runner receives `FUGUE_DIAGNOSTIC_REQUEST` as a bounded JSON
request and emits `fugue.diagnostic.probe_report.v1`. The report repeats the
frozen identity and declares `complete`, `degraded`, or `unavailable` evidence
quality. Complete reports cannot contain gaps, truncation, or unavailable
sources.

Capability profiles are separate from target selection:

- `cluster-read` uses a dedicated service account and read-only RBAC. It does
  not use host PID, host mounts, or kernel capabilities.
- `host-read` reads bounded `/proc` and cgroup facts with host PID and read-only
  mounts.
- `process-profile` adds only the capabilities required by the process profile.
  It explicitly uses an unconfined AppArmor profile for the temporary signed
  administrator-only probe: the runtime default peer policy can deny reading a
  host process executable even when SYS_PTRACE is present. This does not change
  the target's policy and does not grant privileged mode or SYS_ADMIN.
- `kernel-profile` separately authorizes SYS_ADMIN for kernels such as Debian
  with `perf_event_paranoid=3`, whose downstream admission check requires it
  even with PERFMON. It remains a bounded, non-privileged, read-only-root
  diagnostic Job. Proc, journal and cluster recipes do not need this profile.

The first production catalog combines bounded node pressure and scheduling
facts, control-plane request/storage metrics, Kubernetes reconcile objects and
logs, and telemetry pipeline flow metrics. The package never mutates observed
objects, exports Secret or environment values, or treats unavailable data as
zero. A future collector can be published as another digest-pinned package
without changing API/controller business code; only a new capability class or
protocol version requires a core change.

The independent package distinguishes missing metric series from a valid zero
sample. A collector can name `required_queries`; empty or non-finite required
results degrade evidence quality while preserving the source response. Process
observations also degrade when IO permissions or scheduler accounting are
unavailable. Node-window summaries compare cumulative counters only across the
same boot identity; process deltas require the same PID and start time.

The independently released pack supports `process-cpu-profile` under
`kernel-profile` and `host-journal` under `process-profile`.
CPU collection captures at 19 Hz for 5-30 seconds with bounded output and
process-group cancellation; changed process identities, lost samples and
unresolved symbols cannot produce complete evidence. Journal recipes specify a
service unit and a lookback of at most 24 hours, with entry and byte limits,
redacted examples, source timestamps and explicit partial-coverage reporting.
These recipes can be registered or removed through catalog configuration.

The journal collector filters literal, case-sensitive `match` strings at the
source before applying its entry limit. `match_param` optionally selects one
literal from a recipe parameter. Reports record the selected literals and
coverage scope; no-match is distinct from a journal read failure. Results cover
retained journal records only, not logs already rotated away. `since_time` and
`until_time` accept optional RFC3339 parameters for an exact incident window;
the entire window must remain inside the past 24 hours. Entry and byte limits
remain fixed even when a longer window is selected.

Components can register bounded, read-only snapshot providers with
`runtimeobservation.Start`. This exposes a root-only Unix socket and a stable
provider discovery endpoint, without opening a network listener. Independent
`runtime-json` recipes select provider paths and output fields. The telemetry
agent registers log-source cursor/timing/outcome metadata and collection-cycle
budgets; observations never include log bodies or alter collection cursors.
Source eviction and output limits are explicit in the snapshot.

Node recipes include bounded `/proc/vmstat` counters. Process observations retain
minor/major fault counts excluding children, and matching PID/start-time windows
include their deltas. These distinguish the selected process from a shared
cgroup; they do not identify a fault's backing file or establish causality for
an individual request.

The `storage-scan-cost` recipe combines host loopback metrics, node counters and
process facts. Run the separate `kubernetes-storage-read-paths` recipe for cached
cluster metrics and join by source timestamps. Host capability profiles do not
mount a Kubernetes service-account credential; composing recipes must respect
that boundary instead of assuming all collectors are available in every profile.

Executable identity recipes may select up to 32 exact `go_modules` names from
the running binary's embedded Go build metadata. Reports retain declared and
replacement module versions/checksums and a small VCS/architecture settings
allowlist. Local replacement paths, build flags and environment values are not
exported. Missing requested modules degrade evidence; a local replacement is
explicitly marked and does not establish its source contents. Module metadata
identifies what the binary records, not an independent source or binary audit.

The independent `process-page-faults` collector uses perf software major-fault
events for one frozen host process selection. A single capture lasts 5-30 seconds
with a 4 MiB data bound and a 192 MiB sampler RSS guard. Reports keep realtime
timestamps, fault/instruction addresses and at most 8,192 events. File attribution
requires identical process lifetime, cgroup membership and address mappings
before and after capture; mappings are identified by range, device, inode and
offset. No mapped contents or command-line values are read. Lost/unparsed events,
unresolved mappings and reached bounds degrade evidence. Fault occurrence alone
does not measure IO latency, identify a lock owner or prove a request's cause.

The production `process-page-faults` recipe also pairs that capture with a bounded
host journal collector for the selected service. Its `since_seconds`, `unit`, and
`match` parameters are independently bounded, and every journal row retains its
timestamp. This makes compaction, slow-apply, and fault windows directly
comparable in one report without changing the service or its configuration;
missing journal coverage remains an explicit evidence gap.

`process-runqueue-latency` observes existing scheduler tracepoints for at most
20 seconds, with kernel filters limited to 128 frozen target threads. It pairs
successful wakeups or runnable switch-outs with the next switch-in using the
shared monotonic clock, excluding blocked time. The report includes quantiles,
the 20 longest completed waits, and unpaired boundary counts. Changed thread
lifetimes, new threads, loss, malformed events, and capture bounds are explicit;
an interrupted event stream has no derived latency table. The 8 MiB capture,
4 MiB decoder output and 192 MiB sampler RSS limits bound observation cost.
The production recipe uses a one-second event burst plus node snapshots; mapping
synthesis is disabled because thread identity comes directly from procfs.
Only the captured burst has thread latency coverage. Failed recordings retain
bounded decode counts for collector troubleshooting but no latency estimates.
Up to eight conflicting transitions retain four preceding events from the same
thread, including numeric thread identities and monotonic timestamps only.
Wakeups that arrive while a thread is still running are counted separately as
stale runnable markers; they do not invalidate otherwise complete wait pairs.
Duplicate wakeups while one runnable wait is pending are handled the same way,
with the earliest timestamp retained.

It reads already mounted host tracefs metadata and opens temporary perf event
descriptors; it does not mount tracefs, enable schedstats, or modify global
tracing configuration. Its filters include wakeups issued outside the target
cgroup. A wait measures runnable scheduling delay, not its workload-level cause.

The telemetry source snapshot includes an `error_summary` grouped by node,
error class, and stage across every retained source, independent of the 512-row
detail projection. This keeps source error attribution complete when the detail
list is marked truncated.

After successful Kubernetes discovery, the same snapshot records the selected
source count and marks retained sources as `present` or `absent` from the latest
successful namespace/label inventory. Sources first retained after that inventory
are `unknown` until the next successful discovery. Membership annotates only the
bounded retained source set; it does not retain a second full inventory. The
summary counts all retained sources, including those outside the 512-row detail
projection. `absent` means only that the source is no
longer selected; it does not establish why it disappeared or whether its logs
were fully delivered. Discovery errors do not replace the last successful
inventory. This metadata does not change collection, retry, checkpoint, or
cursor behavior.

The source snapshot also keeps an independent `gap_history` ring of 64 new gap
observations. Ordinary reads, backoff, retained cumulative counts, source-detail
eviction and the 512-row projection cannot evict those receipts. New gap
observations alone advance its sequence and overwrite its oldest entry; `total`
counts observations (each has its own `events` delta), and `evicted` reports
overwrite. Receipt bounds are captured at detection and are not silently updated
by later interval widening. Read outcome and latest error provide context, not a
proven cause. No log contents or cursors are reconstructed or changed.

New rotation-boundary gaps include a `boundary_check` captured at the actual
missing-witness branch, before newer records can change the cursor. It names
the cursor/checkpoint witness and newer-record/EOF phase, exact distinct and
occurrence counts, and separate deterministic four-hash samples of missing and
seen messages with their required/observed counts. Omitted sample counts are
explicit. No message text is stored. These owner-only hashes allow matching
the original comparison to retained node files; timestamps alone cannot prove
that the required body and multiplicity were present. The receipt does not
change missing-boundary decisions, recovery, admission or cursor advancement.

Each retained source and new gap receipt can include `last_admission`, the most
recent failed queue admission for that source. Fixed reason codes distinguish
ordinary-slot and byte bounds, a full physical queue, canceled/expired context,
sampling, quota, disabled collection and encoding failure. Slot/byte rejection
records the exact atomic reservation value and limit used by the failing check;
other queue gauges are independent concurrent samples after refund, not an
atomic snapshot. The timestamp may precede the current source outcome or gap:
context is not causal attribution. Only numeric metadata is retained, never an
event payload. Recovery preserves this evidence without changing admission,
queue budgets, retries, cursors or loss counters.

The API operations snapshot's `captures.agent_failures` has an independent
24-receipt budget for completed real Agent-request capture errors. DNS reads,
previews, internal calls and successful requests cannot displace those failures.
Its total/evicted counters describe that ring, not all HTTP 503 responses: failures
after capture (for example authority recheck or response signing) remain separate
operation counters. Both rings are process-local metadata with explicit overwrite
and restart boundaries, not durable or complete 24-hour request tracing. They do
not renew facts, change admission or erase historical failures.

The API capture snapshot's independent `traffic_failures` ring retains twelve
failed published-traffic diagnostic reads, including DNS route-invariant checks.
It records the source-defined failure stage and, for failed convergence, the
same-read publication/expectation hashes, revision, required/passing counts,
unexpected-consumer count and up to eight nonpassing assessments. Assessments
retain hashed consumer/binding identities, heartbeat/freshness timestamps and
fixed reason codes; omitted assessments and ring overwrites are counted.
Successful reads cannot evict these failures. Credentials, nonces, proofs,
hostnames, arbitrary errors and consumer payloads are not retained. This adds
no reads, probes or retries and changes no convergence decision or HTTP schema.
A reason identifies the failed check, not the upstream cause of that condition;
process-local retention does not establish complete 24-hour coverage.

Runtime snapshots deduplicate shared Unix sockets across helper processes and
bind each row to the socket's kernel-reported peer PID in the frozen target set.
Only that provider's lifetime and socket identity determine snapshot continuity;
short-lived helpers do not create false gaps. Missing providers, changed peers
and incomplete source data still degrade the report. The reported scope and
skipped process count make this provider-level coverage explicit.

Source snapshots retain the last read error's timestamp, stage, normalized
class and elapsed time after recovery. They retain no error payload. A failed
stream open saves the existing cursor and unresolved coverage boundary, and
waits a normal polling interval before retrying. It cannot consume catch-up
requests intended for known unread records. This preserves the first attempt's
lower bound through an outage; it does not persist cursors across process restarts
or recover records already rotated away by the source.

API and controller also expose an `http-client` provider for their shared
Kubernetes client. It retains 2,048 recent completed requests with normalized
resource/cache selection, response-header time, body-consumption time and bytes.
It excludes URLs, object names, query values, headers and payloads. Ring overwrite
and in-flight requests are explicit, so a snapshot cannot imply full historical
coverage. These client facts complement audit logs when the audit policy excludes
read requests, without changing Kubernetes audit settings.

Kubernetes observation recipes can use `field_selector` and explicit
`annotation_keys` to inspect event reasons and controller state without dumping
all annotations. Credential-related and last-applied configuration annotations
are excluded. Catalog configuration recovery runs independently of all code
jobs. A successful package build activates its new digest in a separate lane;
both writers serialize through the same production concurrency group. The
versioned `diagnostics/package.json` permits an explicit package rebuild without
changing a serving component.

The `host-loopback-metrics` collector uses a fixed helper in the host network
namespace, under `kernel-profile`. It only issues GET `/metrics` to IPv4 loopback
at a configured port, without credentials or redirects. Responses are bounded at
2 MiB and projected to explicit metric families; missing families degrade the
report. This can observe services whose metrics are not scraped by Prometheus
without reconfiguring or restarting them.

`runtime-stage-cost` reads fixed operation counters exposed by the reusable
`runtimeobservation.Operations` provider. Its stage set cannot grow from request
values. Image-cache registers local/network registry operation counts and work
durations, including inventory phases. Add/remove recipes in the signed catalog
to inspect these counters without redeploying the serving component.

Audit recipes can specify a resource filter and `fields: ["object_name"]` to
group by namespace/name as well as caller, verb and response. The 256-group,
30,000-event and 32 MiB source limits still apply. Audit policy exclusions remain
explicit. For runtime client snapshots, `subresource` separates node proxy/log
requests and `metadata_only` identifies metadata content negotiation.

Frame-pointer CPU reports include a bounded dictionary of ordered call paths
in `stack_paths`. Frame IDs run from leaf to root; repeated frames remain in
their original positions. Cumulative function counts count a function once
per sampled stack. The path table is summarized before raw-text export limits
are applied, and has its own 512-path, 4,096-frame and 256-KiB frame-text limits.
`observed_samples`, `omitted_samples` and `truncated` distinguish coverage from
the complete leaf table. Missing symbols still degrade report quality.

`kubernetes-object-changes` performs an exact namespaced GET followed by a
resource-version-bound five-second watch. It emits only configured fields and
at most 64 events / 2 MiB. An unchanged version establishes no persisted changes
in that window; use the separate audit recipe to establish attempted writes.

Example registered CPU capture (the session includes analysis headroom):

```sh
fugue diagnostics node-process start --node NODE --process k3s \
  --probe process-cpu-frame-pointer --duration 50 --wait
```


`pod-runtime-state` observes the explicitly named namespace/Pod/UID on a node
through fixed read-only CRI list/inspect operations. It records sandbox identity,
process status, network namespace closure, container state and host PID presence,
with the same node boot and CRI peer process identity before and after two reads.
Missing fields, empty inventories, truncation and identity changes are unavailable,
never proof of quiescence. An exited container can retain an old PID in CRI; the
collector also checks that PID's current presence in host proc. Raw CRI runtime
specs, environment variables, labels and annotations are not exported. The signed
recipe selects an allowlisted local runtime binary and socket. This diagnostic
report describes current state only and does not authorize deletion.

`postgres-memory-window` observes an exact namespace/Pod UID/container through
fixed read-only CRI/psql calls on the selected node. It never accepts SQL or
credentials. Database reads have a 500 ms statement timeout and use only
pg_stat_activity; query text and parameters are replaced by fingerprints.
A five-second baseline becomes one second for at most 60 seconds after 80%
of the closest cgroup memory limit. The 300-second/300-sample/2-MiB bounds
preserve pre-exit samples when the database restarts. A coincident idle query
is never claimed as the cause of retained memory. Process start ticks,
namespace PID, backend_start, postmaster start, container and boot identities
must agree before correlating samples across sources.
The query-size setting is converted with PostgreSQL's unit-aware size parser.
Failed SQL observations expose only a fixed failure class and back off for
15 seconds while process sampling continues. The cgroup `memory.stat` byte
counters distinguish anonymous memory, file cache and shared memory; these
overlapping categories must not be added together or treated as process RSS.

`runtime-heap-export` and `runtime-allocs-export` save a single raw gzip profile
in the retained report with SHA256, length and bounded base64 chunks. They do
not run pprof on the production node. Concatenate decoded chunks, verify the
digest and analyze offline. Oversized captures are explicitly unavailable.
Memory analysis also retains completed process/cgroup samples in a checkpoint
before invoking pprof, so analyzer RSS protection does not discard that window.
The memory-profile recipe needs a session of at least 90 seconds.

Node memory recipes preserve scrape and container-lifecycle labels. A zero
six-hour OOM series conflicting with a recent Kubernetes OOMKilled termination
makes the report degraded; the original values remain unchanged. Collection
success is not proof of cross-source agreement or causal attribution.

The API's `database` runtime provider correlates pgx query fingerprints with
backend PID, a bounded connection identity, operation lineage and exact lengths
of existing string/byte parameters. Other encodings remain explicitly unknown;
no re-serialization is performed to measure them. Known result boundaries may
record buffer lengths, separate from protocol bytes. The recorder keeps at most
256 recent queries, 128 large input/output queries and 128 unfinished queries.
Writers drop observations on contention instead of blocking application work;
dropped completions and overwrites are explicit. Compiler input persistence
propagates operation lineage without changing the store's existing timeout or
cancellation semantics. Runtime requests never expose SQL text, parameters,
result payloads, credentials or raw error messages.

An independent 32-entry `failures` ring retains original driver query failures,
including starts that could not enter the ordinary active-query map. Successful
queries cannot evict them. Fixed error classes and allowlisted typed SQLSTATEs
are retained alongside the query's existing metadata; error text is not parsed.
Failure-ring contention drops an observation without blocking business work.
Total, recorded, dropped and evicted counters make that coverage explicit;
concurrently sampled counters are not a transactional snapshot. This observes
driver query errors, not every store, transaction-commit or HTTP failure.

The API operations snapshot also retains up to 24 `captures.consumer_failures`
from the original assignment attempt or verified heartbeat read/admission failure.
Fixed operation/stage names, error categories, typed SQLSTATE and store-conflict
flags are recorded without identifiers, request bodies or arbitrary errors.
Successful requests cannot evict them. Heartbeat store writes copy only the
operation lineage, preserving their existing independent timeout/cancellation.
Operation IDs are local to one process; absent lineage must not be inferred by
pairing unrelated failures. Authentication, decoding, expected conflict responses
and other handlers are outside this ring's coverage. Neither ring reconstructs
events before deployment or authorizes serving traffic.

Assignment discovery retries once only when its final independent authority
check reports `assignment_changed`. Each attempt builds a new body-read session
and reruns discovery, current membership/serving selection, complete input
verification and the unchanged final equality check. A failed attempt stays in
the ring even when the reread succeeds: these are not HTTP failure totals.
Fixed `consumer-assignment-retry`, `consumer-assignment-retry-ok`,
`consumer-assignment-retry-empty` and `consumer-assignment-retry-exhausted`
counters distinguish outcomes. A cold input, database error, absent assignment
or selection error does not initiate this retry; continued changes return 503
without returning the first attempt's assignments.

Assignment `input_recheck` failures additionally retain the original synchronous
read's fixed substage. Store failures distinguish preparation, pin selection
and admission checks. A failed dependency read retains its original typed error
category/allowlisted SQLSTATE even when the existing public conflict response
discards that cause. A changed physical witness records its fixed relation,
SHA-256 key identity and changed physical field names, never raw keys or tuples.
Final authority comparisons retain only changed top-level field names from the
fixed artifact, assignment and release structs; map keys and values are omitted.
The failure receipt itself adds no query, replay, retry, public response field
or admission exception; the bounded assignment reread is described above.
It describes the immediate failing check, not the upstream cause of a concurrent
publication or evidence change.

The API's local `storage` provider retains `platform_mutations` for completed
publication, rollback and LKG verification operations: 12 recent receipts and
an independent 12-failure ring. Each receipt includes the existing operation
lineage, fixed stage durations and at most 48 original driver query timings
(the first 16 and final 32). A separate eight-fact promotion-lock head/tail
(the first four and final four) retains original `pgLockPromotionScope` driver
facts even when preparation fills the ordinary head and later queries erase
the middle. Its completed and evicted counts are independent and explicit.
Query fingerprints, caller, backend PID and bounded
connection identity can be matched to a separately observed PostgreSQL lock
witness. SQL text, parameter values, result bodies, raw errors and credentials
are never retained. Successful operations cannot evict the failure ring;
contention drops observations and all overwrites/drops are counted. These are
completed driver and code-stage durations, not measurements of CPU use or exact
server lock-hold intervals. They add no SQL, replay, retry, timeout change or
admission exception, and do not explain failures predating deployment.

Release and rollback admission receipts additionally retain at most four
fixed subphase samples per mutation. They partition validation wall time into
expiry, composition, DNS references, member integrity, source authorization,
physical policy, lineage, topology, capability projection/fact scans and
completeness. Samples include only bounded collection cardinalities and counts
of required consumers/projections/facts examined. They omit artifact and
consumer identities, content, policy values and errors; omitted samples are
counted. Subphase durations are wall time, not CPU attribution.

Each DNS reference check also partitions its existing work into six fixed nested
stages: artifact authentication, producer bindings, dependency decoding, source
authorization, selected-source observation and dependency ledger checks. Early
rejection retains only stages actually reached. The nested stages share the
four-admission retention limit and contain only names and elapsed milliseconds;
they do not retain content, source identities or error strings. No check is
cached, reordered or skipped, and the existing clocks used for authorization,
transaction locks, query deadlines and returned errors remain unchanged.

On Linux a non-root component's runtime profile socket accepts only kernel
credentials identifying root or the component's own UID. Diagnostic Jobs drop
DAC_OVERRIDE, so filesystem owner-only mode by itself prevents those Jobs from
connecting to non-root providers. The listener validates SO_PEERCRED before
HTTP parsing and closes other peers. Root-owned sockets keep mode 0600; non-root
sockets permit connection but do not grant profile access without the peer
check. No TCP listener or extra diagnostic capability is introduced. Non-Linux
providers retain the owner-only filesystem mode.

The telemetry agent's owner-only `exports` runtime provider has five fixed
exporter buckets (metrics, logs, analytics, noop, other). It records attempts,
completed attempts, retries, inflight work, attempted event counts, summed/max
attempt duration and fixed success/deadline/canceled/timeout/error outcomes.
No destination, event contents, tenant labels or raw errors enter this view.
Compare counters from the same process and use `inflight` with the timestamps;
the independently sampled atomic fields are not a transactional snapshot, and
the last-start timestamp is not an identity of every concurrent active attempt.
Attempt events include retries, not unique received or durably stored events.
Durations include exporter encoding/network work, not queue wait or backoff;
these counters do not identify the slow phase within an exporter. Collection,
admission, memory, batch, retry and timeout behavior is unchanged.
