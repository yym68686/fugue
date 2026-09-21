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
