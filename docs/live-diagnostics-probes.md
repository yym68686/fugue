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

The independently released pack also supports `process-cpu-profile` and
`host-journal` collectors under the existing `process-profile` capability.
CPU collection captures at 19 Hz for 5-30 seconds with bounded output and
process-group cancellation; changed process identities, lost samples and
unresolved symbols cannot produce complete evidence. Journal recipes specify a
service unit and a lookback of at most 24 hours, with entry and byte limits,
redacted examples, source timestamps and explicit partial-coverage reporting.
These recipes can be registered or removed through catalog configuration.

Components can register bounded, read-only snapshot providers with
`runtimeobservation.Start`. This exposes a root-only Unix socket and a stable
provider discovery endpoint, without opening a network listener. Independent
`runtime-json` recipes select provider paths and output fields. The telemetry
agent registers log-source cursor/timing/outcome metadata and collection-cycle
budgets; observations never include log bodies or alter collection cursors.
Source eviction and output limits are explicit in the snapshot.

Kubernetes observation recipes can use `field_selector` and explicit
`annotation_keys` to inspect event reasons and controller state without dumping
all annotations. Credential-related and last-applied configuration annotations
are excluded. Catalog configuration recovery runs independently of all code
jobs. A successful package build activates its new digest in a separate lane;
both writers serialize through the same production concurrency group. The
versioned `diagnostics/package.json` permits an explicit package rebuild without
changing a serving component.
