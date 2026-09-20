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

The first production catalog combines bounded node pressure and scheduling
facts, control-plane request/storage metrics, Kubernetes reconcile objects and
logs, and telemetry pipeline flow metrics. The package never mutates observed
objects, exports Secret or environment values, or treats unavailable data as
zero. A future collector can be published as another digest-pinned package
without changing API/controller business code; only a new capability class or
protocol version requires a core change.
