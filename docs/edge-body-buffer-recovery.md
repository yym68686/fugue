# Request body spool recovery

SSE JSON POSTs used to return 503 before reading a byte whenever the local
request-body spool budget was zero. The disk reserve threshold could disable
otherwise valid requests even with no outstanding reservations. Ordinary
non-SSE requests already streamed in this situation.

The worker now streams the original body if reservation, directory creation or
temporary-file creation fails **before any body read or origin connection**.
The selected body-size limit remains in force for known and chunked bodies.
Signed route policies still run first and retain their size, concurrency and
timeout limits. This is resource recovery, not a route or LKG change.

There is no whole-body memory fallback and no new POST retry. Once any bytes
have been consumed, a spool error remains an error: storage failures are 503,
size violations 413, policy deadlines 408 and client cancellation/incomplete
uploads 499. SSE/upload POSTs cannot use peer fallback. Origins may reject a
streamed request before consuming all bytes; applications must validate complete
bodies before committing work, as with other streaming uploads.

## Evidence

Every buffering attempt records its selected limit and outcome, including
attempts rejected before a temp file exists. The reservation snapshot captures
mode, requested bytes, budget, occupancy, disk reserve/ratio, available bytes
when known, statfs time, parent fallback, errno and a stable reason code under
the manager lock. Later manager changes do not rewrite this decision.

`request_body_buffer_stream_fallback=true` means spooling failed but forwarding
continued. It is not a failed HTTP request. Retained spool failures use
`platform_error_class=edge_body_buffer`; they no longer blame the origin.
Request explain also recognizes historical buffer errors even when their budget
fields were omitted, and never invents missing measurements or copies raw errors.

Metrics expose `fugue_edge_body_buffer_{budget_bytes,used_bytes,active_requests,
reserve_bytes,available_bytes,statfs_available}` and
`fugue_edge_body_buffer_stream_fallback_total{reason=...}`. Available bytes are
omitted when not measured; fixed budgets do not claim a statfs measurement.
The reason vocabulary is bounded. There are no per-request/app/path labels.
Observation of a zero budget does not automatically mutate DNS or readiness.

Early origin responses can overlap the transport's body-read/write callbacks.
Body counters use the existing observation lock; completion records a separate
snapshot rather than overwriting the object still referenced by callbacks.

## Validation and release

Regression coverage uses synthetic loopback origins and real filesystem calls,
without filling a disk: reserve exceeds available space or an isolated path and
its parent are absent. It covers size variants, positive-budget recovery, busy
reservations, setup failure, mid-read budget loss, streaming before upload EOF,
SSE flushing, early auth rejection, source failure, cancellation, timeouts,
known/chunked limits, reservation cleanup and diagnostic classification.

Run `go test -race ./internal/edge ./internal/config ./internal/model` and the
request explain API tests, followed by `make test` and the canonical prepush gate.
Release through the declarative API and per-group A/B worker lanes. Observe the
active slot and OCI revision, retained artifact identities, request facts,
fallback counters and readiness/restarts. A successful build is not evidence
that the new slot is serving. No buffer threshold, application route, signed
traffic artifact or DNS intent is modified by this change.
