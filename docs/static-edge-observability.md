# Independent static edge request observations

Implementation and rollout acceptance ledger. This feature must never change
request buffering, timeouts, routing, flow-control windows, or replay traffic.
An observed wait is not proof of its ultimate external cause. Missing evidence
must remain explicit. Old serving artifacts remain usable without telemetry.

## Required deliverables

- [x] Versioned independent observation/management contracts, typed safe metadata.
- [x] Per-request/hop/attempt identities and authenticated internal correlation.
- [x] Bounded request lifecycle/read/write/connection observers and live snapshots.
- [x] Protocol-level HTTP/2 stream/data/window evidence with pinned build inputs.
- [x] Explicit HTTP/3 capability coverage; no protocol downgrade for observation.
- [x] Independent bounded local collector, retention, loss counters and async export.
- [x] Direct mTLS/SSH CLI status, slow search, explain and evidence export.
- [x] Conservative multi-hop explanation with measured/unknown boundaries.
- [x] Optional client instrumentation and evidence provenance.
- [ ] Existing application observation fields preserved through Fugue ingestion.
- [ ] Isolation, protocol fidelity, failure injection, race and overhead verification.
- [ ] Declarative CI/package/release integration and reproducible artifacts.
- [ ] Production baseline, candidate health, natural draining and rollback evidence.
- [ ] ovhusstaticedges updated to the verified release; origin observations verified.

## Safety and attribution contract

Only bounded structured metadata may leave a process. Never record request or
response bodies, request headers, credentials, client addresses or TLS secrets.
Use monotonic durations within each process. Do not subtract unsynchronized
host timestamps to infer network delay, add overlapping waits, or attribute a
connection-wide retransmit counter to one multiplexed stream. A caller-provided
identifier is not a trusted identity. Export failure must not block serving.

The public Caddy, loopback proxy and mTLS origin are distinct observation hops.
Existing Caddy health-scope fixes and serving behavior must be retained in a
new binary. Replacing a binary requires an independently verified handoff;
configuration reload alone does not upgrade a running process. Never force
in-flight connections to exit to finish this rollout.

## Acceptance boundary

Complete observations can prove a supported blocking mechanism. Client-internal
work and intermediate Internet device failures require evidence from those
systems. A bounded fail-open observer can lose records on overload or crash;
queries must report loss/retention/unsupported coverage instead of inventing a
root cause. The original historical 47.048-second request remains unproven.

## Query and interpret

```
fugue static-edge observability status entry-context --source direct
fugue static-edge requests slow entry-context --since 15m --min-read-ms 1000
fugue static-edge requests explain entry-context --request-id ID --peer origin-context
fugue static-edge requests export entry-context --request-id ID --peer origin-context --file evidence.json
```

Contexts use existing explicit mTLS or SSH credentials. No API availability or
central query is required. A public entry replaces caller correlation. Only a
valid MAC from a configured loopback or verified mTLS peer links internal hops.
Set `forward_correlation` only on hops forwarding to trusted observers; the
last hop removes the internal carrier. Each reverse-proxy RoundTrip gets its
own span, including a repeated Caddy attempt. Multiple connections inside Go's
transparent transport retry remain visible as repeated trace events; they are
not falsely advertised as separately authenticated wire attempts.

An explicit `--transport` override applies to the primary context and every
`--peer` for that invocation; it never rewrites saved contexts or silently falls
back to another transport. Collector reads do not acquire the manager's serving
transaction lock. At most two collector calls run per manager at once, with a
three-second deadline; excess observation calls return HTTP 429 while signed
configuration staging/recovery and serving-status operations remain available.
The collector's own disk-query limit can still return incomplete evidence under
concurrent queries; management concurrency is not a guarantee of completeness.

The Caddy handler is `fugue_observation`, the transport is
`fugue_observed_http` (the existing HTTP transport options are embedded without
changes). A server's `fugue_observations: true` enables optional HTTP/2 hooks.
Go 1.26.3, Caddy 2.11.4 and x/net 0.55.0 are pinned and checksum checked. Health
scope fixes shipped before these observations remain in the build. Source and
patch hashes accompany each independently built release archive.

`Body.Read` time is elapsed blocking time and may include scheduling. HTTP/2
receiver counters distinguish DATA arriving from body consumption. Local
credit / queued WINDOW_UPDATE is not proof of credit reaching the peer. Only
an actual sender wait with a complete matching start/end pair confirms waiting
for flow-control credit; the reason the peer withheld credit can remain unknown.
HTTP/3 is preserved, with body API evidence and **no frame/credit attribution**.
TCP_INFO samples are user-space socket queries at span boundaries, no probes;
connection counters are never attributed to an individual multiplexed stream.
Response Write/Flush duration and complete ReadFrom copy duration are separate.
Neither establishes CPU time or client receipt. Cross-host timestamps are not
subtracted and overlapping waits are not added.

Optional Go clients can use `pkg/staticedgeclient.Transport` and `NewQueue`.
Their evidence is explicitly client-reported and unattested, and stays local
until the client chooses an export destination. It is not an authorization or a
trusted parent identity. Uninstrumented clients leave client-side causes unknown.

## Bounds and failure behavior

The serving exporter has 128 slots and one network worker; the collector has
128 queue slots, a 512-record cache (maximum 1024), up to eight 8 MiB segments,
and a dedicated private Unix socket. Live spans are snapshotted every second
by one bounded registry. Fast body calls use counters; selected calls over
50 ms retain intervals, reserving event capacity for lifecycle/protocol facts.
No eager reads, body copies, timeout changes or flow-control window changes
are introduced. Loss is explicit, including cumulative exporter loss separately
from per-span loss. A lost terminal remains incomplete.

The collector alone performs disk I/O. Record queries apply the configured
retention (default 24h); physical segments rotate hourly and expire within
retention plus one hour and one cleanup minute. Disk capacity can shorten that
window. Searches inspect bounded disk segments for 1.5s, retain at most 200
matching spans, and cap replies at 2 MiB. Eviction, concurrent rotation, timeout,
partial writes, unsupported protocols and unavailable collectors are exposed;
`not_observed` is never equivalent to "no delay". A process-level file lock
prevents competing collectors, and restart appends to the newest segment.

An optional `remote` collector setting exports final scalar summaries to an
authenticated HTTPS structured-telemetry receiver, with a separate 64-slot
queue and at most five submissions per second. It requires mTLS or a private
token file, rejects redirects and carries no credentials in URLs. Configure a
receiver that authorizes this node; the full evidence remains local. Central
failure never controls the observer, manager, serving process or LKG artifact.

## Release gates

`make test-static-edge-observability` reconstructs dependencies from checksums,
runs race/failure/protocol tests, and checks Windows CLI compilation. The main
CI job builds Linux amd64/arm64 standalone archives with source provenance.
The synthetic tests include delayed upload, consumer pause, actual HTTP/2
credit exhaustion, H1 SSE flushing/cancellation, real Caddy JSON provisioning,
HTTP/3 preservation, authenticated multi-hop joins, disk failure/recovery and
queue saturation. Local overhead measurements are synthetic and do not prove
zero production impact. Production activation requires separate immutable
artifact checks, candidate health, natural draining and rollback evidence.

The subprocess test `TestFiniteGraceDoesNotProveSafeBinaryHandoff` exercises
the actual Caddy command exit while synthetic HTTP/2 and HTTP/3 streams are
still active. Its fast default uses a 200 ms grace; an exact production-budget
check can run with `FUGUE_HANDOFF_TEST_GRACE=120s` and a test timeout above 140s.
This test demonstrates a finite-grace termination boundary, not an assurance
that all requests will fit inside it. A green candidate health check or an
empty recent access-log window does not prove the old process is drained.
Do not replace its binary or stop it based on those signals alone.

The pinned Caddy 2.11.4 dependency also canceled a finite shutdown context when
`App.Stop` returned from a configuration reload, while the old HTTP/3 server was
still draining asynchronously. Isolated active streams ended immediately rather
than receiving their configured grace. `003-preserve-reload-grace.patch` keeps
that context alive until both Stop callbacks and server shutdown finish. It
preserves the configured deadline: changing the new configuration to unlimited
grace does not extend the previous generation's finite grace.

Real subprocess regressions cover H2/H3 completion inside a finite grace, active
H2/H3 survival under unlimited grace, and H3 termination at the previous finite
deadline. The original finite-grace process-exit regression remains in place.
This fixes future reload behavior in the patched binary; it does not alter an
already-running older binary or establish cross-process TCP/QUIC handoff.

Dependency patches are applied with enclosing Git-worktree discovery disabled.
The packaging tests verify a Git-format patch actually changes its copied
dependency and cannot escape into its parent. A skipped or failed patch must not
be advertised as an applied build input.
