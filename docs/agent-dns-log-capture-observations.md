# Agent, DNS and log capture evidence

The API process's existing local observation socket exposes
`GET /v1/snapshots/operations`. Its `captures` member retains at most 24 completed
Agent grant / DNS runtime-fact reads, with at most 12 events per read. It performs
no network or database work when projected. It is not a public HTTP API.

Receipts distinguish actual Agent requests from administrator previews and DNS
reads. They retain fixed decision stages, completion times, candidate bounds,
authenticated node capacity samples and their original timestamps, numeric HTTP
statuses, and mismatched route-proof **field names**. DNS runtime-fact failures
retain only allowlisted rejection codes. They do not retain audiences, origins,
paths, response bodies, credentials or raw error text.

For a backend `503 state_replaced`, an updated DNS executor can attach fixed
metadata to the existing failure. The API retains it as `dns_replacement` only
if all three fields are present and allowlisted. `base` identifies the writer of
the final base observation if the base publication changed, `renewal` indicates
whether the observed renewal pointer changed, and `selection` identifies which
path was selected at each comparison point. This does not identify the first
writer if several changes occurred between the two reads. A legacy executor or
missing metadata leaves the original rejection classification intact. The
executor still rejects a replaced state by pointer identity; wire DNS is
unchanged.

For a single explicit backend `503 state_replaced` reason, the API attempts
one new snapshot under the same pinned transport and original deadlines. It
does not reuse a snapshot, reselect a backend, or retry other failures. The
successful read must still pass all transport, authority, proof and freshness
checks. `backend_snapshot_replacement_retry` preserves the first rejection;
`dns-snapshot-replacement-retry` counts completed reads that retried, and
`dns-snapshot-replacement-retry-verified` counts those that returned a verified
observation, including verified negative readiness. Neither is a wire DNS
success metric.

DNS assignment and trusted-heartbeat HTTP 503 responses may include
`X-Fugue-Consumer-Failure-Id`. The same opaque value identifies the exact failed
attempt in that API process's bounded `consumer_failures` ring. Retries have
distinct IDs; a successful request does not echo a failed attempt's ID. A DNS
sync failure logs a fixed report step and, when available, the client request
observation including the ID. No response or a missing/invalid ID means no
exact join; timestamps alone do not identify a matching attempt. Old clients
can ignore the header and old API versions omit it. Neither side uses the ID to
accept, reject, retry, or change an assignment or heartbeat.

Heartbeat admission observations also distinguish the bounded local wait for
the compiler-input writer barrier from failures returned by a PostgreSQL
statement, including the shared lifecycle lock query. Both can carry SQLSTATE
`55P03`; that code alone cannot distinguish them. A check name identifies the
failing call, not the lock owner. These diagnostics add no database reads.

Rejected Agent route proofs also retain a same-read `publication` pair. Each
side has separate SHA-256 hashes of the release-set, route-artifact and release
IDs, an allowlisted channel, and its numeric fence. Hashes are SHA-256 of the
original identity bytes (with the `sha256:` prefix), not hashes of JSON. This
allows read-only inventory correlation without raw identities or another
network probe. The observed side is omitted if the probe returned an error or
no binding. A version mismatch is not itself proof of a bug: publication
changes and consumer propagation still require independent evidence. This does
not accept a stale proof or alter any equality, freshness or authorization
check. The existing 24-receipt / 12-event bounds remain unchanged.

`snapshot_validation_rejected` records the first failed local DNS validation
check: snapshot identity/digests/assignment, observation/checkpoint bounds,
duplicate or unknown probes, and physical proof identity/deadline/publication.
These fixed reason names describe the rejected check, not why the source became
inconsistent. Negative or expired evidence that remains structurally valid does
not invent a validation rejection or become ready.
`source_rejected` separates consumer/expected-set reads, identity ambiguity,
backend disambiguation and current assignment failures. Stale heartbeats get
their own fixed event; their rejection and freshness rules are unchanged.
When no current source can be selected, `source_backend_rejected` retains up
to four skipped 403/409 results from that source observation, including fixed
reasons for Pod identity or policy, Service selection, EndpointSlice matching,
and metadata changes on recheck. `source_backend_rejections_truncated` marks
additional skipped results; the receipt-wide loss counter still applies.
Successful selection does not emit ordinary standby refusals. No additional
Kubernetes reads are made, and no candidate is accepted based on these events.
`source_assignment_rejected` further separates assignment resolution, selected
publication, the first mismatched heartbeat binding field (expected set, parent,
fence or generation sequence), heartbeat time bounds, artifact/view validation
and route projection. These are fixed local check names, not raw errors or
identities. The original source failure, HTTP status and validation order remain
unchanged; a check name alone does not establish the cause of publication lag.
Snapshot assignment rejection also records only the differing assignment field
names, including expected-set revision, publication fence and deadlines. It does
not retain either assignment or treat a same-content old assignment as current.

Each DNS runtime-fact receipt also carries bounded `dns_read` wall-clock
partitions for initial source resolution, the selected-backend read, independent
authority recheck and final validation. Source-resolution work is aggregated by
fixed stage (inventory, assignment resolution, selected release, artifact decode,
publication verification and bindings). Repeated consumers increment counts;
they cannot create additional diagnostic keys. Phase totals include their nested
stages and must not be added to them. Offsets are relative to the same request;
signed remaining budgets distinguish work begun or completed after its existing
deadline. `has_deadline=false` denotes no deadline, with zero budget fields.
These timings are wall-clock observations, not CPU, SQL or network root-cause
verdicts. They share the existing completed/failure receipt retention, consume
none of the 12 rejection-event slots, and add no reads, retries or policy changes.

The management read uses one 10-second context, bounded by the caller's
deadline, for both source observations and the backend read. PostgreSQL
inventory, release and artifact reads use that context as their parent, so they
cannot start a new independent budget. The final validation does not return
success after expiry. A budget failure remains HTTP 503 and does not imply
which earlier operation consumed the budget or a failure of wire DNS.

`started`, `completed`, `in_flight`, `evicted` and `events_dropped` describe the
coverage limits. The ring is process-local and non-durable. Polling it cannot
reconstruct already-evicted events or failures before that process started. A
successful grant capture is not proof that signing or the eventual response
succeeded; compare the existing `agent-response-*` counters.

The telemetry process's existing `GET /v1/snapshots/log-sources` observation adds
typed error class / HTTP status, source-local cumulative gap count, and the most
recent gap's reason and lower / upper bounds. The bounds mark the interval that
could not be proven complete, not proof that every record in it was lost.
Recovery and node backoff preserve this last error / gap evidence. The summary
groups source-lifetime error totals by each source's **latest** error, not by the
historical class of each individual failure. Detail truncation and source
eviction remain explicit.

One missing rotation boundary is counted once while its cursor remains unread,
including queue, cycle-budget and transport retries. If retention widens that
same unproven interval, its upper bound is updated without adding another gap
event. Admission at or after the cursor changes the inclusive boundary; a
subsequent missing boundary is a new event. This does not advance an unread
cursor, erase historical loss, or claim that retained records were recovered.

`last_error_read` is one fixed-size transport receipt for the failing log HTTP
stream, preserved through recovery/backoff. Its phase distinguishes the Pod log,
both retained inventories, opening the pinned current file, and retained-file
reads. It records only observed HTTP milestones, stream-open time and body byte
counts, including a body failure from an older stream held across inventory
reads. Milestones are not a root-cause verdict: a false value means not observed,
and open time includes client admission, retry, server wait and error-response
processing. `stream_opened` means that client-go returned a successful body,
not that an HTTP error response had no headers. No trace callback
arguments, addresses, URLs, headers, bodies or raw errors are retained. A new
failure without transport evidence cannot borrow a previous failure's receipt.

All these observations are metadata-only. They do not authorize an Edge, renew
an evidence lease, loosen a capacity threshold, change a collector cursor,
change public DNS transport, or recover traffic on their own.

DNS management preflight messages distinguish an unavailable selected-backend
observation, a known but not-ready selected backend, and legacy cache errors.
The stable `cache_errors_zero` check name does not imply that an unknown
observation proves cache corruption: enrolled consumers still require current
serving/cache readiness, independently of historical inventory error counters.
Only diagnostic wording changes; all existing preflight gates remain intact.

The Edge worker's owner-only observation socket exposes
`GET /v1/snapshots/traffic-serving`. It keeps the latest completed serving check
and a separate 24-entry failure ring, with completed/failed/evicted counts. Each
entry records the actual check stage, fixed error category, independently hashed
applied/expected publication identities, probe reuse, and route/TLS failure
counts. At most eight hashed route or TLS identities and fixed reasons are kept
per check, with an explicit omitted count. Unknown errors remain `unclassified`;
raw errors, hostnames, paths, proof payloads and credentials are never retained.

Negative reports retain their original verification cause separately from the
failure-report stage, trusted heartbeat sequence and attempted/accepted counts.
An accepted negative report means the control plane acknowledged that failure;
it does not make the route ready. Success does not erase prior failures. These
process-local receipts cannot reconstruct failures predating deployment or
surviving only in rotated-away logs. Reading the provider performs no additional
probe, network or filesystem work, and none of the checks, reports, retries,
signed probe bounds, executor configuration or public DNS selection change.

When a platform request fails, the original check and its subsequent negative
report each retain their own `request_failure`, if present. It distinguishes a
non-200 HTTP response from a failed request or timed-out response decode, with
the status, fixed operation/class, start time, elapsed milliseconds and observed
DNS/connect/TLS/connection/write/first-byte callbacks. Completion callbacks only
mean the callback ran, not that the phase succeeded. Missing callbacks are not
proof of zero latency; connection attempts may overlap. Late callbacks cannot
rewrite a completed receipt. No URL, query, address, body, token or raw error is
retained. Existing outage and conflict classifications, request budgets, retries,
response validation and reporting remain unchanged.

The telemetry process's owner-only `GET /v1/snapshots/exports` also separates
export attempts from their HTTP work. Exporter counters include encoding and all
of that attempt's requests; `http` has eight fixed purposes (metrics, logs, five
allowlisted analytics tables, and other). Each purpose holds concurrent counters
and only its latest completed and latest failed HTTP receipt. A success does not
erase the failed receipt. Queries are not export requests and are excluded.

HTTP receipts record status, start/completion times, total `http.Client.Do`
duration, bounded connection/reuse counts and the first observed DNS, connect,
TLS, connection-ready, request-write and first-response-byte callbacks. These
are offsets from that Do call's start, not exclusive phase durations; a missing
callback does not mean zero latency. Redirects, parallel connection attempts and
transport retries may share one Do call. Callbacks arriving after Do returns do
not rewrite its completed receipt. Inflight counts reveal unfinished calls but
do not claim to describe their current phase.

This does not measure encoding, response-body reads, queue wait or backoff, and
does not prove that a successful HTTP status means all records were persisted.
It preserves existing trace hooks, response bodies, timeouts and retries. No
destination, URL, query, arbitrary table name, header, credential, event body or
raw error is retained. Read repeated same-process snapshots and the global gap
counters before attributing a throughput bottleneck or declaring recovery.

Successful metrics, logs and analytics exports best-effort consume at most
4 KiB of the response before closing it, within the existing request/client
timeout. This permits HTTP/1 connection reuse for small responses, including
empty chunked responses whose terminating framing still needs to be read.
Large or interrupted bodies may still prevent reuse. Draining does not change
the existing status-based acknowledgement or cause another write/retry when a
successful response body cannot be read. Its time is included in exporter
attempt duration, not the HTTP Do receipt. Non-2xx error handling is unchanged.
