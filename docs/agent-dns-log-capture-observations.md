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
`source_assignment_rejected` further separates assignment resolution, selected
publication, the first mismatched heartbeat binding field (expected set, parent,
fence or generation sequence), heartbeat time bounds, artifact/view validation
and route projection. These are fixed local check names, not raw errors or
identities. The original source failure, HTTP status and validation order remain
unchanged; a check name alone does not establish the cause of publication lag.
Snapshot assignment rejection also records only the differing assignment field
names, including expected-set revision, publication fence and deadlines. It does
not retain either assignment or treat a same-content old assignment as current.

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
