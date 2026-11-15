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
