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
reads. It records only observed HTTP milestones, header-wait time and body byte
counts, including a body failure from an older stream held across inventory
reads. Milestones are not a root-cause verdict: a false value means not observed,
and header time includes client admission, retry and server wait. No trace callback
arguments, addresses, URLs, headers, bodies or raw errors are retained. A new
failure without transport evidence cannot borrow a previous failure's receipt.

All these observations are metadata-only. They do not authorize an Edge, renew
an evidence lease, loosen a capacity threshold, change a collector cursor,
change public DNS transport, or recover traffic on their own.
