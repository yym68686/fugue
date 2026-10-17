# Edge origin transport diagnostics

The text request log and durable request fact retain `origin_transport_error_kind`
when RoundTrip or a subsequent origin response-body read fails. Values are `eof`,
`unexpected_eof`, `connection_reset`, `broken_pipe`, `connection_refused`,
`connection_closed`, `timeout`, `canceled`, and `other`. Classification follows
typed, wrapped errors; it does not log the final error text or origin credentials.
An empty text value (`-`) or absent request-fact field means no recorded origin
transport failure, not proof that the entire response body was delivered successfully.

Durable facts also distinguish `origin_response_body_error_kind` from
`response_write_error_kind`. The former identifies a failure after origin headers;
the latter records a downstream response-writer error (including `short_write`).
Both use bounded classifications, without raw error text. A confirmed request-context
cancellation is recorded as `error_type=client_closed` and is not charged to the
origin. A downstream write error without that evidence remains
`error_type=response_write_error` / `platform_error_class=evidence_unknown`, so it
cannot silently pass release checks.

If response copying aborts, the original net/http abort still propagates and no
request is replayed. Facts preserve the already-committed HTTP status and count
bytes accepted by the response writer, including partial writes; those bytes do
not prove delivery to the final client. A 2xx status alone is therefore not a
successful response: inspect `error_type`, `platform_error_class`, and the body/write
error fields. Incomplete copies cannot populate the HTTP cache.

`origin_conn_was_idle` and `origin_conn_idle_ms` come from Go's GotConn callback.
The duration measures time spent idle before assignment to this request, not the
connection's age. `origin_conn_reused` alone does not prove that the connection
previously carried a completed HTTP request. If the transport tries another
connection, these fields describe the last GotConn callback.

Correlate these fields with request/trace IDs, origin local/remote addresses,
write completion, first response byte, and contemporaneous TCP evidence. EOF
after a completed write does not establish whether the origin processed the
request. These diagnostics do not change routing, retries, error responses,
artifact validation, or the serving LKG.
