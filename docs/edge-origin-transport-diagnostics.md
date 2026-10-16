# Edge origin transport diagnostics

The text request log and durable request fact retain `origin_transport_error_kind`
when RoundTrip fails before returning a response. Values are `eof`,
`unexpected_eof`, `connection_reset`, `broken_pipe`, `connection_refused`,
`connection_closed`, `timeout`, `canceled`, and `other`. Classification follows
typed, wrapped errors; it does not log the final error text or origin credentials.
An empty text value (`-`) or absent request-fact field means no recorded RoundTrip
failure, not proof that the entire response body was delivered successfully.

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
