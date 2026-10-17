# Reuse successful Edge heartbeat connections

`HeartbeatOnce` closed successful response bodies without consuming them. Go's
HTTP/1.1 transport cannot reliably reuse a connection in that state. A local
HTTP server reproduces two new connections for two successful heartbeat POSTs;
after this change the same requests share one connection, with no POST replay.

The Edge now discards at most 64 KiB plus one byte of a successful response.
Known oversized responses are closed immediately. The optional read is limited
to one second using a request-owned context cancellation; it does not change
the shared client or another request. Bodies are never retained or parsed.
Normal known-length and chunked responses reach EOF and can reuse the socket.

A 2xx response remains an acknowledged heartbeat even if its optional body is
oversized, stalls or fails while reading. This preserves telemetry-baseline
commit behavior and avoids retrying an already accepted POST. Error responses,
the existing narrowly scoped old-API compatibility retry, node authentication,
serving authority, signed bundles and configuration recovery are unchanged.

Tests use a real HTTP server to verify connection reuse and stalled-body
cancellation, and a bounded synthetic body to verify size/read-error handling.
Existing heartbeat inventory, performance-baseline, serving authority and
compatibility tests remain part of validation.

Production observation found two Edge-initiated TCP resets. A successful API
heartbeat ended near one reset, but the packet headers do not establish exact
request ownership and the older Edge logs had rotated. The independently
reproduced unread-body defect is fixed without claiming it explains every reset
or changing network timeout policy.
