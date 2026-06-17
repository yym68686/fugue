# Edge request body and TCP observability

Fugue edge request body buffering is intentionally split into request-level and
connection-level evidence.

## Request-level evidence

`fugue-edge` emits `edge_request_body_buffer_progress` every configured progress
interval while a buffered body is being read, and emits one
`edge_request_body_buffer_slow` event after the slow threshold is crossed.

These events include:

- `edge_request_id`, `trace_id`, `request_id`
- `bytes_read`, `content_length`, `elapsed_ms`, `last_read_age_ms`
- `body_read_block_ms`, `file_write_ms`, `first_body_byte_ms`,
  `last_body_byte_ms`, `max_read_gap_ms`
- `read_calls`, `avg_bps`, `min_window_bps`
- `client_remote_addr`, `client_country`, `client_asn`
- `edge_proxy_tcp_*` from Linux `TCP_INFO` for the Caddy-to-Go edge connection

`edge_proxy_tcp_*` is useful for ruling out local Caddy-to-Go backpressure. It is
not the public client TCP connection when Caddy or edge-front terminates the
public connection before proxying to Go.

The live debug endpoint is:

```sh
curl http://127.0.0.1:7832/edge/request-body-buffers
```

It returns active body buffer reads with the same request identifiers, byte
progress, read timing, and latest `edge_proxy_tcp_info`.

## Public TCP evidence

`fugue-edge-front` owns the public TCP connection in the blue/green data plane.
It exposes:

```sh
curl http://127.0.0.1:7831/edge/tcp-connections
curl 'http://127.0.0.1:7831/edge/tcp-capture-hints?remote=203.0.113.10:45678'
curl http://127.0.0.1:7831/metrics
```

Completed public TCP connections emit `edge_front_tcp_connection` JSON events.
Join them to slow body-read events by:

1. `edge_id` / pod node
2. `client_remote_addr` from the body-read event and `downstream_remote` from
   the edge-front event
3. overlapping timestamps

The edge-front event includes client-side Linux `TCP_INFO` fields such as RTT,
retransmits, unacked/lost packets, bytes received, delivery rate, and receive
window data. If `client_tcp_info_available=false`, use the accompanying error to
see why the kernel snapshot was unavailable.

## Node TCP metrics

`fugue-edge-front` also reads edge-node `/proc/net/snmp` and
`/proc/net/netstat` through the read-only host proc mount and exposes fixed-name
TCP counters as:

```text
fugue_edge_node_tcp_counter{edge_id,edge_group_id,node_host,protocol,name}
```

The Prometheus rules include alerts for:

- slow edge request body reads
- low request body throughput
- high edge-front client TCP retransmits
- high node TCP retransmit rate
- unavailable node TCP proc metrics

Request IDs and connection remote addresses stay in logs/events and debug
endpoints only; Prometheus labels use low-cardinality edge/app/route/client
segment labels.
