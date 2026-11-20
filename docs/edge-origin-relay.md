# Single-hop Edge origin relay (v1)

The worker is **off by default**. DNS, ASN ranking and the existing safe-method
peer fallback are unchanged. When enabled, the ingress worker makes a choice
before sending a fully buffered streaming JSON POST: direct to its selected
origin, or through one explicitly authorized egress worker. The egress does
not reenter public routing, rebalance a release or select another peer.

## Policy and private transport

Use a dedicated CA and a separate policy signing key. Provision a certificate
with client and server authentication EKUs and one exact, slot-specific DNS
SAN per worker. Use a dedicated private IP and port on each worker, reachable
only from authorized peers; do not use a public hostname, wildcard bind, NAT
or load-balanced Service in place of a pinned worker. Mount those files
through the normal declarative release mechanism. The worker never needs the
policy signing private key. The following are process-level file paths:

| Variable prefix `FUGUE_EDGE_ORIGIN_RELAY_` | File |
| --- | --- |
| `POLICY_FILE` | Signed JSON envelope (optional; unset means no listener or probes) |
| `POLICY_VERIFY_KEY_FILE` | Hex-encoded raw 32-byte Ed25519 public key |
| `TLS_CERT_FILE` | PEM leaf certificate and chain |
| `TLS_KEY_FILE` | PEM TLS private key |
| `CA_FILE` | PEM trust bundle for the dedicated relay CA |

Compile the two route proofs from their actual applied route bindings
(`internal/routeproof.Digest`, or a verified active route's
`X-Fugue-Route-Proof`). Do not substitute the narrower app-traffic proof.
The same signed payload must be present on both sides. Schema example (replace
the placeholder times and digests at issuance):

```json
{
  "schema": "fugue.edge.origin-relay/v1",
  "generation": 1,
  "not_before": "<UTC RFC3339 timestamp>",
  "expires": "<UTC RFC3339 timestamp; at most 15 minutes after not_before>",
  "mode": "shadow",
  "relay_percent": 0,
  "max_concurrent": 2,
  "endpoints": [
    {"edge":"edge-a","slot":"a","address":"10.0.0.1:8443","tls_name":"slot-a.edge-a.example.test"},
    {"edge":"edge-b","slot":"b","address":"10.0.0.2:8443","tls_name":"slot-b.edge-b.example.test"}
  ],
  "bindings": [
    {
      "id":"pilot",
      "source":"edge-a",
      "peer":"edge-b",
      "host":"app.example.test",
      "path":"/v1/stream",
      "tenant":"tenant-example",
      "app":"app-example",
      "release":"release-example",
      "origin":"http://service.namespace.svc.cluster.local:8000",
      "source_digest":"<sha256: followed by 64 lowercase hex characters>",
      "peer_digest":"<sha256: followed by 64 lowercase hex characters>",
      "max_body_bytes":524288
    }
  ]
}
```

The envelope fields `payload` and `signature` are base64 of the exact payload
bytes and their Ed25519 signature, respectively. The pilot's offline issuer
checks both exact `EdgeRouteBinding` records and their observed digests before
producing a lease. Capture the two records and successful live route-proof
responses for the exact host and path from the **current serving** workers.
Do not synthesize the expected proof from the cached file being checked.
Use time-bounded material and keep the PKCS#8 Ed25519 signing key owner-only:

```sh
go run ./cmd/fugue-edge-relay-policy -policy policy.json -signing-key policy-signer.pem \
  -source-route source-route.json -peer-route peer-route.json
```

The output is a signed envelope, not an applied change. Active policies
require an explicit `-allow-active` after review. An `off` lease can be issued
without live route files so withdrawal does not depend on origin health.

Publish atomically and keep the trusted mount current across restarts. Workers
reject a regressed or conflicting generation within a running process; **that
fence is not durable across restart**. A stale unexpired signed file must never
be restored by deployment tooling. Expiry, invalid content, changed slot or
listener identity stop new relays. Certificates and CA are read at startup;
rotate through the normal drain/restart procedure. Automatic policy issuance,
renewal and production secret/endpoint provisioning are not part of the
initial worker release. Initial material must be mounted before startup;
if the worker started without a valid private listener configuration, it stays
direct-only until restarted with valid material.

## What changes traffic, and what does not

- Each worker runs bounded TCP checks of its authorized current Service
  targets and authenticated, directional peer status checks. An authenticated
  peer advertises its own origin state, oldest contributing evidence age and
  capacity. Neither kind of private check becomes a public ranking sample.
- Two recent failures are needed to mark direct origin degraded. Two recent
  successful rounds are needed on both legs to use a peer; expiration,
  missing capacity or a failed route proof keeps the request on the old path.
  Evidence is valid for at most 45 s; peer observations expire after 15 s.
  Recovery waits at least 60 s and three new successful rounds. Business write
  timings and model latency are diagnostic only and cannot trigger switching.
- In `shadow`, business traffic remains direct. In `active` a random
  1–25% of **eligible degraded** requests can relay, subject to the configured
  concurrency and byte cap; a fixed client cohort is not implemented. Limits
  are an upper bound, not a reservation of application capacity.
- PREPARE carries no user headers or body. It expires within 5 s (or sooner
  with policy/request expiry). A rejected or missing PREPARE can still use the
  existing direct route. After FORWARD starts there is no automatic resend,
  direct fallback, redirect or third hop, even if the result is unknown.
  This does not deduplicate a separate request retried by a client.

Check the ingress `origin_relay` request fact and join its `dispatch_id` to the
egress `origin_relay_egress` event. Inspect reason, fresh sample counts and ages,
source/peer, PREPARE, A→B and B→origin timings separately. Neither host's
cumulative local write time proves when the application finished reading;
an egress HTTP 200 does not prove the final client consumed the response.

## Rollout

Roll the default-off worker binary through its normal component workflow.
Check the exact image and both workers' current serving authority before
adding any relay settings. Stage the egress first, then observe in shadow.
Against an isolated, countable origin, verify both directions on the intended
private network, expiry, wrong certificates, capacity, client cancellation,
POST execution count and streaming flush. Do not replay production payloads.
Only then use an explicitly scoped active policy. To stop submitting new
relay traffic, publish a higher-generation signed `off` policy, first on
ingress and then on egress; allow already dispatched streams to complete
before removing the listener. Never change DNS as a relay rollback.
