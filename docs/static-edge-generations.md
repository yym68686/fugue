# Independent generation staging

`fugue static-edge generation stage --file plan.json --package official.tar.gz`
validates a pinned CI archive and explicit plan without contacting a node.
Adding `--execute` starts a **new** Linux/systemd generation beside predecessors.
It never stops/reloads another Caddy, mutates DNS, clears connections, or retires
an old generation. A stage result alone is not a traffic-switch readiness proof.

The plan schema is `fugue.static-edge.generation/v1`. Required fields are `id`
(lowercase name, at most 32 characters), `edge_id`, `role` (`edge` or `origin`),
`ssh_host`, `management_ip`, `management_listen`, `source_commit`,
`package_sha256`, native `caddy_config`, `checks`, `listeners`, `assets`, and
`preserve`. `assets` maps relative destination paths to absolute local files;
private values are never command-line arguments. `preserve` lists predecessor
`unit`, exact `pid`, configuration `config` path and `sha256`. Empty preserve is
appropriate only for an independently verified unused address/port deployment.

The destination is `/opt/fugue-static-generations/<id>`. `${GENERATION_ROOT}` in
native Caddy JSON is replaced by that path. All business listener addresses must
match `listeners` exactly; implicit HTTP redirects must be disabled and separately
declared if required. New generations require an unlimited HTTP shutdown grace.
Service units also have no forced-stop timer. They remain independently supervised
when CLI, manager, collector, or Fugue API is unavailable. Two versions need enough
capacity to coexist; fixed new-service resource caps do not apply to predecessors.

Stage uses create-only files, reviewed archive SHA256/source provenance, separate
manager PKI, pinned Caddy binary, and before/after predecessor checks. It refuses
occupied ports and differing receipts. It preserves the previous configuration
and process even if the new service fails. The operator must inspect a failed
generation; choosing a different id cannot authorize taking over its listeners.

After staging, use the generated context and signed bundle path to `adopt` it
with revision zero, then verify original Host/SNI, certificates, health, routing,
SSH/mTLS management, observations and long-stream continuity. Coordinate DNS
through the pool's existing single writer. Switch only the explicit authorized
records after both paths pass. Retain old paths for caches and existing flows;
elapsed TTL alone does not establish safe retirement. This staging mechanism
does not claim arbitrary TCP/QUIC state migration or automatic retirement.
