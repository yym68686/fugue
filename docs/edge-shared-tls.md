# Edge TLS certificate consumers

`FUGUE_EDGE_CADDY_TLS_MODE=shared-only` explicitly selects a certificate consumer.
It requires shared certificate synchronization (`FUGUE_EDGE_CADDY_SHARED_TLS_ENABLED`)
and Caddy storage. This mode does not change the default `public-on-demand`
issuance behavior of other workers. Deploy capable code before independently
activating the mode in component configuration.

The worker fetches certificates through its existing authenticated, assignment
bound interface. It checks hostname, validity and key pairing before loading
ACME material; imported certificates also require the existing public chain
validation. Only hostnames authorized by the selected route/TLS artifacts are
loaded. Static platform certificates retain their existing validation.

Shared certificates are loaded as unmanaged Caddy certificates. Automatic
certificate management and redirects are disabled, no on-demand issuer is
configured, and the local TLS ask endpoint denies issuance. This prevents a TLS
probe from starting account registration, ARI requests or renewal on an isolated
consumer. It does not grant network access or move issuance authority.

Periodic synchronization remains enabled. Certificate content participates in
the Caddy configuration signature, and a changed certificate forces a real
Caddy reload even when its file path is unchanged. Missing, invalid or expired
material cannot create positive TLS readiness; the authenticated artifact runtime
still requires successful per-host TLS probes before an applied/passed receipt.
An unavailable certificate producer never grants the consumer local issuance
permission. Producers must continue issuing and renewing certificates separately.

`FUGUE_TEST_CADDY_BINARY=/path/to/caddy go test ./internal/edge -run SharedOnly`
adds a real Caddy TLS check to the standard tests. It verifies serving a
near-expiry shared certificate, rejecting an unknown hostname, rotating the
certificate through the admin API and avoiding certificate acquisition/renewal.
