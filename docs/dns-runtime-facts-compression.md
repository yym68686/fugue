# Compress DNS runtime facts without reusing authority

Selected DNS backends return hundreds of current probe facts. The same release
bindings occur in many facts, so an approximately 800 KB response can spend
hundreds of milliseconds transferring through the Kubernetes Pod proxy.

The DNS HTTP listener now negotiates gzip for `/runtime-facts`. Clients without
gzip support retain the JSON representation. Explicit `gzip;q=0` overrides a
wildcard, and malformed or contradictory gzip preferences do not enable it.
Responses remain `application/json`, `private, no-store`, and vary on
`Accept-Encoding`. Go's existing HTTP transport decodes negotiated gzip before
the API applies its original size limit and strict JSON validation.

Compression runs after current trust and checkpoint verification. Serving
identity, listener state and request cancellation are checked again after
encoding. Original probe times, proof expiry, negative facts and release
bindings are preserved. Neither encoded responses nor authorization results
are cached. A pool reuses bounded gzip scratch and releases response-buffer
references before a writer is returned.

A synthetic 500-fact response compressed from 531,061 to about 45,025 bytes
using Go's fastest compression level, at approximately 1.1 ms on the development
machine. These figures demonstrate potential transfer savings; they do not
establish production tail-latency or CPU improvement.

Tests cover encoding negotiation, concurrent response isolation, real HTTP
proxying and automatic decompression, legacy clients, expired proofs, failed
listeners, revoked trust keys and absent serving state. Deployment uses an
independent inactive DNS slot first. Transport selection is a separate
declarative configuration change after live verification; updating the active
single-writer slot in place is not part of this release.
