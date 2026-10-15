# Certificate Issuance Authority

Issuance policy is persisted separately from serving artifacts and code releases.
Each domain binds its verified tenant/app owner to HTTP-01, DNS-01 or external
issuance. A generation CAS prevents stale configuration writers from changing
the current issuer. The API's independent issuer workers use durable per-domain
leases; worker identity changes do not reset CA/domain retry barriers or account
keys. The five-minute operation deadline is shorter than the fifteen-minute
takeover lease. No new CA work starts during a control-plane state outage.

Before any CA request, a retry barrier of at least one hour is persisted.
CA Retry-After can extend it beyond the normal backoff ceiling. An issuer crash
retains this barrier. Account-registration failures also persist an account-wide
barrier. Ownership/generation is checked before acceptance, finalization and
certificate publication. Publication validates the public chain, hostname and
private key, then compares the previous certificate fingerprint and domain owner.
Failure does not delete, expire or invalidate existing certificates.

HTTP-01 proof is stored once and retrieved by all workers at a fixed API path.
The public endpoint exposes only the exact token's current, unexpired key
authorization. It exposes no account keys or certificate private material.
Workers forward no credentials and reject redirects and mismatched responses.
Every configured direct entry and every currently resolved public A/AAAA entry
must return the same proof before the issuer accepts the CA challenge.

DNS-01 requires an active delegated Fugue zone owned by the same tenant. Existing
leased DNS challenge intents carry the TXT proof through the DNS artifact path.
Every declared authority must answer with the proof authoritatively over both
UDP and TCP before acceptance. Cleanup removes only the specific challenge;
expiration bounds a failed cleanup.

Non-issuing workers use `shared-only`. An explicit HTTP listener handles
challenges and redirects normal requests to HTTPS. Existing valid certificates
remain available offline. Shared-only Caddy configurations reference immutable
certificate/key generations; a renewal never changes the files referenced by the
previous configuration. The consumer rejects older certificate material and
validates centrally issued chains. Certificate-history garbage collection is
deliberately separate from activation and must protect every loaded/LKG generation.

Rollout order: publish the API capability, verify issuer policy ownership and all
existing certificates, install challenge-capable workers, converge consumers to
shared-only with positive TLS observations, then enable the independent issuance
policy. External/imported domains retain their external renewal responsibility.
Use a local ACME fixture or staging for failure tests, preserving production CA
limits. Production can be enabled only after all entry observations succeed.
