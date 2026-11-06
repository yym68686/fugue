# Managed application mail provisioning

Mailflare's authenticated binding reconciliation returns `dnsRecords` and a
public `setup` observation. The control plane validates the exact app, tenant,
project, hostname, lifecycle and generation before accepting relative MX, TXT or
CNAME requirements. It never accepts address records or a root CNAME. Public
binding responses expose these persisted requirements as `provider_dns_records`.

Provider requirements are separate from lifecycle intent: refreshing DNS must
not advance the user's generation. Store updates compare the lifecycle and the
starting DNS snapshot, so a delayed response cannot overwrite newer requirements
or resurrect a suspended binding. Identical replays do not rewrite the row.

The managed DNS producer reads requirements from its frozen business snapshot.
It does not call the provider during compilation. Explicit operator rules retain
authority: identical requirements deduplicate, while conflicting MX, SPF or DKIM
requirements fail closed. Web address records and unrelated applications retain
their existing ownership and lifecycle.

Mailflare performs bounded provider registration, DNS observation and verification
with durable retry/backoff. A user needs only the existing app.read/app.write
grant and the Mailflare integration key. Provider domain-management credentials,
quotas and ingress availability remain platform configuration. Public `setup`
codes distinguish pending DNS, pending provider verification, missing/restricted
platform credentials, provider limits and temporary unavailability. GET remains
read-only and no status response proves final recipient delivery.

Verification includes PostgreSQL/file-store generation and response-order fencing,
frozen DNS projection, namespace restrictions, manual-policy conflict handling,
the generated OpenAPI drift gate and the synchronized console contract.

The shared compiler-input writer permits up to two minutes for queueing,
encoding and complete readback, while honoring shorter caller deadlines and
cancellation. A fixed thirty-second transaction could repeatedly reject a valid
large global snapshot and prevent newly provisioned mail DNS from publishing.
The writer remains serialized and verification and publication fences are unchanged.
