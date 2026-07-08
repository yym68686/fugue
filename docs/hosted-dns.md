# Fugue Hosted DNS

Fugue Hosted DNS lets a tenant delegate a public DNS zone to Fugue nameservers and manage DNS records from Fugue CLI, API, or Console. It is independent from App custom domains: a tenant may use Fugue only as authoritative DNS, or let an AppDomain manage the DNS record for a Fugue app.

## Nameservers

The public Fugue nameservers are:

```text
ns1.dns.fugue.pro
ns2.dns.fugue.pro
```

After creating a zone, set those nameservers at the registrar, then run preflight until the parent delegation and Fugue DNS nodes are healthy.

## CLI

Create and verify a hosted zone:

```bash
fugue dns zone add example.com
fugue dns zone preflight example.com
fugue dns zone show example.com
fugue dns zone ls
```

Manage records:

```bash
fugue dns record ls example.com
fugue dns record add example.com www A 203.0.113.10
fugue dns record add example.com www CNAME target.example.net
fugue dns record add example.com @ CNAME target.example.net --flatten
fugue dns record add example.com @ ALIAS target.example.net
fugue dns record add example.com '*' ALIAS target.example.net
fugue dns record add example.com @ MX "10 mail.example.com"
fugue dns record edit example.com www --ttl 300
fugue dns record delete example.com www A
```

Bind a Fugue app to a hosted zone:

```bash
fugue app domain add my-app example.com --dns managed
fugue app domain verify my-app example.com --dns managed
fugue app domain diagnose my-app example.com
```

`--dns external` preserves the old external DNS flow. `--dns manual` verifies a record the user created inside Fugue Hosted DNS without taking ownership of that DNSRecord.

## API

Hosted DNS uses OpenAPI-first endpoints:

```text
GET    /v1/dns/zones
POST   /v1/dns/zones
GET    /v1/dns/zones/{zone}
DELETE /v1/dns/zones/{zone}
GET    /v1/dns/zones/{zone}/preflight

GET    /v1/dns/zones/{zone}/records
POST   /v1/dns/zones/{zone}/records
PATCH  /v1/dns/zones/{zone}/records/{record_id}
DELETE /v1/dns/zones/{zone}/records/{record_id}
```

AppDomain create, verify, and repair accept:

```json
{
  "dns_mode": "managed",
  "dns_zone_id": "optional-zone-id",
  "dns_record_id": "optional-record-id",
  "overwrite": false
}
```

## CNAME Flattening

Standard apex CNAME records are not valid DNS. Fugue supports the apex use case through flattening:

```bash
fugue dns record add example.com @ CNAME target.example.net --flatten
fugue dns record add example.com @ ALIAS target.example.net
fugue dns record add example.com @ ANAME target.example.net
```

Flattened records are resolved outside the authoritative DNS request path. The DNS artifact publishes cached A/AAAA answers from:

- `flattened_a`
- `flattened_aaaa`

Resolver behavior:

- Follows CNAME chains up to depth 8.
- Detects loops.
- Deduplicates targets during a resolver run.
- Refuses private, loopback, link-local, benchmark, documentation, multicast, unspecified, and other reserved IP ranges for public zones.
- Supports `stale_if_error`, `fail_closed`, and `empty_noerror`.

## Record Ownership

Every DNSRecord has a `source`:

- `user`: manually managed by the tenant.
- `app_domain`: managed by a Fugue AppDomain.
- `system`: platform protected records.
- `acme`: certificate validation records.

AppDomain managed DNS does not silently overwrite user records. A conflict requires an explicit overwrite path and still cannot overwrite system protected records.

## Monitoring

The API `/metrics` endpoint exposes:

```text
fugue_hosted_dns_zones{status=...}
fugue_hosted_dns_records{status=...}
fugue_hosted_dns_records_by_source{source=...}
fugue_hosted_dns_record_publish_lag_seconds
fugue_app_domain_managed_dns_pending
```

Use these with the existing edge DNS artifact metrics:

```text
fugue_edge_dns_artifact_runs_total
fugue_edge_dns_artifact_errors_total
fugue_edge_dns_artifact_last_success_timestamp_seconds
```

## Troubleshooting

- NS delegated but apex A is NXDOMAIN: see `docs/runbooks/hosted-dns-ns-delegated-a-nxdomain.md`.
- DNSSEC SERVFAIL: see `docs/runbooks/dnssec-servfail.md`.
- TLS waiting for shared certificate bundle: see `docs/runbooks/tls-waiting-shared-certificate.md`.
