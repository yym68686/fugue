# Hosted DNS: NS Delegated But A Is NXDOMAIN

Use this when the registrar parent NS points at Fugue nameservers, but an A query returns NXDOMAIN or no useful answer.

## Checks

1. Confirm parent delegation:

```bash
dig NS example.com +short
```

Expected:

```text
ns1.dns.fugue.pro.
ns2.dns.fugue.pro.
```

2. Confirm Fugue zone state:

```bash
fugue dns zone show example.com
fugue dns zone preflight example.com
```

The zone should be `active` and delegation should be `ready`.

3. Confirm records exist:

```bash
fugue dns record ls example.com
```

For a Fugue app apex, expected record:

```text
@ FUGUE_APP <app-id-or-name>
```

For an ALIAS or CNAME flatten record, confirm `flattened_a` or `flattened_aaaa` exists and `flatten_status` is `resolved` or `stale`.

4. Query Fugue authoritative nameservers directly:

```bash
dig @ns1.dns.fugue.pro example.com A +short
dig @ns2.dns.fugue.pro example.com A +short
```

If authoritative answers are correct but recursive resolvers are stale, wait for delegation and negative cache TTLs to expire.

5. If authoritative answers are missing, inspect the edge DNS bundle:

```bash
fugue api request GET '/v1/edge/dns?zone=example.com' --json
```

Check that the hosted record is present and the DNS node generation is current.

## Likely Causes

- HostedZone exists but is still `pending_delegation`.
- No DNSRecord exists for the queried name and type.
- AppDomain is `external` instead of `managed`.
- `FUGUE_APP` value points to a missing app.
- Flattening failed and fallback policy is `fail_closed` or `empty_noerror`.
- DNS node is serving an older artifact generation.

## Recovery

Run:

```bash
fugue dns zone preflight example.com
fugue app domain repair my-app example.com --dns managed
fugue dns record ls example.com
```

If the record is missing for a managed app domain, recreate it with:

```bash
fugue app domain add my-app example.com --dns managed
```

If the record is user-owned and conflicts with managed DNS, decide explicitly whether to edit the user record or rerun the AppDomain command with an overwrite path.
