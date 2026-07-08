# Hosted DNS: DNSSEC SERVFAIL

Use this when the domain is delegated to Fugue DNS but public recursive resolvers return SERVFAIL.

## Symptom

```bash
dig example.com A @1.1.1.1
```

returns `SERVFAIL`, while direct authoritative queries may work:

```bash
dig @ns1.dns.fugue.pro example.com A
```

## Cause

If the registrar or parent zone still publishes DS records for a previous DNS provider, validating resolvers expect DNSSEC signatures from Fugue. First-stage Hosted DNS does not publish DNSSEC signatures or DS material, so validation fails.

## Check

```bash
dig DS example.com +short
dig +dnssec example.com A @1.1.1.1
```

If DS records exist at the parent while Fugue Hosted DNS is unsigned, the domain can fail with SERVFAIL.

## Recovery

1. Remove DS records at the registrar.
2. Keep NS set to:

```text
ns1.dns.fugue.pro
ns2.dns.fugue.pro
```

3. Wait for parent DS TTL to expire.
4. Re-run:

```bash
fugue dns zone preflight example.com
dig example.com A @1.1.1.1
```

Do not re-enable DNSSEC until Fugue publishes signed zones and DS instructions for this zone.
