# AppDomain: TLS Waiting For Shared Certificate Bundle

Use this when DNS is verified and the route is active, but HTTPS is still not ready.

## Checks

1. Confirm AppDomain state:

```bash
fugue app domain diagnose my-app example.com
```

Expected DNS state:

```text
dns_status: ready
status: verified
```

2. Confirm DNS answers route to Fugue edge:

```bash
dig @ns1.dns.fugue.pro example.com A +short
dig example.com A @1.1.1.1 +short
```

3. Confirm edge TLS bundle state:

```bash
fugue api request GET '/v1/edge/domains/example.com/tls-bundle?token=<edge-token>' --json
```

Use the control-plane edge token only in a trusted admin shell. Do not paste it into issue comments or chat logs.

## Likely Causes

- Certificate issuance is still pending after DNS verification.
- Edge reported TLS ready but no shared certificate bundle was uploaded.
- A previous TLS error is preserved in `tls_last_message`.
- DNS only recently converged and ACME has not retried yet.

## Recovery

1. Run AppDomain repair:

```bash
fugue app domain repair my-app example.com --dns managed
```

2. If DNS is ready but TLS remains pending, inspect the TLS report and certificate bundle endpoints from a trusted admin shell.
3. If ACME failed, resolve the challenge reason, then rerun repair.
4. Verify:

```bash
curl -I https://example.com
```
