# Independent entry failover

`fugue traffic-pool` manages a standalone DNS failover executor directly over
mTLS. Its signed policy, encrypted Cloudflare credential, operation journal and
systemd service live outside the Fugue API and database. A Fugue control-plane
release does not deploy or restart this executor or a standalone static edge.
Each executor process owns one pool; use separate instances for separate pools.

The optional `fugue static-edge registry` commands use the Fugue API to associate
edge metadata with an account and project. The registry stores a digest of the
CLI's direct manager observation. It does not independently perform that
handshake, store secrets, grant DNS authority, or drive the executor. Registry
`ready` is an observation at `last_proof_at`, not continuously measured health.
Revoking a registry record does not drain or stop its independent edge.

## Read-only operations

```sh
fugue traffic-pool context ls
fugue traffic-pool status --executor-context sample-entry
fugue traffic-pool evidence --executor-context sample-entry
fugue traffic-pool preflight --executor-context sample-entry
fugue traffic-pool operation --executor-context sample-entry
fugue static-edge status sample-west
```

`status` reports the loaded policy identity, expiration, executor version,
credential availability, current DNS target and most recent decision.
`preflight` queries Cloudflare and all configured probe vantages without writing
DNS. It refuses a planned operation when any configured target/vantage lacks
healthy evidence. Automatic decisions instead require a quorum of two vantages
when two or more are configured. Missing observations do not prove failure.

## Provisioning and enabling automatic decisions

Prepare an unsigned policy using the schema in
[`contracts/entry-failover.openapi.yaml`](../contracts/entry-failover.openapi.yaml). Declare tenant,
project, exact hostnames and Cloudflare record IDs, full DNS baseline, candidate
identities, business Host/SNI checks, independent vantages, thresholds and expiry.
Use `mode: shadow` initially. A static candidate uses an IP; a Fugue candidate
uses the service's stable shared domain. Multiple hostnames may use the same
Fugue domain while keeping their original HTTP Host and TLS SNI.

```sh
fugue traffic-pool policy validate --file sample-shadow.json
fugue traffic-pool bootstrap --name sample-entry --ssh control-vps \
  --public-ip 192.0.2.20 --file sample-shadow.json
fugue traffic-pool vantage enroll --help
fugue traffic-pool credential-import --executor-context sample-entry \
  --token-stdin < /secure/path/cloudflare-token
fugue traffic-pool preflight --executor-context sample-entry
```

Bootstrap installs a dedicated unprivileged executor in shadow mode, provisions
its management identity, and records a local CLI context. Enroll remote vantages
with a pinned SSH host key and a forced read-only probe command. Probe requests
are bound to the signed policy, target, vantage and a fresh nonce; vantages
receive no Cloudflare credential. Put the executor and vantages in failure
domains separate from the primary entry.

After observing shadow decisions and validating both business paths, prepare a
higher policy generation with `mode: automatic` and the intended priorities.
Sign with the executor's trusted offline Ed25519 key and apply over mTLS:

```sh
fugue traffic-pool policy sign --file sample-automatic.json \
  --signing-key /secure/path/signing.key --key-id bootstrap \
  --out sample-automatic.signed.json
fugue traffic-pool policy apply --executor-context sample-entry \
  --file sample-automatic.signed.json
fugue traffic-pool status --executor-context sample-entry
```

The bootstrap key ID is `bootstrap`; use the actually configured signer when
managing an existing installation. Keep private keys outside the repository.
Switch modes by signing a higher generation; mode changes do not require a code
release. An expired policy forbids new DNS writes while preserving the existing
records and allowing configuration recovery. Renew it before its reported expiry.

## Planned switching and recovery

```sh
fugue traffic-pool preflight --executor-context sample-entry
fugue traffic-pool switch --executor-context sample-entry --to managed
fugue traffic-pool operation --executor-context sample-entry
```

Switching uses the current policy digest, an exact DNS snapshot and a durable
single-writer operation. Only the declared records may be patched between the
configured A and CNAME targets; unrelated records remain outside the write
set. Attributes and record IDs are preserved. DNS drift or an uncertain write
outcome stops further mutation until the outcome is reconciled. After a timeout,
read `operation`, `status` and the provider's exact records before retrying.

Keep both paths serving during planned switching and retain the old path for
cached answers and in-flight connections. Cloudflare's batch transaction does
not make distributed DNS propagation atomic. A hard node fault can break an
existing TCP stream, and cached answers can still point to the failed node.
Thresholds and probe intervals add detection time; DNS cannot guarantee absolute
zero interruption. Default manual failback avoids switching a healthy fallback
merely because the higher-priority candidate recovered.

All candidates must retain working business routes and valid certificates even
while idle. Preflight checks present TLS validity, not the future success of ACME
renewal. Operators must maintain the appropriate standby renewal mechanism and
monitor expiry, executor health and its signed-policy expiry. One executor is
one DNS writer, not a redundant controller cluster. Multiple unmanaged writers
must not share the same records. Geographic, weighted and performance routing
through Fugue DNS are not implemented by this independent priority executor.

## Account registry

```sh
fugue static-edge registry register --project-id PROJECT_ID \
  --edge-context sample-west --signing-key-id bootstrap
fugue static-edge registry proof REGISTRATION_ID \
  --edge-context sample-west --signing-key-id bootstrap
fugue static-edge registry ls --project-id PROJECT_ID
```

Register/proof first contact the independently configured manager and verify its
edge ID, readiness and runtime/configuration agreement. The API enforces tenant,
project and `static_edge.read` / `static_edge.write` scopes. Direct management
commands and DNS decisions continue without the registry API. CLI, API, static
manager and executor versions are separate and must be observed separately.
