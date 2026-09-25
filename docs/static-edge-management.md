# Independent static edge management

`fugue static-edge` connects directly to a standalone manager over mTLS or the
local OpenSSH client. It does not use the Fugue API, API keys, node-updater,
edge inventory, dynamic route authority, or Kubernetes to manage configurations.
Management failure does not stop the separately supervised Caddy process.

This release ships the CLI and `fugue-static-edge-manager` (Linux amd64/arm64)
artifacts. Installing the CLI does not alter any server. Explicit `bootstrap
--execute` provisions a dedicated Linux amd64 candidate, its management PKI and
initial signed configuration. Explicit `cutover run --execute` updates only the
allowlisted Cloudflare A records. Neither command uses the Fugue API.

## Ownership and boundaries

Each manager owns one dedicated Caddy instance and exact startup JSON file.
`role=edge` and `role=origin` use the same implementation, separate state,
certificates and local policy. A dedicated, permission-restricted Caddy Unix
admin socket and a pinned Caddy binary are required. The manager never restarts
Caddy, sends a kill signal, duplicates customer requests, or forces connections
to close after a drain timer.

The business mTLS CA and the management mTLS CA must be separate. Management
TLS uses client certificate verification **and** a local SHA256 fingerprint to
`read`, `operator` or `admin` mapping. The current trust and grants are checked
on every request, including existing TLS connections after rotation. Bundle
signatures are separately verified using configured Ed25519 signing keys.
Operators can stage/apply signed configuration. Adoption, management certificate
rotation and interrupted-operation recovery require admin grants. A bundle
signer is a configuration administrator: review the full native Caddy JSON,
including upstreams and file references, before signing it.

Configuration and code remain separate: the bundle has no executable command
or code version dependency; the manager pins an already installed Caddy binary.
Manager and Caddy binaries have independent release/install lifecycles. This
release does not implement arbitrary remote binary upgrades. Keep the previously
verified binary and configuration available when separately upgrading a process.

The manager protocol is `contracts/static-edge-management.openapi.yaml`, not the
Fugue control-plane OpenAPI. No Fugue API write endpoints are added. Central
observation/receipt export is optional future integration; it is **not** a new
runtime dependency or an already deployed telemetry pipeline.

## Local contexts

```sh
fugue static-edge context add production-entry \
  --edge-id entry-a \
  --manager https://192.0.2.10:9443 \
  --server-name entry-manager.example.test \
  --ca ~/.config/fugue/static-edge/management-ca.pem \
  --client-cert ~/.config/fugue/static-edge/operator.pem \
  --client-key ~/.config/fugue/static-edge/operator.key \
  --ssh-host entry-recovery
fugue static-edge context use production-entry
fugue static-edge status
fugue static-edge health --deep
fugue static-edge evidence --json
```

Contexts save connection metadata and local credential **paths**, not private
key bytes or API tokens. On macOS the default is the user application support
folder; on Linux it is the user config directory. Override with
`FUGUE_STATIC_EDGE_CONTEXT_FILE`. `context show` reports the saved paths.
`ls` lists local contexts without contacting managers.

SSH reuses `~/.ssh/config` aliases, IdentityFile, User, Port, ProxyJump and
known_hosts through OpenSSH. Host verification is required; agent forwarding,
TTYs and port forwarding are disabled. The remote command is one validated
absolute executable path followed by `ssh-rpc`; the structured request is sent
on stdin. No shell fragment or private key is placed in argv.

```sh
fugue static-edge status production-entry --transport ssh
```

There is no automatic fallback after a failed mutation: the remote operation
may already have committed. A restricted SSH account/forced command can invoke
`fugue-static-edge-manager ssh-rpc`, which connects to the manager's local socket.
Socket access is a local administrator trust boundary; its Unix permissions
must restrict callers. A dedicated SSH account can be granted a fixed sudo
wrapper that accesses **only** this socket. Do not grant arbitrary sudo/shell
access for routine manager operations. An already authorized root SSH alias
works without copying SSH private keys into Fugue.

## Automated bootstrap and DNS cutover

Create a Cloudflare API token with **Zone / DNS / Edit**, restricted to the exact
zone. A zone ID avoids needing Zone Read. The token can change all DNS records in
that zone; the CLI additionally restricts writes to the explicit hostname list.
Store it only on the operator machine, not on the edge or in a bundle:

```sh
fugue static-edge cloudflare auth import --zone example.com --token-stdin
fugue static-edge cloudflare auth show --zone example.com
```

The import reads a single token from stdin and saves it with mode `0600`. It does
not echo the token or put it in process arguments. The default state directory is
the OS user configuration directory under `fugue/static-edge`; override with
`FUGUE_STATIC_EDGE_STATE_DIR`. `FUGUE_STATIC_EDGE_CLOUDFLARE_TOKEN_FILE` selects an
explicit credential file, and `FUGUE_STATIC_EDGE_CLOUDFLARE_TOKEN` is an ephemeral
environment override. No Fugue API token is reused.

Bootstrap supports a clean Linux amd64/systemd VPS reachable through an existing,
host-key-verified root SSH alias. It checks the claimed local public IPv4, clock,
ports and existing ownership before writes. The source Caddy binary must match
the reviewed digest. The manager comes from the same tagged release as the CLI,
with its published archive checksum verified. No source service is changed.

```sh
fugue static-edge bootstrap --ssh-host entry-next --source-ssh entry-current \
  --edge-id entry-next --public-ip 192.0.2.20 \
  --hostname example.com --hostname api.example.com \
  --origin-ip 192.0.2.40 --origin-port 19443 \
  --origin-server-name origin.example.internal \
  --caddy-sha256 REVIEWED_64_CHARACTER_SHA256 \
  --business-ca /secure/business-ca.pem \
  --business-ca-key /secure/business-ca.key --execute
```

The business private key is generated on the candidate. Only its CSR leaves the
node; the existing business CA signs it locally. Neither the CA private key nor
the old edge's business private key is copied to the new node. Management CA,
client credentials and signing key are separate and saved privately on the
operator machine. The server receives only its management server key, CA public
certificate, client fingerprint grant and bundle verification public key.

For the supported Caddy storage layout, bootstrap copies only the named hosts'
valid public certificate/key/metadata files as a migration seed. Caddy on the new
node keeps normal automatic HTTPS enabled and maintains its own writable storage
and renewal after DNS points to it. This is an explicit certificate-key transfer,
not DNS-01 issuance. It does not copy the source ACME account or DNS API token.
Public certificates must have at least seven days remaining; operator-managed
management/business certificates are valid for at most one year and require
rotation before expiry. The offline management CA is retained locally.

Bootstrap writes a durable receipt and immutable local assets before touching the
candidate. Repeating the same command resumes the exact intent. Once the data
plane is running, bootstrap verifies rather than overwrites or restarts it.
An inactive previously started candidate or configuration drift stops recovery
for inspection. Bootstrap only manages its dedicated instance, never a shared
public Caddy. A read-only default invocation reports intent; it is not a remote
readiness assertion.

After bootstrap, one command validates both public endpoints and the candidate
manager, snapshots exact DNS records, patches only their `content`, reads every
write back and observes overlap health:

```sh
fugue static-edge cutover run --zone example.com --zone-id ZONE_ID \
  --hostname example.com --hostname api.example.com \
  --from-ip 192.0.2.10 --to-ip 192.0.2.20 --candidate entry-next \
  --check example.com/=307 --check api.example.com/v1/health=200 \
  --observe 60s --execute
```

Only non-billable GET endpoints should be passed as checks. The default primary
probe is `/_static-edge/health` and must return 200 with normal public TLS
verification and exact SNI. There is no production skip-probe flag. Extra checks
are constrained to the hostname allowlist. The candidate must have an active,
healthy, non-draining bundle whose runtime/startup state matches the manager.

`cutover plan` runs the same preflight without DNS writes. Repeating `run` with the
same intent resumes its deterministic operation ID: previously committed writes
are recognized, including writes whose HTTP response was lost. The CLI never
blindly retries a mutation. Unexpected A/AAAA/CNAME/HTTPS/SVCB state, record
attribute drift or a different manager identity fails closed. DNS TTL, proxy
mode, record ID, comments and tags are preserved. A crash-safe local lock excludes
concurrent invocations on the same machine. Operators must maintain a single
writer across machines; GET/PATCH/readback is not a Cloudflare server-side CAS.

```sh
fugue static-edge cutover status --operation fse_OPERATION_ID
fugue static-edge cutover rollback --operation fse_OPERATION_ID --execute
```

Rollback also verifies both endpoints, preserves record attributes and refuses
external drift. It keeps both servers running. A DNS update is not globally
atomic, and existing connections/cached old addresses still need the old server.
Completion means `completed_old_retained`, with bounded probe evidence and API
record verification, not proof about every client or permission to retire the
old server. No drain/restart/stop/kill operation is part of DNS cutover. Natural
business traffic and authoritative/recursive DNS should also be reviewed before
declaring the production migration accepted.

The automated renderer deliberately implements only the fixed two-hop proxy
template used by this subsystem; custom source header/path rewrites are not
inferred. Review the generated configuration and origin contract before cutover.
Source certificate storage and paths are explicit flags. Advanced certificate
issuance, arbitrary operating systems/architectures, automatic old-node retirement
and global distributed locks are not implemented.

## Manual manager bootstrap (separate from business cutover)

1. Download the tagged manager artifact and verify its checksum. Keep a previous
   manager binary. Install with a controlled release/bootstrap process.
2. Provision management server/client certificates and local fingerprint grants.
   Keep management PKI independent of Fugue API and the business data CA.
3. Provision the bundle verification public key and local health-check policy.
4. Prepare a **dedicated** Caddy instance using its own startup JSON and restricted
   admin socket. Never adopt an unrelated shared public Caddy implicitly.
5. Start the manager with `--config /etc/fugue-static-edge/manager.json`.
6. Use mTLS/SSH status and adopt the exact healthy serving configuration. Adoption
   changes manager ownership records only; it does not reload Caddy.

`scripts/render_fugue_static_edge_manager_systemd_unit.sh` renders a unit; it
never installs or starts it. The policy file controls listen/socket/state paths,
Caddy binary digest, credential slots, verification keys and health checks.
Example: `docs/examples/static-edge/manager.json`. The systemd unit uses separate
state/runtime directories and has no `Requires`, `BindsTo` or stop hook for the
business process. Provision writable policy/config paths to the service user;
keys must be readable only by the necessary service identity.

SSH bootstrap uses the existing host administrative credentials and the normal
release process. The CLI does not silently install a remote daemon merely by
adding a context. Port reachability, certificates, exact listen addresses and
existing connection draining require an explicit, reviewed deployment.

## Signed bundles and verified adoption

Bundles contain native Caddy JSON, explicit edge identity, role, generation,
serving/draining mode and references to bounded **local policy** health checks.
No arbitrary URL, shell command or private key can be supplied as an operation.
Native configuration may reference provisioned certificate files. Do not include
private key bytes, tokens or customer bodies in a bundle.

```sh
fugue static-edge keygen --private-key ./signing.key --public-key ./signing.pub
fugue static-edge validate --file ./edge-bundle.json
fugue static-edge sign --file ./edge-bundle.json \
  --signing-key ./signing.key --key-id release-admin --out ./signed.json
fugue static-edge plan --file ./signed.json
fugue static-edge adopt production-entry --file ./signed.json \
  --expected-revision 0 --request-id initial-adoption
```

`plan` and `validate` are local schema/digest checks, not assertions of remote
health or signature trust. `stage` verifies signature, target, generation, local
policy and real Caddy validation without applying traffic configuration.
Adoption also verifies loaded config and health; it establishes both active and
LKG. An empty manager cannot activate traffic before adoption.

## Stage, activate, rollback and recovery

```sh
fugue static-edge stage production-entry --file ./next-signed.json \
  --expected-revision 1 --request-id change-2-stage
fugue static-edge activate production-entry --digest sha256:EXACT_DIGEST \
  --expected-revision 2 --request-id change-2-activate
# Or perform the same two operations sequentially:
fugue static-edge apply production-entry --file ./next-signed.json \
  --expected-revision 1 --request-id change-2
```

`apply` uses `<id>_stage` and `<id>_activate`, preserving the staged receipt's
revision. It never retries a mutation automatically. Every write uses CAS on
the local state revision, explicit target digest and request ID. A request ID
reused with different content or actor is rejected. Up to 256 recent receipts
are retained; older keys remain protected by monotonically increasing revisions.

Activation captures the original runtime, writes a durable pending journal,
applies through Caddy's graceful configuration API, reads back the exact runtime,
checks health, persists the verified startup config, then records active/LKG and
a receipt. Failures attempt to restore the old runtime and startup config. If
recovery cannot be verified, the journal remains pending and further writes are
blocked. Corrupt state is never silently reset; only one manager can own a state
directory. Configuration files are fsynced and atomically renamed.

On successful activation LKG retains the previous verified active configuration;
it is not overwritten by a failed candidate. Adoption creates the initial LKG.
An interrupted CLI/SSH/mTLS connection does not cancel an accepted operation.
Query its receipt and status before deciding whether to recover:

```sh
fugue static-edge operation production-entry --id change-2-activate
fugue static-edge status production-entry
fugue static-edge recover production-entry --transport ssh \
  --expected-revision 2 --request-id recover-change-2
fugue static-edge rollback production-entry --digest sha256:EXACT_LKG_DIGEST \
  --expected-revision 3 --request-id rollback-change-2
```

For an edge/origin pair, prepare a backward-compatible origin first, verify it,
then stage/activate the edge. Each node has independent revisions and receipts.
This is an ordered rollout, **not** a distributed atomic transaction. Keep the
old origin reachable until old edge streams drain. Do not remove a route or old
certificate from the origin while existing edge generations still need it.

## Drain and certificate rotation

Drain requires a signed candidate with `mode=draining` whose HTTP handlers only
return static responses (normally a health 200 and business 503); it cannot
create upstream requests. Redirect new traffic to an already verified alternate
entry before draining the sole serving node. A signed serving candidate restores
new-request service. Neither operation kills existing streams or claims they
have all finished. Use runtime/connection evidence before retiring a process.

```sh
fugue static-edge drain production-entry --digest sha256:DRAIN_CANDIDATE \
  --expected-revision 4 --request-id drain-1
fugue static-edge undrain production-entry --digest sha256:SERVING_CANDIDATE \
  --expected-revision 6 --request-id undrain-1
fugue static-edge cert status production-entry
fugue static-edge cert rotate production-entry --slot next \
  --expected-revision 7 --request-id credentials-2
```

Certificate rotation selects a pre-provisioned management slot containing server
cert/key, client CA and fingerprint grants. It does not mint certificates or
retrieve keys from Fugue. Retain old+new trust/grants during the transition,
verify a new-client connection, then explicitly retire old trust. The slot is
persisted and survives manager restart. Business certificates change through
signed Caddy configuration and separate overlap/drain verification.

## Verification and limitations

Tests cover API independence, mTLS server/client identity, fingerprint revocation,
redirect rejection, SSH argument isolation, signature tampering, state CAS,
idempotency, runtime drift, restart recovery and failed-apply restoration.
`FUGUE_TEST_CADDY=/path/to/caddy go test ./internal/staticedgemanager -run RealCaddy`
uses only an isolated localhost fixture to verify a streaming response survives
activation and rollback. The CLI build and tag-release jobs download pinned Caddy and run this test with race
checks. No production requests are replayed.

A successful health check or receipt is not proof that every customer saw zero
downtime. This manager isolates ingress configuration management; the origin's
business Pods, ClusterIP, databases, host and network remain real dependencies.
No claim is made that all Fugue-origin workload failures are isolated.
