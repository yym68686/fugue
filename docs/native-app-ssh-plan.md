# Native App SSH Plan

Status: draft
Last updated: 2026-06-24

## Summary

Fugue should support native SSH access for app workloads that can provide an SSH
server. The user-facing contract is standard OpenSSH-compatible TCP SSH, so
tools such as `ssh`, `scp`, `sftp`, VS Code Remote SSH, and agent products can
connect without Fugue-specific protocol adapters.

This plan uses a dedicated SSH data plane with per-app public ports. The SSH
client connects to a Fugue hostname plus an assigned port. The edge node routes
the TCP stream by local port to the selected app Service and target SSH port.
The container runs a real OpenSSH daemon.

Example:

```bash
ssh -p 23417 fugue@app-abc.ssh.fugue.pro
scp -P 23417 ./task.py fugue@app-abc.ssh.fugue.pro:/workspace/
```

The hostname is used for DNS and regional edge selection. The assigned public
port identifies the app endpoint. This is required because SSH does not include
an HTTP Host header or TLS SNI value that Fugue can use for shared-port
hostname routing.

## Goals

- Provide native SSH connectivity to app containers that support SSH.
- Preserve compatibility with standard SSH clients and tooling.
- Keep cloud-side agents running even after the user's local machine disconnects.
- Make SSH a first-class Fugue app capability with explicit config, status,
  audit, access control, diagnostics, and lifecycle behavior.
- Avoid forcing arbitrary app images to run SSH when they do not support it.
- Keep SSH routing separate from HTTP app routes.
- Support managed-shared and managed-owned runtimes first.

## Non-Goals

- Do not implement SSH as Kubernetes exec, WebSocket terminal, or HTTP tunnel.
- Do not terminate SSH in the control plane for the first version.
- Do not require every arbitrary container image to support SSH.
- Do not route multiple apps through one shared `:22` listener by hostname.
- Do not use manual node patching or SSH hotfixes as the normal rollout path.

## Current System Context

The current app model has generic `ports []int`, `network_mode`, workspace,
persistent storage, runtime, and deployment fields. It does not have a
first-class SSH endpoint or TCP route model.

Relevant implementation points:

- `internal/model/model.go` defines `AppSpec` and app network modes.
- `internal/runtime/objects.go` renders the app Deployment and Service from
  `AppSpec.Ports`.
- `internal/api/edge_routes.go` derives HTTP edge route bindings from app route
  state and runtime placement.
- `internal/model/edge_routes.go` models edge route bundles around hostname,
  path prefix, HTTP upstream URL, TLS policy, and cache policy.
- `internal/edge/service.go` proxies HTTP routes and applies Caddy host routes.
- `internal/edgefront/service.go` already contains reusable TCP copy/proxy
  mechanics, but only for public 80/443 blue-green fronting today.
- Helm edge charts expose fixed public host ports for HTTP/HTTPS. Dynamic
  per-app SSH ports require a different listener model.

Current HTTP route behavior should not be overloaded for SSH. SSH needs a TCP
endpoint object and a TCP route bundle.

## Product Model

Native SSH should be represented as an app capability, not as a special case of
HTTP routes.

Suggested app-level spec:

```go
type AppSSHSpec struct {
    Enabled            bool     `json:"enabled,omitempty"`
    TargetPort         int      `json:"target_port,omitempty"`
    User               string   `json:"user,omitempty"`
    AuthorizedKeyIDs   []string `json:"authorized_key_ids,omitempty"`
    AllowTCPForwarding bool     `json:"allow_tcp_forwarding,omitempty"`
}
```

Suggested app-level status:

```go
type AppSSHStatus struct {
    Supported          bool   `json:"supported"`
    Ready              bool   `json:"ready"`
    Hostname           string `json:"hostname,omitempty"`
    PublicPort         int    `json:"public_port,omitempty"`
    TargetPort         int    `json:"target_port,omitempty"`
    User               string `json:"user,omitempty"`
    HostKeyFingerprint string `json:"host_key_fingerprint,omitempty"`
    Message            string `json:"message,omitempty"`
}
```

Suggested durable endpoint record:

```go
type AppSSHEndpoint struct {
    ID                 string     `json:"id"`
    TenantID           string     `json:"tenant_id"`
    ProjectID          string     `json:"project_id"`
    AppID              string     `json:"app_id"`
    RuntimeID          string     `json:"runtime_id,omitempty"`
    RuntimeType        string     `json:"runtime_type,omitempty"`
    EdgeGroupID        string     `json:"edge_group_id,omitempty"`
    Hostname           string     `json:"hostname"`
    PublicPort         int        `json:"public_port"`
    TargetPort         int        `json:"target_port"`
    User               string     `json:"user"`
    Status             string     `json:"status"`
    StatusReason       string     `json:"status_reason,omitempty"`
    HostKeyFingerprint string     `json:"host_key_fingerprint,omitempty"`
    CreatedAt          time.Time  `json:"created_at"`
    UpdatedAt          time.Time  `json:"updated_at"`
    ReleasedAt         *time.Time `json:"released_at,omitempty"`
}
```

Suggested SSH key record:

```go
type SSHKey struct {
    ID          string    `json:"id"`
    TenantID    string    `json:"tenant_id"`
    Label       string    `json:"label"`
    PublicKey   string    `json:"public_key,omitempty"`
    Fingerprint string    `json:"fingerprint"`
    Status      string    `json:"status"`
    CreatedAt   time.Time `json:"created_at"`
    UpdatedAt   time.Time `json:"updated_at"`
}
```

## Addressing And Port Allocation

The first version should use one public hostname plus one assigned port per app:

```text
app-abc.ssh.fugue.pro:23417 -> app app_abc SSH target
```

Suggested default port pool:

```text
22000-32000
```

Port allocation requirements:

- Allocate a globally unique public port per active SSH endpoint.
- Reserve released ports for a cooldown window before reuse.
- Prevent duplicate assignment with a database unique constraint.
- Keep the same port stable across app restarts and redeploys.
- Allow explicit port rotation for compromised or noisy endpoints.
- Support future expansion by assigning ports per edge IP or per edge group.

Suggested endpoint states:

- `pending`: endpoint requested, port allocated, runtime not ready yet.
- `ready`: SSH route is publishable and app target is available.
- `unsupported`: app image or runtime cannot provide SSH.
- `unavailable`: app has no running replica or target Service is unavailable.
- `disabled`: SSH disabled by app config.
- `released`: port removed from routing and waiting for reuse cooldown.

## Runtime Rendering

SSH should not be appended blindly to `AppSpec.Ports`, because existing app
ports currently drive Service generation and readiness behavior for HTTP
traffic. SSH needs separate rendering logic.

When SSH is enabled and supported:

- Add a named Service port such as `ssh` or `tcp-22`.
- Add a matching container port for documentation and inventory.
- Mount an `authorized_keys` Secret into the SSH server location expected by
  the supported image.
- Mount or generate stable SSH host keys from a Secret.
- Include the SSH port in app runtime inventory and diagnostics.
- Keep existing HTTP readiness behavior unchanged.
- If the app has restricted ingress NetworkPolicy, allow ingress from the
  Fugue SSH front component to the SSH target port.

Supported container strategies:

1. Fugue official SSH-enabled agent/workspace base image.
   - Includes OpenSSH server.
   - Provides a non-root `fugue` user.
   - Uses `/workspace` by default.
   - Uses `tini`, `supervisord`, or another explicit process supervisor.
   - Can keep the cloud agent as the main workload process.

2. User-provided image with SSH support.
   - User explicitly sets `ssh.enabled=true`.
   - User supplies or confirms `target_port`.
   - Fugue exposes the endpoint but does not attempt to install sshd.

Unsupported examples:

- Distroless images.
- Scratch images.
- Images without an SSH daemon.
- Images that cannot safely accept injected authorized keys or host keys.

## Agent Workload Semantics

For agent products, SSH should be the control channel, not the lifetime of the
agent process.

Recommended workload profile:

- The agent runs as the app main process or as a supervisor-managed process.
- SSH login starts a shell in `/workspace`.
- Disconnection does not stop the agent.
- tmux/screen can be available but should not be forced by default.
- `replicas=1` is the default for SSH-enabled agent apps.
- Persistent workspace or persistent storage is strongly recommended.
- Scale-to-zero should be disabled while SSH is enabled unless the user
  explicitly accepts cold-start connection failures.

## Edge SSH Data Plane

Add a dedicated `fugue-ssh-front` service. This should be separate from the HTTP
edge proxy and Caddy host route machinery.

Responsibilities:

- Fetch signed SSH route bundles from the control plane.
- Dynamically listen on assigned public ports.
- Route accepted TCP connections by local listener port.
- Dial the target app Service and SSH target port.
- Proxy bytes bidirectionally without modifying SSH payloads.
- Track active connections.
- Emit metrics and structured logs.
- Enforce connection limits and rate limits.
- Degrade safely when route bundles are missing or stale.

Suggested route bundle shape:

```go
type EdgeSSHRouteBundle struct {
    SchemaVersion      string         `json:"schema_version,omitempty"`
    Version            string         `json:"version"`
    Generation         string         `json:"generation,omitempty"`
    PreviousGeneration string         `json:"previous_generation,omitempty"`
    GeneratedAt        time.Time      `json:"generated_at"`
    ValidUntil         time.Time      `json:"valid_until,omitempty"`
    Issuer             string         `json:"issuer,omitempty"`
    KeyID              string         `json:"key_id,omitempty"`
    Signature          string         `json:"signature,omitempty"`
    EdgeID             string         `json:"edge_id,omitempty"`
    EdgeGroupID        string         `json:"edge_group_id,omitempty"`
    Routes             []EdgeSSHRoute `json:"routes"`
}

type EdgeSSHRoute struct {
    AppID              string    `json:"app_id"`
    TenantID           string    `json:"tenant_id"`
    RuntimeID          string    `json:"runtime_id"`
    RuntimeType        string    `json:"runtime_type,omitempty"`
    EdgeGroupID        string    `json:"edge_group_id,omitempty"`
    Hostname           string    `json:"hostname"`
    PublicPort         int       `json:"public_port"`
    TargetHost         string    `json:"target_host"`
    TargetPort         int       `json:"target_port"`
    User               string    `json:"user,omitempty"`
    Status             string    `json:"status"`
    StatusReason       string    `json:"status_reason,omitempty"`
    HostKeyFingerprint string    `json:"host_key_fingerprint,omitempty"`
    RouteGeneration    string    `json:"route_generation"`
    CreatedAt          time.Time `json:"created_at"`
    UpdatedAt          time.Time `json:"updated_at"`
}
```

Listener model:

- Prefer `hostNetwork: true` for `fugue-ssh-front`.
- Run only on edge nodes.
- Bind only ports in the configured SSH pool.
- Avoid static Helm `hostPort` entries for each app port.
- Use a local cache of the last known good SSH route bundle.
- Keep existing HTTP edge blue-green rollout independent.

## DNS

SSH endpoint DNS should point to edge nodes, not app pods.

First version:

- Publish `*.ssh.<app-base-domain>` or a platform-owned SSH base domain.
- Return healthy edge IPs for that region/group.
- Keep app selection by port, not by hostname.

Future version:

- If Fugue allocates multiple edge IPs, ports can be reused per IP.
- DNS can pin an SSH endpoint to a chosen edge group for lower latency or
  runtime locality.

## Control Plane API

The backend must remain OpenAPI-first. Every API addition starts in
`openapi/openapi.yaml`, then generated artifacts and handlers follow.

Suggested user-facing endpoints:

- `GET /v1/ssh-keys`
- `POST /v1/ssh-keys`
- `DELETE /v1/ssh-keys/{id}`
- `GET /v1/apps/{id}/ssh`
- `PATCH /v1/apps/{id}/ssh`
- `POST /v1/apps/{id}/ssh/rotate-port`
- `GET /v1/apps/{id}/ssh/diagnose`

Suggested edge-facing endpoint:

- `GET /v1/edge/ssh-routes`

Suggested response for `GET /v1/apps/{id}/ssh`:

```json
{
  "ssh": {
    "supported": true,
    "ready": true,
    "hostname": "app-abc.ssh.fugue.pro",
    "public_port": 23417,
    "target_port": 22,
    "user": "fugue",
    "host_key_fingerprint": "SHA256:..."
  }
}
```

## CLI

Suggested commands:

```bash
fugue ssh-key add ~/.ssh/id_ed25519.pub --label laptop
fugue ssh-key ls
fugue ssh-key rm key_123

fugue app ssh enable my-app
fugue app ssh disable my-app
fugue app ssh show my-app
fugue app ssh diagnose my-app
fugue app ssh rotate-port my-app
fugue app ssh config my-app
fugue app ssh my-app
```

`fugue app ssh config my-app` should print an OpenSSH config block:

```ssh-config
Host fugue-my-app
  HostName app-abc.ssh.fugue.pro
  Port 23417
  User fugue
  IdentitiesOnly yes
```

`fugue app ssh my-app` can execute the local `ssh` binary with the resolved
hostname, port, and user.

## Security

SSH is remote code execution. It must not be authorized by `app.read`.

Recommended scopes:

- `ssh.key.read`
- `ssh.key.write`
- `app.ssh.read`
- `app.ssh.write`
- `app.ssh.connect`

Minimum policy:

- Disable password authentication.
- Disable root login.
- Use public key authentication only.
- Default login user is `fugue`.
- Audit key creation, key deletion, SSH enable/disable, port rotation, and
  connection attempts.
- Record connection metadata but not full session contents.
- Apply per-IP, per-app, and per-tenant connection rate limits.
- Apply max active connection limits.
- Make TCP forwarding explicit and disabled by default unless the agent product
  needs it.
- Never log private keys or full secret values.

Container-side SSH server defaults:

```text
PasswordAuthentication no
PermitRootLogin no
PubkeyAuthentication yes
AuthorizedKeysFile .ssh/authorized_keys
AllowTcpForwarding no
X11Forwarding no
PermitTunnel no
```

## Observability And Diagnostics

Metrics:

- `fugue_ssh_front_connections_total`
- `fugue_ssh_front_active_connections`
- `fugue_ssh_front_connection_duration_seconds`
- `fugue_ssh_front_bytes_total`
- `fugue_ssh_front_dial_errors_total`
- `fugue_ssh_front_auth_failures_total` if auth failure signals are available
  from app logs or optional integration
- `fugue_ssh_route_bundle_sync_total`
- `fugue_ssh_route_bundle_stale`

Diagnostics should answer:

- Is SSH enabled for the app?
- Is the app image known to support SSH?
- Is a public port allocated?
- Is the port present in the SSH route bundle?
- Is the edge listener active?
- Can edge dial the app Service target port?
- Are authorized keys rendered?
- Is the host key stable?
- Is the app running exactly one expected SSH target replica?

## External-Owned Runtime

Managed-shared and managed-owned should ship first.

External-owned support needs an explicit runtime-agent capability because the
central edge cannot always dial tenant-owned cluster Services directly. Options:

- Agent advertises `ssh_route` support and exposes a reverse tunnel to the
  control plane or edge.
- Tenant-owned cluster exposes a controlled TCP ingress that Fugue can target.
- Fugue marks SSH unsupported for external-owned runtimes until one of the
  above is implemented.

Do not silently publish an SSH endpoint for external-owned runtimes without a
verified data path.

## Rollout Strategy

1. Build the backend contract and storage model.
2. Build port allocation and endpoint status without publishing public traffic.
3. Render app Service/Secrets/NetworkPolicy for managed runtimes.
4. Deploy `fugue-ssh-front` behind a feature flag on a canary edge group.
5. Enable an internal test app using the official SSH-enabled base image.
6. Publish one endpoint from the port pool.
7. Verify `ssh`, `scp`, `sftp`, VS Code Remote SSH, and a long-running agent
   use case.
8. Expand to all healthy edge groups.
9. Add Web console UI after the backend and CLI are stable.

Control plane deployment must use the standard GitHub Actions path:

- Commit changes to the `fugue` repository.
- Push to `main`.
- Let `.github/workflows/deploy-control-plane.yml` update the remote control
  plane.

## Open Questions

- Should SSH endpoint hostname be app-scoped, project-scoped, or tenant-scoped?
- Should the first port pool be global or per edge group?
- What cooldown period is enough before reusing a released port?
- Should `AllowTCPForwarding` default to true for agent products?
- Should SSH-enabled apps be forced to `replicas=1`, or only warn on multiple
  replicas?
- Should Fugue provide an optional debug SSH sidecar for non-SSH images, or keep
  SSH limited to images that explicitly support it?
- How should usage billing account for long-running SSH sessions and egress?

## Operations Runbook

### Firewall Requirements

- Edge nodes that run `fugue-ssh-front` must allow inbound TCP traffic for the
  configured SSH public port range, default `22000-32000`.
- The Kubernetes network path from `fugue-ssh-front` pods to app Services must
  allow TCP to the app SSH target port, default `22`.
- Production values keep `edge.sshFront.enabled=false` until canary rollout.
  Enable it per canary values file or explicit Helm override, then expand after
  route sync, listener, and connection metrics are healthy.

### Rotate An Endpoint Port

```bash
fugue app ssh rotate-port <app>
fugue app ssh show <app>
fugue app ssh config <app>
```

Rotation allocates a different public port from the configured pool and causes
the next signed SSH route bundle to move the listener. Existing connections may
continue until the old listener is reconciled away; users should reconnect with
the new port from `show` or `config`.

### Disable A Compromised SSH Key

```bash
fugue ssh-key ls
fugue ssh-key rm <key>
fugue app ssh diagnose <app>
```

Deleting a key removes it from apps that referenced the key ID. The app
Deployment template includes an SSH authorized-keys checksum annotation, so a
resolved key change rolls pods through the normal app deploy operation path.

### Exhausted Port Pool

If enabling SSH returns a conflict, inspect the active endpoint count and the
configured port range:

```bash
fugue app ssh diagnose <app>
```

Operational options are:

- Increase `FUGUE_SSH_PUBLIC_PORT_START` / `FUGUE_SSH_PUBLIC_PORT_END` on the
  control plane and matching `edge.sshFront.publicPortStart` /
  `edge.sshFront.publicPortEnd` Helm values.
- Disable unused SSH endpoints to release ports.
- Wait for the released-port cooldown before reusing recently released ports.
- In a future per-edge-IP design, shard the pool by edge IP or edge group.

## TODO

Checked items are implemented or verified in the current codebase. Unchecked
items remain deliberately pending because they need live canary validation,
durable host-key storage design, external-owned runtime relay work, or web UI
implementation.

### Contract And Model

- [x] Add `AppSSHSpec`, `AppSSHStatus`, `AppSSHEndpoint`, and `SSHKey` model types.
- [x] Add SSH endpoint statuses: `pending`, `ready`, `unsupported`,
      `unavailable`, `disabled`, `released`.
- [x] Add SSH route bundle model types.
- [x] Do not add SSH connection records in v1; use edge connection logs until
      SSH sessions are authenticated at the control plane.
- [x] Extend snapshot/backup model to include SSH endpoint and SSH key metadata
      where appropriate.
- [x] Decide whether `AppSSHSpec` lives inside `AppSpec` or in a separate app
      feature table.

### Database And Store

- [x] Add migration for `fugue_ssh_keys`.
- [x] Add migration for `fugue_app_ssh_endpoints`.
- [x] Add unique constraint for active `public_port`.
- [x] Add released-port cooldown fields and queries.
- [x] Add store methods for SSH key CRUD.
- [x] Add store methods for app SSH endpoint get/upsert/delete/release.
- [x] Add store method for atomic public port allocation.
- [x] Add tests for port allocation races.
- [x] Add tests for released port cooldown behavior.
- [x] Add tests for tenant isolation on SSH keys and endpoints.

### OpenAPI

- [x] Add `GET /v1/ssh/keys`.
- [x] Add `POST /v1/ssh/keys`.
- [x] Add `DELETE /v1/ssh/keys/{id}`.
- [x] Add `GET /v1/apps/{id}/ssh`.
- [x] Add `PATCH /v1/apps/{id}/ssh`.
- [x] Add `POST /v1/apps/{id}/ssh/rotate-port`.
- [x] Add `GET /v1/apps/{id}/ssh/diagnose`.
- [x] Add `GET /v1/edge/ssh/routes`.
- [x] Add schemas for all SSH request and response bodies.
- [x] Add security requirements for SSH operations.
- [x] Run `make generate-openapi`.
- [x] Verify generated route/spec artifacts are updated.

### Authorization And Audit

- [x] Add `ssh.key.read` scope.
- [x] Add `ssh.key.write` scope.
- [x] Add `app.ssh.read` scope.
- [x] Add `app.ssh.write` scope.
- [x] Do not add `app.ssh.connect` in v1 because connection events are not
      authenticated at the control plane.
- [x] Add audit event for SSH key creation.
- [x] Add audit event for SSH key deletion.
- [x] Add audit event for app SSH enable.
- [x] Add audit event for app SSH disable.
- [x] Add audit event for public port rotation.
- [x] Add edge log record for route bundle publication/sync failures.
- [x] Add edge log record for SSH connection start/end.

### Runtime Rendering

- [x] Add SSH Service port rendering independent of `AppSpec.Ports`.
- [x] Add SSH container port rendering independent of HTTP readiness.
- [ ] Add durable host key Secret generation/storage and fingerprint tracking.
- [x] Add authorized keys Secret naming and rendering.
- [x] Add volume mounts for supported Fugue SSH-enabled images.
- [x] Add app template annotations so SSH key changes roll pods safely.
- [x] Add NetworkPolicy ingress allowance from `fugue-ssh-front` to SSH target
      port.
- [ ] Add runtime inventory fields for SSH port and target readiness.
- [x] Add tests for apps with HTTP ports plus SSH.
- [x] Add tests for background/internal apps with SSH enabled.
- [x] Add tests that SSH does not change HTTP readiness behavior.
- [x] Add tests for restricted NetworkPolicy with SSH enabled.

### Official SSH-Enabled Image

- [x] Define the official base image name and tag policy.
- [x] Add OpenSSH server.
- [x] Add non-root `fugue` user.
- [x] Add `/workspace` directory defaults.
- [x] Add process supervisor or entrypoint strategy for agent plus sshd.
- [x] Add SSH server config with password and root login disabled.
- [x] Add authorized keys mount path contract.
- [x] Add host key mount path contract.
- [x] Add smoke test for `ssh`.
- [x] Add smoke test for `scp`.
- [x] Add smoke test for `sftp`.
- [x] Add smoke test for agent process surviving SSH disconnect.

### Edge SSH Front

- [x] Add `fugue-ssh-front` command.
- [x] Add config loader for SSH route endpoint, token, listen port range,
      cache path, limits, and sync interval.
- [x] Implement signed SSH route bundle fetch.
- [x] Implement last-known-good SSH route bundle cache.
- [x] Implement dynamic listener reconciliation.
- [x] Implement TCP proxy by local listener port.
- [x] Reuse or extract existing TCP copy helpers from `internal/edgefront`.
- [x] Add dial timeout and idle timeout handling.
- [x] Add graceful shutdown for active listeners.
- [x] Add per-IP connection rate limiting.
- [x] Add per-app active connection limits.
- [x] Add per-tenant active connection limits.
- [x] Add structured connection start/end logs.
- [x] Add Prometheus metrics.
- [x] Add tests for listener add/remove.
- [x] Add tests for stale bundle fallback.
- [x] Add tests for port with missing route.
- [x] Add tests for target dial failure.

### Helm And Deployment

- [x] Add Helm values for `sshFront.enabled`.
- [x] Add Helm values for SSH public port range.
- [x] Add Helm values for SSH route cache path.
- [x] Add Helm values for SSH front resources.
- [x] Add Helm values for SSH front node selector and tolerations.
- [x] Add `fugue-ssh-front` DaemonSet.
- [x] Use `hostNetwork: true` for dynamic port listeners.
- [x] Add release safety labels for the SSH data plane.
- [x] Add chart tests for enabled and disabled SSH front.
- [x] Add production values with SSH front disabled until canary rollout.
- [x] Document firewall requirements for the SSH port range.

### DNS And Edge Bundle

- [x] Decide SSH base domain.
- [x] Add SSH endpoint hostname derivation.
- [ ] Add DNS records for SSH endpoint hostnames or wildcard SSH domain.
- [x] Add SSH route bundle derivation by edge group.
- [x] Include runtime locality in SSH route selection.
- [x] Add route bundle signing and verification.
- [ ] Add route bundle invariants to prevent accidental empty publish.
- [x] Add route readiness checks for edge SSH front.
- [ ] Add tests for DNS answer generation.
- [x] Add tests for edge group fallback behavior.

### CLI

- [x] Add `fugue ssh-key add`.
- [x] Add `fugue ssh-key ls`.
- [x] Add `fugue ssh-key rm`.
- [x] Add `fugue app ssh enable`.
- [x] Add `fugue app ssh disable`.
- [x] Add `fugue app ssh show`.
- [x] Add `fugue app ssh diagnose`.
- [x] Add `fugue app ssh rotate-port`.
- [x] Add `fugue app ssh config`.
- [x] Add `fugue app ssh <app>` wrapper around the local `ssh` binary.
- [x] Add table output.
- [x] Add JSON output.
- [x] Add copy-paste friendly OpenSSH config output.
- [x] Add CLI tests for missing local ssh binary.
- [x] Add CLI tests for unsupported app status.

### Web Console Follow-Up

- [x] Sync frontend OpenAPI after backend contract lands.
- [ ] Add app SSH endpoint status to app settings/details.
- [ ] Add SSH enable/disable controls.
- [ ] Add SSH key management UI.
- [ ] Add copyable SSH command.
- [ ] Add copyable OpenSSH config block.
- [ ] Add unsupported, pending, ready, disabled, and error states.
- [ ] Add accessibility and responsive checks according to the `fugue-web`
      frontend process before UI implementation.

### Diagnostics And Operations

- [x] Add `GET /v1/apps/{id}/ssh/diagnose` implementation.
- [x] Check endpoint config.
- [x] Check port allocation.
- [x] Check route bundle presence.
- [ ] Check edge listener presence.
- [ ] Check edge-to-Service dial.
- [ ] Check app pod and target port readiness.
- [x] Check authorized keys Secret render.
- [ ] Check host key Secret render.
- [ ] Add admin CLI output for SSH front active connections.
- [x] Add runbook for rotating an endpoint port.
- [x] Add runbook for disabling a compromised SSH key.
- [x] Add runbook for an exhausted port pool.

### Security Hardening

- [x] Enforce public key format validation.
- [x] Store public key fingerprints.
- [x] Reject duplicate active public keys per tenant.
- [x] Disable password authentication in official images.
- [x] Disable root login in official images.
- [x] Disable TCP forwarding by default.
- [x] Add optional TCP forwarding flag.
- [x] Add brute-force rate limiting at edge.
- [ ] Add max session duration option if needed.
- [x] Add idle timeout option if needed.
- [x] Ensure private keys are never accepted, stored, or logged.
- [x] Ensure session payloads are not logged by Fugue.

### External-Owned Runtime

- [x] Mark external-owned SSH unsupported in first version.
- [ ] Add runtime capability field for `ssh_routes`.
- [ ] Design agent relay or tenant-owned TCP ingress path.
- [ ] Add agent heartbeat capability reporting.
- [ ] Add external-owned SSH route derivation only after the data path is
      verified.

### Verification

- [x] Run unit tests for model/store/API changes.
- [x] Run `make generate-openapi`.
- [x] Run `make test`.
- [x] Build official app SSH image locally.
- [x] Local Docker smoke test for `ssh`, `scp`, `sftp`, and agent process
      surviving SSH disconnect.
- [ ] Deploy SSH front to a canary edge group.
- [ ] Verify `ssh` to a Fugue-managed canary test app.
- [ ] Verify `scp` to a Fugue-managed canary test app.
- [ ] Verify `sftp` to a Fugue-managed canary test app.
- [ ] Verify VS Code Remote SSH.
- [ ] Verify agent keeps running after local disconnect in a Fugue-managed
      canary test app.
- [x] Verify port rotation.
- [ ] Verify key removal blocks future logins in a Fugue-managed canary test
      app.
- [x] Verify disabled SSH endpoint closes the public listener.
- [x] Verify edge stale-bundle fallback.
- [x] Verify monitoring and audit events.
- [x] Sync `fugue-web` OpenAPI if frontend work is included.
- [x] Run `npm run openapi:sync`.
- [x] Run `npm run openapi:generate`.
- [x] Run `npm run contract:check`.
