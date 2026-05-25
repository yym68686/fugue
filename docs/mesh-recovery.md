# Fugue Mesh Recovery

Fugue treats Headscale as the normal mesh control server, not as the only
source of truth for whether nodes can rediscover each other. The recovery
subsystem keeps a signed peer directory and a signed mesh generation manifest
outside the main control plane so a bad Headscale database, an empty replacement
Headscale, or a control-plane outage does not silently erase the cluster's
ability to coordinate a rejoin.

## Components

- `fugue-mesh-recovery` runs on edge or DNS nodes. It accepts node heartbeats,
  persists a last-known peer directory, signs `/mesh/directory` and
  `/mesh/generation`, and can expose a rejoin auth key during an explicit reset
  generation.
- `fugue-mesh-agent` runs on every infrastructure node. It sends heartbeat
  metadata to multiple recovery authorities, verifies signed bundles, writes
  last-known-good files under `/var/lib/fugue/mesh-agent`, and can rejoin
  Tailscale when `FUGUE_MESH_AGENT_REJOIN_ENABLED=true`.
- Headscale remains the Tailscale coordination server at the login server URL,
  normally `https://mesh.fugue.pro`. Recovery authorities only publish signed
  recovery intent and peer state; they do not mutate the main Fugue API.

## Failure Model

- If Headscale is healthy, agents keep refreshing the signed directory and do
  nothing disruptive.
- If Headscale loses state and a new mesh generation is intentionally issued
  with `FUGUE_MESH_RECOVERY_MODE=reset`, agents fetch `/mesh/rejoin`, verify the
  signed manifest, and run `tailscale logout` followed by `tailscale up` with
  the recovery auth key.
- If the main control plane is down, edge or DNS hosted recovery authorities can
  still serve signed directory and generation data.
- If one recovery authority is down, agents try the next endpoint in
  `FUGUE_MESH_AGENT_ENDPOINTS`.

## Systemd Path

Render a recovery authority unit on edge/DNS nodes:

```bash
scripts/render_fugue_mesh_recovery_systemd_unit.sh \
  --output-dir /tmp/fugue-mesh-recovery \
  --listen-addr 0.0.0.0:7840 \
  --generation meshgen-$(date +%Y%m%d%H%M%S) \
  --login-server https://mesh.fugue.pro \
  --tls-cert-file /etc/fugue/mesh-recovery/tls.crt \
  --tls-key-file /etc/fugue/mesh-recovery/tls.key
```

Install `/tmp/fugue-mesh-recovery/fugue-mesh-recovery.env` and
`fugue-mesh-recovery.service`. The secret env file must define:

```bash
FUGUE_MESH_RECOVERY_TOKEN=...
FUGUE_MESH_RECOVERY_SIGNING_KEY=...
FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY=...
```

Use `ed25519-private:<base64raw-private-key>` for
`FUGUE_MESH_RECOVERY_SIGNING_KEY`. The matching public value
`ed25519-public:<base64raw-public-key>` is distributed to agents.

Render an agent unit on every infrastructure node:

```bash
scripts/render_fugue_mesh_agent_systemd_unit.sh \
  --output-dir /tmp/fugue-mesh-agent \
  --endpoints https://mesh-recovery-us.example,https://mesh-recovery-eu.example \
  --node-id "$(hostname -s)" \
  --roles control-plane \
  --login-server https://mesh.fugue.pro \
  --ca-cert-file /etc/fugue/mesh-recovery/ca.crt
```

The agent secret env file must define:

```bash
FUGUE_MESH_AGENT_TOKEN=...
FUGUE_MESH_AGENT_SIGNING_KEY=...
```

Agents should receive only the Ed25519 public verification key. The legacy HMAC
format is still accepted for compatibility, but it is not the production
security model because a node with the HMAC key can forge recovery bundles.

Keep `FUGUE_MESH_AGENT_REJOIN_ENABLED=false` until the recovery endpoints,
signing key distribution, and Headscale recovery auth key have been validated.
Turn it on only after confirming a signed reset generation is intentional.

Do not run the public recovery endpoints over plain HTTP. Use a public CA
certificate or distribute a private CA file with `FUGUE_MESH_AGENT_CA_CERT_FILE`.
`FUGUE_MESH_AGENT_TLS_INSECURE_SKIP_VERIFY=true` exists only for isolated lab
validation and must not be used in production.

## Helm Path

The chart includes an optional `meshRecovery` DaemonSet for cluster-local
operation. This is useful for staged rollout and observability, but the target
HA architecture should run the recovery authority and agent as host-level
systemd services so they remain available when Kubernetes or the Fugue control
plane is degraded.

`meshRecovery.enabled=true` requires explicit token and signing-key secrets.
The chart intentionally does not generate those keys because agents outside the
cluster must verify the same signing key.

## Reset Generation Procedure

1. Generate or rotate the Headscale reusable preauth key.
2. Put the key in `FUGUE_MESH_RECOVERY_REJOIN_AUTH_KEY` on every recovery
   authority.
3. Bump `FUGUE_MESH_RECOVERY_GENERATION` and set
   `FUGUE_MESH_RECOVERY_MODE=reset`.
4. Verify `/mesh/generation` returns a valid signature and
   `rejoin_required=true`.
5. Enable `FUGUE_MESH_AGENT_REJOIN_ENABLED=true` on the intended nodes.
6. Watch agent logs for successful rejoin and confirm Headscale node inventory
   repopulates.
7. Return `FUGUE_MESH_RECOVERY_MODE=normal` once the mesh is stable.

The generation bump is the guardrail that prevents a stale recovery endpoint or
old auth key from causing repeated joins.
