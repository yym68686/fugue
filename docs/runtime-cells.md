# Fugue runtime cells

Fugue runtime cells are the resilience substrate for attached runtimes. The
control plane publishes intent; cells keep the last accepted intent locally,
preserve Fugue system events during outages, and rejoin the control plane when
it returns.

This layer is deliberately below application failover. Fugue must not promote a
database, move an app, create a new business replica, or shift traffic unless a
user has explicitly configured that business continuity behavior.

## Default behavior

When the Fugue API or controller is unavailable, a cell:

- keeps existing workloads running
- keeps previously applied manifest paths, route metadata, and completion events
  in local durable state
- retries control-plane reports from a local outbox
- avoids duplicate local applies for operations that already completed locally
- keeps a local system snapshot available to other cells over the mesh
- does not delete workloads just because the control plane is unreachable
- does not perform application or database failover by default

The default mode is frozen desired state: preserve the last accepted
control-plane intent and report local outcomes later.

## Current implementation

`fugue-agent` now opens a local `CellStore` at `FUGUE_AGENT_CELL_STORE_PATH`
(default: `$FUGUE_AGENT_WORK_DIR/cell-store.json`).

The current store backend is an atomic local file. The API is isolated behind
`CellStore` so it can move to SQLite without changing the agent reconcile path.

The store records:

- sanitized desired-task metadata, not the full app spec or secret-bearing task
  payload
- rendered manifest paths after local apply/render succeeds
- route cache entries for hostnames already assigned to this cell
- the latest cell snapshot
- peer snapshots discovered from other cells
- completion events in an outbox
- retry metadata for deferred completion reports

If the control plane accepts a task but is unavailable when the agent tries to
report completion, the completion event stays in the outbox. Later polls and
heartbeats replay due completion events before taking more work. If the same
operation is still returned while its local completion is pending, the agent
does not apply it again.

## Mesh and discovery

On every heartbeat, `fugue-agent` probes the local Tailscale client:

- `tailscale ip -4` supplies the cell mesh endpoint.
- `tailscale status --json` supplies known peers from the cached netmap.
- The heartbeat reports mesh provider, mesh IP, hostname, peer count, cached
  route count, outbox depth, and observation time as reserved
  `fugue.io/cell-*` runtime labels.
- The control plane merges only reserved cell labels and preserves user labels.

The agent also starts a local cell HTTP surface by default:

- `GET /cell/health`
- `GET /cell/snapshot`
- `GET /cell/routes`
- `GET /cell/peers`
- `GET /cell/bundle`

`FUGUE_AGENT_CELL_LISTEN_ADDR` defaults to `:7831`. The handler rejects callers
outside loopback, private, link-local, and CGNAT/mesh address ranges. It exposes
system substrate state only and does not expose desired-task payloads.

When peers are visible in the Tailscale netmap, the agent probes
`http://<peer-mesh-ip>:7831/cell/snapshot` and caches successful observations.
This lets cells keep exchanging runtime identity, endpoint, route generation
shape, and health metadata even while the Fugue API or Headscale server is
temporarily unavailable. Existing Tailscale sessions still depend on the local
tailscaled cached netmap; Fugue does not need Headscale online for these cached
peer observations.

## Edge cache

The Route A edge custom-domain sync now writes the successful
`/v1/edge/domains` response to
`/var/lib/fugue/edge/domains-cache.json`. If the Fugue API is temporarily
unavailable, the sync renders Caddy custom-domain blocks from that cache instead
of erasing the current route set.

## Rejoin behavior

When the control plane returns:

- each cell resumes heartbeats with its latest snapshot
- deferred completion events are replayed from the local outbox
- reserved `fugue.io/cell-*` labels refresh observed system state
- user labels and business topology are not overwritten by cell heartbeat data
- duplicate operation applies are avoided while a local completion is pending

The default rejoin path is deliberately conservative: report and converge Fugue
system state first; do not move apps, promote databases, invent replicas, or
shift traffic unless a user configured that higher-level policy.

## Business continuity boundary

Business-level automation remains opt-in:

- automatic app failover requires an explicit app failover policy
- database promotion requires an explicit database policy plus fencing/quorum
- traffic shifting requires an explicit routing policy
