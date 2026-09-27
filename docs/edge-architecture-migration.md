# Edge topology migration

The serving system still uses `edge_group_id` as its compatibility identity.
`deploy/edge/topology.json` is a separate placement intent. Its authority cell
maps each existing group to a neutral release boundary; a serving pool can span
multiple cells; an Edge has a stable ID and explicit risk dimensions. Country
and region are labels, not risk domains or release identities.
The `legacy_group_id` alias is transitional. Once a cell's runtime, release
ledger, inventory and LKG use the neutral cell ID, remove the alias in a
separate verified configuration step.

The topology file contains no endpoints, health, traffic weights, loaded bundle
digests, or code revision. Those are runtime facts or signed artifacts. A pool
membership is not route authorization. In particular, the current public
DiscoveryBundle lists globally healthy Edge nodes and does not grant any tenant
or hostname access. No consumer may expand a signed route's serving scope based
only on this file or on the discovery audit.

The same topology schema can now be carried in a signed static `PlatformIntent`
as `edge_topology`. The producer preserves it in the projected intent and its
lineage. The field is optional, and neither its presence nor a clean audit
changes route bundles, DNS answers, or current Edge selection. Serving
authorization still requires an explicit grant derived from that signed intent
and matching runtime facts.

`edgetopology.EligibleCandidates` is a non-serving compiler for a verified
tenant/hostname grant and authenticated runtime facts. Its grant must list every
path route digest required by the hostname for each authorized authority cell:
the existing route proof includes the serving group ID, so digests differ
between cells. Cells omitted from the grant are not candidates. Each Edge must have current proof
for all of them, the matching TLS hostname, health, capacity and pool/capability
membership; exclusions, optional residency and requested risk diversity are
hard gates. An optional `edge_selection_constraints` entry in the signed
PolicySnapshot now derives a hostname grant from the exact projected route
artifact, every path's cell-specific digest, topology, and existing DNS
placement constraints. The grant is embedded in that signed route artifact
only when explicitly configured. Compilation rejects an unknown tenant,
inactive path, missing DNS dependency, impossible candidate minimum, or an
unproved cell. It does not alter the current DNS answer or Edge bundle.

The caller must bind both the grant and authenticated facts to the current
signed TrafficReleaseSet before using any result for serving. Runtime fact
collection, shadow comparison with the old selection, and cutover gates are
still outstanding. The grant alone cannot affect DNS or Agent traffic.

Runtime Agent already initiates HTTP control requests through `ServerURL` in
`internal/runtime/agent_service.go`. This is a real selection surface even
without an Agent-to-Edge business tunnel. Dynamic control-request transport
must keep the configured API hostname and TLS identity, choose only fresh
authorized endpoints, and avoid automatically replaying mutating requests.
Bootstrap, enrollment, heartbeat, operation polling and completion must retain
their existing authentication and retry semantics.

The public DiscoveryBundle currently uses HMAC authentication. Its signing key
must not be distributed to Runtime Agents merely to let them verify discovery:
that would also let a verifier mint authority. Independent client validation
requires a public verification mechanism and separately managed trust anchors,
bound to exact publication, hostname, audience and absolute expiration. An
authenticated API response or a healthy TLS endpoint alone is not a replacement
for that candidate authorization.

For a read-only consistency check against the currently visible Edge nodes:

```sh
go run ./cmd/fugue-edge-topology -discovery https://api.fugue.pro/v1/discovery/bundle
```

`unknown_edges` and `mismatched_groups` fail the audit. `unobserved_edges` are
reported separately because the public discovery endpoint omits unhealthy or
draining nodes. The command does not verify the DiscoveryBundle signature and
must never be used as a serving authorization gate. It writes no traffic, DNS,
release, or inventory state.

The migration proceeds under the existing signed traffic release and positive
LKG boundaries:

1. Keep the old group route and DNS outputs unchanged while checking every
   observed Edge against the new topology intent.
2. Compile per-tenant, per-hostname candidate authorization from signed route
   intent plus pool membership and current route/TLS/health facts. Compare its
   shadow result with the old decision before allowing any traffic.
3. Add measured sticky primary/standby selection only for control requests
   actually initiated by a Runtime Agent. Public clients continue to use the DNS/entry
   path; the independent DNS failover executor remains the sole writer for its
   declared records.
4. Migrate publication, inventory, Lease, and LKG one authority cell at a time.
   Retire the country-derived group identity only after both cells serve from
   neutral identities with verified rollback and public route probes.

Before each cutover, verify the exact previous positive LKG, route and TLS
proof for the target hostname, DNS ownership, cell health, and the other cell's
unchanged state. A rejected or incomplete candidate preserves the current
serving artifact. A failed code release cannot revoke a serving configuration.

Production observation on 2026-09-27: during periodic producer refresh, DNS
candidate readiness can briefly be incomplete while Edge route proofs move to
the new release. The old positive checkpoint remains intact, but once its
route proofs expire, the authoritative DNS node may temporarily return no A
answer. The next release converged and both authoritative nodes returned the
same A answer, but a positive LKG alone did not guarantee answer continuity.
Before enabling dynamic placement or neutral-cell cutover, require an explicit
overlap/ordering acceptance check that proves public DNS answers remain present
through gray/full transitions and validates the exact current ReleaseSet on
both DNS consumers. Never extend a stale proof merely to keep an answer.
