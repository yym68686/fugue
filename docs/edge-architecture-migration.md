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
hard gates. The caller must bind both the grant and facts to the current signed
TrafficReleaseSet before using any result for serving. This binding and the
shadow comparison are not yet connected to the production compiler, so the
new candidate result cannot affect DNS or Agent traffic.

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
3. Add measured sticky primary/standby selection only for connections actually
   initiated by a Runtime Agent. Public clients continue to use the DNS/entry
   path; the independent DNS failover executor remains the sole writer for its
   declared records.
4. Migrate publication, inventory, Lease, and LKG one authority cell at a time.
   Retire the country-derived group identity only after both cells serve from
   neutral identities with verified rollback and public route probes.

Before each cutover, verify the exact previous positive LKG, route and TLS
proof for the target hostname, DNS ownership, cell health, and the other cell's
unchanged state. A rejected or incomplete candidate preserves the current
serving artifact. A failed code release cannot revoke a serving configuration.
