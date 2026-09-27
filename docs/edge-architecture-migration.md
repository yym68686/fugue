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

Platform services use explicit `owner_kind: platform` with an empty tenant ID.
Every path must be a `platform`, `platform-route`, or canonical
`control-plane-*` route with no application or tenant owner.
Omitted `owner_kind` retains the tenant grant contract and requires a tenant ID.
This distinction lets Agent control requests obtain a hostname grant without
inventing a tenant or admitting application routes into platform authority.

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

The DNS executor now has two staged mitigations. Temporary loss of a configured
dynamic address returns SERVFAIL instead of cacheable NOERROR/NODATA. A retained
positive checkpoint may consume fresh HTTPS observations from a verified newer
release when each retained record's route proof plan and hard policy agree. Per-record
query authorization and selection rules must still match; changes only to
candidate score, score breakdown, or explanatory reason may retain the old
selection. A changed address, Edge identity, owner, weight, quorum or record
rule is rejected. Neither the checkpoint's applied time nor a proof deadline is
extended. Cross-release observations are not reported as successful application
of the new release. These mitigations must pass isolated DNS observation before
public DNS rollout; they do not complete the architecture migration.

The producer's source digest excludes raw runtime scores, so a score change
alone does not immediately trigger publication. Periodic refresh still embeds
those observations in the DNS query view; a derived selection-mode change also
changes policy and can trigger publication. Future placement must keep those
ranking observations separate from endpoint authorization.

Each DNS location now has independent A/B execution slots, with separate Pod
selectors, identities and durable caches. Code updates target only an unselected
slot. A separate versioned transport declaration moves a public listener after
both backends prove the same current artifact assignment and record readiness.
The workload guard rejects replacement of a publicly selected DNS executor.

Serving refreshes retain an unexpired in-memory proof after transport timeout
or an unobserved requirement, without extending its original deadline or the
checkpoint authority. TLS, route-state, digest and identity errors remain
negative. A restarted process cannot reuse persisted transient readiness.
An assignment race rejects the whole observation and rereads authority at most
three times; both positive and negative observations must pass the assignment
check before replacing serving facts. A rejected candidate scan can supply its
original observations to the retained release only for identical requirements
and the existing release/compatible-successor authority checks. Changed,
missing, ambiguous or foreign-release requirements are independently reprobed.
This avoids a second full scan of unchanged requirements. Failure diagnostics
count bounded reason codes rather than exposing hostnames or raw probe errors.

Cutover prerequisites remain:

- Both worker slots and public DNS consumers must understand the explicit
  platform owner and platform route kinds before enabling signed grants.
- Route, TLS, health, drain/quarantine and capacity observations must be bound
  to the exact signed release; pool membership alone remains insufficient.
- Agent endpoint authorization needs a public verification key and independently
  managed trust anchors, followed by hostname-preserving transport selection.
- Public DNS continuity must be verified through real gray/full refresh cycles,
  including fail-closed behavior for changed route authorization.
- Neutral authority-cell identities must replace country-derived identities in
  release authority, inventory, leases and LKG before removing compatibility
  aliases. Country labels remain optional locality or residency inputs.
