# DNS address continuity across publication changes

Two compatibility defects could leave an address record without either an old
proof bridge or a usable successor while unrelated records delayed whole-bundle
activation:

- Adding or updating an MX/TXT RRset at the same name changed the hostname-wide
  comparison, including the query inventory length. That also blocked an
  otherwise equivalent physical-order representation.
- A signed physical selection could change its primary or readmit an endpoint
  already present in the unchanged signed candidate inventory. Forward recovery
  incorrectly compared that new selector as though it would execute the old one.

Address compatibility now compares A, AAAA and CNAME intent, rules and queries.
Independent RR types retain their own normal serving path. Address ownership,
values, expiry, TTL, aliases and the complete readiness plan remain boundaries.

Forward recovery permits a signed physical selection within the identical
candidate inventory. It executes the successor's original selector with one
complete, fresh successor quorum and exact publication binding. It cannot mix
publications, reduce quorum, change endpoints, tenant, application, availability
policy, ECS, business allocation or other hard policy. The old-proof bridge
remains stricter and keeps its original selector. Neither path rewrites signed
payloads, promotes an incomplete positive checkpoint, or acknowledges a partial
bundle as applied.

Recovery history still belongs to the exact immutable route requirement. An
application replacement that changes that requirement must satisfy the configured
success count and observation span again. Publication mismatch diagnostics alone
do not establish proof, and this fix does not borrow old-release recovery history.

Tests cover same-name independent RRsets combined with order representation
changes, primary changes, exact successor answers and replay, incomplete quorum,
proof expiry, insufficient recovery history and independent authorization changes.
Production updates first replace the existing private DNS slots, then use the
declarative transport handoff after both backends satisfy its assignment and
freshness checks. No additional capacity is introduced. Both private slots allow
240 seconds for cold recovery and health verification; the signed recovery
policy is unchanged.
