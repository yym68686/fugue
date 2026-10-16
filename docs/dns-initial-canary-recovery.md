# Preserve DNS resolution when an initial canary starts

Starting a canary changes the route digest before every record in the successor
DNS bundle has converged. The retained single-release plan then rejects the new
route proofs. Previously, per-record recovery handled an existing canary's weight
changes but rejected its first admission, producing SERVFAIL for an otherwise
ready hostname.

Forward recovery now recognizes a single-release policy becoming a canary when
the exact stable release retains a positive share and the newly admitted candidate
also has a positive share. Weights must total 100. Tenant, app, policy identity,
fallback and stickiness remain identical. A previously configured candidate
cannot be replaced or revived through this path. All DNS endpoint, selection,
stopped-node, quorum and scope comparisons remain in force. The compiler still
requires fresh release observations belonging to the correct app and tenant.

Only comparison copies change. Execution uses the original signed successor and
its exact, fresh proof quorum, after the current assignment is checked again.
The old-proof bridge remains strict, and partial recovery does not replace the
positive LKG or attest that the entire candidate is ready.

A second timing gap occurred when a worker activated between the candidate scan
and the retained plan's additional probes. A newly authenticated successor proof
was kept only as a mismatch against the old plan and discarded from the candidate
until another scan. These later observations are now evaluated against the
successor's exact requirement and release binding. Cached or failed observations
cannot supply readiness, observation timestamps never advance, and validity is
bounded by the original proof and the successor's freshness policy. Requirement
lookups are indexed and linear; this adds no network probes.

Regression coverage includes compiled and signed single-to-canary publication,
worker activation between scans, live UDP/TCP answers, exact decision replay,
assignment races, immutable checkpoints, expiry, incomplete quorum, duplicates,
foreign releases and policies, and control-plane loss and restart behavior.
