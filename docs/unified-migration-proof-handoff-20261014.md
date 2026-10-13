# Unified allocation migration and retained route proofs

The Germany DNS E process returned three captured SERVFAIL responses at
2026-10-13 20:47:51.757–20:47:51.763 UTC. Receipt
`4a9fe8a91a65fb651841767513515e58-3367`, query 16803, replays exactly.
Its old full DNS artifact was `artifact_1791923850_52fd3d275ad3`; the
verified successor was `artifact_1791924402_2bd2da8835ae`. Both fresh edge
proofs carried the successor gray publication and the unchanged per-app
traffic digest. The retained view filtered both as `traffic_release_mismatch`.
The five-minute collection recorded four counter increments, three retained
receipts, one unattributed increment and no collection errors. The missing
receipt is not assigned a cause.

Exact artifact comparison found two independent selection changes. The unified
decision advanced from sequence 1 to 2, moving the Germany execution map from
0% to 10% toward the common primary and the US map from 100% to 99%. A shared
probe alias independently changed its physical ranking. The route readiness
plan, target endpoints and per-app route digest were identical. Strict
selection comparison rejected the business record; the shared alias also
removed its otherwise identical probe from the compatibility set.

Readiness proof compatibility is now a separate comparison from permission to
execute a successor DNS selection. The former can refresh the original signed
selection when only its physical ranking or unified decision state differs.
It still compares exact policy intent, leases, candidates, execution identity,
stopped candidates, tenant, app, endpoint address, scope, route digest, quorum,
fault-domain constraints and freshness policy. V1 business behavior is retained.
The actual retained allocation, ranking, evidence expiry and positive checkpoint
are never rewritten. Successor execution still needs its own complete proof
and signed publication path; the transition path keeps strict selection
comparison and cannot borrow this proof-only projection.

Tests exercise migration and promotion, reject changed authority and route
requirements, exhaust all 10,000 allocation buckets to verify the retained
weights, and cover a shared physical alias. The two exact private artifacts
pass proof compatibility after the fix while remaining incompatible for direct
successor selection through the retained path. Historical failure replay stays
unchanged. Full DNS race tests pass.

Production release creates independent DNS F instances with separate cache
directories and service accounts. Public Services remain on E until signature,
positive current publication, wire protocol and exact handoff checks pass.
Each country's Service selection then changes separately through the normal
configuration lane. No serving DNS process or business workload is restarted
to apply this repair.
