# Image-cache protection replica reads

Claiming a prune-image-cache task recomputes its protection plan before granting
destructive work. The plan previously read replicas once for every image,
including retirement authority, then read present replicas again for each image
eligible for minimum-replica protection. A history of thousands of images made a
single claim perform thousands of database round trips.

The candidate pass now reads replicas for the complete selected image ID set in
one query. The minimum-replica pass performs a second, independent query for the
eligible IDs and present status. It does not reuse the candidate snapshot or
cache replicas between requests. Other plan inputs and the claim-time safety
gate remain in place.

`ListImageReplicasByImageIDs` applies the existing tenant and replica filters,
uses one PostgreSQL text-array parameter, and preserves each image's observation
ordering and complete replica fields. An empty ID set performs no storage read.
Database, decode and iteration errors fail the plan instead of producing empty
success. The file backend scans state once per pass rather than reopening and
decoding it once per image.

For N candidate images and E eligible images, replica query count falls from
N + E to at most two. Result processing still covers every selected replica;
this is not a limit, pagination shortcut, or a change to retirement eligibility.
Live Kubernetes references, pins, migrations, leases, deletion budgets and
minimum keeper selection retain their original checks. These other inputs can
still contribute to total plan latency.

Tests compare batched and individual reads on file and real PostgreSQL stores,
including tenant scope, filters, ordering, duplicates, absent IDs, and query
errors. A 70,000-ID case checks the single-array binding. API tests exercise
stale/failed/missing candidates, expired leases, lifecycle eligibility, exact
node bindings and fresh minimum-replica reads after a state change. The inventory
integration fixture also asserts that an invalid completion count preserves the
previous inventory before a valid final chunk can mark omitted manifests absent.

An indexed local PostgreSQL benchmark with 1,024 images and 2,048 replicas measured
78.9–92.1 ms per individual-read pass versus 5.4–20.2 ms per batch, across three
runs of three iterations. Allocations decreased from about 8.28 MB to 3.20 MB per
pass. These are synthetic local measurements, not a claim of production latency
improvement; release acceptance requires a live read-only plan comparison and
natural task-claim observations.
