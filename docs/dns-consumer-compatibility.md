# DNS reader compatibility and failure diagnosis

DNS artifacts carry executable policy versions as well as record data. A reader
that does not understand a signed strategy must reject it; discarding unknown
fields or accepting unknown business-policy versions can change routing authority.
Keep the existing positive checkpoint independent of rejected candidates and
preserve its original maximum age and route-proof deadlines.

`DecodeCellRoutePublications` previously decoded the entire `PolicySnapshot`
inside a struct containing Cell references. Any nested policy decoder failure
became `DNS Cell references cannot be decoded`, even for ordinary DNS artifacts
without `cell_route_publications`. The decoder now reports policy and reference
failures separately and wraps the underlying error. It continues validating the
full policy and all existing reference ownership, order and cardinality checks.
This change improves diagnosis; it does not make old binaries understand newer
policy versions or change DNS answers.

Use the running image's OCI revision and the exact immutable artifact when
reproducing compatibility failures. A current source checkout can accept a
policy that an older standby rejects. In particular, an exploration-v1 reader
cannot execute a unified-v2 business policy simply because both artifacts share
the outer platform configuration schema version.

Treat a rollback binary as usable only after it can authenticate and execute the
currently serving artifact. Existing publication admission checks require fresh
authenticated executor capabilities, including `business_edge_unified_v2` when
needed. Do not remove those checks to accommodate obsolete standby processes.
Code release and configuration recovery remain independent.

Before replacing or retiring an unused DNS slot:

1. Inspect actual public UDP/TCP Service endpoints, their Pod UIDs, expected
   consumer membership, current selected artifact and positive checkpoint.
2. Prepare a compatible replacement through the normal declarative component
   release path, verify its exact capabilities and direct DNS behavior, and keep
   the current public backend serving while validation runs.
3. Use a separate configuration declaration for any transport handoff or slot
   retirement. Require exact workload identity and resource-version checks; a
   public backend or required recovery target cannot be silently removed.
4. Preserve compatible rollback artifacts and the last positive checkpoint.
   Repeated errors from an obsolete non-serving process must not be hidden by
   classifying it as a healthy standby.

`fugue_dns_health` is the complete serving-record readiness view. `/readyz`
answers whether the authoritative listener has usable checkpoint authority.
Neither alone establishes what a particular DNS query returned. `SERVFAIL`
must be correlated with the original decision receipt, loaded artifact,
record-specific proof quorum, selector and evidence expiry. A healthy listener
can correctly refuse one unsafe record while answering unrelated records.

The recent-query journal is bounded to the last 64 entries. Failed queries also
use an independent queue and a 1,024-file bounded disk history, documented in
`dns-decision-receipts.md`. Query counters establish
that failures happened but cannot reconstruct their hostnames or reasons after
receipt eviction. For an incident requiring long-term attribution, export
failure receipts to independent observation storage with explicit retention and
resource budgets. Observation failures must never prevent serving, publish an
artifact, extend proof validity or replace a positive checkpoint.

Regression coverage verifies stage-specific decode errors, preserved wrapped
causes, reference authority boundaries, and rejection of a signed incompatible
candidate while the previous positive release continues serving unchanged.

## Declarative executor retirement

`scripts/retire_dns_slots.py` consumes a separate retirement declaration from the
production configuration lane. It verifies an active public backend and a distinct
standby against the current signed release, exact Pod identities, checkpoint
signatures and complete record readiness. It rejects any obsolete workload still
selected by a Service unless that exact internal validation/health Service is
explicitly declared quiescent by UID and spec digest. Public port 53, external IPs
and NodePorts cannot use this exception. Host listeners and active TCP DNS
connections also block retirement, including alternate DNS validation ports.
Each mutation tests the workload UID, resource version and complete prior spec.
DaemonSets receive an impossible scheduling constraint; Deployments scale to zero.
Neither operation deletes checkpoint volumes or changes an image, signed artifact,
record policy or public selector. Replays require the exact recorded stopped spec.
The lane shares the DNS transport concurrency fence and retains prewrite and result
evidence independently of code compilation. Reactivation requires a fresh explicit
release and compatible executor; retired slots leave the active component registry.
Their retained templates and isolation tests use `retired-dns-components.json`,
which the production planner does not load. Existing LKG/PVC data and internal
Service objects are preserved.

Diagnostic DNS listeners can recover through the same signature-verified transport
handoff as port 53. The listener port is explicitly declared; the backend still
must own both UDP and TCP DNS on target port 53. Version 3 transport declarations
can bind multiple independent listener recoveries, each with its own exact old/new
Pod identities and immutable candidate image. Duplicate listener recovery entries
are rejected. An unhealthy predecessor is never silently treated as a valid
rollback target.
