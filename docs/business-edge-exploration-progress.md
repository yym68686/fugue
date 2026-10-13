# Real-business edge exploration implementation

## Approved scope

Use real business connections only. No client observer, measurement payload,
request mirroring, application replay, or inference about unobserved client
attempts. Measure acknowledged-byte deltas, RTT, retransmission ratios and
qualified TCP delivery rates. DNS allocation is a query-answer budget, not an
exact HTTP-request or byte quota.

The starting revision is `91a5c380b942525ac921717fc551bc4aef2c6079`.
Its normal CI run `38240056288` completed successfully. Subsequent legitimate
releases must be preserved. Existing SSH identities, application containers,
offline-node exclusions and positive serving artifacts must not be changed by
this implementation.

## Ordered work

1. Define explicit, versioned exploration intent and deterministic offline
   evidence/decision contracts. No serving activation accompanies code changes.
2. Implement bounded baseline exploration and adaptive increments with complete
   consecutive buckets, independent connections, shared peer cohorts, cooldown,
   per-change caps, lease expiration and sticky stop conditions.
3. Bind receipts to authoritative current topology, route witnesses, actual DNS
   assignment and the previous verified allocation. A replayable decision is
   not a signature or serving authorization.
4. Add signed DNS distribution artifacts, executor capability gating, recorded
   selection entropy, historical replay and positive-LKG fallback. Never reuse
   the retired legacy exploration selector.
5. Replace route-only sample exclusion with bounded cohort-aware sampling and
   coverage diagnostics. Explain scheduling, opportunity, sampling, ingestion,
   validation and expiration separately. No business-path telemetry waits.
6. Add operator inspection/replay, frontend contract synchronization, adversarial
   tests, full repository validation and normal CI release.
7. Explicitly activate a bounded phase-one intent; observe real allocations and
   compare DNS receipts, edge identity and samples. Enable phase two separately
   only after phase-one acceptance. Configuration rollback remains independent
   of code deployment.

## Safety invariants

- A code release does not activate exploration or replace configuration intent.
- Static/pinned/excluded/offline edges cannot be enrolled by sampling or scores.
- Unknown metrics are not zero, and multiple snapshots are not independent
  connections. Existing counters remain transport metrics, not client success
  rates.
- Comparison uses all sufficiently observed common cohorts and reports its
  coverage, context, windows and rejected inputs. Conflicting cohorts do not
  manufacture a global winner.
- Paused/expired/stopped exploration returns allocation authority to the explicit
  safe baseline; it does not invalidate the previous positive route/TLS artifact.
- No large automatic promotion: phase one has fixed exploration only; phase two
  requires explicit bounded adaptive intent and independently verified evidence.

## Status

Both implementation phases have been published and activated separately.
The 1% observe intent produced deterministic real-evidence comparisons before
the adaptive intent was enabled by CI `38402009038`, selected configuration job
`115312353454`. At 13:27:37 UTC on 2026-10-13, both public DNS consumers loaded
the adaptive allocation from full `artifact_1791897786_527a83f0b9a8`, fence 3487,
with verified positive LKG. The real comparisons remain insufficient, so there
has been no automatic share increase. The current intent expires at
2026-10-13 19:00:04 UTC; the deadline does not authorize renewal or revival of
offline edges. The chronological release and repair evidence is retained in
`business-edge-exploration-runtime-acceptance.md` and
`business-exploration-publication-validation-20261013.md`.

Completed locally:

- OpenAPI-first, fully explicit bounded intent and offline evidence contracts;
  historical quality schemas and evaluators remain unchanged.
- Pure deterministic real-business TCP evaluator: fixed observe allocation,
  adaptive bounded increase/decrease, cooldown, independent connection counting,
  complete consecutive buckets, common cohort coverage, metric regression guards,
  sticky stop conditions and expired-intent safe-baseline decisions.
- Snapshot follow-ups cannot inflate independent connection counts. The same
  complete evidence bucket cannot authorize multiple adjustments. Insufficient
  transport samples remain in the coverage denominator.
- Offline integrity/replay commands under `fugue admin edge business-exploration`.
  `capture` reads existing quality/DNS evidence and requires an explicit policy,
  traffic class, DNS instance and route prefix. It does not activate the policy.
- Passive sampler's existing one-minute route suppression now also matches the
  actual coarse peer cohort, retaining the global rate and concurrency bounds.
  This is a coverage improvement, not an asserted diagnosis of the old incident.
- Focused model/evaluator/sampler/CLI tests and the final full `make test` pass.
- Frontend contract snapshot/types regenerated from this backend checkout.
  `contract:check` remains blocked by 147 pre-existing TypeScript errors in
  historical documentation and other unrelated code. Baseline comparison showed
  no new diagnostic in the synchronized consumer baseline.
- Signed `physical-edge-business-v1` allocation, per-DNS-node initial digest/fence
  anchors, actual-serving predecessor continuity, independent source reconstruction,
  route/TLS/DNS capability admission, recorded DNS entropy and replay. Stops and
  cooldown cannot be reset by merely omitting a prior allocation from a receipt.
- Independent `business_exploration` configuration transaction: exactly one
  hostname, new intent identity for modifications, ordinary preview/gray/full,
  and pause/removal without changing unrelated constraints or deploying code.
- Coverage separates raw context-valid records, independent connection windows,
  RTT/ACK/delivery/retransmission availability, application-limited intervals,
  unbound samples and rejected contexts. Zero retained records does not establish
  zero business traffic or identify an unobserved loss stage.
- Full `make test`, targeted adversarial tests and evaluator/sampler/DNS race tests
  passed before the main-branch synchronization; final validation is repeated
  before publication. Two distinct DNS child digests/fences now initialize the
  same intent independently; missing or cross-node anchors fail closed.
- Post-sync full `make test` passed. Publication integration now tests the exact
  owned service and atomic rejection of extra paths, a changed traffic class,
  shared aliases and a modified allocation. The compatible-runtime release
  explicitly updates API, both guarded worker groups and the two non-public DNS
  C instances. See `business-edge-exploration-runtime-acceptance.md` for partial
  release outcomes, repairs and separately gated public handoff.

Acceptance scope and limitations:

- Initial enrollment is restricted to a directly owned hostname with one exact
  service path/class. Shared aliases and multiple service paths are rejected,
  not silently ranked using evidence from only one owner. They are not covered
  by the initial serving implementation.
- Exact initialization anchors must match the actual serving publication at
  activation. The explicitly configured `latest_verified_same_baseline` mode
  permits only same-producer, same-service, same-baseline positive renewal;
  concurrent preview changes retry boundedly or abort. It cannot discard sticky
  state or adopt different routing intent.
- The explicit phase-one 1% observe intent and separate bounded adaptive intent
  are live through independent signed publication paths. Both actual public DNS
  consumers have been accepted against a matching verified full/LKG.
  The first real adaptive comparisons replay deterministically but have zero
  complete shared-cohort windows, so the candidate remains at the 1% floor.
  No synthetic samples satisfy consecutive-window or minimum-connection gates.

An offline receipt hash is not authentication. A caller can construct offline
inputs; neither `evaluate` nor `replay` is a producer authorization boundary.
The producer independently reconstructs and verifies these receipts before
signing any allocation. Local implementation is not production acceptance.

Local full-validation log: `/tmp/fugue-business-exploration-final-test.log`.
Release prerequisite evidence: `business-exploration-release-prerequisite-20261013.md`.
