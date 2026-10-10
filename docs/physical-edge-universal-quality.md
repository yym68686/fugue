# Universal dynamic-domain physical edge quality

## Work in progress

The target is every Fugue-managed dynamic DNS route, with explicit static and pinned constraints preserved. A code deployment cannot activate policy or invalidate a positive serving artifact. Configuration activation uses the existing independent signed producer transaction and declarative CI lane.

The existing retirement projection preserves 251 hostname orders. It is a compatibility baseline, not measured quality adoption. The new default must derive eligibility from route ownership and constraint policy and expose learning, measured selection and constrained/static exclusions separately. An isolated failed or unmeasured route must not block configuration recovery for other routes.

Implementation stages:

1. Capture public TCP delivery windows with raw counters and exact physical/socket/route identity. Preserve old executors and historical receipt replay. Verify zero additional business-path waits.
2. Add a separately versioned network evaluator and authenticated metric provenance. Keep failure-rate denominators distinct from packet retransmissions. Score only comparable client cohorts; unknown paths remain unknown.
3. Add generic dynamic-route coverage, bounded asynchronous evidence refresh and controlled measurement. Preserve safe current service while evidence is incomplete. Do not silently leave a hostname permanently in the retired frozen-order strategy.
4. Publish compatible code before independent policy activation. Validate a small rollout using same-network, same-content comparisons and matching real DNS receipts, then expand the policy. Static/pinned routes remain governed by their existing explicit constraints.

## Public TCP delivery measurement

Paired observations retain the same connection ID, slot, peer prefix, connection start and selected Front backend witness. They retain acknowledged-byte, kernel busy-time and data-segment/retransmission counters. No raw peer endpoint is retained. Counter reset, a changed Front or a stale sample rejects the pair.

Kernel busy time excludes periods with no outstanding send data. Delivery speed is admitted only with a sufficiently large acknowledged-byte delta, positive busy-time delta, recent data, and a kernel delivery-rate sample that is not application-limited. A low application-limited value is unknown, never evidence that the link is slow. The resulting units are bytes per second. HTTP request lifetime, inference wait and application queueing do not enter this calculation.

Retransmitted data segments divided by sent data segments describe transport degradation. They are not a connection failure rate. Failed-connection evidence requires explicit attempts and outcomes from a trusted bounded probe; missing outcomes must not be filled with zero.

Public packet forwarding and DNS answers never wait for telemetry. The worker performs bounded asynchronous follow-ups; old Front processes omit delivery counters and remain compatible. A code rollback can discard the optional observations without discarding the currently serving artifact.

No production acceptance is claimed by this work-in-progress document.

## Bounded universal capture

`dynamic_quality` is an explicit signed producer strategy, separate from code deployment. It derives owned dynamic hostnames from the frozen route intent. Static and pinned routes are excluded with a recorded reason. Shared DNS aliases and unmeasured address families remain visible as incomplete evidence coverage; they are not mislabeled as measured adoption.

Capture uses a per-cycle query budget, bounded concurrency and least-recently-attempted scheduling. Incomplete queries retain the previous verified signed order; completed queries carry actual DNS receipts and independently reconstructed network evidence. V4 can publish `quality_state=learning` only for the existing fresh route-ready primary. A learning receipt does not claim comparative quality and starts a known cooldown epoch. A failed query cannot block other completed queries.

The independent `dynamic_quality` producer transaction may change only the default quality strategy and producer/input generations. It cannot modify static intent, pinned constraints, configured baseline orders, timing or other producer controls. Current verified full/LKG and absence of pending gray are checked under publication locks.

Universal source TCP probing is opt-in through explicit worker configuration. It rotates all loaded eligible routes with a bounded batch and opens no application request. A single stable upstream entry matching the declared destination is measurable; multiple distinct upstreams remain ambiguous until their identity can be captured separately.
