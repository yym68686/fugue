# Unified business edge decisions

## Authorized objective

Publish one business decision for a service and evidence scope to every DNS
executor. Keep executor identity, publication fences, ACKs and positive LKG
independent from quality selection. Separate a bounded exploration budget from
quality allocation and explicitly replay promotion of a sustained winner.
Remove the historical baseline's candidate-share ceiling. Admit independent RTT
evidence without inventing unavailable delivery or retransmission measurements.

## Compatibility and implementation

The new `business-edge-unified-v2` policy preserves v1 decoding, evaluation and
historical receipt replay. All declared DNS predecessors must be captured and
reconstructed together. Each DNS envelope retains its own binding, but embeds
the identical allocation receipt and state. Partial or divergent same-intent
predecessors cannot advance the decision. Initialization is a new explicit
intent; node-local anchor baselines describe migration facts only.

Initial per-node weights are reconstructed from the exact serving receipts.
The common decision includes a temporary migration map and a shared target;
initialization does not change either node's answer distribution. Each cooldown
moves at most the configured step toward that target. The map is removed after
all nodes converge. These temporary execution differences are labeled
`business_migration`, never claimed as different user-quality rankings.

Quality and exploration weights are separate, add to 10000 and use independent
decision labels. RTT, delivery and retransmission qualify separately. Shared
cohorts require fresh complete windows, deduplicated independent connections,
coverage and consistent advantage. Observed deterioration can veto a move even
when another metric is absent. A new evidence bucket and cooldown are required
for each bounded step. Promotion changes the common primary only with the
declared stronger evidence; expiration retains that primary, never resurrects
an obsolete historical baseline.

Code and configuration release separately through `.github/workflows/ci.yml`.
The new policy is admitted only after every required route/TLS/DNS consumer
advertises its v2 capability. No business workload, SSH identity or trust file
is changed. Production activation and acceptance remain outstanding.

## Local verification

`make test` passes, including generated-contract drift, Go tests, Python release
and configuration tests, and the static-edge compatibility checks. Race-enabled
business/unified tests pass in model, evaluator, API and DNS packages. Tests cover
canonical cross-DNS evidence, contradictory predecessors, unavailable metrics,
observed transport stops, promotion beyond the old ceiling, evidence expiration,
bounded migration and comparisons retained while observing or migrating.

The frontend contract snapshot and generated types are synchronized. Its existing
147 TypeScript diagnostics are identical to the previous captured baseline; the
contract snapshot and generated-artifact checks pass. No unrelated frontend fixes
are included.

The code release retry declares API generation 751, US/DE edge-worker generations
213/387, and isolated US/DE DNS E generation 2. Public DNS D remains selected until
the isolated instances have been accepted; transport handoff and business intent
activation are separate configuration changes.

CI 38440948436 rejected the first attempt before any component deployment because
one input struct needed gofmt alignment. All affected tests, vet, compilation,
release and configuration checks passed. The retry keeps the exact live
predecessor and changes only formatting and monotonically increasing release
generations; it does not declare the failed build as a serving predecessor.
