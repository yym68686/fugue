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
If fresh comparable evidence shows the migration target is outperformed,
migration toward it pauses. A uniquely qualified winner can be promoted while
the exact current executing weights remain unchanged; subsequent convergence
still obeys the configured step and cooldown. Historical migration intent thus
cannot override a qualified business decision.

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
is changed. The v2 intent was activated through CI 38461304882; production
handoff and subsequent verification are recorded below.

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

The active generation 5 declaration removes the candidate ceiling (`10000` basis points),
sets a separate 200-point exploration budget, allows a 1000-point step and a
120-second cooldown. Comparison still requires three complete 300-second
windows, at least one connection per window, five distinct connections per
cohort, 30% coverage, and both 15% and 20 ms RTT advantage. Promotion requires
eight distinct connections and 50% coverage. Qualified delivery/retransmission
regression guards and transport stops remain in force. The explicit 48-hour
intent expires at 2026-10-15 20:07:50 UTC. DNS answer
shares are not represented as exact HTTP request or byte shares.

US worker staging exposed a separately proven durable state size limit; see
`edge-authority-storage-pressure-20261014.md`. The compatible reader was released
first, then compressed writes were enabled independently. At 19:05:49 UTC,
the live state was 18,775,914 encoded bytes and 100,161,024 expanded bytes,
preserving 57,308 shadow and 55,038 authority audit entries. No audit history
or serving identity was deleted.

The observed DNS proof-renewal regression and exact replay are documented in
`dns-business-proof-renewal-20261014.md`. The retry at `892b7176` selects only
API generation 754, US worker 216 and isolated DNS E generation 4 in both
regions. `36f484ae` was rejected during preflight; its successor plan validates
against the same verified production predecessors.

CI 38440948436 rejected the first attempt before any component deployment because
one input struct needed gofmt alignment. All affected tests, vet, compilation,
release and configuration checks passed. The retry keeps the exact live
predecessor and changes only formatting and monotonically increasing release
generations; it does not declare the failed build as a serving predecessor.

## Production activation and follow-up

Both DNS executors now consume `business-edge-unified-v2`. At 21:39:36–38 UTC,
both independently recorded sequence 9, allocation receipt
`sha256:d0214f2127e4e8ce9145165cd022033acfd572ac070f440b2cc8e87be132aafe`
and the same full publication fence 3561. The shared quality primary and target
were vmarck, with 9800 quality points and a separate 200-point exploration
budget. The US execution map was 9900/100 and Germany was 6000/4000 during
the bounded migration. At 21:52 UTC the shared migration map had advanced
Germany to 8000/2000. These are temporary execution shares, not competing
DNS-specific quality rankings.

Production acceptance exposed two exact regressions, each documented with
private evidence and a focused reproducer. The compiler-input persistence
repair is described in `unified-producer-input-cost-20261014.md`; the migration
proof compatibility repair is described in
`unified-migration-proof-handoff-20261014.md`. API generation 755 was deployed
through CI 38465718827. Isolated DNS F runtimes were deployed through
CI 38468543536. Both F instances passed signature and current verified-full
checks, exact predecessor handoff checks and 24 isolated UDP/TCP checks.
The US public listener was handed to F by transport generation 41 through
successful CI 38472764012; 48 public and recursive checks passed afterward.

At 21:47–21:48 UTC, native poly and argus SSH login both exited successfully
with Bash and the expected workspace, strict host verification enabled and
the trust-file hash unchanged. Their business Pod UIDs, container IDs and
zero restart counts matched the captured pre-handoff baseline.
