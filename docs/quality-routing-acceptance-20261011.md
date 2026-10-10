# Universal physical-edge quality routing acceptance

Observation date: 2026-10-10 UTC / 2026-10-11 Asia/Shanghai.

## Published configuration

`physical-network-comparable-delivery-v6` is the signed `all_dynamic` default. The universal declaration is `deploy/environments/production/routing-dynamic-quality/universal.json`, generation 4. It contains no per-host physical quality overrides. New dynamic domains enter the same ownership and quality pipeline automatically.

The producer authority is `artifact_1791652775_a51f03b36448`, release `artifactrel_1791652888_0edac82a6c60`, fence 37. Configuration CI [38070917824](https://github.com/yym68686/fugue/actions/runs/38070917824) succeeded. The first universal full publication, `artifact_1791653020_46b2d8862fb3`, was verified at 17:27:53 UTC under release `artifactrel_1791653149_14704e5afae7`, fence 2991.

The second captured publication accounts for 474 eligible dynamic queries across two authorities, representing 237 active dynamic hostnames. It records 115 measured hostnames, 17 learning on a ready current primary, and 105 awaiting a complete capture. Another 17 hostnames retain explicit pinned constraints, eight retain static constraints, and 67 have no applicable active dynamic query in this inventory. Static and pinned sets are identical between successive captures. The separate static-edge service is outside this change.

Coverage advanced by 64 newly captured hostnames between the first two cycles. Missing evidence retains the verified safe order and rotates through a bounded capture queue. It is not assigned zero latency, zero failure rate, or an overwhelming permanent penalty. The second DNS artifact contains 264 `physical_quality` records and 244 `physical_order` records, including pinned records and safe learning baselines. No geographic or legacy latency selector appears in these dynamic answer policies.

By the fourth capture at 17:42:36 UTC, all 237 active dynamic hostnames had obtained a measured or ready-primary learning receipt at least once: cumulative coverage progressed 68 → 132 → 196 → 237. This is complete rotation coverage, not a claim that every metric is simultaneously fresh. Expired comparison evidence returns to safe-order service and is captured again in later cycles.

## Same-network, identical-content acceptance

The authenticated `GET /v1/codex/models` on `i00.pro` was sent from the same local network to each physical edge, using the same hostname, path and headers. Both returned HTTP 200 and the complete same body:

| Measurement | OVH US | BWG |
| --- | ---: | ---: |
| Complete response | 18.012414 s | 1.630617 s |
| First byte | 1.689881 s | 0.909104 s |
| Response size | 288,046 bytes | 288,046 bytes |
| Origin completion, actual request trace | 349 ms | 430 ms |
| Physical edge in request trace | `vps-591f4447` | `bwg` |

Both SHA-256 values are `5f6443cb8ab1671a173c1c996927f7d34e5d7c93e6ad33e07e9fbccba5ba6113`. Request IDs are `edge_18dd3a1e7d6a72f7_3899` and `edge_18dd3a1e4fc1a677_12d2`. This comparison separates the slow response delivery from application or model waiting. Local TCP connect timing was intercepted by the local network stack and is not treated as public RTT.

Additional authenticated generated-body measurements use the same 1 MiB content per cross-edge round, signed public Front TCP identity/RTT, explicit outcomes and monotonic body reception time. They do not call the application. Rejected measurement requests and TLS integrity failures are not counted as network transfer failures.

## Actual routing evidence

The first canary change to BWG was triggered by OVH US capacity pressure. That event is explicitly a failover and is not the normal-health acceptance result.

The subsequent signed canary input at 17:06:43 UTC has both physical candidates ready, no hard gates, OVH US utilization 0.3354 and BWG utilization 0.6476. V6 retains BWG. The observed common TCP cohort is `122.233.152.0/24`; OVH US does not show a sustained advantage over BWG. The retained source receipt replays exactly.

At 17:11:24 UTC on US DNS and 17:12:16 UTC on DE DNS, the actual A answers are `95.169.10.156`, with `physical_quality`, `quality_state=measured`, and `physical_selected_primary`. Both candidates are materialized as healthy with route/TLS proofs. Publication reports `sync_succeeded`, `serving_lkg=false`. Offline DNS replay matches the original answers.

After universal activation, actual US and DE receipts at 17:28:25/26 UTC still select BWG with measured physical quality, successful publication sync and no LKG fallback. Both replay exactly. `wdwdl.fugue.pro` also answers `15.204.94.71` from both authorities; its US receipt at 17:32:15 UTC uses `physical_quality / physical_selected_primary`, successful sync and no LKG fallback, with matched replay.

These results establish normal-health routing for the observed acceptance network. They do not assert that DNS alone can identify the fastest entry for every terminal or every future request. Recursive resolvers, caches and absent client measurements retain their stated limits.

## Safety and release corrections

All production changes used main commits, normal GitHub Actions component releases, and the signed configuration publication workflow. No manual deployment, hardcoded BWG priority, country exclusion, threshold reduction, forced LKG rewrite, or fabricated measurement was used.

A V5 mixed-version release caused authoritative SERVFAIL when old DNS rejected the embedded strategy while workers loaded its traffic binding. This was a real rollout regression. Recovery restored the verified baseline; subsequent releases added exact strategy capabilities to route, TLS and DNS admission and unified DNS serving/candidate heartbeat capability lists. Compatible standby DNS was verified against the same assignment and fresh route proofs before normal public listener handoff. The final public executors advertise both V5 and V6.

Other corrected causes were node-first sampling that separated paired authority evidence beyond its lifetime; passive samples hashing upstream-refined metadata instead of the proven loaded route; optional one-sided retransmission evidence vetoing sufficient delivery evidence; static TXT/MX/CAA/NS records incorrectly blocking dynamic A selection; and repeated generic JSON expansion of large retained compiler inputs. Historical evaluator versions retain their original replay semantics.

V6 removes an optional retransmission cost from both sides when only one side observed it and sufficient delivery evidence exists, then adds bounded challenger uncertainty. The receipt records that exclusion. Large compiler-input persistence keeps the existing JSON format and digest, reads the actual stored content back, and rejects altered or malformed content. Code rollback therefore does not require a new artifact encoding.

## Validation

Backend `make test`, focused regression tests, prepush checks and materialized release plans passed. Frontend generated OpenAPI refresh and type checks passed; contract CI [38060846945](https://github.com/yym68686/fugue-web/actions/runs/38060846945) succeeded. Backend runtime release [38070482560](https://github.com/yym68686/fugue/actions/runs/38070482560) succeeded. API replicas were Ready with zero restarts; public DNS, actual loaded artifacts, authority receipts and offline replay were checked separately from CI and preview results.

A bounded inventory check queried all 237 active dynamic hostnames on both public authorities. Of 474 UDP queries, 468 initially returned NOERROR and six timed out. All six subsequently returned NOERROR over both UDP and TCP; no SERVFAIL or NXDOMAIN was observed in this final check. This does not establish the cause of transient packet timeouts.

Raw observation files remain local to the investigation, including the same-content report, signed raw quality receipts, published compiler evidence and real DNS replay exports. Credentials are not part of this report.
