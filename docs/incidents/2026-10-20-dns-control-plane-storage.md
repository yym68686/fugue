# 2026-10-20 DNS outage and control-plane storage exhaustion

All times below are Asia/Shanghai (UTC+8). Investigation used public DNS/HTTPS,
the local Fugue CLI and read-only SSH/Kubernetes/PostgreSQL queries. Production
changes go through `main` and `.github/workflows/ci.yml`.

## Verified causal chain

1. Both authoritative servers, `15.204.94.71` and `51.38.126.103`, returned
   authoritative `SERVFAIL` for `fugue.pro A`. `api.fugue.pro A` also failed.
   SOA queries succeeded, proving that the DNS listeners were reachable.
   The control-plane host, US edge host and German agent could not resolve the
   names either. This was not a fault confined to the user's local network.
2. HTTPS requests pinned to either edge IP, with normal SNI/Host and certificate
   validation, returned HTTP 200 for `https://fugue.pro/`. The same request path
   with a valid route-proof nonce returned HTTP 503. Frontend code was still
   serving through the existing edge routes.
3. Both DNS processes retained the positive checkpoint with parent
   `artifact_1792440439_5f2e10d22d82`, DNS digest
   `sha256:758b22d540c13bc2658fa92a705ec751ded66e29d0ec946b04eb0b38becaec85`
   and fence 4905. Their health observations showed 0/675 ready probes and
   0/257 ready records, with `control_plane_unavailable`. The DNS checkpoint's
   own offline deadline had not expired.
4. The US edge's applied bundle was still present and Caddy's applied version
   matched it. Its original bundle deadline was **04:39:44.633570888**.
   `handleRouteProof` rejected all proofs after this deadline, despite ordinary
   proxying continuing under the configured 24-hour LKG window. This mismatch
   propagated control-plane unavailability into DNS loss for healthy edges.
5. The control-plane PostgreSQL primary's Longhorn data volume was full:
   98 GiB used, 4 KiB available, 100% usage. The volume was declared as 100 GiB.
   CNPG explicitly logged `no free disk space for WALs` and refused startup.
   The Cluster's Ready condition had changed to false at **04:13:27**. Both API
   replicas were unready and their Service had no ready endpoints.
6. The host root filesystem still had approximately 290 GiB available. Within
   the database volume, `base/16385` used about 97 GiB and `pg_wal` about 865 MiB.
   This was exhaustion of the isolated database data volume, not the host disk,
   inode pool or a WAL-only accumulation.

## Recovery and verification

Commit `8a851eb882c5f4533842c75e2ae4ddba3f64dbd9` added an independent, identity-
fenced recovery lane and declared expansion from 100 to 140 GiB. It checked the
Cluster/PVC/PV/Longhorn identities, attached healthy replica, CSI expansion
support and disk reserve. The server-side dry run passed. GitHub Actions applied
only the declared capacity increase and preserved the desired Cluster size.
No data was deleted, no primary was switched, and no manual restart was used.

Kubernetes recorded successful online filesystem expansion. CNPG automatically
restarted after its existing backoff and logged `database system is ready to
accept connections` at **05:30:34.496**. Both API replicas became ready, both
authoritative servers returned normal A answers, and an ordinary local HTTPS
request returned 200 at approximately **05:31**. The expanded filesystem had
approximately 40 GiB free (72% used). The recovery lane succeeded; its workflow's
separate public-DNS observation job failed while the original outage was still
present, which must not be confused with failure of the volume expansion.

The DNS fix in `6fa1577d5dcc1a2eca1303ae8936129978c1aa8e` separates an explicitly
requested retained-LKG observation from a fresh release proof. It requires
current trust verification, an actually applied Caddy version, unchanged route
and release identity, and a fixed offline deadline. Ordinary rollout parsers
reject retained observations. DNS preserves its original positive checkpoint
and independently caps fresh observations by policy and TLS validity. Subsequent
hardening distinguishes control-plane outages from explicit authority rejection.
Regression tests simulate the outage locally; production databases and traffic
were not intentionally interrupted to test failure handling.

## Storage evidence and deletion scope

Read-only catalog queries after recovery reported approximately:

| Relation | Total physical size |
| --- | ---: |
| `fugue_platform_input_chunks` | 24 GiB |
| `fugue_source_uploads` | 24 GiB |
| `fugue_platform_artifact_contents` | 15 GiB |
| `fugue_platform_artifacts` | 12 GiB |
| `fugue_image_cache_prune_plans` | 9.4 GiB |
| `fugue_audit_events` | 4.8 GiB |
| `fugue_edge_performance_samples` | 1.8 GiB |

These sizes are observed allocation, not estimates of safe reclaimable data.
The user authorized automatic removal only for heartbeat history, probe history
and diagnostic snapshots. The new retention sweep therefore excludes source
uploads, compiler inputs, artifacts, LKGs, image cleanup decisions and business
audit events. Its exact limits and signature-preserving boundary mechanism are
documented in [observation history retention](../observation-history-retention.md).
The largest protected tables still require capacity planning; observation-only
retention cannot promise a bounded total database size.

## Completed rollout and final checks

The retention implementation (`403e2efc`) and its generic-plan index correction
(`75de4daf`) are running on both API replicas. The live PostgreSQL generic query
plan uses the heartbeat-chain partial index. The production policy (`e2a5b239`)
is enabled independently of code, with a 30-second interval and batches of 500.
Heartbeat history uses seven days and 10,000 rows per chain, subject to the
24-hour minimum and preserved current head. Probe history uses seven days and
1,000,000 rows per table, subject to the seven-day safety floor. Diagnostic Jobs
retain their existing one-hour completion TTL.

Two independently targeted `runtime-storage-budget` diagnostic results reported
the accepted policy, no policy-read error and no sweep error. At **06:49:30** and
**06:54:37**, their process counters recorded 51,372 and 69,500 deleted heartbeat
rows, 51 and 33 performance samples, and 656 and 328 resource samples. These are
logical deletion counts, not reclaimed physical bytes. The diagnostic catalog
was updated (`73a6ee68`) so these counters and failure stages remain observable.

The US edge release initially stopped before changing serving state: its node
count was three after a suspended member returned, and that member still had
activation generation 877 while its peers were at 890. The baseline and Guardian
declarations were corrected. The independent recovery lane (`40de9f17`) checked
the exact old identity, prepared the current approved publication in an inactive
worker, and verified fresh TLS/nonce route proofs against a healthy peer before
atomically reconciling the returned member. The recovery reported
`activated=true, verified=true`. The subsequent US release (`e12693ba`) succeeded
at **06:58:35**; all three new workers and fronts were ready with zero restarts
in the final snapshot. German edge rollout also succeeded. Both public DNS
Services select the verified new `e` backends (`841b09d6`, `1d00521d`). Existing
standby workers and serving artifacts were preserved.

At **07:06**, both DNS backends reported **675/675 ready probes and 257/257 ready
records**. At **07:08:53**, the current CLI's `admin dns answer-check fugue.pro`
passed with consistent authoritative answers, both DNS nodes healthy, both edge
groups route-ready, and successful HTTPS/TLS/SNI/Host validation. A transient
earlier management observation had returned `unknown`; subsequent direct and
selected-backend observations both passed. It was not treated as a proven new
code defect. The homepage, `/auth/sign-in`, and API `/healthz` each returned 200
with normal TLS verification. Frontend static assets also returned 200. The
database reported `Cluster in healthy state`, one ready primary and the retained
140 GiB volume declaration; both API replicas were ready with zero restarts.

The final `make test` completed successfully, including generated OpenAPI checks
and static-edge observability tests. Isolated PostgreSQL tests covered retention
boundaries, protected rows, signed-chain continuity, concurrent ingestion and
the online timestamp-index migration. Pre-push checks passed. The latest CLI
build (`5009e260`, run `39337652092`) succeeded. Individual production deployment
lanes succeeded; some enclosing CI runs are marked cancelled because later
commits superseded unrelated configuration lanes. That aggregate status does not
describe the completed deployment jobs. No production outage was deliberately
injected for verification.
