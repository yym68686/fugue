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
