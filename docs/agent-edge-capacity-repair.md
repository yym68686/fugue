# Agent Edge acquisition: capacity pressure repair

## Scope and evidence

The 24-hour incident ending 2026-10-28 19:28 UTC contained 6,543 distinct
Agent candidate requests returning 503. This is not evidence that the signing
key was missing. Read-only operation snapshots from both API processes at
20:33 UTC showed 245 acquisition attempts, 92 grant constraint failures,
152 capacity-pressure observations and 90 route-binding mismatches. Counts of
per-candidate failures overlap; they cannot be summed into failed requests.
Capacity read/clock/expiry errors, physical probe transport errors and signing
authority changes were zero in these processes. These counters cover their
current process lifetime, not the entire incident window.

Independent DNS readers co-located with public Edges consumed approximately
0.8–1.8 CPU cores each during follow-up samples. Node utilization sometimes
exceeded the signed Agent policy's 85% ceiling. That rejection is intentional;
raising the ceiling or signing an old publication would hide the underlying
resource/rollout problem.

The European reader's sampled allocation profile attributed substantial work
to repeated route-source compilation, JSON projection and new TLS handshakes.
Cumulative allocations are not resident memory and are not a leak estimate.
An independent decode-reuse repair is already in the parent revision.

## Implemented changes

- `dnsroutesource.Compiler` retains one deterministic route projection per
  worker. Every call still verifies actual artifact content, signatures,
  revoked keys, lane bindings and LKG deadlines. Only then does the cache use
  the recomputed snapshot digest, including its verified DNS content binding
  and every actual source envelope, except the observation timestamp. Returned
  plans are deep copies; neither positive proofs nor trust decisions are cached.
- Serving, renewal and shadow workers keep independent compiler instances.
  Pure projection reuse does not serialize these independent workers.
- Route probes reuse HTTPS connections only for the exact public address and
  TLS hostname. A bounded 512-entry LRU and 45-second idle expiry bound retained
  transports. Each request still generates a fresh nonce and validates the
  complete response, publication binding, expiry and certificate validity.
  Dial/handshake attempts remain bounded, including abandoned shared dials.
- Releases use normal declarative GitHub Actions lanes, one DNS reader at a
  time. Capacity limits and all publication authorization checks are unchanged.

## Complexity and benchmark

For repeated observations with unchanged inputs, full pure compilation is
replaced by input verification/hash plus an owned O(P) plan copy. Verification
remains O(N); this is not an O(1) authorization cache. Compilation on changed
inputs retains its original behavior and complexity. Idle transport lookup is
O(1); network probes are not skipped. The additional retained memory is bounded
but must be checked against the readers' 256 MiB limits after release.

`GOMAXPROCS=1 go test ./internal/dnsroutesource -run '^$' -bench
BenchmarkSourceCompiler -benchmem -benchtime=2s -count=3` on the same Apple M1
Pro synthetic fixture measured:

| Path | Time/op | Bytes/op | Allocations/op |
| --- | ---: | ---: | ---: |
| Ordinary verified compilation | 7.75–7.92 ms | 2,101,763–2,101,767 | 34,039 |
| Warm projection, same verification | 5.42–6.00 ms | 1,418,482–1,418,483 | 24,738 |

These are local fixture measurements, not a promised production CPU/RSS gain.

## Verification and remaining checks

Regression cases cover identical projection output, caller mutation,
concurrent callers, new releases, unchanged claimed hashes with corrupted
content, revoked keys, expired LKG, publication clocks, per-address TLS identity,
fresh nonce per request, nonce replay rejection and pooled certificate expiry.
Existing DNS tests retain assignment races, negative probes, wrong bindings,
mixed publications, checkpoint authentication and original proof deadlines.

After deployment, compare same-process API counter deltas, unique candidate
request statuses, canary acquisition/poll/heartbeat failures, DNS CPU/RSS,
container restarts/OOM and actual OCI revisions. A short quiet observation
window does not establish that every historical 503 was fixed. Persistent
route-binding mismatches require investigating publication/application lag;
they must never be accepted as a successful current observation.
