# DNS OOM: same-execution decode reuse

## Evidence and scope

The European independent DNS reader experienced 24 kernel-confirmed 256 MiB
cgroup OOM kills during the 24-hour incident window ending 2026-10-28 19:28 UTC.
Its last termination was 09:52 UTC. The process inspected at 20:15 UTC was still
the same deployed revision (`83e64e77`), with `GOMEMLIMIT=48MiB`. A soft Go budget
is not an RSS limit, and absence of a recent restart does not resolve the risk.

Live Diagnostics `process-runtime-identity` established a stable process and
container. Complete, untruncated `runtime-heap-export` and `runtime-allocs-export`
reports were decoded, SHA256-verified, and analyzed with local `go tool pprof`.
The raw profiles were taken at 20:15:16.959 and 20:15:51.489 UTC, respectively.
Over that approximately 35-second interval, sampled allocations increased by
499.27 MiB. Cumulative candidate decoding accounted for 54.33%, shadow execution
46.60%, and embedded Cell verification 29.17%. These call paths overlap and
must not be added together. The sampled live heap was 46.96 MiB, while direct
process observations showed RSS 181.8 MiB and lifetime HWM 236.5 MiB.

The combined memory-profile probe exceeded its Job deadline; that failed session
does not supply memory evidence. Raw exports succeeded without running an
analyzer on the production node. The profiles themselves allocate memory and
their costs are visible in the samples.

## Changes

- Strict candidate decoding now supplies the embedded publications for their
  signature/trust checks, instead of separately serializing and decoding the
  complete artifact for Cell and predecessor verification.
- Shadow ReleaseSet validation consumes that same strict candidate decode.
- Readiness and query execution share the transient payload within that shadow
  execution. Persisted facts never carry this in-memory pointer, and proof age,
  assignment and consumer bindings are still checked.
- Checkpoint recovery consumes the verified payload returned by checkpoint
  decoding instead of verifying/decoding the complete checkpoint a second time.

These are constant-factor improvements to O(N) work and temporary storage, not
a new cross-observation cache. No signing-key, revocation, fence, topology,
expiry, physical probe, or positive-serving requirement is relaxed. Independent
serving, renewal and shadow workers are not serialized behind a slow candidate.
The 256 MiB hard limit and 48 MiB soft budget are unchanged.

## Reproducible synthetic benchmark

Run `go test ./internal/dnsserver -run '^$' -bench
'^BenchmarkDNSVerifiedPayloadDecode$' -benchmem -count=3` on the same machine.
The existing neutral fixture contains 512 additional static DNS records.

| Metric | Before | After embedded decode reuse |
| --- | --- | --- |
| Allocated bytes/op | 10.65–10.81 MB | 7.59–7.65 MB |
| Allocations/op | 127,295–127,304 | 71,355–71,364 |
| Time/op | 21.6–27.5 ms | 17.0–22.1 ms |

This benchmark measures one full release validation, not the additional removed
shadow validation or restart decode, and does not establish an RSS reduction.
Live post-release RSS, CPU, restart and OOM observations are required separately.
No tenfold memory claim can be inferred from these allocation numbers.

Regression tests cover strict nested fields, changed predecessor and Cell
signatures, revocation after warm pure validation, omitted declarations,
parent trust, exact checkpoint payloads, query behavior and receipt
fencing. Relevant DNS/configuration tests and race tests must pass before push.

## First production window and second bounded optimization

Revision `f4eaf95d` was deployed by CI 40765214458 to the European independent
reader at 2026-10-28 20:44:34 UTC. The limit remained 256 MiB and GOMEMLIMIT
remained 48 MiB. A 61-sample, 60-second direct process window changed peak RSS
from 196.27 MiB to 78.09 MiB and CPU consumption from 69.49 to 22.49 core-seconds.
The new PID/start ticks and Pod/image identity were established independently.
This is a short window, not proof of a tenfold improvement or 24-hour stability.
Readiness is not established by the Pod's livez-based Ready condition.

Complete Live Diagnostics raw profiles at 20:50:33.216 UTC and 20:51:10.922 UTC
showed 280.59 MiB of sampled allocations over 37.71 seconds. The remaining
`reuseDNSArtifactContent` deep comparison accounted for 33.79 MiB (12.04%).
It compared full native JSON trees using reflect's general cycle index and
boxed map values. A bounded typed traversal now preserves full value/type,
nil/empty and identity semantics, with reflect fallback for non-JSON values
and depth over 128. It still compares actual fresh content, not claimed hashes,
and does not cache any trust decision. Complexity remains O(N); native-tree
comparison scratch storage changes from O(N) to O(depth).

The 4096-row neutral comparison benchmark changed from 5.27–5.28 MB and about
37,075 allocations per operation (5.34–7.67 ms) to zero allocations (0.44 ms).
Differential tests against reflect.DeepEqual include native scalars/containers,
nil/empty distinctions, uncommon types, deep trees and cyclic fallback; fuzzing
also compares both implementations. These numbers are not an additional RSS
claim. Production post-release measurements remain necessary.

Both old and new reader logs return 404 from current and retained route-source
reads. A scoped identity reproduction confirmed assignment discovery succeeds
while route-source reads fail. No missing source authority is fabricated and no
proof deadline is extended to hide the error. This pre-existing condition is
tracked separately from allocation reduction and prevents a claim that all
independent DNS behavior is healthy.
