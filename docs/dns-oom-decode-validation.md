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
signatures, revocation after warm pure validation, omitted declarations, field
aliases, parent trust, exact checkpoint payloads, query behavior and receipt
fencing. Relevant DNS/configuration tests and race tests must pass before push.
