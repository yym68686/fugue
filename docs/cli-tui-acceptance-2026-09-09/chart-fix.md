# Startup chart correction

The fixed 15-minute axis compressed short sessions into the right edge. Empty
pixel buckets between normal observations were also rendered as missing data.

Charts now grow with observed history up to the selected window. Fresh series
end at the latest observation, avoiding movement between polls. Stale series
include the missing tail up to wall time. The axis reports the displayed span,
for example `30s / 15m`.

Only consecutive valid observations within 1.5 times the declared sampling
interval are connected. This is display interpolation, never stored history.
Null observations, missed intervals and unknown cadence remain gaps. Extrema
survive downsampling, and interpolated cursor values are explicitly labeled.

## Verification

- `go test ./internal/tui ./internal/cli`: passed.
- `go test -race ./internal/tui`: passed.
- Chart regressions cover startup, full windows, polling stability, stale tails,
  future samples, single samples, nulls, unknown cadence, downsampled spikes,
  window boundaries, cursor timestamps and all four graph modes at 1–200 columns.
- Xterm/Chromium screenshots reviewed at 140 and 200 columns for three-minute
  startup history and explicit gaps. The standard 60–200 column fixtures also
  render successfully. Reproduce using `scripts/tui-qa/README.md`.
- Actual cluster, candidate binary: after two observations spanning 25 seconds,
  the CPU curve occupied all 31 drawable columns in a 34-column panel. The axis
  showed `25s / 15m`; q restored the alternate screen and mouse state.
- Seven PTY scenarios passed: 106–111 ms first paint, input P95 at most 35.54 ms,
  no HTTP writes, no remaining subscriptions. See `chart-fix-pty.json`.
- The first launch of the newly built candidate measured 1791 ms and failed the
  existing 1000 ms startup assertion. The subsequent complete run passed without
  changing that threshold; the initial measurement is retained here rather than
  being counted as a pass.
- Dashboard benchmark (1000 rows, five charts, 200×50): 3.64 ms/frame,
  1.85 MB/frame on Apple M1 Pro.
- Local prepush passed compilation, affected tests/vet, generated-contract
  drift, formatting and diff checks in 15.3 seconds before rebasing on the
  concurrent database instrumentation change.
