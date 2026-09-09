# Fugue human dashboard redesign

The previous dashboard put four small charts above an oversized table and a
sparse metadata column. Selecting a node did not select its metrics, and
percentage charts scaled to their historical peak. This made both scope and
load severity difficult to read.

The new dashboard uses a large CPU plot, a current-capacity panel, separate
memory/disk plots, a content-sized resource table, selected-resource details,
workloads and health/activity. Fine colored borders, gradient plots and load
colors establish hierarchy. Unsupported metrics occupy a status line instead
of an empty chart. Narrow terminals retain the primary charts and table;
`n`/`b` page additional charts, and Enter opens full resource details.

## Data and interaction

- Node selection immediately changes all node charts and capacity. Each plot
  names its subject. This reads the already-authorized node collection and
  does not issue requests on keyboard/mouse movement.
- `o` switches between selected-node history and cluster per-node peaks. Peak
  plots name the owning node and use that node's observation timestamp.
- Percentages use a fixed 0–100% axis. Other units use their actual unit and
  configured limits when present. Capacity shows used/total or used/limit.
- Histories grow to the selected window, preserve gaps and spikes, and retain
  separate identities across selection, sorting and refresh. At most 128 rows
  plus the selected row retain history in the current view; removed rows are
  dropped. Each series remains bounded by the existing hour/point limit.
- Pausing freezes chart time and rejects in-flight refreshes from the frozen
  display. Resuming restores live time. Receipt time makes newly arrived data
  immediately visible without waiting for the next clock tick.
- Rendering can respond at 60 FPS. Data acquisition intervals, request limits,
  authentication and action confirmation remain independent.
- Existing plain/JSON CLI contracts and server HTTP contracts are unchanged.

## Validation

- TUI and CLI unit tests, TUI race tests, and the repository prepush gate pass.
- Regression tests cover row/metric identity, reordering, source timestamps,
  null capacity, terminal-control sanitization, fixed percentage axes, pause,
  receipt time, small terminals and mouse coordinates.
- Eight PTY scenarios pass across truecolor, 256-color, basic ANSI and NO_COLOR:
  startup 78–111 ms, input P95 18.1–18.8 ms, zero HTTP writes and no leaked
  subscriptions. The entire node selection/overview scenario performs one
  node-list request and one policy request. See the accompanying PTY receipt.
- Actual production cluster: selected-node CPU/memory/disk match the table;
  switching rows, toggling overview, real history and 0–100% axes all pass;
  q restores the alternate screen and mouse state.
- 1000-row dashboard benchmark at 200×50: 4.9 ms/frame and 1.65 MB/frame without
  color; 5.5 ms/frame and 1.95 MB/frame with color on Apple M1 Pro.
- Before optimization, repeated row scans cost approximately 10 ms/frame and
  5.27 MB/frame. Early PTY runs failed at input P95 63.9 ms and 114.6 ms. Removing
  repeated unfiltered row scans and raising the render cap resolved the tested
  input latency failures without changing the 50 ms acceptance threshold.
- The first launch of a newly built candidate measured 2694 ms and failed the
  existing 1000 ms startup assertion. Subsequent complete runs after invoking
  the binary's version command passed. The cold measurement is retained as a
  limitation rather than counted as a pass.

Synthetic screenshot: [cluster dashboard](cli-tui-acceptance-2026-09-09/redesign-cluster.png).

## Released and installed

- Official release: `v0.4.0`, code commit `98a6afb`, built at
  `2026-09-09T13:57:17Z`. Release workflow `34360183496` completed successfully
  with six platform archives and checksums; main CI `34360177852` also passed.
- `/opt/homebrew/bin/fugue` was upgraded through `fugue upgrade`; `--version`
  confirms this release and `upgrade --check` reports `up-to-date`.
- The installed binary passed the real-cluster check: selected CPU/memory/disk
  values agree with the table, keyboard selection and overview switching work,
  real curves use fixed percentage axes, and q restores terminal state.
- All eight installed-binary PTY scenarios passed: first paint 79–529 ms,
  input P95 at most 30.84 ms, zero writes and zero active subscriptions.
  See [installed receipt](cli-tui-acceptance-2026-09-09/redesign-installed-pty.json).
- The ten-minute integration soak passed 972 loads, page switches, resizing,
  pause/resume and simulated outages. Heap fell from 6,848,128 to 5,713,584 bytes;
  goroutines fell from 11 to 5; retained logs stayed at 2000 and active streams
  returned to zero. See [soak receipt](cli-tui-acceptance-2026-09-09/redesign-soak.txt).
