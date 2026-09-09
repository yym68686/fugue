# TUI acceptance harness

All data in this harness is synthetic. It starts a loopback HTTP server and does not contact Fugue production.

```sh
npm ci --prefix scripts/tui-qa
go build -trimpath -ldflags='-s -w' -o /tmp/fugue-tui ./cmd/fugue
FUGUE_TUI_BINARY=/tmp/fugue-tui node scripts/tui-qa/smoke.cjs
FUGUE_TUI_SOAK=10m go test ./internal/tui -run '^TestSoak$' -v -count=1 -timeout 12m
FUGUE_TUI_SNAPSHOT_DIR=/tmp/fugue-screens go test ./internal/tui -run '^TestExportVisualFixtures$' -count=1
FUGUE_TUI_SNAPSHOT_DIR=/tmp/fugue-screens node scripts/tui-qa/screens.cjs
```

Playwright uses its installed Chromium; `FUGUE_TUI_BROWSER` can select an existing Chrome executable. Some macOS distributions of node-pty omit the executable bit on `prebuilds/darwin-arm64/spawn-helper`; restore that bit if `posix_spawnp failed` occurs.

Screenshot exports include startup charts with only three minutes of history and the same charts with a missing observation. Startup history should use the chart width and show `3m / 15m`; the missing observation must remain a visible gap. Interpolation only connects consecutive samples within the declared cadence, with 50% jitter tolerance, and never changes stored observations. Cursor values between observations are labeled `interpolated`.

The PTY checks execute the real binary across truecolor, 256-color, basic ANSI and NO_COLOR environments. They assert the startup budget, mouse tab navigation, app log SSE, 1000-row scrolling, resize, stale retention, recovery, administrator component drilldown, all four public entrypoints, Ctrl-C/q cleanup, and zero writes.

The node scenario additionally checks that keyboard and mouse selection update the metric subject and disk value, and that `o` switches explicitly between selected-node data and cluster peaks. Row selection uses the existing collection snapshot and adds no requests. The rendering cap is 60 FPS for input responsiveness; telemetry still uses its independent collection intervals.

Cluster fixtures cover 60–250 columns, selected-node capacity, workload details, fixed percentage axes and unavailable metrics. To review specific sizes/themes, set `FUGUE_TUI_SCREEN_FILTER='cluster-(200x50|80x30)-(carbon|light)'` when running `screens.cjs`.

The ten-minute test runs 1000 rows, five charts at 200 columns, ten operations, a stream with repeated text, pause/resume, resizing, and synthetic outages. It reports active streams, goroutines, heap and retained logs. Run startup measurements without simultaneous compiler jobs; record failures as well as passing measurements.

For an isolated PostgreSQL integration database, set `FUGUE_TEST_ACTION_DATABASE_URL` and run `TestAppActionConcurrentRetryCreatesOneOperation` in `internal/store`. Never point that variable at production.
