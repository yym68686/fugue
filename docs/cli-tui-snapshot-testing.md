# Fugue CLI TUI Snapshot Testing

Date: 2026-06-13

This document defines the review rules for terminal UI output as Fugue adds rich text, monitor views, and full TUI screens.

## What Needs A Snapshot

Add or update a golden snapshot when a change affects visible terminal UI:

- Rich text renderer output.
- Table, panel, status chip, route chain, metric bar, timeline, error block, copy block, or confirm dialog components.
- Monitor first screen, refresh state, pause state, filter/search state, transient API error state, and resize state.
- Full TUI pages and overlays: project list, app detail, logs viewport, command palette, help overlay, danger confirmation.

Do not use TUI snapshots for JSON output. JSON compatibility stays in structural tests and command-specific baseline tests.

## Required Viewports

Every component or screen snapshot should cover the smallest useful set of widths:

- Wide: 120 columns or wider.
- Narrow: 80 columns.
- Tight: 48 columns for single-column fallback when relevant.

When color matters, include:

- Color-enabled semantic output.
- `NO_COLOR` / color-disabled output.

When state matters, include:

- Loading.
- Empty.
- Error.
- Permission denied.
- Long object names and long diagnostic evidence.
- Disabled / unavailable actions.

## Snapshot Review Rules

- Snapshots must not contain secrets, real tenant IDs, real emails, or real customer project names.
- Use neutral synthetic fixtures: `tenant_123`, `project_123`, `app_123`, `demo`, `web`, `api`, `runtime-a`.
- Diff visible semantics, not only pixels: confirm focus, selected row, status, next command, and error evidence remain readable.
- Confirm no terminal control bytes are present in non-TTY snapshots.
- Confirm copy blocks preserve exact commands, URLs, operation IDs, and env values.
- Confirm narrow snapshots do not overlap text or hide critical action labels.

## Update Flow

1. Run the focused renderer or TUI test first.
2. Inspect the snapshot diff manually.
3. Verify the same command still passes its `--json` compatibility test.
4. Verify non-TTY output still has no ANSI escape bytes.
5. Only then update the golden snapshot.

Snapshot updates should be reviewed as product changes, not formatting churn. If a snapshot changes because the renderer was refactored, the PR must say what user-visible behavior changed.

## Current Test Harness

The current base covers the terminal primitives and the first rich / monitor / console surfaces:

- `internal/cli/terminal.GuardedWriter` strips ANSI when a stream is not allowed to emit terminal controls.
- `internal/cli/terminal.Palette` maps semantic roles to terminal color fallbacks.
- `internal/cli/terminal.RunBoundedProbe` tests startup probes with a hard timeout.
- `internal/cli/terminal.RunWithSession` tests terminal restore for success and panic paths.
- `internal/cli/output_compatibility_test.go` protects first-wave JSON and copy-sensitive text output.
- `internal/cli/ui` tests table, panel, status chip, route chain, timeline, metric, error, copy, and danger confirmation rendering.
- `internal/cli/monitor` tests snapshot rendering, refresh acceptance, filtering, sorting, transient error overlays, and Ctrl+C summaries.
- `internal/cli/console` tests page navigation, command palette flow, narrow layout fallback, mouse affordance labels, and preview rendering.
- `internal/cli/monitor_commands_test.go` tests `operation watch --once`, `project watch --once`, and `admin cluster top --once/--json`.
- `internal/cli/console_command_test.go` tests `fugue console --plain`, `--color never`, and `--json`.
- `internal/cli/admin_cockpit_test.go` tests read-only cockpit rendering, JSON output, user redaction, permission errors, tenant failure, empty cluster, and offline node evidence.
- `internal/cli/action_plan_test.go` tests restart, redeploy, and unavailable cancel-operation action plans.

Future renderer packages should build on these primitives and store focused golden files next to the component tests.

## Ergonomics Review

Every TUI or monitor PR should include a short ergonomics note covering:

- Primary workflow: what the user can answer in the first screen.
- Keyboard path: quit, pause, tab/next, palette/help, and safe confirmation behavior.
- Mouse path: whether mouse labels are shown and whether the workflow still works without mouse support.
- Density: whether wide rows can be scanned and narrow rows wrap without hiding commands.
- Safety: whether read-only views are obviously read-only and mutating views require exact confirmation.
- Recovery: whether error, permission, offline, and empty states tell the user what to run next.
