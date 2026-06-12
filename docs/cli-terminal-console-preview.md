# Fugue Terminal Console Preview

Date: 2026-06-13

This document defines the first Terminal Console release: a human-facing terminal workbench beside the Web console and the existing machine-facing JSON CLI.

## Three Entry Points

- Web console: browser product surface for project management, account flows, docs, auth, and team workflows.
- Terminal console: human terminal surface for dense project, operation, runtime, and admin investigation.
- JSON CLI: stable machine surface for agents, jq, scripts, and CI.

The Terminal Console does not replace existing commands. It is an explicit preview entered through:

```bash
fugue console
fugue console --plain
fugue console --project marketing --plain
fugue console --admin --mouse --alt-screen
```

The machine contract remains:

```bash
fugue console --json
fugue project watch marketing --json
fugue operation watch op_123 --json
fugue admin cluster top --json
fugue admin cockpit --json
```

## Shared Information Architecture

Web console and Terminal console should use the same product semantics:

- Project workbench: project identity, app list, route status, backing services, latest operations, and next commands.
- App health: app phase, runtime placement, route URL, replica count, active operation, diagnosis, and logs.
- Route health: public route, runtime target, edge/DNS evidence, and check status.
- Operation timeline: operation id, type, status, phase, queue or controller timing, diagnosis, and resume command.
- Runtime capacity: runtime id/name, status, type, access mode, node, pool, region, and health gate.
- Admin cockpit: tenants, projects, users, runtime capacity, cluster nodes, node policy drift, edge, DNS, control-plane rollout, and release path.

Terminal layout can be denser than the Web console, but naming should stay aligned so a user can move between both surfaces without relearning state names.

## Shared State Language

Use these states consistently in Web copy, Terminal copy, and view models:

- `ready`: data loaded and usable.
- `loading`: initial data is still loading.
- `empty`: the resource exists but has no rows or no matching rows.
- `error`: the control plane returned an application error.
- `permission`: the current key/session cannot read the requested resource.
- `offline`: the API, network, or terminal session could not reach the control plane.
- `running`: an operation or monitor target is actively progressing.
- `pending`: the operation is queued or waiting for another dependency.
- `completed`: the operation reached a successful terminal state.
- `failed`: the operation reached a failed terminal state.
- `degraded`: the resource is reachable but one or more health gates failed.

Operation copy should include the next command when possible, for example `fugue operation watch op_123`, `fugue operation explain op_123`, or `fugue app logs runtime web --follow`.

## Current Endpoint Decision

The preview does not add new console semantic endpoints. It builds on existing control-plane endpoints already used by CLI and Web console paths:

- `/v1/console/gallery`
- `/v1/console/projects/{id}`
- runtime, cluster, node-policy, edge, DNS, tenant, and operation endpoints
- optional fugue-web admin page snapshot routes for user summaries

If a future console page needs fields that are not in the OpenAPI contract, the change must start in `openapi/openapi.yaml`, then follow code generation, backend tests, frontend sync, and contract checks before the TUI consumes it.

## Monitor Commands

The first monitor commands are high-density, mostly read-only views:

```bash
fugue project watch marketing
fugue project watch marketing --once
fugue project watch marketing --plain --filter api
fugue project watch marketing --sort APP

fugue operation watch op_123
fugue operation watch op_123 --once --sort STEP
fugue operation watch --app web --plain --interval 10s

fugue admin cluster top
fugue admin cluster top --once
fugue admin cluster top --plain --filter runtime-a
```

Rules:

- `--once` renders one deterministic frame and exits.
- `--plain` forces scrollback-friendly output.
- `--filter` narrows rows by substring.
- `--search` highlights or focuses matching rows where the renderer supports it.
- `--sort` sorts by a visible column when available.
- Ctrl+C prints a final summary and a resume command.

## Terminal Fallback

Terminal output must stay readable and safe in constrained environments:

- Non-TTY stdout: no alternate screen, no raw mode, no cursor movement, no ANSI controls.
- `NO_COLOR`: suppress color while keeping text structure.
- `--color=never`: suppress ANSI even on a TTY.
- Low width: prefer single-column sections and wrapped copy blocks over truncated commands.
- SSH/tmux: avoid assumptions about mouse support or truecolor support.
- Windows Terminal: avoid hard dependency on non-portable control sequences.
- CI logs: prefer `--json`, `--plain`, or `--once`.

The preview uses alternate screen only for explicit interactive paths such as `--alt-screen`; snapshot and plain modes should stay copyable.

## Dangerous Actions

The Terminal Console preview is read-only by default. Any future mutating TUI action must first render an `ActionPlanView` with:

- action name
- target resource
- destructive flag
- effect summary
- expected operation
- confirmation text
- next commands

The user must type the exact confirmation text, such as:

```text
restart app web in project project_123
redeploy app web
cancel operation op_123
```

No TUI mutating action should invent a backend call. If the OpenAPI contract lacks an endpoint, the action remains unavailable and the console should point to the existing safe command or explain that the action is not enabled.

## Snapshot Review

Visible TUI changes must follow [CLI TUI snapshot testing](cli-tui-snapshot-testing.md). At minimum, review:

- wide, narrow, and tight widths
- color and no-color output
- loading, empty, error, permission, and offline states
- long names and long diagnostic evidence
- no ANSI/control bytes in non-TTY snapshots
- copy blocks for commands, operation ids, URLs, and env values

## Rich Output Changelog

Rich output changes must be recorded as user-visible CLI changes. The first release is split into:

- Text upgrades: richer output for allowlisted commands such as `app status`, `app overview`, `app diagnose`, `operation explain`, and `project overview`.
- Experimental commands: `console`, `project watch`, `operation watch`, `admin cluster top`, and `admin cockpit`.
- Compatibility guarantee: `--json`, `--output json`, copy-sensitive env export, raw API diagnostics, debug bundles, and log exports remain stable machine outputs.

Release notes should call out whether a change affects human text only or creates a new experimental command.

## Feedback Entry

Collect Terminal Console feedback through a dedicated issue label or issue title prefix:

```text
[cli-tui] <short problem>
```

Ask users to include:

- command and flags
- terminal app, `TERM`, width, and whether SSH/tmux was involved
- screenshot or copied plain output
- expected workflow
- whether the problem is readability, keyboard flow, mouse flow, contrast, wrapping, latency, or safety

Do not ask users to paste real secrets. For env, API, and debug output issues, prefer redacted fixtures unless the investigation explicitly requires local raw values.
