# Fugue CLI Output Compatibility

Date: 2026-06-13

This document freezes the output boundaries that must survive the Terminal Console / rich output work. The intent is to let human-facing text evolve while keeping machine-facing output stable.

## Machine Output Contract

- `--json` and `--output json` are the machine contract.
- JSON is written to stdout only.
- Progress, warnings, update notices, and transient polling status are written to stderr only.
- JSON output must not contain ANSI escape sequences, panel borders, interactive prompts, cursor movement, or terminal control bytes.
- `--output-file` mirrors the command stdout payload exactly. It must not receive extra stderr progress or terminal control bytes.
- Non-TTY stdout defaults to JSON or plain text. It must not enable rich text or TUI controls automatically.
- Exit codes are part of the compatibility surface:
  - `0`: success
  - `2`: user input error
  - `3`: permission denied
  - `4`: not found
  - `5`: system fault
  - `6`: indeterminate / uncategorized failure

## Current Output Call-Site Inventory

Inventory source: `rg` over `internal/cli` on 2026-06-13.

- `wantsJSON()` is used across the command tree to split machine output from text output. High-use files include `admin.go`, `data.go`, `runtime.go`, `app.go`, `project.go`, `app_release.go`, `admin_cluster_diagnostics.go`, and `ops.go`.
- `writeJSON()` is the single JSON encoder helper in `internal/cli/output.go`; command handlers call it directly for JSON branches.
- `--output-file` is centralized in `root.go` through `configureOutputWriter`; tests and help docs reference the flag, and there is no separate `writeOutputFile` helper.
- `progressf()` is the primary stderr progress helper. It suppresses progress when `wantsJSON()` is true.

Files with output-mode branches should be treated as migration-sensitive. Any rich renderer migration must leave their JSON branches unchanged unless a separate compatibility note and test update is made.

## Existing JSON Output Commands

The current command tree uses JSON branches broadly. The first compatibility inventory groups them by command family rather than listing every individual subcommand:

- Root / auth / account selection: `auth`, `tenant`, `workspace`.
- App inventory and detail: `app ls`, `app status`, `app overview`, `app diagnose`, `app source show`, `app route`, `app storage`, `app resources`, `app workload`, `app command`.
- App mutation results: `app create`, `app deploy`, `app restart`, `app scale`, `app start`, `app stop`, `app remove`, `app release`, `app failover`, `app network`.
- App env and dependencies: `app env`, `env`, `app service`, `service`, `app db`, `app db query`, `app db import`, `app db restore`.
- Operations: `operation ls`, `operation show`, `operation explain`, `operation watch`, `operation audit`.
- Project: `project ls`, `project show`, `project overview`, `project apps`, `project ops`, `project watch`, `project split`, `project move`, `project images`, `project routes`, `project verify`.
- Terminal console and monitors: `console`, `project watch`, `operation watch`, `admin cluster top`, `admin cockpit`.
- Runtime and admin: `runtime`, `admin runtime`, `admin cluster`, `admin dns`, `admin edge`, `admin domains`, `admin users`, `admin node-updater`, `admin discovery`, `admin autonomy`, `admin billing`.
- Diagnostics and raw access: `api request`, `diagnose timing`, `diagnose fs`, `logs collect`, `logs query`, `debug bundle`, `web diagnose`, `workflow run`, `data`.

The current field-structure inventory is tracked at payload-root level. Nested object fields intentionally remain owned by `internal/model` and the OpenAPI contract instead of being hand-copied here as a second schema.

| Command surface | JSON payload root |
| --- | --- |
| `tenant ls` | `{ "tenants": [...] }` |
| `tenant show` | `{ "tenant": {...} }` |
| `workspace resolve/show` | endpoint response object |
| `auth status/login/logout` | auth state / key-value response object |
| `app ls` | `{ "apps": [...] }` |
| `app status` | `{ "app": {...}, "active_operations": [...] }` |
| `app overview` | `appOverviewSnapshot`: `{ "app", "domains", "bindings", "backing_services", "operations", "images", "pod_inventory", "diagnosis" }` |
| `app diagnose` | `{ "diagnosis": {...} }` or observability diagnosis object when `--window` is used |
| `app source show` | `{ "app": {...} }` |
| `app route show/check/set` | route response objects |
| `app storage show/set/reset/disable` | `{ "app": {...}, "operation": {...}, ... }` response objects |
| `app resources show/set/clear/recommend/apply/auto` | resource response objects |
| `app workload show/set/clear` | workload response objects |
| `app command show/set/clear` | `{ "app": {...}, "operation": {...}, "already_current": bool }` style response |
| `app env ls/export/set/unset` / `env ls/export/set/unset` | `envCommandResult`: `{ "app_name", "app_id", "env", "entries", "operation", "already_current" }`; text export writes reusable `.env` lines |
| `app env generated show/set/unset` | `{ "app": {...}, "generated_env": {...}, "operation": {...}, "already_current": bool }` |
| `app service ls/attach/detach` | binding / service response objects |
| `service ls/show/create/remove` | `{ "services": [...] }`, `{ "service": {...} }`, or operation response objects |
| `app db show/configure/disable/switchover/localize` | database state and `{ "app": {...}, "operation": {...}, ... }` mutation responses |
| `app db query` | app database query response: `{ "database", "host", "user", "columns", "rows", "row_count", "max_rows", "read_only", "duration_ms", "truncated" }` |
| `app db import/status/access/restore` | database import, access, and restore response objects |
| `app logs runtime/build/query/table/pods` | runtime/build log response objects or structured query result |
| `app request` / `app request stream` / `app request compare` | app request diagnostic result objects |
| `app create/deploy/restart/scale/start/stop/remove` | `{ "app": {...}, "operation": {...}, ... }` mutation responses |
| `app release ...` | release tracking, deploy, rollback, prune, and policy response objects |
| `app failover ...` | failover assessment / operation response objects |
| `operation ls` | `{ "operations": [...] }` |
| `operation show` | `{ "operation": {...} }`, plus diagnosis when available |
| `operation explain` | `{ "operation": {...}, "diagnosis": {...} }` |
| `operation watch` | `{ "operation": {...} }` |
| `operation audit` | `{ "audit_events": [...] }` |
| `console` | `console.View`: `{ "state", "preview", "active_page", "summary", "tables", "actions", ... }` |
| `project ls` | `{ "projects": [...] }` |
| `project show` | `{ "project": {...} }` |
| `project overview` | `{ "project": {...}, "summary": {...}, "status": {...}, "services": [...], "domains": [...], "databases": [...] }` |
| `project apps` | `{ "apps": [...] }` |
| `project ops` | `{ "operations": [...] }` |
| `project watch` | project overview snapshot / stream-derived response |
| `project move/split/delete/routes/images/verify` | plan, mutation, route, image usage, or verification response objects |
| `runtime ls/show/doctor/enroll/access` | runtime and runtime diagnostic response objects |
| `admin cluster top` | `clusterTopPayload`: `{ "cluster_nodes", "runtimes", "control_plane", "node_policy_summary", "node_policies" }` |
| `admin cockpit` | `adminCockpitPayload`: `{ "tenants", "projects", "runtimes", "cluster_nodes", "node_policy_summary", "edge_nodes", "dns_nodes", "control_plane", "users", "route_trace", "warnings", "release_path" }` |
| `admin api-keys/node-keys/runtime/cluster/dns/edge/domains/users/node-updater/discovery/autonomy/billing` | admin response objects, usually rooted by the resource name, for example `{ "runtimes" }`, `{ "cluster_nodes" }`, `{ "control_plane" }`, `{ "users" }` |
| `api request` / `curl` | raw HTTP diagnostic: `{ "method", "url", "status", "status_code", "headers", "body", "body_encoding", "body_size", "timing" }` |
| `diagnose timing` | `timingCommandResult`: `{ "command", "requests", "error" }` |
| `diagnose fs` | `filesystemDiagnosisResult`: `{ "schema_version", "app", "source", "path", "failure_class", "diagnosis", "pods", "events", "redacted", ... }` |
| `logs collect` | `logsCollectResult`: `{ "schema_version", "app", "operation", "timeline", "sources", "warnings", "redacted", ... }` |
| `logs query` | `logsQueryResult`: `{ "schema_version", "app", "summary", "entries", "warnings", "redacted", ... }` |
| `debug bundle` | debug bundle manifest: `{ "schema_version", "archive", "files", "redacted", ... }` |
| `workflow run` | workflow run result: `{ "schema_version", "status", "steps", "extracted", ... }` |
| `data ...` | data workspace, snapshot, transfer, backend, grant, and GC response objects |
| `web diagnose` | web diagnostic result with target path, status code, timing, and response summary |

Baseline tests for the Terminal Console work start with:

- `app status --json`
- `app overview --json`
- `app diagnose --json`
- `operation explain --json`
- `project overview --json`
- `console --json`
- `project watch --json`
- `operation watch --json`
- `admin cluster top --json`
- `admin cockpit --json`

## Text Output Protection Classes

### Copy-Sensitive Text

These commands must remain plain and copyable by default. Rich terminal output must not become their default behavior:

- `app env ls/show <app>` and the compatibility alias `env ls/show <app>`.
- `app env export <app>` and `env export <app>`; exported content must stay reusable `.env` text.
- `app db query <app>`.
- `api request`.
- `logs collect`.
- `logs query`.
- `debug bundle`.
- `diagnose fs`.
- `diagnose timing`.
- Raw diagnostics and compatibility aliases such as `curl`.

### First Rich Text Allowlist

Only these commands are in the first rich text migration allowlist:

- `app status <app>`
- `app overview <app>`
- `app diagnose <app>`
- `operation explain <operation>`
- `project overview <project>`

Rich output for this allowlist must still:

- Keep `--json` unchanged.
- Fall back to plain text for non-TTY stdout.
- Respect `NO_COLOR` and future `--color=never`.

## Experimental Terminal Surfaces

These commands are explicit human-facing terminal surfaces and may change text layout while the preview evolves:

- `console`
- `project watch`
- `operation watch`
- `admin cluster top`
- `admin cockpit`

Their compatibility rules are narrower than stable JSON commands:

- `--json` remains stable machine output and must contain no terminal controls.
- `--once` renders one deterministic snapshot for tests, tickets, and docs.
- `--plain` renders scrollback-friendly text and must work without a TTY.
- Interactive paths may use alternate screen only when explicitly enabled or when terminal mode negotiation succeeds.
- Ctrl+C summaries go to text output, not JSON.
- Avoid hiding copyable next commands, URLs, IDs, or error evidence.

## Redaction Boundaries

- User-initiated env viewing is raw by default: `app env ls/show` and `env ls/show` show real values so users can compare, copy, migrate, and debug.
- Diagnostic bundles, operation outputs, status outputs, and shareable evidence default to redacted values.
- Unredacted diagnostic output requires `--redact=false --confirm-raw-output`.
- `--show-secrets` is scoped to commands that explicitly expose app / operation secrets.

## Stdout / Stderr Boundary

- stdout is for the command payload: JSON, plain tables, key/value text, copied diagnostics, and exported data.
- stderr is for progress, warnings, update notices, retry notices, and non-payload status.
- `progressf()` must stay quiet in JSON mode.
- New monitor and TUI code must not write terminal control sequences to stdout when stdout is not a TTY.

## Review Checklist

Before changing output for any command:

- Identify whether the command is machine contract, copy-sensitive text, or rich-text allowlist.
- Add or update a JSON compatibility test if the command has `--json`.
- Add a non-TTY no-ANSI test if the command has plain text output.
- Keep progress and warning text on stderr.
- Do not change env raw visibility defaults.
- Do not add OpenAPI fields from CLI guesses; update `openapi/openapi.yaml` first when API shape changes.
