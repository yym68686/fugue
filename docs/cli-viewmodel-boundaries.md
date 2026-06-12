# Fugue CLI View Model Boundaries

Date: 2026-06-13

This document records the P2 view model extraction boundary for the Terminal Console work.

## Package Boundary

The shared CLI view models live in:

```text
internal/cli/viewmodel
```

The package depends only on `internal/model` and standard library packages. It must not import command renderers, cobra commands, HTTP clients, or terminal UI widgets.

The current bridge from existing CLI aggregation code lives in:

```text
internal/cli/viewmodel_bridge.go
```

The bridge is intentionally thin. It lets existing command fan-out code produce product-level views without forcing every renderer migration into P2.

## Current View Models

- `AppHealthView`
- `ProjectWorkbenchView`
- `RoutePathView`
- `OperationTimelineView`
- `RuntimeCapacityView`
- `DiagnosisEvidenceView`
- `ActionPlanView`

Each view carries a shared `State`:

- `ready`
- `loading`
- `empty`
- `error`
- `permission`
- `offline`

This gives rich text, monitor, and full TUI renderers a consistent way to display loading gaps, empty inventories, API failures, and permission failures.

## Source Of Truth

The view model fields are derived from the existing Go model layer, which is generated from or aligned with the OpenAPI-first backend contract:

- `model.App`
- `model.AppRoute`
- `model.Operation`
- `model.Project`
- `model.Runtime`
- `model.BackingService`
- `model.OperationDiagnosis`

No new backend API field was added for P2. If later TUI work needs fields that do not exist in `internal/model`, the change must start in `openapi/openapi.yaml`, then follow the normal generation and frontend sync workflow.

## Extraction Mapping

- `app_overview.go` can build `AppHealthView` and `DiagnosisEvidenceView` from `appOverviewSnapshot`.
- `app_runtime_diagnosis.go` can build `DiagnosisEvidenceView` from `appDiagnosis`.
- `project_overview.go` / project status paths can build `ProjectWorkbenchView` from console project detail, services, apps, and operations.
- `ops.go` can build `OperationTimelineView` and operation diagnosis evidence from `model.Operation` and `model.OperationDiagnosis`.
- Runtime and admin cockpit paths can build `RuntimeCapacityView` from `model.Runtime`.
- TUI safety paths build `ActionPlanView` before any restart, redeploy, or operation-cancel style action is enabled.

## Web / Terminal Alignment

The current Web / Terminal convergence decision is to reuse existing console, project, operation, runtime, cluster, edge, DNS, and admin snapshot endpoints for the preview instead of introducing a new semantic endpoint immediately.

When the Web console and Terminal console describe the same object, use the same names:

- Project workbench
- App health
- Route health
- Operation timeline
- Runtime capacity
- Admin cockpit

When either surface needs a new field that is not present in `internal/model` or the OpenAPI contract, the change must begin in `openapi/openapi.yaml`, followed by backend generation/tests and frontend contract sync before the CLI view model consumes it.

## Safety Model

`ActionPlanView` is the only supported bridge from a TUI selection to a mutating operation. A plan must name the action, target, destructive flag, expected operation, exact confirmation text, and next commands.

If the backend lacks a typed OpenAPI endpoint for the action, the plan remains unavailable. The Terminal Console should not call raw or guessed endpoints from a UI gesture.

## Renderer Rule

New rich text, monitor, and full TUI renderers should consume view models, not raw API responses.

Legacy text renderers may continue to consume existing models until their command is migrated. When a command enters the rich renderer allowlist, the command should:

1. Load API data.
2. Build a view model.
3. Render from the view model.
4. Keep `--json` output on its existing compatibility contract.

This avoids a big-bang rewrite while still making the target boundary explicit.
