# Fugue Image Tracking Observability Plan

This document captures the observability plan for Fugue external image tracking.
It is motivated by a production investigation where an app source image tag had
advanced in the upstream registry, but Fugue did not provide enough evidence to
prove whether the automatic tracker skipped, failed, lagged, or never observed
the new digest before a manual sync was triggered.

The immediate goal is not to change image tracking behavior. The first goal is
to make every controller decision explainable after the fact.

## Incident Evidence

For `uni-api-web-api`, Fugue image tracking was enabled for:

```text
docker.io/yym68686/uni-api-backend:main
```

Known production facts from the investigation:

- At `2026-07-02T15:22:22Z`, image tracking queued an automatic import for
  `sha256:17a996...`.
- At `2026-07-02T15:22:54Z`, the corresponding automatic deploy completed.
- Docker Hub updated the tracked tag to `sha256:6bc02...` at
  `2026-07-02T15:28:38Z`.
- No new image-tracking import operation was recorded before a manual bootstrap
  import was triggered at `2026-07-02T15:33:45Z`.
- After the manual import, Fugue tracking state showed `last_seen_digest` as
  `sha256:6bc02...`, but the app was already deployed to that digest, so later
  polls were no-ops.

The missing evidence is the important part: Fugue did not retain per-poll
decision records. Therefore the investigation could not prove whether the
tracker:

- did not poll during the window,
- polled and still saw the old remote digest,
- failed to resolve the remote digest,
- skipped because of an active operation,
- suppressed a retry,
- decided the app was already current, or
- was delayed by controller leader-loop or reconcile work.

## Principle

All automatic controller decisions must leave decision evidence, not only action
evidence.

Operations are action records. They show what the system actually queued or
deployed. They are not enough to explain why the system did not queue something.

Image tracking needs durable decision records for both positive and negative
paths.

## Goals

- Record enough data to explain every image tracking poll after the fact.
- Make no-op and skip decisions as visible as queued imports.
- Keep app deploy and request paths unaffected when observability storage is
  unavailable.
- Keep metrics labels low-cardinality.
- Expose the evidence through CLI commands so operators do not need to combine
  registry APIs, operation history, controller logs, Kubernetes leases, and
  manual database queries.

## Non-goals

- Do not change automatic update behavior in the first implementation slice.
- Do not make image tracking depend on an external observability backend.
- Do not store credentials, registry auth tokens, image config secrets, or raw
  environment values in decision history.
- Do not add app-specific or repository-specific heuristics.
- Do not expose high-cardinality Prometheus labels by default.

## Decision History

Add a bounded control-plane history table for image tracking checks. A possible
shape:

```text
fugue_app_image_tracking_checks
  id
  tenant_id
  app_id
  tracking_id
  image_ref
  observed_digest
  current_app_digest
  last_queued_digest
  last_deployed_digest
  decision
  skip_reason
  operation_id
  active_operation_id
  resolver_error
  checked_at
  duration_ms
  controller_pod
  controller_leader_identity
```

Suggested `decision` values:

```text
queued
no_change
replicas_zero
active_operation
retry_suppressed
resolver_error
queue_conflict
queue_error
already_deployed
```

Retention should be bounded. A practical default is either:

- keep the last N checks per app, or
- keep checks for a short time window such as 7 days.

This history is diagnostic state. It is not billing data, audit data, or a
product-facing deployment source of truth.

## Structured Controller Logs

The controller should emit one structured log for each image tracking decision.
Queued and error decisions are not enough; no-op decisions must also be visible.

Example:

```json
{
  "event": "image_tracking_decision",
  "tenant_id": "tenant_123",
  "app_id": "app_123",
  "tracking_id": "imgtrack_123",
  "image_ref": "docker.io/example/api:main",
  "observed_digest": "sha256:remote",
  "current_app_digest": "sha256:deployed",
  "last_queued_digest": "sha256:queued",
  "last_deployed_digest": "sha256:deployed",
  "decision": "queued",
  "skip_reason": "",
  "operation_id": "op_123",
  "active_operation_id": "",
  "duration_ms": 842,
  "controller_pod": "fugue-controller-abc",
  "controller_leader_identity": "fugue-controller-abc"
}
```

Log records are for live troubleshooting. The database history is for durable
post-incident analysis. They should contain the same decision vocabulary.

## Metrics

Metrics should answer whether tracking is alive, whether it is lagging, and
which decisions are happening.

Suggested metrics:

```text
fugue_image_tracking_decisions_total{decision}
fugue_image_tracking_last_check_timestamp_seconds
fugue_image_tracking_last_queued_timestamp_seconds
fugue_image_tracking_last_error_timestamp_seconds
```

Avoid labels such as raw image ref, full app id, operation id, delivery id, or
digest in high-cardinality Prometheus metrics. App-specific detail belongs in
the tracking history API and CLI, not global scrape labels.

Controller health metrics should also expose:

```text
fugue_controller_leader_active{identity}
fugue_controller_active_loop_running
fugue_controller_active_loop_started_timestamp_seconds
fugue_controller_image_tracking_sync_running
fugue_controller_image_tracking_sync_started_timestamp_seconds
fugue_controller_last_image_tracking_sync_timestamp_seconds
fugue_controller_image_tracking_sync_duration_seconds
fugue_controller_image_tracking_sync_last_error_timestamp_seconds
```

These metrics make it possible to distinguish image tracking lag from controller
leader-loop lag.

## CLI And API Surfaces

Extend the existing image tracking surface.

Existing command:

```text
fugue app release tracking <app>
```

Proposed commands:

```text
fugue app release tracking history <app>
fugue app release tracking diagnose <app>
```

`history` should show recent decision records:

```text
CHECKED_AT            DECISION          OBSERVED_DIGEST  CURRENT_DIGEST  OPERATION  REASON
2026-07-02T15:29:40Z  no_change         sha256:old       sha256:old      -          remote digest unchanged
2026-07-02T15:30:40Z  queued            sha256:new       sha256:old      op_123     remote digest changed
```

`diagnose` should show a compact current-state explanation:

```text
image_ref=docker.io/example/api:main
remote_digest=sha256:new
current_app_digest=sha256:old
last_check=2026-07-02T15:29:40Z
last_decision=resolver_error
last_error=...
next_expected_check<=2026-07-02T15:30:40Z
active_operation=none
```

`fugue app diagnose <app>` should include this image tracking section when the
app uses a tracked Docker image source.

Implemented API endpoints:

```text
GET /v1/apps/{id}/image-tracking/history?limit=50
GET /v1/apps/{id}/image-tracking/diagnosis
```

Common diagnosis output shapes:

```text
category=active_operation
summary=image tracking is waiting for an active app operation to finish
active_operation_id=op_123
evidence=latest check decision=active_operation at=2026-07-02T15:29:40Z
```

```text
category=resolver_error
summary=image tracking could not resolve the remote digest
warning=registry timeout
```

```text
category=already_deployed
summary=tracked image digest already matches the app desired source
remote_digest=sha256:6bc02...
current_app_digest=sha256:6bc02...
```

## Unified Timeline

App diagnosis should eventually render one timeline for image-based delivery:

```text
external image pushed
tracking check observed digest
tracking decision
import operation queued
image imported or mirrored
deploy operation queued
kubernetes apply
rollout ready
runtime pod serving
```

This should remove the need to manually combine upstream registry timestamps,
tracking state, operation history, build logs, controller logs, Kubernetes
leases, and pod state.

## Alerting

After decision history and metrics exist, add alerts for:

- enabled tracking app has no check for more than two configured intervals,
- remote digest differs from current app digest for longer than the threshold,
- resolver errors repeat N times,
- queue errors repeat N times,
- an active operation blocks tracking for longer than the threshold,
- `last_seen_digest` is newer than `last_queued_digest` for longer than the
  threshold.

Alerts should point to `fugue app release tracking diagnose <app>` rather than
requiring an operator to inspect raw tables first.

## Rollout Plan

Phase 1 adds observability without changing behavior:

- Add the decision history table.
- Record one decision row per check.
- Emit structured decision logs.
- Add low-cardinality metrics.
- Preserve existing image tracking queue logic.

Phase 2 exposes the evidence:

- Add history and diagnose API endpoints.
- Add CLI commands.
- Include image tracking in app diagnosis output.

Phase 3 changes behavior only after evidence proves a specific bug:

- Fix scheduler lag, resolver caching, active operation filtering, retry
  suppression, or leader-loop behavior based on decision records.
- Add regression tests for the proven failure mode.

## Todo List

- [x] Define the `fugue_app_image_tracking_checks` schema and retention policy.
- [x] Add store methods for inserting and listing image tracking check records.
- [x] Record a decision row for every enabled tracking check.
- [x] Record explicit skip reasons for replicas zero, active operation, retry
      suppression, resolver error, queue conflict, queue error, no change, and
      already deployed.
- [x] Add structured `image_tracking_decision` controller logs.
- [x] Add low-cardinality image tracking metrics.
- [x] Add controller leader-loop and image tracking sync health metrics.
- [x] Extend OpenAPI with image tracking history and diagnosis endpoints.
- [x] Generate OpenAPI server artifacts.
- [x] Add CLI support for `fugue app release tracking history <app>`.
- [x] Add CLI support for `fugue app release tracking diagnose <app>`.
- [x] Include image tracking evidence in `fugue app diagnose <app>`.
- [x] Add unit tests for each decision value.
- [x] Add a controller integration test for remote digest changed but no active
      operation.
- [x] Add a controller integration test for active operation skip recording.
- [x] Add a controller integration test for resolver error recording.
- [x] Add retention tests so history remains bounded.
- [x] Add documentation examples for common diagnosis outputs.
- [ ] Release through the normal control-plane deployment workflow.
- [ ] Use the next production image-tracking delay to verify whether behavior
      changes are needed.
