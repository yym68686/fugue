# Safe Zero Downtime Rollout Runbook

Safe zero downtime rollout protects user traffic by separating the old stable
release from a new candidate release. It is intentionally stronger than strict
drain: old connections are preserved, and a bad candidate is not allowed to take
over production traffic.

## Baseline service contract

Every app with a cluster Service and positive desired replicas is zero downtime
by default. Normal deploys use `RollingUpdate` with `maxUnavailable=0` and
`maxSurge=1`. If Fugue cannot validate an online plan for a running service, the
controller refuses the operation before changing Kubernetes desired state. To
authorize downtime, stop the app first, perform the maintenance change, and then
start it again.

`continuity.zero_downtime` is an advanced policy, not the switch for this
baseline guarantee. `drain_only` configures an explicit policy and `safe` adds
stable/candidate releases, canary gates, and rollback behavior. Inspect both the
configured and effective state with:

```bash
fugue app continuity show <app>
# zero_downtime_enabled=true
# zero_downtime_configured=false
# zero_downtime_effective=true
# zero_downtime_source=service-default
```

## Normal behavior

For an app with `continuity.zero_downtime.mode=safe`, a deploy should additionally produce:

1. `candidate_create`: Fugue creates an AppRelease with role `candidate`.
2. `candidate_ready`: the candidate runtime revision has an upstream URL and is ready.
3. `gate_check`: active probes run before real traffic.
4. `canary_shift`: candidate traffic moves to the configured step weight.
5. `canary_gate`: active probes and passive request metrics pass for that step.
6. `final_gate`: the candidate is checked once more before full promotion.
7. `promote`: candidate becomes `stable`; the old stable becomes `previous/draining`.

If a gate fails, Fugue records `app_release_gate_failure` evidence, sets
candidate traffic to zero, marks the candidate failed, and keeps traffic on the
previous stable release.

## First checks

```bash
fugue app continuity show <app>
fugue app continuity audit <app>
fugue app release ls <app>
fugue app release traffic <app>
fugue app release attempts <app>
```

Expected traffic after a healthy promote:

- `mode=single`
- `stable_weight=100`
- `candidate_weight=0`
- current stable release has `rollback_target_release_id`
- previous stable release is `role=previous`, `status=draining`

Expected traffic after an abort:

- `mode=single`
- `stable_weight=100`
- `candidate_weight=0`
- candidate release is `status=failed`
- release timeline has failed `gate_check`, `canary_gate`, or `final_gate`

## Debug bundle

Fetch the scoped debug bundle first:

```bash
fugue app release debug-bundle <app> --attempt <release-attempt-id>
```

Use the API-level global summary only when comparing platform-wide evidence:

```bash
curl -sS "$FUGUE_API_URL/v1/apps/<app-id>/release-attempts/<attempt-id>/debug-bundle?include_global_summary=true" \
  -H "Authorization: Bearer $FUGUE_API_KEY"
```

Do not paste secrets from bundles into tickets or chats.

## Metrics

Controller metrics expose low-cardinality safe rollout counters:

- `fugue_safe_rollout_steps_total{phase,status}`
- `fugue_app_release_gate_failures_total{confidence}`
- `fugue_safe_rollout_abort_restore_total{phase,status}`

Use these to determine whether failures are isolated to one app or systemic.

## Triage

1. If active probe failed, check candidate upstream health and probe path first.
2. If passive metrics failed, inspect release-id-scoped request facts for 5xx,
   upstream errors, TTFB, and total duration.
3. If canary traffic was shifted and then failed, verify the traffic policy has
   returned to stable-only.
4. If the old stable is still `previous/draining`, this is expected during the
   rollback window. It must not receive new traffic after stable-only restore.
5. If the candidate is healthy but metrics are unavailable, treat that as an
   observability gap. Do not manually promote unless active probes and direct
   request tests are clean.

## Rollback

The normal rollback is automatic: failed gates call abort and restore
stable-only traffic. Manual rollback should use AppRelease APIs or CLI commands;
do not patch edge route bundles directly unless this is an emergency recovery.

```bash
fugue app release abort <app> <candidate-release-id> --mark-failed --reason "manual rollback"
fugue app release traffic <app> --stable 100 --candidate 0 --mode single
```

After any emergency manual action, backport the intended behavior into the Fugue
repository and release through the control plane pipeline.
