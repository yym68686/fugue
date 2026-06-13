# Fugue backup system plan

This document describes a product-level backup system for Fugue. It is a
design proposal only; it does not prescribe any manual production hotfix path.
The default product posture is intentionally narrow: Fugue backs up only the
control-plane database by default, once per hour, with three successful history
records retained. User apps, app-owned databases, persistent files, and Data
Workspaces are not backed up by default.

The backup system should become a first-class Fugue capability managed through
the Fugue control plane and CLI. It should not remain a collection of ad-hoc
operator scripts.

## 1. Goals

- Back up the Fugue control-plane database by default on a conservative
  platform schedule.
- Keep user workload backup opt-in. No tenant-owned app, app-owned database,
  persistent file, or Data Workspace is backed up until the user explicitly
  enables a backup policy.
- Use platform-configured R2 as the default backend when Fugue has R2 available.
- Let users configure backup backends, policies, schedules, retention, manual
  runs, restore plans, and verification from `fugue` CLI.
- Separate backup from replication and failover. Replication improves
  availability; backups provide historical recovery points and recovery from
  accidental deletion, bad writes, and corrupted replicated state.
- Make every backup observable: last success, next run, failure reason, size,
  checksum, retention status, and restore drill status.
- Make restore safer than backup. A restore must have a plan, preflight checks,
  optional protective backup, verification, and an explicit confirmation before
  destructive cutover.
- Support both hosted Fugue and self-hosted Fugue without hardcoding production
  node names, buckets, projects, apps, or one-off deployment facts.

## 2. Non-goals

- Do not enable backup automatically for all apps.
- Do not enable backup automatically for user app databases.
- Do not treat failover, standby replicas, or VolSync replication as backup.
- Do not require SSH into production nodes as the normal backup or restore path.
- Do not depend on the API being online for control-plane disaster recovery.
- Do not back up external-owned databases unless the user explicitly registers
  them as backup targets.
- Do not hand-maintain API request and response shapes outside the OpenAPI
  contract.

## 3. Current baseline

Fugue already has several partial building blocks:

- `scripts/backup_fugue_postgres.sh` creates one-off custom-format PostgreSQL
  dumps for the control-plane database.
- `docs/ha-dr.md` already distinguishes PostgreSQL failover from backup and
  recommends regular logical backups outside the database cluster.
- The Helm chart has a `controlPlanePostgres` path with CloudNativePG backup
  configuration and a restore drill CronJob seed.
- App-owned managed PostgreSQL already exists through the app/backing-service
  model.
- `fugue app db import` and `fugue app db restore plan/verify` already provide
  parts of an app database restore workflow.
- Fugue Data Workspaces already have object backends, manifests, transfers, and
  snapshots.
- Store promotion has protective backup and restore-readiness concepts for
  control-plane store migration.

The proposed system should consolidate these capabilities into one product
surface rather than create a separate side channel.

## 4. Default policy

Fugue ships with a platform backup baseline:

- Scope: control-plane database only.
- Schedule: once per hour.
- Retention: keep the latest 3 successful backup records and their artifacts.
- Backend selection: use the platform R2 backend automatically when Fugue has
  R2 configured.
- Backend missing state: if no production-safe backend is configured, the
  default policy should report `blocked_no_backend` instead of pretending local
  disk is a durable backup.
- Restore posture: online restore plan and offline restore instructions are
  available, but restore still requires explicit operator action.

User workload backup is disabled by default:

- App managed PostgreSQL backup is off until enabled by the user.
- App persistent storage backup is off until enabled by the user.
- Data Workspace scheduled backup is off until enabled by the user.
- External-owned database backup is off until the user registers a target and
  credentials.

When users manually enable R2-backed app backup, they can customize:

- Backup target selection, for example database only, persistent storage only,
  or both.
- Backup interval.
- Retention count or retention duration.
- Backup version label, such as `before-migration`, `daily`, `release-2026-06`.
- Restore verification checks.

## 5. Backup domains

### 5.1 Platform backup

Platform backup protects Fugue itself.

Primary targets:

- Control-plane authoritative store from `FUGUE_DATABASE_URL`.
- Bundled CloudNativePG control-plane PostgreSQL, when
  `controlPlanePostgres.enabled=true`.
- Control-plane config metadata needed to recover API, controller, cluster join,
  registry, edge, and DNS behavior.
- Data backend configuration and encrypted credential references.
- Audit events, tenants, projects, apps, runtimes, operations, API keys, node
  keys, edge nodes, DNS nodes, route policies, image tracking records, billing
  records, and source upload metadata stored in the control-plane database.

Conditional targets:

- Bundled registry data, only when Fugue is running the registry itself.
- Bundled headscale or mesh control state, only when Fugue is running it itself.
- Bundled DNS or edge persistent state, only when the state is not externalized.

Externalized components:

- External PostgreSQL, registry, secret manager, and load balancer platforms keep
  their own infrastructure-level backups.
- Fugue should still record their configuration, health, and restore-readiness
  checks in platform backup posture.

### 5.2 App backup

App backup protects user workloads managed by Fugue.

App backup is not part of the default baseline. Every app backup policy must be
created explicitly by the user or an authorized tenant operator.

Primary targets:

- App-owned managed PostgreSQL.
- App `persistent_storage` volumes and workspace PVCs.
- Data Workspace snapshots and object manifests.
- App files and secret-backed app files that are represented in the
  control-plane store.
- App env and generated env, through the control-plane store backup.

Conditional targets:

- External-owned app database connections only when the user explicitly creates
  a backup target and supplies credentials.
- Live runtime files only when they are inside a declared persistent mount.

Excluded by default:

- Ephemeral container filesystems.
- Image registry layers for externally hosted registries.
- Third-party SaaS databases or object stores that Fugue only references.

## 6. Architecture

The backup system has five layers.

### 6.1 Control-plane API

The API owns durable backup configuration and exposes status. All API changes
must start in `openapi/openapi.yaml`, then generated route and spec artifacts
must be regenerated.

Responsibilities:

- Validate backup backend configuration.
- Store backup policies.
- List backup runs and artifacts.
- Trigger manual backup runs.
- Create restore plans.
- Record restore verification and restore run status.
- Enforce tenant, app, and platform-admin permissions.
- Append audit events for policy changes, backup runs, retention deletes, and
  restore confirmations.

### 6.2 Scheduler

The scheduler decides when a policy needs a run.

Preferred implementation:

- Add scheduling to the active controller leader loop, or to a dedicated
  `fugue-backup-worker` process if the backup queue grows.
- Use the existing controller leader-election pattern so only one scheduler
  enqueues due runs.
- Persist due run claims in the store to avoid duplicate scheduled runs across
  restarts.

The scheduler should not run heavy backup work inside the controller process.
It should enqueue or create a worker job.

### 6.3 Worker execution

Backup execution should happen in short-lived workers.

Preferred execution modes:

- Kubernetes Job for managed runtime and control-plane Kubernetes deployments.
- Local process mode only for local development and single-binary testing.
- Agent mode later for external-owned runtimes.

Worker responsibilities:

- Acquire a per-target backup lock.
- Build a source inventory.
- Run the backup command or operator-native backup request.
- Upload artifacts to the configured backend.
- Write an artifact manifest.
- Verify artifact checksum and basic readability.
- Update run status and metrics.

### 6.4 Artifact storage

Artifacts are stored outside the source system. For production, the MVP should
support S3-compatible object storage:

- AWS S3.
- Cloudflare R2.
- Backblaze B2 S3 endpoint.
- MinIO.

Development can support a local filesystem backend, but it must be clearly
marked as non-production because it can fail with the source cluster.

Object layout:

```text
backups/
  control-plane/
    <cluster-id>/
      <backup-run-id>/
        manifest.json
        postgres.dump
        store-fingerprint.txt
  apps/
    <tenant-id>/
      <project-id>/
        <app-id>/
          <backup-run-id>/
            manifest.json
            database.dump
            persistent-storage/
            data-workspaces/
```

Each backup directory must contain a self-describing `manifest.json` so a
control-plane restore can be planned even if the Fugue API database is lost.

### 6.5 Retention

Retention is policy-driven and should be implemented as a separate safe sweep.

Retention policy fields:

- Default platform policy keeps the latest 3 successful control-plane database
  backups.
- Keep last N successful backups.
- Keep backups for duration, for example `30d`.
- Keep monthly backups for a longer archive window.
- Never delete backups marked `protected=true`.
- Dry-run sweep before deletion.

Retention deletes should produce audit events and run records.

## 7. Data model

The model should be explicit enough that CLI, Web, automation, and diagnostics
can all use the same API.

### 7.1 BackupBackend

Fields:

- `id`
- `tenant_id`, empty for platform-owned backend
- `name`
- `provider`: `s3`, `cloudflare-r2`, `backblaze-b2`, `minio`, `local-dev`
- `bucket`
- `region`
- `endpoint`
- `prefix`
- `status`
- `capabilities`
- `credential_secret_id`
- `encryption_key_ref`
- `created_at`
- `updated_at`

Credentials must not be stored in plain JSON. Reuse or extend the existing data
backend secret approach.

### 7.2 BackupPolicy

Fields:

- `id`
- `tenant_id`
- `project_id`
- `app_id`, empty for platform policies
- `scope`: `control-plane`, `app`, `project`, `data-workspace`
- `target_kind`: `postgres`, `persistent-storage`, `data-workspace`,
  `registry`, `config`, `all`
- `target_id`
- `backend_id`
- `enabled`
- `schedule`
- `retention`
- `version_label`
- `quiesce_mode`
- `consistency_mode`
- `include`
- `exclude`
- `created_by`
- `created_at`
- `updated_at`

Consistency modes:

- `logical`: `pg_dump`, file scan, object manifest.
- `operator-native`: CNPG Backup, CSI snapshot, VolumeSnapshot.
- `filesystem`: restic/kopia-style file backup.
- `manifest-only`: Data Workspace snapshot reference.

Quiesce modes:

- `none`: online best-effort.
- `read-only`: request app-level write freeze when supported.
- `scale-down`: scale app down before backup.
- `operator`: defer consistency to CNPG/CSI/operator.

Default policy values:

- Platform control-plane database: `schedule="0 * * * *"`, retain 3 successful
  backups, backend auto-selected from platform R2 when available.
- User app/database/storage: no policy exists until user opt-in.

### 7.3 BackupRun

Fields:

- `id`
- `policy_id`
- `tenant_id`
- `project_id`
- `app_id`
- `scope`
- `target_kind`
- `target_id`
- `reason`: `scheduled`, `manual`, `pre-restore-protective`, `drill`
- `version_label`
- `status`: `pending`, `running`, `completed`, `failed`, `canceled`
- `started_at`
- `completed_at`
- `artifact_ref`
- `artifact_count`
- `bytes_total`
- `checksum`
- `error_code`
- `error_message`
- `operation_id`, if backed by Fugue operation
- `worker_id`

### 7.4 BackupArtifact

Fields:

- `id`
- `run_id`
- `kind`
- `uri`
- `size_bytes`
- `sha256`
- `content_type`
- `compression`
- `encryption`
- `created_at`
- `metadata`

Artifact metadata should include:

- Fugue version.
- Schema version.
- OpenAPI contract generation.
- Source runtime id.
- Kubernetes namespace and object names when applicable.
- PostgreSQL server version and database name when applicable.
- Manifest digest for file and Data Workspace backups.

### 7.5 RestorePlan

Fields:

- `id`
- `tenant_id`
- `project_id`
- `app_id`
- `source_backup_run_id`
- `target_kind`
- `target_id`
- `mode`: `clone`, `replace`, `pitr`, `verify-only`
- `status`: `planned`, `blocked`, `ready`, `expired`
- `checks`
- `warnings`
- `required_confirmations`
- `protective_backup_required`
- `created_at`
- `expires_at`

### 7.6 RestoreRun

Fields:

- `id`
- `restore_plan_id`
- `status`
- `protective_backup_run_id`
- `started_at`
- `completed_at`
- `cutover_at`
- `verification`
- `error_code`
- `error_message`

### 7.7 BackupUsage

Fields:

- `tenant_id`
- `backend_id`
- `provider`
- `managed_by_fugue`
- `billable_bytes`
- `artifact_count`
- `price_basis`: Cloudflare R2 public storage price for the billing period
- `markup_percent`: `5`
- `estimated_amount`
- `currency`
- `window_start`
- `window_end`

Only Fugue-managed R2 storage should be charged through Fugue backup billing by
default. Bring-your-own R2 storage can still report artifact bytes for
visibility, but `managed_by_fugue=false` should keep it out of Fugue's
R2-storage markup unless a separate managed-service fee is added later.

## 8. API surface

All paths and schemas are illustrative. The final shape must be defined in
`openapi/openapi.yaml` first.

Platform:

- `GET /v1/admin/backups/backends`
- `POST /v1/admin/backups/backends`
- `POST /v1/admin/backups/backends/{id}/test`
- `GET /v1/admin/backups/policies`
- `POST /v1/admin/backups/policies`
- `PATCH /v1/admin/backups/policies/{id}`
- `POST /v1/admin/backups/runs`
- `GET /v1/admin/backups/runs`
- `GET /v1/admin/backups/runs/{id}`
- `POST /v1/admin/backups/restore-plans`
- `POST /v1/admin/backups/restore-runs`

Tenant and app:

- `GET /v1/backups/backends`
- `POST /v1/backups/backends`
- `POST /v1/backups/backends/{id}/test`
- `GET /v1/apps/{id}/backups/policies`
- `POST /v1/apps/{id}/backups/policies`
- `PATCH /v1/apps/{id}/backups/policies/{policy_id}`
- `POST /v1/apps/{id}/backups/runs`
- `GET /v1/apps/{id}/backups/runs`
- `GET /v1/apps/{id}/backups/runs/{run_id}`
- `POST /v1/apps/{id}/restore/plans`
- `POST /v1/apps/{id}/restore/runs`
- `POST /v1/apps/{id}/restore/verify`
- `GET /v1/backups/usage`
- `GET /v1/apps/{id}/backups/usage`

Billing and admin usage:

- `GET /v1/admin/backups/usage`
- `GET /v1/admin/backups/usage/{tenant_id}`

Data Workspace:

- Reuse existing Data Workspace snapshot and transfer endpoints where possible.
- Add backup policy endpoints only when a Data Workspace needs schedule and
  retention management.

## 9. CLI surface

The CLI should be the primary interaction surface.

### 9.1 Backend commands

```bash
fugue backup backend create prod-r2 \
  --provider cloudflare-r2 \
  --bucket fugue-backups-prod \
  --endpoint https://<account>.r2.cloudflarestorage.com \
  --access-key-id "$ACCESS_KEY_ID" \
  --secret-access-key "$SECRET_ACCESS_KEY"

fugue backup backend test prod-r2
fugue backup backend ls
fugue backup backend show prod-r2
fugue backup backend delete prod-r2
fugue backup usage
```

Platform-admin variants:

```bash
fugue admin backup backend create platform-r2 ...
fugue admin backup backend test platform-r2
```

### 9.2 Control-plane commands

```bash
fugue admin backup status control-plane

fugue admin backup enable control-plane \
  --backend platform-r2 \
  --schedule "0 * * * *" \
  --retain-count 3 \
  --include postgres

fugue admin backup run control-plane --wait
fugue admin backup ls control-plane
fugue admin backup show backup_run_123
fugue admin backup restore plan control-plane --from backup_run_123
fugue admin backup restore verify control-plane --from backup_run_123
```

Control-plane disaster recovery must also have an offline entrypoint that does
not depend on the existing Fugue API being healthy:

```bash
fugue backup restore control-plane \
  --backend platform-r2 \
  --artifact backups/control-plane/<cluster-id>/<run-id>/manifest.json \
  --target-dsn "$NEW_FUGUE_DATABASE_URL" \
  --verify-only
```

### 9.3 App commands

App backup commands are explicit opt-in commands. Running `fugue app backup
status <app>` before configuration should show `disabled`, not silently create a
policy.

```bash
fugue app backup enable my-app \
  --database \
  --persistent-storage \
  --backend prod-r2 \
  --schedule "0 3 * * *" \
  --retain 14d \
  --version daily

fugue app backup run my-app --database --wait
fugue app backup status my-app
fugue app backup ls my-app
fugue app backup show my-app backup_run_123
fugue app backup usage my-app
```

Restore:

```bash
fugue app restore plan my-app --from backup_run_123
fugue app restore verify my-app --plan restore_plan_123
fugue app restore run my-app --plan restore_plan_123 --clone
fugue app restore run my-app --plan restore_plan_123 --replace --confirm-destructive-restore
```

For app databases, keep compatibility with existing restore UX:

```bash
fugue app db restore plan my-app --from-backup backup_run_123
fugue app db restore verify my-app --expected-database demo --table-min-rows users=1
```

Users can customize backup interval and version labels:

```bash
fugue app backup enable my-app \
  --database \
  --backend prod-r2 \
  --schedule "*/30 * * * *" \
  --retain-count 12 \
  --version before-migration
```

### 9.4 Output principles

Text output should be direct and copyable:

- Show backend name, provider, bucket, prefix, and status.
- Show backup run id, target, status, artifact ref, size, checksum, started,
  completed, and error message.
- Do not print raw backend secret values.
- JSON output should preserve stable field names for automation.
- Diagnostics and debug bundles should redact secret material by default.

## 10. R2 billing

R2-backed user backup is manually enabled. The default control-plane database
backup may use platform R2 automatically when the platform has R2 configured,
but user app and database artifacts are not created until the user opts in.

Billing model for Fugue-managed R2 backup storage:

- Bill by occupied backup artifact storage.
- Meter the sum of live, non-deleted user backup artifacts.
- Stop billing artifacts after retention deletes them.
- Charge the Cloudflare R2 public storage price for the billing period plus a
  5% Fugue markup.
- Do not use request count or operation count as the primary product billing
  unit for backup MVP.
- Show current billable bytes in backup status and usage output.

Bring-your-own R2 credentials can still be supported as a backend mode. In that
mode, Cloudflare bills the user's R2 account directly; Fugue should not also
charge the same artifact bytes as Fugue-managed R2 storage unless a separate
managed-service fee is explicitly introduced.

## 11. Backup execution by target

### 11.1 Control-plane PostgreSQL

MVP:

- Use `pg_dump --format=custom --no-owner --no-privileges`.
- Upload the dump and manifest to the configured backend.
- Record source fingerprint and store invariant summary.
- Verify the dump is non-empty and checksum matches the uploaded object.
- Run hourly by default for the platform control-plane database.
- Retain the latest 3 successful backups by default.
- Use platform-configured R2 by default when available.

Production CNPG path:

- If Fugue is using bundled CloudNativePG control-plane Postgres, create or
  reconcile CNPG `Backup` or `ScheduledBackup` resources.
- Store the CNPG backup name and object-store path as Fugue backup artifacts.
- Preserve the existing Helm `controlPlanePostgres.backup` values as the
  low-level operator configuration, not the user-facing product API.

Phase 2:

- Add WAL archiving and PITR restore plan support.
- Record recovery target time and timeline metadata.

### 11.2 App managed PostgreSQL

App managed PostgreSQL backup is disabled by default. The user must explicitly
enable it per app or through an authorized tenant-level policy.

MVP:

- Use `pg_dump` custom format through a controlled worker job.
- Use the app's managed Postgres connection details from Fugue's model.
- Record database name, user, service name, CNPG cluster name, runtime id, and
  source app id.
- Optionally run user-provided read-only probes after dump.

Large database path:

- Prefer CNPG native Backup when the backing service is CNPG-managed and a
  backup object store is configured.
- Keep logical dump as an operator mistake recovery and migration artifact.

Consistency:

- Default online logical backup.
- Allow `--quiesce read-only` when the app exposes a future Fugue freeze hook.
- Allow `--quiesce scale-down` for apps that cannot tolerate online logical
  backup inconsistency.

### 11.3 Persistent storage and workspace PVCs

Persistent storage backup is disabled by default. The user must explicitly
enable it before Fugue creates storage snapshots or file-level artifacts.

Possible implementations:

- CSI VolumeSnapshot when the storage class supports snapshots.
- Restic or kopia file backup when snapshots are not available.
- Clone PVC and scan the clone when the storage substrate supports clone.

Policy should choose automatically based on runtime capability, but the plan
must show the selected strategy.

Consistency:

- `scale-down` is safest for write-heavy filesystem state.
- `online` is acceptable for append-only or read-mostly directories if the user
  explicitly chooses it.
- Fugue should warn when backing up write-heavy storage without quiescing.

Restore:

- Restore to a new PVC or directory first.
- Verify manifest and permissions.
- Cut over by updating app spec or storage claim reference through a normal
  deploy operation.
- Keep old PVC until the user deletes it or retention expires.

### 11.4 Data Workspaces

Data Workspace backup should reuse existing snapshots:

- A backup run creates or references a Data Snapshot.
- The artifact manifest records snapshot id, manifest digest, file count, total
  bytes, and backend object prefix.
- Retention must avoid deleting blobs still referenced by protected snapshots.

Data Workspace scheduled backup is disabled by default.

### 11.5 Registry

If registry is externalized:

- Record registry endpoints and health.
- Do not attempt to back up registry layers.

If bundled registry is enabled:

- Back up registry data from the PVC or object backend.
- Prefer registry-native blob/index copy to a backup backend when possible.
- Coordinate with registry GC so backup does not race deletion.

## 12. Restore workflow

### 12.1 Universal restore phases

1. Resolve artifact manifest.
2. Validate artifact checksum and encryption metadata.
3. Build a restore plan.
4. Run preflight checks.
5. Create a protective backup unless explicitly disabled for a non-destructive
   clone.
6. Restore into an isolated target.
7. Verify data and permissions.
8. Cut over only after explicit confirmation.
9. Record audit event, restore run, verification results, and rollback pointer.

### 12.2 Control-plane restore

Control-plane restore has two modes.

Online mode:

- Used for drill, migration, or restore into a new target store while the old API
  is healthy.
- Uses admin APIs for plan, status, and promotion gate.

Offline mode:

- Used when the Fugue API database is lost.
- The CLI or Helm Job reads backup backend credentials from the operator and
  downloads the manifest directly.
- Restore writes to a new PostgreSQL DSN.
- Operator updates `FUGUE_DATABASE_URL` through the formal control-plane release
  path, not manual live patching as the normal workflow.

### 12.3 App database restore

Default mode should be `clone`:

- Restore the dump into a temporary database or temporary CNPG cluster.
- Run verification queries.
- Let the user inspect before cutover.

Replace mode:

- Requires `--confirm-destructive-restore`.
- Creates protective backup first.
- Scales app down or enables read-only mode when required.
- Restores into the managed database.
- Verifies row-count and app-specific probes.
- Unfreezes or scales app up only after verification passes.

### 12.4 Persistent storage restore

Default mode:

- Restore to new PVC.
- Attach to an inspection job.
- Verify manifest, size, mode, and selected file checksums.
- Cut over app storage reference through a deploy operation.

Rollback:

- Keep the previous PVC reference in the restore run.
- Allow `fugue app restore rollback <app> --restore-run <id>` while the old PVC
  still exists.

## 13. Security model

Scopes:

- `backup.read`
- `backup.write`
- `backup.restore`
- `backup.admin`

Rules:

- Platform backup requires platform admin.
- Tenant backup backends and app backup policies are tenant-scoped.
- Restore requires stronger permission than backup creation.
- Destructive restore requires explicit confirmation and audit.
- Backend credentials are encrypted at rest.
- Artifact content should support encryption at rest independent of the object
  store.
- Backup manifests may include object names and schema metadata but must not
  expose raw secrets.

Recommended encryption:

- Envelope encryption per backend or per policy.
- Store key id in artifact metadata.
- Never print the encryption key in CLI output.

## 14. Observability

CLI status should answer:

- Is backup enabled?
- What is protected?
- Where is it stored?
- When did it last succeed?
- When will it run next?
- What failed last time?
- How old is the newest verified restore drill?
- How much data is protected?
- How many billable R2 bytes are currently retained for this user?

Metrics:

- `fugue_backup_policy_enabled`
- `fugue_backup_last_success_timestamp_seconds`
- `fugue_backup_run_duration_seconds`
- `fugue_backup_run_failures_total`
- `fugue_backup_artifact_bytes`
- `fugue_backup_retention_deletes_total`
- `fugue_restore_run_duration_seconds`
- `fugue_restore_run_failures_total`

Events and audit actions:

- `backup.backend.create`
- `backup.backend.test`
- `backup.policy.enable`
- `backup.policy.disable`
- `backup.run.start`
- `backup.run.complete`
- `backup.run.fail`
- `backup.retention.delete`
- `restore.plan.create`
- `restore.run.confirm`
- `restore.run.complete`
- `restore.run.fail`

## 15. Failure handling

The backup system should make failure explicit and actionable.

Common failures:

- Backend credential invalid.
- Bucket or prefix not writable.
- Backup lock already held.
- Source target no longer exists.
- PostgreSQL dump failed.
- CNPG Backup did not complete before timeout.
- PVC snapshot unsupported.
- File scan found permission errors.
- Artifact upload checksum mismatch.
- Retention delete blocked by protected artifact.

Behavior:

- Failed runs remain visible.
- Scheduler should back off but not silently disable the policy.
- CLI should show root cause and the next retry time.
- Repeated failures should surface in admin cockpit and diagnostics.

## 16. Release and operations

Implementation that changes API, controller, Helm, control-plane routing, or
runtime behavior belongs in the `fugue` repository and follows the formal
control-plane release path:

1. Update `openapi/openapi.yaml` first for API changes.
2. Run `make generate-openapi`.
3. Implement store, API, controller, worker, CLI, and tests.
4. Run `make test`.
5. If frontend consumes new endpoints, sync `fugue-web` generated artifacts:
   `npm run openapi:sync`, `npm run openapi:generate`, and
   `npm run contract:check`.
6. Release the remote control plane through the GitHub Actions
   `deploy-control-plane.yml` workflow. Do not treat manual SSH changes,
   ad-hoc Kubernetes patches, or `install_fugue_ha.sh` as the normal release
   path.

## 17. Suggested implementation phases

### Phase 1: MVP

- S3-compatible backup backend.
- Tenant and platform backup backend CRUD.
- Default platform control-plane PostgreSQL logical backup, hourly, retaining 3
  successful backups.
- Platform R2 default backend selection when configured.
- User app and database backup status shows disabled until explicit opt-in.
- Manual app managed PostgreSQL logical backup only after user enables policy.
- Manual run and scheduled run.
- Backup run list/show/status.
- Restore plan and verify for control-plane and app DB.
- Protective backup before destructive restore.
- Fugue-managed R2 storage usage metering and 5% markup billing model.
- Audit and metrics.

### Phase 2: Stateful files

- Persistent storage backup by CSI snapshot or file backup.
- Restore to new PVC.
- Storage restore verify and cutover.
- Data Workspace scheduled backup policies.
- Retention sweep.

### Phase 3: Production DR

- CNPG native backup and PITR support.
- Control-plane offline restore CLI/Job.
- Restore drill automation.
- Bundled registry backup.
- Object lock / immutability integration.
- Cross-region backup copy.

### Phase 4: UX and policy hardening

- Admin cockpit backup posture.
- App continuity audit includes backup readiness.
- Web console pages.
- App-level write-freeze hooks.
- External-owned runtime backup agent.

## 18. Open product decisions

- Should MVP support only S3-compatible backends, or include local filesystem for
  self-hosted operators from day one?
- Should app managed PostgreSQL MVP use logical dump only, or include CNPG Backup
  immediately?
- Should persistent storage MVP require scale-down for consistency, or allow
  online best-effort with warnings?
- Should backup policies be app-scoped first, or project-scoped with app
  selectors?
- If no platform R2 backend exists, should self-hosted Fugue require an explicit
  backend before the default control-plane backup becomes healthy, or provide a
  clearly unsafe local-dev fallback?
- Should user-supplied BYO R2 have any Fugue management fee, or should billing
  apply only to Fugue-managed R2 storage?

## 19. Todo list

### Product and API

- [x] Confirm MVP default target scope: control-plane PostgreSQL only.
- [x] Confirm user app and app database backup remain disabled by default.
- [x] Confirm platform R2 as the default backend when configured.
- [x] Confirm default schedule `0 * * * *` and default retain count `3`.
- [x] Confirm first supported user-configurable backend providers.
- [x] Decide whether backup policies are app-scoped, project-scoped, or both.
- [x] Define OpenAPI schemas for backend, policy, run, artifact, restore plan,
      and restore run.
- [x] Add API endpoints to `openapi/openapi.yaml`.
- [x] Generate OpenAPI artifacts with `make generate-openapi`.
- [x] Add scopes: `backup.read`, `backup.write`, `backup.restore`,
      `backup.admin`.
- [x] Define audit event names and redaction behavior.
- [x] Define R2 storage usage and billing schemas.

### Store

- [x] Add store tables for backup backends.
- [x] Add store tables for backup policies.
- [x] Add store tables for backup runs.
- [x] Add store tables for backup artifacts.
- [x] Add store tables for restore plans and restore runs.
- [x] Add indexes for due scheduled runs and target history.
- [x] Add retention metadata and protected artifact flags.
- [x] Add default platform policy state for control-plane database backup.
- [x] Add billable backup artifact byte accounting.
- [x] Add tenant deletion and project deletion cleanup rules.

### Backend storage

- [x] Reuse or extend DataBackend credential encryption for backup backends.
- [x] Implement S3-compatible upload, download, list, head, and delete.
- [x] Implement backend test with write/read/delete probe.
- [x] Implement platform R2 backend auto-selection.
- [x] Define artifact `manifest.json` schema.
- [x] Implement checksum verification.
- [x] Add optional artifact encryption metadata.

### Scheduler and workers

- [x] Add due-policy scanner under controller leader loop or a dedicated backup
      worker.
- [x] Add per-target run locking.
- [x] Add manual run enqueue.
- [x] Add scheduled run enqueue.
- [x] Add default hourly control-plane database schedule.
- [x] Add worker heartbeat and timeout handling.
- [x] Add retry and backoff rules.
- [x] Add cancellation handling.

### Control-plane backup

- [x] Productize current `pg_dump` script behavior.
- [x] Upload control-plane dump to backup backend.
- [x] Write control-plane artifact manifest.
- [x] Record store fingerprint and invariant summary.
- [x] Enforce default retain count of 3 successful control-plane backups.
- [x] Add online restore plan.
- [x] Add offline restore CLI path.
- [ ] Integrate CNPG `Backup` and `ScheduledBackup` when bundled CNPG is used.
- [x] Add restore drill status.

### App database backup

- [x] Ensure app database backup policy is absent/disabled by default.
- [x] Resolve app-owned managed Postgres backup target from Fugue app model.
- [x] Run logical `pg_dump` in a worker job.
- [x] Upload app database dump and manifest.
- [x] Record database metadata, runtime id, service name, and CNPG cluster name.
- [x] Add app database restore plan from backup artifact.
- [x] Add clone restore mode.
- [ ] Add destructive replace restore mode with protective backup.
- [x] Extend existing `fugue app db restore plan/verify` to accept backup refs.

### Persistent storage backup

- [x] Ensure persistent storage backup policy is absent/disabled by default.
- [ ] Detect runtime storage capabilities.
- [ ] Detect CSI VolumeSnapshot support.
- [ ] Choose snapshot, clone, or file backup strategy.
- [ ] Add file-level backup worker.
- [ ] Add restore to new PVC.
- [ ] Verify manifest, checksums, ownership, and permissions.
- [ ] Cut over storage through a normal deploy operation.
- [ ] Add rollback while old PVC is retained.

### Data Workspace backup

- [x] Ensure Data Workspace scheduled backup policy is absent/disabled by
      default.
- [ ] Map backup policy to Data Workspace snapshot creation.
- [ ] Record Data Snapshot id and manifest digest in backup artifact.
- [ ] Add retention rules that respect protected snapshots.
- [ ] Add Data Workspace restore plan integration.

### Registry and bundled platform state

- [ ] Detect externalized registry and mark as externally backed up.
- [ ] Design bundled registry backup strategy.
- [ ] Coordinate registry backup with registry GC.
- [ ] Inventory bundled headscale, DNS, and edge state that needs backup.
- [ ] Add platform backup posture checks for externalized components.

### CLI

- [x] Add `fugue backup backend ...`.
- [x] Add `fugue admin backup backend ...`.
- [x] Add `fugue admin backup enable/run/status/ls/show ...`.
- [x] Add `fugue app backup enable/run/status/ls/show ...`.
- [x] Make `fugue app backup status <app>` report disabled before opt-in.
- [x] Add `--version`, `--schedule`, `--retain-count`, and `--retain` policy
      flags for user-enabled R2 backup.
- [x] Add `fugue app restore plan/verify/run ...`.
- [x] Add offline `fugue backup restore control-plane ...`.
- [x] Add JSON outputs and text tables.
- [x] Add secret redaction tests.

### Observability and diagnostics

- [x] Add backup metrics.
- [x] Add restore metrics.
- [ ] Add backup status to admin cockpit.
- [x] Add billable backup bytes to usage/status output.
- [x] Add backup readiness to app continuity audit.
- [ ] Add operation diagnosis hints for stale or failed backups.
- [ ] Add debug bundle redaction for backup config.

### Testing

- [x] Unit-test backend validation.
- [x] Unit-test retention selection.
- [x] Unit-test schedule due calculation.
- [x] Unit-test default platform policy: hourly control-plane DB, retain 3.
- [x] Unit-test user app/database backup disabled by default.
- [x] Unit-test platform R2 backend auto-selection.
- [x] Unit-test R2 billable byte accounting and 5% markup calculation.
- [x] Unit-test artifact manifest validation.
- [x] Unit-test app managed Postgres target resolution.
- [ ] Integration-test control-plane logical backup and restore plan.
- [ ] Integration-test app database backup and clone restore.
- [x] Integration-test backend credential redaction.
- [x] Add OpenAPI drift checks through `make test`.
- [x] Add CLI output compatibility tests.

### Documentation

- [x] Update `docs/ha-dr.md` to link to this backup system plan.
- [x] Add user-facing backup CLI guide after MVP implementation.
- [x] Add operator runbook for offline control-plane restore.
- [x] Add restore drill runbook.
- [x] Add backend provider examples for R2, S3, B2, and MinIO.
