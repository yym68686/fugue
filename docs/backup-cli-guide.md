# Fugue backup CLI guide

Fugue backup is opt-in for user workloads. The default platform **logical dump**
policy backs up the control-plane database hourly and retains three successful
artifacts. This is independent of CNPG's full physical backups and continuous
WAL archive. App databases, persistent storage, and Data Workspaces stay disabled
until a user enables a policy.

## Check platform status

```bash
fugue admin backup status
fugue admin backup ls
```

When platform R2 is configured through the Fugue data backend environment,
Fugue seeds a default backup backend and attaches it to the default
control-plane database policy. If no safe backend exists, the policy reports
`blocked_no_backend`.

## Create or test a backup backend

Cloudflare R2:

```bash
fugue backup backend create prod-r2 \
  --provider cloudflare-r2 \
  --r2-account-id <account-id> \
  --bucket fugue-backups-prod \
  --region auto \
  --access-key-id <access-key-id> \
  --secret-access-key <secret-access-key>

fugue backup backend test prod-r2
```

AWS S3:

```bash
fugue backup backend create prod-s3 \
  --provider s3 \
  --bucket fugue-backups-prod \
  --region us-east-1 \
  --access-key-id <access-key-id> \
  --secret-access-key <secret-access-key>
```

Backblaze B2 S3-compatible endpoint:

```bash
fugue backup backend create prod-b2 \
  --provider backblaze-b2 \
  --bucket fugue-backups-prod \
  --endpoint https://s3.us-west-004.backblazeb2.com \
  --region us-west-004 \
  --access-key-id <key-id> \
  --secret-access-key <application-key>
```

MinIO:

```bash
fugue backup backend create minio \
  --provider minio \
  --bucket fugue-backups \
  --endpoint https://minio.example.com \
  --region us-east-1 \
  --access-key-id <access-key-id> \
  --secret-access-key <secret-access-key>
```

Credential values are stored encrypted by the control plane and are redacted in
normal list/show output.

## Manage control-plane logical dumps

```bash
fugue admin backup enable \
  --schedule '0 * * * *' \
  --retain-count 3

fugue admin backup run --wait
fugue admin backup show <backup-run-id>
fugue backup artifact ls --target-type control-plane-db
```

`--version` can attach an operator label such as `before-migration` or
`release-2026-06`.

## Manage control-plane physical backup retention

`admin backup enable --retain-count` only changes logical dump artifacts. To
manage CNPG full physical backups and their WAL archive, use the independent
operator commands (Kubernetes authorization, not a Fugue API/bootstrap token):

```bash
fugue admin backup physical status
fugue admin backup physical plan
fugue admin backup physical configure --retain-count 3 --ack-reduced-recovery
fugue admin backup physical reconcile --dry-run --wait
fugue admin backup physical reconcile --wait
fugue admin backup physical status
```

Use `--namespace`, `--policy`, `--kubeconfig` and `--kube-context` to select an
installed policy. Alternatively, append `--ssh <operator-host-alias>` to run
kubectl on an explicitly selected trusted SSH host. The latter uses existing
SSH authentication and host-key verification; it does not copy executors or
credentials to a node. Commands continue working if the Fugue API/database is
unavailable. The operator needs access to the policy/receipt ConfigMaps, Lease,
CNPG Cluster/Backups/ScheduledBackups, and retention CronJob/Jobs in the selected
namespace. Status/shutdown also inspect schedules and the retention CronJob;
shutdown checks archive-dependent recoveries across namespaces (list Clusters).

The executor is installed only through the declarative `ci.yml`
`cnpg_backup_count_retention` lane, configured by
`deploy/environments/production/control-plane-backup-retention/install.json`.
It starts **disabled**, uses a digest-pinned existing PostgreSQL/Barman image,
and reads S3 credentials through existing Secret references. Subsequent code
releases preserve the independently saved policy. The protection writer rejects
an attempt to overwrite an owned count policy with a recovery window; changing
writer code alone does not reapply unrelated protection configuration.

`configure` persists and activates **successful-backup count**, not days:
CNPG's native `7d` means a seven-day recovery window and may require more than
seven base backups, whereas Barman `REDUNDANCY 3` retains the latest three
successful full backups. Activation removes only the native window and sets a
policy-ownership annotation with UID/resourceVersion fencing. It does not alter
backup schedules, compression, database storage, replicas, or PostgreSQL settings.
Increasing the count cannot recover already removed backups.

The worker runs every 15 minutes. It acquires the policy Lease and revalidates
the database UID/system ID, archive destination/endpoint/server identity,
policy revision, cluster health, pending backups, and active archive-dependent
recoveries before calling `barman-cloud-backup-delete --retention-policy
"REDUNDANCY N" --minimum-redundancy N`. Only Barman removes backup files and
unneeded WAL; Fugue never deletes archive keys by prefix. Incomplete/foreign
catalogues and incremental backup chains fail closed. Archival `keep` flags
remain honored, so the actual retained count may be higher than requested.

`plan` reads Kubernetes records only and explicitly reports
`remote_catalogue_verified=false`: old Backup CRs can outlive their remote data.
`reconcile --dry-run` creates a Job but does not delete archive data. Check the
last receipt for verified `remaining`, `removed`, `count_satisfied`,
`remaining_above_count`, `dry_run`, and any blocking reason. A completed Job or
saved configuration is not by itself proof of reduced R2 storage. Failed or
unfinished multipart uploads are not counted as successful backups and are not
aborted by this policy.

Count retention permanently shortens the recovery horizon to the oldest retained
base backup (plus its required WAL). It is not a database recovery or disk cleanup
operation: deleting R2 WAL cannot free local PostgreSQL `pg_wal`. Unhealthy
databases may save configuration intent, but remote cleanup stays blocked until
the database is healthy. Resolve local disk incidents separately.

### Disable native Barman without deleting history

```bash
fugue admin backup physical disable --dry-run --ssh <operator-host-alias>
fugue admin backup physical disable --ack-loss-of-pitr --wait --ssh <operator-host-alias>
fugue admin backup physical status --ssh <operator-host-alias>
```

This is a separate, explicit lifecycle operation, **not** `--retain-count 0`.
It stops future native Barman base backups **and** continuous WAL uploads. It
does not alter PostgreSQL's local WAL, Longhorn policies, database volumes,
replication, Secrets, historical Backup records, R2 objects or multipart uploads.
Existing archive data continues to cost storage until separately retired.
Confirm a recent successful alternative backup and perform an isolated restore
drill before relying on Longhorn alone. Future PITR coverage is lost; restore
points are limited to the alternative backups that actually succeed.

`--dry-run` only reads Kubernetes state: no Lease, Job or intent is written.
Execution acquires the retention Lease, persists `barman_disabled=true` and
`enabled=false` in the policy, pins cluster ownership with
`fugue.pro/barman-disabled=<policy UID>`, and suspends the retention CronJob and
every native Barman ScheduledBackup targeting that cluster. Foreign clusters and
volumeSnapshot/plugin schedules are untouched. Active backups (including unknown
phases), unhealthy databases, archive-fed replica clusters and active archive-dependent recoveries prevent
archive removal. No backup process is cancelled. Do not start concurrent manual
backups while shutting down.

`disable_pending` is **not** completed shutdown: WAL archiving remains configured
while a backup drains or the database is unhealthy. Rerun `disable` after those
conditions clear. `--wait` retries for at most five minutes; an interrupted or
timed-out command keeps its safe intent and never schedules automatic completion.
UID/resourceVersion conflicts fail explicitly and can be retried. Only after
revalidation does the command remove native `barmanObjectStore` and its retention
window. `completed=true` proves configuration read-back, not PostgreSQL runtime
reload, no future R2 writes, or successful Longhorn restoration; check database
health, archiver logs/metrics and R2 activity separately.

Subsequent retention installations preserve shutdown and leave their CronJob
suspended, even after native credentials/configuration have been removed from
the Cluster. The protection writer refuses to re-enable backups, and an online
Helm upgrade refuses `backup.enabled=true` for an explicitly disabled Cluster
(set `controlPlanePostgres.backup.enabled=false` for such an upgrade). Offline
rendering cannot discover live intent; do not apply offline manifests over it.
`configure` cannot re-enable Barman. Re-enabling is a separate, reviewed recovery
configuration operation; do not simply remove the ownership annotation.

### Retire ALL disabled Barman history (including unfinished uploads)

```bash
# Default: R2 LIST/metadata reads only; creates a Kubernetes Job and receipt.
fugue admin backup physical purge --ssh <operator-host-alias>
fugue admin backup physical purge --status --ssh <operator-host-alias>

# Irreversible; use the exact plan_sha256 returned by a preview within one hour.
fugue admin backup physical purge --execute \
  --plan-sha256 <preview-sha256> \
  --ack-delete-all-history --ack-no-external-writers \
  --ssh <operator-host-alias>
```

`purge` defaults to preview (`--dry-run` is an explicit equivalent), and waits
for a receipt bound to the exact Job UID/operation ID. `--wait=false` returns the
Job identity immediately; `--status` reads the latest history receipt without
creating a Job. Interrupting/timing out the CLI does not cancel the Job. Do not
start concurrent Jobs or interpret an old receipt as the current result.

This is **archive retirement**, not retention: execution deletes **every** base
backup, archived WAL and unfinished multipart upload in the installed policy's
exact `destination/server_name/` prefix, including the most recent three backups.
It never accepts an arbitrary bucket/prefix, changes retention count, disables
Barman, or deletes Kubernetes Backup records, Secrets, local PostgreSQL WAL,
PVCs or Longhorn objects/shared blocks. Check independent backups and complete a
restore drill before destroying the last Barman recovery points.

Prerequisites are owned and completed native Barman shutdown, disabled count
retention, suspended schedules/CronJob, a healthy stable primary, no pending or
active backup. Preview lists any referencing CNPG clusters in `archive_consumers`
and `execution_blockers` alongside the inventory; `can_execute=false` means the
preview is informational, not approval to delete. Execution additionally requires
no other CNPG cluster referencing the archive (even a healthy one). It never
removes these references or deletes recovery clusters automatically.
The independent executor is upgraded through the declarative release lane;
disabled installations preserve shutdown and reuse retained Secret **references**.
Missing references or an old executor fail closed instead of enabling archiving.

Inventory includes all paginated object/upload/part listings with explicit size
and page bounds. It reports base/WAL bytes, pending-upload bytes and a snapshot
SHA256 bound to policy UID/resourceVersion. Unknown layouts, keep/other retention
annotations, foreign database metadata and incremental/snapshot catalogues block
retirement. Full/orphan tar uploads are supported; a failed upload is not counted
as a successful backup. Versioned-object history is not supported: this operator
path requires a native Cloudflare R2 endpoint (including jurisdiction endpoints),
not a custom endpoint or a general versioned-S3 bucket cleaner.

Execution requires a matching fresh preview, no writes in the last 15 minutes,
and a second unchanged inventory. It shares the retention Lease and rechecks
Kubernetes gates before each batch, checks exact objects using HEAD/ETag, deletes
only inventoried keys/upload IDs, rejects partial batch errors and verifies the
prefix has no complete objects or pending uploads afterward. `purged` with
`verified_empty=true` is the only successful execution state. A stopped/failed
Job can have removed data; `partial_or_unconfirmed` and progress counters are not
proof of a complete cleanup. Always preview the remainder before retrying.

R2 may return a different opaque multipart `UploadId` on successive listings of
the same unchanged upload. Plan schema `fugue.barman-history-plan/v2` binds the
upload's key, initiated timestamp and every part number, byte count, ETag and
modification timestamp, rather than that rotating handle. Execution re-lists and
verifies this complete identity before aborting with the latest returned handle.
Multiple concurrent uploads for one key are ambiguous and block cleanup; content,
timestamp, upload-count and object checks are not relaxed. Older preview hashes
must be regenerated after an executor upgrade.
HEAD's HTTP `Last-Modified` header has only second precision, unlike R2 LIST's
fractional timestamps. The HEAD guard accepts an exact match or truncation to
that same second, with unchanged ETag and byte count; conflicting fractional
times or another second still block deletion. LIST snapshot hashes retain their
full timestamp precision.

The Lease does **not** fence external S3 writers, and R2 does not document atomic
conditional DELETE in its [S3 compatibility table](https://developers.cloudflare.com/r2/api/s3/api/).
The operator must exclude external/manual writers throughout execution;
`--ack-no-external-writers` explicitly acknowledges this requirement. HEAD plus
DELETE cannot eliminate an external write race. This command never revokes
shared credentials or silently falls back from failed conditional requests.

## Enable app backup explicitly

```bash
fugue app backup status my-app
fugue app backup enable my-app \
  --target app-database \
  --backend prod-r2 \
  --schedule '0 */6 * * *' \
  --retain-count 7 \
  --version daily
```

Backup schedules use numeric five-field cron syntax in UTC. The `@hourly`
shortcut is also accepted; timezone prefixes and other descriptors are not.

The status command reports disabled targets before opt-in. Creating an app
database policy resolves the app-owned managed PostgreSQL target from the app
model. Backup execution for app database and persistent storage workers remains
guarded by explicit target support; unsupported targets fail with a clear
`unsupported_target` error instead of silently taking partial backups.

## Usage and billing

```bash
fugue backup usage
```

Fugue-managed R2 backup storage is metered by occupied active artifact bytes.
The effective billing model is Cloudflare R2 public storage price plus 5%.
Bring-your-own R2 credentials can be used as a backend, but those bytes are not
charged by Fugue as managed R2 storage unless the backend is marked billable.

The same command independently lists the physical R2 object inventory. Physical
bytes include manifests, objects still inside deletion grace, and any
unreferenced uploads; they are not silently substituted for the billing ledger.
`reconciliation_status` reports `complete`, `reconciling`, `drift`, `partial`,
or `unavailable`. Exact physical totals are omitted when every visible R2
backend could not be measured, so an object-store outage is never displayed as
zero usage.
