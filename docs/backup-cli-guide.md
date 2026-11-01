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
CNPG Cluster/Backups, and retention CronJob/Jobs in the selected namespace.

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
