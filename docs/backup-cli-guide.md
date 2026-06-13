# Fugue backup CLI guide

Fugue backup is opt-in for user workloads. The default platform policy only
backs up the control-plane database, runs hourly, and retains three successful
backup artifacts. App databases, persistent storage, and Data Workspaces stay
disabled until a user enables a policy.

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

## Manage control-plane backup

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

## Enable app backup explicitly

```bash
fugue app backup status my-app
fugue app backup enable my-app \
  --database \
  --backend prod-r2 \
  --schedule '0 */6 * * *' \
  --retain-count 7 \
  --version daily
```

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
