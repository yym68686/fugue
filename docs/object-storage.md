# Project object storage

Object storage is opt-in. Fugue provisions a private Cloudflare R2 or Backblaze B2 bucket per resource,
owned by a tenant and associated with a project. It never stores application
objects in the backup or DataWorkspace garbage-collection namespace. Apps read
and write directly to the provider using standard S3 credentials; the control plane manages
resource metadata, authorization, and credentials only.

## Configure the platform

Platform administrators can save or rotate both providers independently at
**Fugue Web → Platform settings** (`/admin/settings`). The page never displays
stored secrets. Existing Web integrations also support credential rotation there.

Create a Cloudflare account API token with Workers R2 Storage Write and Account
API Tokens Write, limited to the platform's R2 account. Keep this management
token on the control plane; never inject it into application environments.

```
fugue s3 configure --account-id <cloudflare-account-id> --token-file <private-file>
```

Alternatively the CLI reads FUGUE_S3_CF_API_TOKEN. The server verifies the
provider then encrypts the token using the existing data-credential encryption
key. Configure FUGUE_DATA_CREDENTIAL_ENCRYPTION_KEY (or the platform signing key)
on production installations and retain it through releases and restores.
Configuration persists independently of code deployments. Changing Cloudflare
account identity is rejected after resources have been created.

For Backblaze, use a dedicated B2 account and a **non-master** application
key for all buckets, with no filename prefix and these capabilities:
`listBuckets`, `writeBuckets`, `deleteBuckets`, `listKeys`, `writeKeys`,
`deleteKeys`, `listFiles`, `readFiles`, `writeFiles`, `deleteFiles`.
The Backblaze console's data-only key presets may not grant all of them.
Create the management key using the [Backblaze Native API](https://www.backblaze.com/apidocs/b2-create-key).
The ability to create keys can grant broad access to the B2 account.
Keep this credential out of app environments. The API derives the account and
region from Backblaze's authorization response; changing either is blocked as
long as Fugue B2 resources exist.

```
fugue s3 configure --provider backblaze-b2 --key-id <b2-application-key-id> --token-file <private-file>
fugue s3 config-status --provider backblaze-b2
fugue --project analytics s3 create cold-archive --provider backblaze-b2
```

Wait for all API replicas to run a B2-aware release before configuring or
managing B2 buckets. If the API is rolled back to a pre-B2 release, keep B2
management operations paused until a B2-aware release is restored. The provider
record is independent of R2 and S3 clients keep using their scoped keys.

The CLI also accepts the B2 secret from `FUGUE_S3_B2_APPLICATION_KEY`.
Existing stores and new requests that omit the provider retain their current
behavior (new stores default to R2). This does not migrate any existing data or
switch any platform backup or compiler archive to B2.

## Use storage

```
fugue --project analytics s3 create request-facts --quota-bytes 10737418240
fugue --project analytics s3 ls
fugue --project analytics s3 credential request-facts --app collector --name collector-v1 --permission read-write --bind
fugue --project analytics s3 credential request-facts --app query-api --name reader-v1 --permission read-only --env-file ./storage.env
fugue --project analytics s3 credentials request-facts
fugue --project analytics s3 usage request-facts
fugue --project analytics s3 revoke request-facts <credential-id>
fugue --project analytics s3 disable request-facts
```

`--bind` merges S3_ENDPOINT, S3_BUCKET, AWS_REGION, AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY into the selected application's configuration. Apply the
normal application deployment/restart workflow for configuration activation.
`--env-file` exclusively creates a file with mode 0600; it never overwrites.
Secrets are not printed. Use a distinct name to rotate a credential, update the
application, then revoke the old credential after verifying the new one.
Reissuing an active named credential is idempotent. If the provider creation
response is lost, Fugue revokes credentials matching the persisted grant ID
before retrying, so unknown credentials cannot escape later revocation.

Storage metadata requires storage.read or storage.admin. Mutations and issuing
credentials require storage.admin. Existing workspace-owner keys with data.admin
also retain access to tenant object storage; this never bypasses tenant/project
authorization. All routes enforce tenant and project
scope. Cross-project application binding requires an explicitly selected app
in the same tenant and a principal authorized for both projects. Each issued
credential is restricted to one bucket and either object read-only or object
read-write permissions, with no bucket/account administration.
Configure S3 clients with the explicit B2 bucket name and region. Fugue does not
grant access to list all B2 bucket names in the account.

Disabling persists `disabling` before revoking every grant upstream and reports
`disabled` only after revocation succeeds. A failed operation remains retryable.
Re-enabling permits new credentials; it does not resurrect revoked ones.
Neither disabling nor compute deletion deletes objects. Resource metadata and
credential records outlive compute. Tenant deletion with owned stores is
blocked to preserve ownership; this release deliberately has no bulk object
purge operation.

Usage measurement lists the bucket to calculate current R2 object bytes/count
or B2 stored object version bytes/count, excluding delete markers and incomplete
uploads. B2 retains historical versions unless removed; review your provider
lifecycle policy before large ingestion. Bucket deletion requires the existing
explicit confirmation and clears versions, markers and unfinished uploads.
It retains the previous successful measurement on failure and includes its
observation timestamp. quota_bytes is a **soft budget**, not a synchronous hard
limit or provider invoice. Multipart pending bytes, request charges, and billing
reconciliation are not represented as measured zero. Measurements are explicit,
so direct S3 traffic does not load the Fugue control plane. Neither provider
indexes JSON contents; maintain an external index if filtering by file content.

## Platform inventory and Longhorn reconciliation

Platform administrators (not tenant `storage.admin` keys) can inspect account
bucket names, configured project/data/backup namespaces, cached backup usage,
CNPG destinations and Longhorn cleanup receipts:

```
fugue admin storage overview
fugue admin storage ls --json
fugue admin storage reconcile --json
```

All three commands are read-only and use `GET /v1/admin/storage`. `reconcile`
reports the background collector's observations; it does not start deletion.
The web console exposes the same view at `/admin/storage`. Inventory refresh
does not scan bucket objects. Source failures and the bounded 500-artifact
Longhorn inventory are explicit; unknown measurements and costs are not zero.
The current endpoint does not import Cloudflare invoices or request metrics.
Cached backup usage covers only configured backup namespaces, not the account
total, CNPG, or unrelated project buckets.

The collector treats Longhorn snapshots differently from ordinary objects.
Only deleted/expired, unprotected artifacts outside the one-hour grace period
and without live restore/shared-artifact references are eligible. It validates
the repository and remote backup identity, then deletes the exact Backup CR
with UID/resource-version preconditions. Longhorn's finalizer owns remote
deletion and shared-block GC. A deletion request is pending, not success;
Fugue records completion only on a subsequent absent-CR observation. No R2
repository block is directly deleted. Do not remove Longhorn finalizers by hand.

Old physical-deletion receipts without `longhorn_cleanup_version=1` are retried
through this engine-aware flow. Failed steps keep the manifest and remain
retryable. Untracked remote backups are displayed but never automatically
deleted; reconcile ownership before planning any manual retirement.

For an orphaned backup manifest, use the explicit one-object cleanup command:

```
fugue admin storage orphan-manifest cleanup <backup-run-id> \
  --object-key <exact-physical-r2-key>
fugue admin storage orphan-manifest cleanup <backup-run-id> \
  --object-key <exact-physical-r2-key> --confirm
```

The first invocation is preview-only. The server requires a failed/canceled
database run older than 24 hours, no registered artifact, zero persisted
bytes/artifacts, and a valid matching manifest as the sole object under that run.
For Longhorn snapshots, both the complete remote inventory and the exact
repository backup configuration must confirm absence.
Execution repeats all checks under a platform-wide lock and rechecks the
observed ETag immediately before deleting the exact key. A still-present Longhorn Backup,
ambiguous inventory, missing manifest identity, or any other run object blocks
cleanup. Fugue never deletes Longhorn repository blocks through this command.

When a control-plane Longhorn backup completed remotely but its artifact could
not be persisted, reconcile it without deleting remote data:

```
fugue admin storage orphan-manifest adopt <backup-run-id> \
  --object-key <exact-physical-r2-key>
fugue admin storage orphan-manifest adopt <backup-run-id> \
  --object-key <exact-physical-r2-key> --confirm
```

This platform-admin operation verifies the failed/canceled run, lack of any
artifact (including retired records), manifest ownership, matching Completed
Longhorn resource, available target, and the remote repository config identity
and block-map structure. Both source ETags are bound to the preview and rechecked
before an atomic record insertion. It uses the same 24-hour grace period as
cleanup. A changed run, active lease, source-version change, or duplicate remote
reference fails closed. Remote objects are read-only throughout the operation.

The recovered artifact is **protected** from automatic retention and deletion.
Its creation time remains the original backup time; the failed run keeps its
status and error while its artifact and byte counters are reconciled. Platform
targets have no synthetic application foreign key. Provenance includes source
ETags, repository config SHA-256, and `restore_verification=not_run`. Repository
identity verification is not a complete block checksum audit or a PostgreSQL
restore drill.

### Retire one recovered control-plane Longhorn backup

No restore drill is required to retire an old recovered point. If one was
performed for the replacement, optionally attach its real receipt digest with
`--restore-evidence-sha256`. Otherwise the preview and audit explicitly record
that no receipt was supplied. A backup status, restore plan, or dry-run is not
restore evidence; Fugue does not perform or certify a drill here.

```sh
fugue backup artifact retire-recovered OLD_ID --replacement RETAINED_ID \
  --reason 'reason for retiring the old recovery point'
# Review the preview. Repeat the same command with --confirm PREVIEW_DIGEST.
```

Only platform admins can retire an active protected artifact recovered from a
failed or canceled control-plane Longhorn run. The old artifact must be at
least seven days old, without another alias, restore reference, live worker
lease, or future retry. The distinct replacement must be the newest unprotected
active artifact of the same policy, successful, at most 72 hours old, and belong
to the same volume and repository.
At least three distinct newer successful points (including the replacement)
must remain under the same policy, volume and repository. The corresponding
Longhorn Backup identities and remote backup configuration objects must exist
for the old point and the retained set. The commit atomically releases
protection, marks only the named old artifact deleted, and audits the actor,
reason, retained set and optional evidence digest. Original run histories
are preserved.

This is logical retirement. The remote collector observes the normal grace
period and lets Longhorn remove the Backup and manage shared blocks; do not
remove R2 blocks directly. The local Snapshot has a separate, fenced
[cleanup process](local-snapshot-cleanup.md) after verified remote retirement.
Neither the CLI result nor the layer size guarantees physical reclamation.

## Verification

Normal tests use an isolated fake provider and file-backed state. The opt-in
`TestR2ObjectStorageIntegration` creates uniquely named disposable resources,
verifies S3 operations and credential revocation, then cleans up only its own
bucket and tokens. Enable it with FUGUE_S3_INTEGRATION=1 and the two platform R2
configuration environment variables; never log the token.
