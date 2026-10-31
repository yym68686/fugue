# Project object storage

Object storage is opt-in. Fugue provisions a private R2 bucket per resource,
owned by a tenant and associated with a project. It never stores application
objects in the backup or DataWorkspace garbage-collection namespace. Apps read
and write R2 directly using standard S3 credentials; the control plane manages
resource metadata, authorization, and credentials only.

## Configure the platform

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

Disabling persists `disabling` before revoking every grant upstream and reports
`disabled` only after revocation succeeds. A failed operation remains retryable.
Re-enabling permits new credentials; it does not resurrect revoked ones.
Neither disabling nor compute deletion deletes objects. Resource metadata and
credential records outlive compute. Tenant deletion with owned stores is
blocked to preserve ownership; this release deliberately has no bulk object
purge operation.

Usage measurement lists the bucket to calculate current object bytes and count.
It retains the previous successful measurement on failure and includes its
observation timestamp. quota_bytes is a **soft budget**, not a synchronous hard
limit or provider invoice. Multipart pending bytes, request charges, and billing
reconciliation are not represented as measured zero. Measurements are explicit,
so direct R2 traffic does not load the Fugue control plane.

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

## Verification

Normal tests use an isolated fake provider and file-backed state. The opt-in
`TestR2ObjectStorageIntegration` creates uniquely named disposable resources,
verifies S3 operations and credential revocation, then cleans up only its own
bucket and tokens. Enable it with FUGUE_S3_INTEGRATION=1 and the two platform R2
configuration environment variables; never log the token.
