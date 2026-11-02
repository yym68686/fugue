# Managed object-store deletion

`fugue s3 delete STORE` and `fugue s3 deletion-plan STORE` only preview the
target. The plan contains cached usage, its measurement time when available,
and a SHA-256 digest of the immutable bucket identity. Cached usage can be
stale or unmeasured. The digest does not freeze a list of objects: execution
permanently deletes **all** contents of that bucket, including unfinished uploads.

After reviewing the plan and stopping external writers, execute with all
three confirmations:

```sh
fugue --project example s3 delete events --execute \
  --confirm-bucket EXACT_BUCKET_FROM_PLAN \
  --plan-sha256 DIGEST_FROM_PLAN \
  --ack-no-external-writers
```

The server requires `storage.admin` or `data.admin` with tenant/project access.
Only independently provisioned Fugue application buckets are accepted. This
command does not manage Longhorn, Barman, shared backup blocks or DataWorkspace
blob storage.

Execution persists `status=deleting` before revoking every Fugue-issued grant.
It then removes at most eight pages of 1,000 objects and 16 unfinished uploads
per request. The CLI continues requests until the provider confirms bucket
deletion. `--wait=false` executes one batch and returns `deleted=false` if more
work remains. `s3 show` includes persisted progress; counters are lower bounds
because a process can stop between provider deletion and receipt persistence.

If interrupted, rerun the same confirmed command. A deleting store cannot be
enabled, recreated under the same name, or issued new grants. Revocation or
provider errors retain the store and its deletion state for retry. The server
removes Fugue metadata and credential records only after provider bucket
deletion succeeds or the provider confirms the bucket is already absent.
If the final response is lost, check `s3 ls` and the provider bucket inventory.

Fugue revokes the credentials it issued. Credentials created outside Fugue must
be stopped separately; the acknowledgement is an operator assertion, not a
claim that Fugue can fence every Cloudflare administrator credential. Provider
rules that prevent bucket deletion surface as errors and are not changed
automatically.

API clients first POST `/v1/object-stores/{store_id}/deletion-plan`, then DELETE
`/v1/object-stores/{store_id}` with `confirm_bucket`, `plan_sha256`, and
`ack_no_external_writers=true`. Repeat a `202` response while `deleted=false`.
A `200` with `deleted=true` means the bucket and Fugue records have been removed.
