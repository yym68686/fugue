# Fugue backup restore runbook

This runbook covers the productized backup MVP. It avoids manual production
patching as the normal path: code and control-plane behavior should still be
released through the Fugue repository and the control-plane GitHub Actions
workflow.

## Offline control-plane restore

Use offline restore when the Fugue API store is unavailable, corrupted, or needs
to be recovered outside the running control plane.

1. Stop control-plane writers.

   Scale the API/controller writers down or otherwise fence writes before
   restoring the database. Do not restore into a live writable database.

2. Locate the artifact.

   ```bash
   fugue backup artifact ls --target-type control-plane-db
   fugue backup artifact show <artifact-id>
   fugue backup restore control-plane \
     --artifact <artifact-id> \
     --mode offline-control-plane
   ```

   If the API database is unavailable, use the object store directly. Each
   backup directory contains `manifest.json` plus the PostgreSQL dump object.

3. Download the dump and manifest from the configured backend.

   The manifest records target type, run id, policy id, object key, size,
   SHA-256 checksum, store fingerprint, and invariant summary.

4. Verify the artifact.

   Compare the dump SHA-256 with the manifest value before restoring.

5. Restore PostgreSQL.

   ```bash
   pg_restore --clean --if-exists --no-owner --no-privileges \
     --dbname "$FUGUE_DATABASE_URL" postgres.dump
   ```

6. Start the control plane in read-only or single-writer mode first.

   Verify `/readyz`, `fugue admin backup status`, and control-plane store
   invariants before bringing all writers back.

7. Record the restore run.

   ```bash
   fugue backup restore run --plan <restore-plan-id>
   ```

## Sealed release recovery archive

Before a GitHub-disconnected recovery drill, stage the last verified release
and its exact rollback record on an encrypted filesystem. Use the dedicated
GET-only Kubernetes observation context; the new absolute directory must not
exist.

```bash
fugue-production-control capture-release-recovery-archive "$NAMESPACE" "$COMPONENT" "$READ_ONLY_KUBECONFIG" "$CONTEXT" "$NEW_PRIVATE_DIRECTORY"
```

The command publishes `sealed-release.json` with mode `0600` only after
checking the current pointer, both immutable records, and the rollback
manifest. The archive contains the original execution plans, manifests,
verified terminal results, and artifact receipts. Treat it as sensitive:
do not put it in Git, an Actions artifact, or a public bucket. The command's
JSON output contains only the archive digest and release identities. Put
the archive in independent custody and pin its digest, source cluster,
namespace, component, pointer UID and verified release identities in a
separately retained inventory. Do not recompute the trusted digest from
the copy presented for recovery.

```bash
fugue-release-bootstrap verify-release-recovery-archive "$ARCHIVE" "$INDEPENDENTLY_PINNED_SHA256"
```

This mode opens no production connection. To also check the independent
registry copies, append the absolute `MIRROR_CONFIG` path with `registry`,
optional `registryAuthFile`, `forwardRepository` and `rollbackRepository`.
It reads only that registry, using the archive's pinned receipt for each
version. It does not modify a historical manifest to use a mirror, authorize
a rollout, recreate provider state, or prove that a node can pull without
GitHub. Record those checks separately in the recovery drill.

For a release still using a registry that is being retired, use the same
archive and mirror configuration to draft a registry-only rehome:

```bash
fugue-release-bootstrap draft-release-registry-rehome "$ARCHIVE" "$INDEPENDENTLY_PINNED_SHA256" "$MIRROR_CONFIG"
```

Use the same independent repository for the current and rollback images.
The draft compares both sealed Deployment PodSpecs, changes only the digest-
pinned image references in the projection, and reads through both mirror tags
and all runtime image blobs. A companion Service is read-only context: its
declarations must agree, ignoring only the three release provenance stamps.
Different Services, other resources, and unverified container or volume
images reject the draft. The result contains identities and digests, not an
apply manifest. It does not prove live Service agreement, runtime pull by
the nodes, writer revocation, or a verified cutover. Do not apply a draft or
modify the original monitor records to treat the draft as a new baseline.

For an approval-ready review, use the existing Deployment's canonical
journal resource (`kubernetes/<namespace>/deployment/<name>`), not a
separate registry lane. With the read-only Kubernetes context, check the
live source first:

```bash
fugue-production-control observe-release-registry-rehome "$ARCHIVE" "$INDEPENDENTLY_PINNED_SHA256" "$GUARDIAN_GROUP" "$READ_ONLY_KUBECONFIG" "$CONTEXT"
fugue-production-control draft-release-registry-rehome "$READ_ONLY_JOURNAL_CONFIG" "$READ_ONLY_KUBECONFIG" "$CONTEXT" "$ARCHIVE" "$INDEPENDENTLY_PINNED_SHA256" "$GUARDIAN_GROUP" "$REQUEST_ID" "$MIRROR_CONFIG"
```

The second command requires a pre-existing verified journal head with no
pending claim. It compares the live Deployment and each read-only Service
to the archived declarations and repeats the source and journal reads after
checking both mirror copies. The output is an intent for `inspect`, owner
`sign`, and journal recording. It is not a Deployment patch or fresh
baseline evidence. An approved registry rehome is explicitly rejected by
the ordinary claim path until a dedicated provider-fenced executor exists.

## Native production journal recovery

Use this additional gate once a resource's native journal can authorize writes.
The ordinary backup manifest verifies the dump object's bytes. It does not
identify the correct control journal, or prevent a valid but older archive from
being accepted. Retain independent evidence for the exact recovery point.

1. Use a dedicated backup principal with `USAGE` and unfiltered `SELECT` on
   every journal table and the app/provider tables required by the dump. Keep
   the principal, local control binary and artifact store independent of
   GitHub, the serving API and their deploy credentials. The app's configured
   backup DSN is not assumed to have this access. Verify the existing backup
   schedule still produces complete backups after installing the journal.
2. Use an independently provisioned, owner-only libpq service file with a
   reader credential and a CA for the same PostgreSQL hostname as the
   source journal configuration. Choose an encrypted staging filesystem and
   pass an absolute directory that does not already exist.

   ```bash
   fugue-production-control journal-recovery-create "$SOURCE_JOURNAL_CONFIG" "$PG_SERVICE_FILE" "$PG_SERVICE" "$NEW_PRIVATE_DIRECTORY"
   ```

   The command holds a read-only PostgreSQL snapshot until the journal
   fingerprint and custom-format database export finish. It creates
   `control-plane.dump` and then `recovery.json` in the new private directory.
   The resulting recovery record stages the pin, but does not itself establish
   independent custody. Copy the verified archive to the independently
   accessible archive store and pin its SHA-256, checkpoint digest, source
   cluster and archive identity in an append-only inventory. Preserve installed
   policy/grant manifests, independent execution witnesses and required release
   artifacts separately. Review the latest eligible entry against known later
   claims and provider writes before choosing a restore point.
3. Restore the pinned dump to a fresh, private database with local tooling.
   No API, controller, bootstrap publisher or Guardian worker may connect until
   the restored state and provider fences are reconciled. Verify the archive
   checksum before restoring with `--exit-on-error --single-transaction
   --no-owner --no-privileges`. Provision a separate read-only principal,
   point its independently installed journal configuration at the restored
   database, and verify the journal without loading runtime credentials:

   ```bash
   fugue-production-control journal-recovery-verify "$RESTORE_JOURNAL_CONFIG" "$PINNED_CHECKPOINT_DIGEST"
   ```

   The check covers all current journal tables, including pending claims,
   consumed launch permits, recorded results and audit rows. A mismatch leaves
   the target isolated. The check does not validate provider state, SQL roles,
   object ownership, functions or trigger definitions.
4. Keep dispatch disabled while reviewing every pending claim and any
   operation that may have happened after this recovery point. Neither a
   missing claim nor a restored approval proves an external write did not
   happen. Independently verify exact provider readback and closure of old
   writers before granting any replacement writer. Reinstall and check
   version-matched roles, grants and database/provider admission from the
   independently retained configuration. Role names alone do not rebind
   stored provider role OIDs on a new database. Stop if an adapter cannot
   prove its restored authority or the last verified state. Never repair the
   checkpoint by editing history or re-enabling a retired writer.

The isolated checkpoint drill uses the real PostgreSQL dump and restore tools.
Passing it verifies snapshot alignment and persistence of a pending release
fence. It is not evidence that production backups, provider role recovery,
or a GitHub-disconnected production upgrade have been exercised.

## Restore drill

Run drills against an isolated target database or staging control-plane store.

1. Trigger a fresh control-plane backup.

   ```bash
   fugue admin backup run --wait --version drill-$(date -u +%Y%m%dT%H%M%SZ)
   ```

2. Create a restore plan.

   ```bash
   fugue backup restore plan --artifact <artifact-id> --mode plan-only
   ```

3. Restore into an isolated database with `pg_restore`.

4. Start a staging Fugue API against that database with production writes
   disabled.

5. Validate store invariants, tenant/project/app counts, runtime inventory, and
   route/DNS posture.

6. Record drill outcome in the restore plan or run notes.

## Safety boundaries

Pending control-plane and application Longhorn backups are revisited by the
normal scheduler, scanning oldest eligible runs first. A run waiting for the
shared snapshot/release coordination Lease is resumed under the same run ID; it is
not failed, replaced, or force-unlocked just because it is old. Each attempt
must still acquire the coordination Lease and atomically claim the database
record before starting backup effects. Retry-triggered runs retain their
separate due-time scanner; future `next_retry_at` values are never bypassed.
Recovered Longhorn workers likewise resume their original run and existing
provider objects. Disabled automatic policies and database lifecycle interlocks
are rechecked by the normal execution path, not bypassed by the scanner.

To investigate a prolonged Pending run, inspect
`fugue backup run show RUN_ID --json` and API logs containing that run ID.
Empty `started_at` and `lease_owner` mean it has not been claimed, not that the
provider is uploading.
Do not cancel an unknown backup, delete its provider objects, or clear a
coordination Lease to make a database migration pass.

- App database and persistent-storage destructive restores require an explicit
  restore plan and protective backup before replacement.
- Clone restore is preferred for app database verification because it avoids
  changing the live service.
- Never treat replication, failover, or VolSync alone as historical backup.
- Do not delete old artifacts during a restore drill unless retention has
  already expired them and they are not protected.
