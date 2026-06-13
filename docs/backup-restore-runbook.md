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

- App database and persistent-storage destructive restores require an explicit
  restore plan and protective backup before replacement.
- Clone restore is preferred for app database verification because it avoids
  changing the live service.
- Never treat replication, failover, or VolSync alone as historical backup.
- Do not delete old artifacts during a restore drill unless retention has
  already expired them and they are not protected.
