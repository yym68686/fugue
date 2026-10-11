# Managed SSH identity maintenance

Storage encryption keys and SSH host identities have different lifecycles.
Rewrapping changes the encryption of a retained private seed; it does not change
the seed, public key, identity owner, client trust, application, or serving route.
The independent `managed_ssh_identity_maintenance` CI lane runs without an API
or controller deployment. Its initial production declaration is read-only `plan`.

## Inventory and backup

Commit a new canonical `planID` and operation `plan` in
`deploy/environments/production/managed-ssh/identity-maintenance.json`, then push
main. The lane checks the current committed keyring generation and digest,
resolves the database's declared Kubernetes Service without changing its TLS
hostname, and takes a repeatable-read snapshot. Both encrypted identity records
and ownership anchors must agree and every private key must decrypt to the
anchored public identity. Missing, corrupt, substituted, or excessive inventory
fails closed. The tool does not initialize or migrate the application database.

The encrypted snapshot and public review plan remain in the declared runner
directory, mode 0700, in new mode-0600 files. Existing files are not overwritten.
CI publishes only a receipt with counts, key identifiers, hashes, and retention
policy. It never uploads the encrypted snapshot or keyring. A repeated plan
checks the existing backup against the current inventory; changes require a new
plan ID. Copy protected snapshots to the organization's encrypted off-host
backup store and retain the corresponding keyring independently; runner-local
storage alone is not a disaster-recovery backup.

## Storage-key rotation

1. Through the independent keyring declaration and existing keyring CI lane,
   install an overlapping ring that retains previous key bytes and selects the
   new active encryption key. Confirm its exact declared generation and digest.
2. Publish a fresh read-only maintenance plan after that ring is installed.
   Review the public identity inventory, backup digest, exact ring digest, and
   old/new storage-key transitions. Confirm an independently retained recoverable
   backup exists before approving any write.
3. Change only the operation to `apply` and `expectedPlanDigest` to the exact
   reviewed plan hash. Push main through the normal production workflow. Apply
   requires the saved backup and plan, recomputes them from current database
   state, then performs conditional rewraps and audit inserts in one serializable
   transaction. Lock and statement waits are bounded. Any audit, identity,
   ownership, or precondition failure rolls back the transaction.
4. Obtain a new read-only plan to confirm all records use the selected encryption
   key and retain unchanged host identities. A committed rewrap makes the old
   plan stale; it cannot silently repeat. If commit acknowledgement is lost,
   inspect and re-plan before retrying. Do not assume rollback from a client error.

Old encryption keys remain necessary for historical backups. The keyring lane
rejects key removal or replacement under an existing key ID. No automatic key
destruction or host identity rotation is part of this operation.

## Retention and recovery

Application deletion retains identity records and ownership anchors. This avoids
silently transferring a logical identity or regenerating a missing key during
recovery. Retired runtime identity Secrets and state volumes remain retained;
drained executables scale to zero only after their sessions close. Retention is
explicitly indefinite until a separately reviewed decommission policy identifies
the affected identities, backups, client trust, and recovery obligations. There
is no background age-based key deletion.

Restore encrypted identities and ownership anchors together into an isolated
database first, using the independently protected matching keyring. Run snapshot
and plan checks against that database and compare public identity metadata with
the authenticated inventory before any production restoration. Existing serving
artifacts remain authoritative during a failed code/configuration release;
maintenance failures do not mutate runtime selection or client trust.

Tests cover exact-plan rejection, missing anchors/records, ciphertext privacy,
file permissions, audit failure rollback, and storage rewrap in a disposable
PostgreSQL database. Production acceptance starts with read-only inventory; it
does not perform an unnecessary production encryption-key rotation.
