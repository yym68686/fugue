# Exact release ledger backup

`fugue-input-storage --mode release-backup --release-backup-contract PATH`
reads an explicit selection of existing publication ledger records. `PATH` may
be `-` for stdin. The strict contract has schema
`fugue.platform-release-backup/v1`, an ordered `release_ids` array of 1–32 unique
IDs, and `max_encoded_bytes` between 1 and 32 MiB. No other flags are accepted.
The API-role `FUGUE_DATABASE_URL` is required. This is an operator read path,
with no new HTTP endpoint or database migration.

The source acquires the ordinary reader barrier before opening one read-only
repeatable-read snapshot: 30 seconds overall, SQL 5 seconds, lock 250 ms,
work_mem 1 MiB, zero_temp_v1, one connection, no parallel query or JIT. Each ID
uses its indexed exact lookup. Missing records, unknown persisted columns or
over-budget records fail the entire selection. The source field text is
conservatively limited to 1 MiB per row before transferring it to Go, allowing
worst-case JSON escaping inside the separate 8 MiB encoded-row ceiling.
The complete envelope, including framing, must fit the caller's batch budget.

Stdout contains one canonical JSON envelope and final newline, emitted only
after commit and release of the DB barrier. It retains all 30 persisted release
columns, including raw nested scope/evidence, original lifecycle status,
generation references, fencing/version integers and timestamps. JSON numbers
retain integer precision. `rows_sha256` covers the exact canonical rows array.
Stderr receives a safe report with observed snapshot/time, count, encoded byte
size, whole-envelope SHA and completion. Keep stdout in a private 0600 file
under a 0700 directory; never display its contents. A short, failed or canceled
write is incomplete and must be discarded. A blocked write cannot retain the
DB barrier and can at most finish that single bounded write after cancellation.

`store.DecodePlatformReleaseBackup` verifies the offline bounded envelope,
canonical framing, row checksum and exact ordered IDs. Neither checksums nor
these snapshot facts establish artifact signing authority, full dependency
closure, current activation, consumer identity, ACK/LKG, a retention decision,
a local pin or permission to restore/retire anything. The source creates no
rows and reads no compiler-input or artifact bodies. An independently reviewed
restore must preserve original facts and use the ordinary runtime's admission
and signature checks; historical states must never be manufactured as active.
