# Bounded encoded compiler-input backup

`fugue-input-storage --mode encoded-backup --backup-contract PATH` exports one
exact v4 separated input using the API-role `FUGUE_DATABASE_URL`. The contract
contains only:

```json
{
  "schema": "fugue.compiler-input-encoded-backup/v1",
  "digest": "sha256:<independently authenticated original SHA-256>",
  "expected_logical_bytes": 12345,
  "max_encoded_bytes": 33554432
}
```

`PATH=-` reads the contract from stdin. The confidential JSONL stream goes to
stdout; the metadata-only report goes to stderr. Capture it in a private
directory/file (0700/0600), never a public log or a CI artifact. The limit is
32MiB for the entire encoded stream, 8MiB for each record, and 128MiB logical.
Oversized inputs fail; this mode does not silently split or resume them.

The source takes the existing session reader barrier before a single read-only
Repeatable Read snapshot, with a 30-second context, one connection, SQL 5s,
lock 250ms, work_mem 1MiB and the installed zero-temp guard. It reads only the
selected v4 layout, component recipes/object placements and distinct encoded
packs, in bounded batches. It never reads legacy chunks or an archive, changes
policy, creates pins, writes data, or bypasses a retirement fence. Stored pack
hashes and metadata are checked before emission. A final stream hash/count is
emitted only after the read transaction commits. An incomplete stream is unusable.

Offline reconstruction requires no DB or keys:

```sh
fugue-input-storage --mode decode-backup --backup-contract contract.json \
  --backup-input private/encoded.jsonl --backup-output private/original.json
```

The input must be a private regular file and the output directory private. A
0600 temporary output becomes the new target atomically only after all checks
pass; an existing target is never replaced, and partial outputs are removed.
The decoder checks strict framing, the complete stream, layout, stored and
decoded packs, every object/component, total logical bytes, and the complete
original SHA-256, preserving repeated occurrences and original byte ordering.
Decoded pack caching is limited to 4MiB. It does not substitute a newer input,
rewrite original timestamps, sign anything, or infer source authority.

Export success is not a full-SHA readback. Offline decode success proves the
original bytes only. Independent original signatures, normal restore validation,
durable storage and verified pins are still required before admission. Neither
operation grants publication, activation, retention deletion or retirement
authority, and neither is a physical space recovery receipt.
