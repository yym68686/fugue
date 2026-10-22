# Compiler input chunk codec: reader compatibility

Readers were deployed before this optional writer. The independent schema lane
admits v2 manifests without rewriting inputs. New writes remain v1 until a separate
storage-policy publication enables `compilerInputCodec: {version: 1, enabled: true}`.
The publisher places it in `compiler-input-codec.json`; the original `policy.json`
is unchanged and remains readable by the prior reader-only rollback version.
Missing or invalid codec policy disables future v2 writes. No data is deleted.
Deploy and verify compatible readers before changing either schema or write policy. A previous code version cannot read v2;
all active readers and allowed rollback/LKG targets must support v2 before any
production writer is enabled. Disabling a future writer does not remove that
reader requirement while v2 inputs remain referenced.

## Wire representation

The manifest format `runtime-snapshot-json-chunk-codec-v2` retains the logical
canonical JSON SHA-256 as `content_hash`, decoded `size_bytes`, decoded
`chunk_size`, and decoded `chunk_count`. The decoded chunk size is at most
262139 bytes, leaving room under the existing 262144-byte stored chunk bound.
Each `data` value contains a five-byte envelope and one payload:

| Bytes | Meaning |
| --- | --- |
| 0 | Codec: 0 = raw, 1 = one RFC 1950 zlib stream |
| 1–4 | Big-endian uint32 decoded length |
| 5 onward | Exact payload; no trailing bytes or concatenated streams |

`chunk_hash` is the SHA-256 of the entire stored envelope. The reader checks
ordinal/count, stored hash, manifest-derived decoded length, envelope length,
zlib checksum and the complete decoded JSON digest. The raw fallback must have
exactly the declared decoded size. Unknown formats/codecs fail closed. Output is
bounded to one logical chunk plus one byte during decompression; compressed
input cannot exceed logical size plus the envelope.

A cold DNS read verifies the full decoded stream and canonical digest before
building its bounded index. The index records the observed physical hashes and
sizes, not assumed output from recompression. A warm selective read checks the
actual database SHA-256 and size of **every** chunk, including omitted chunks,
and decodes only requested chunks. Changing both stored data and its declared
hash cannot satisfy the previously verified index. No receipt bodies or positive
serving decisions are cached. Noncanonical valid JSON remains on the full read
path. Existing-input checks independently hash the supplied canonical snapshot, then
use a previously verified physical index to recheck every current database chunk
without returning bodies. A cold input still verifies its whole decoded stream
and records actual physical hashes. The v1 metadata-only hash verification remains
unchanged. Positive serving decisions are never inferred from these indexes.

## Writer verification

The writer uses one reusable zlib BestSpeed encoder and verification buffer per
input. It chooses raw fallback when compressed output is not smaller. Each encoded
chunk is decoded and compared before its physical hash is retained. Sixteen chunks
share one bounded database payload. Within the existing advisory-locked transaction,
PostgreSQL hashes the actual stored bytes and reports lengths/ordinals; all must
match the independent vector before commit. A mismatch rolls back the entire input,
even when both stored data and its claimed hash were changed. Existing v1 rows are
never rewritten and new v1 writes remain possible after disabling the policy.

The migration only expands the known format check, rejecting an unexpected schema.
It installs a NOT VALID check in a short lock transaction, then validates manifest
metadata without scanning chunk bodies or retaining ACCESS EXCLUSIVE. Retrying a
partially completed validation is supported. The old migrator preserves the wider
constraint on code rollback.

## Activation gates

Before the independent writer policy is enabled, verify atomic
readback and rollback on mismatch; compression cost/ratio and bounded allocation
on representative inputs; all public/compiler/DNS replay reader compatibility;
active and rollback version compatibility; independently reversible policy; and
natural production monitoring. Reader deployment alone does not reduce database
growth or prove a capacity/restore objective. Protected inputs remain retained.
