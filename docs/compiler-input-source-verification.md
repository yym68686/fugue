# Original compiler-source verification

The native `fugue-input-storage` mode `compiler-source-verify` authenticates
one retained compiler parent and its original intent and policy. Run the
normally released binary in the trusted API role; its existing signing keyring
and current revocations remain inside the process. The contract contains no
keys, source contents, mutation flags or caller assertions of authority.

Supply only `--mode compiler-source-verify` and
`--source-verification-contract PATH` (`-` accepts stdin), with these fields:

```json
{
  "schema": "fugue.compiler-input-source-verification/v1",
  "artifact_id": "the-exact-retained-parent-id",
  "expected_content_hash": "sha256:the-exact-parent-content-hash"
}
```

The strict contract is at most 16KiB. One read-only repeatable-read transaction
reads the exact parent and the two exact kind/scope/generation source rows.
Each row's variable-length fields must total at most 8MiB. The transaction has
a 30s context, 5s statement timeout, 250ms lock timeout, 1MiB work memory and
zero temporary-file allowance through the existing administrator-owned fixed
guard. The ordinary API role receives no general SET privilege. A missing or
altered guard refuses verification before artifact reads. No input chunks,
archive objects or business
rows are read or written. Missing, oversized, altered, revoked or inconsistent
sources fail without a partial successful source list or a newer substitute.

Verification checks the original signatures and canonical artifact content
hashes, signed parent lineage and metadata agreement, strict typed source
schemas, exact scope/generation and typed intent/policy digests. Typed lineage
digests and map-encoded artifact content hashes are distinct and both matter.
Only a committed read-only transaction returns `verified=true`, with safe
source identities, hashes and signing-key IDs; contents, signatures and keys
are never included in its report.

This is a source-authentication receipt. It does not establish the external
authority registry, current retention roots, complete dependency closure,
full input SHA, restoration, durable pins, rollback compatibility or physical
retirement. Those require their separate real evidence. It neither enables
the production configuration policy nor changes the public retirement refusal.
