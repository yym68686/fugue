# Original group compiler input retention

`FUGUE_EDGE_CONTROL_RETAIN_COMPILE_INPUTS` is a reader-first durable format
capability. It defaults to false. No deployment intent enables it by default.
All current and promised rollback readers must first understand the new format;
the previous strict JSON reader refuses the added fields. Source custody,
retention and the complete storage/exit budget need review before enablement.

When explicitly enabled, a successful native shadow CAS also retains its exact
route projection, inventory, authenticated producer observations, original clock,
freshness bound, bootstrap choice, previous successful generation and final
ledger sequence. The source is supplied by a process-local native compiler
witness; a decoded ledger entry or caller-provided digest cannot supply it.
The inventory is compared in full under the same durable writer lock. A changed
heartbeat cannot substitute for the original observation even when its semantic
hash is unchanged. Source and ledger advance in the same atomic file replacement.
New captures also carry a domain-separated HMAC over the complete original input
and its group, final sequence, original time, key ID and keyring revision. The
native CAS loads the private projected group signing policy before and after
sealing, rejects a changed policy, and never stores raw key material. Missing or
invalid keys refuse the entire CAS. A decoded original cannot request a new seal.

The group records `compile_inputs_required` on its first capture. Closing and
reopening with the option off, or an old Append caller, cannot bypass that
requirement. Repeated reconciliation of an unchanged legacy generation does not
backfill it. Old absent originals stay absent. A new generation can begin capture
without pretending that earlier generations have been recovered.
The separate durable `compile_input_attestations_required` flag prevents later
unsigned captures after the first sealed record. Existing unsigned prefixes
remain unchanged and readable; no historical input is re-signed. Revoked keys
do not make retained files unreadable, but prevent source authentication under
current trust. Capture still defaults to false and requires compatible current
and promised rollback readers before enablement.

Reading a captured source reruns native compilation with its original time,
freshness, bootstrap and producer projection, and compares the whole unsigned
bundle and ledger binding. Compaction can archive the redundant shadow payload
but never prunes the captured original. There are at most 32 records, at most
8MiB per record and 32MiB total. Exhaustion refuses the entire next successful
CAS; it does not drop protected records or silently continue without capture.
This is an explicit operational stop condition, so the option must not be
enabled as an unreviewed way to repair a historical source gap.

Selective original export includes only requested captured generations and
validates each selected input against its exact shadow sequence. It remains a
diagnostic: native replay and a file checksum do not authenticate source custody,
current authority, complete dependency roots or retirement. Captured producer metadata is a historical consistency witness, not
permission to trust a serialized `verified` flag. Existing missing historical
sources and all external registry/admission obligations remain in force.

For each retained original input and exact signed publication body, the export
can additionally derive a `publication_bindings` entry only when the original
input attestation verifies under the independently loaded current group policy.
Its `input_digest` excludes the attestation itself; the binding's
`original_input_digest` includes the complete sealed record. Recomputing native
file checksums or a semantic-equivalent heartbeat cannot replace the source MAC.
The binding reruns compilation,
checks the complete unsigned candidate, publication sequence and candidate
sequence, original signing time, version and inherited recovery epoch, and
verifies the original primary signature against the independently loaded current
projected keyring. A still-valid secondary signature cannot substitute for a
revoked original issuance key. Historical expiry is checked at original issuance;
this does not authorize serving or reactivation with an expired bundle today.

A signed traffic-release binding supplies concrete release-set artifact, route
artifact, release and original input references. They still need native DB
resolution, signatures, complete registry/custody coverage and the final shared
snapshot before admission. Legacy unbound originals have
`dependency_binding_known=false`; an empty reference list is not proof of a
dependency-free leaf. Missing originals or unverifiable publications do not gain
a binding, and a refreshed signature never backfills the old issuance. Binding
commitments and exported verification fields are diagnostic values, not accepted
external roots or caller-supplied admission tokens. The existing 8MiB generation,
32MiB batch, current-keyring recheck and read-only boundaries also apply to these
derived entries. No production capture or retirement policy is enabled by export.
The source seal does not prove independent custody or complete registration.
Publish-before-register exclusion and the final same-RR database root check
remain necessary; a file export alone cannot close that cross-store race.
