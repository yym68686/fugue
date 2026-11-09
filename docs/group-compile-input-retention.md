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

The group records `compile_inputs_required` on its first capture. Closing and
reopening with the option off, or an old Append caller, cannot bypass that
requirement. Repeated reconciliation of an unchanged legacy generation does not
backfill it. Old absent originals stay absent. A new generation can begin capture
without pretending that earlier generations have been recovered.

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
current authority, original publication signatures, complete dependency roots or
retirement. Captured producer metadata is a historical consistency witness, not
permission to trust a serialized `verified` flag. Existing missing historical
sources and all external registry/admission obligations remain in force.
