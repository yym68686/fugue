# Configuration-only image history

Operation source envelopes can contain only `config_base_spec`, with neither
`desired_source` nor `desired_origin_source`. The image-candidate projection
previously treated this shape as a legacy source and copied the full rollback
configuration. Image inventory fetched and decoded unused configuration, and
different configurations prevented otherwise identical inputs from folding.

The write trigger and idempotent bootstrap backfill now remove `config_base_spec`
from legacy object shapes too. Existing named source envelopes, JSON null,
non-object error behavior, legacy source fields, image references and timestamps
are preserved. Authoritative operation configuration is not modified. The real
PostgreSQL regression checks four distinct 1 MiB configurations, repeated
backfill, and legacy sources against the existing narrow operation reader.
Candidate folding keeps the first input, the first greatest observation time,
and the last completed operation for each source/image pair. The latter retains
historical immutable-image provenance even when newer attempts are incomplete.
SQL and file-store folding agree; randomized API tests preserve candidate
output across interleaved source, image, time and status combinations.

API, Controller and SSH Control all initialize this schema. Their release
intents advance together so a later restart does not reinstall the old trigger.
Old readers remain compatible. Rolling back code preserves business semantics,
although an old bootstrap can reintroduce the redundant projection. Serving
artifacts, configuration intent and positive LKG are not changed.

A read-only production measurement found 55 affected projection rows and
4.72 MB of unnecessary source JSON. The all-app candidate query's source JSON
falls from about 14.02 MB to approximately 9.30 MB before adding the completed
provenance representative. This is a payload estimate, not a measured
end-to-end latency gain or attribution of historical total traffic.

After declarative deployment, verify all three component receipts, the trigger,
absence of config_base_spec in the derived table, unchanged authoritative
configuration sizes, API errors, consumer convergence and query payload bytes.
