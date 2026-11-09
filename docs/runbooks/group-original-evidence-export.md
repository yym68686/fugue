# Selected original group evidence

`fugue-edge-control export-group-original-evidence` reads one explicitly chosen
original state file and its independently chosen current group signing keyring.
Run it only through the normal published binary in the exact source owner.
It initializes no serving runtime, store, HTTP listener, database, locks or
filesystem writes. Capture stdout directly into a private local file (0700
directory, 0600 file); the selected source values may contain private routes.
Do not print the report in a terminal, CI log or public artifact.

Supply absolute canonical `-state-file` and `-signing-keyring-dir` paths, the
exact `-edge-group-id`, and 1–32 distinct comma-separated `-generations`. Bind
the capture independently to the source Pod UID, image/source revision,
Deployment owner and declared paths before and after. An unknown generation
returns empty evidence for that selection; it does not prove an absent source.

The scanner permits at most 32MiB encoded and decoded source in total, 8MiB per
JSON value, 64 levels of nesting, 32,768 array entries across the source, the
native eight-candidate history limit and 60 seconds including cancellation.
It hashes typed values one at a time in native field order, checking the full
canonical state checksum without retaining or exporting unselected history.
Selected evidence is limited to 8MiB per generation and 32MiB total. There is
no truncated success or retry. Gzip checksums, trailing data, multiple streams,
unknown fields, non-native field ordering, duplicate top-level fields, invalid
sizes, non-private/non-regular files and changed source/keyring are refused.

The report retains the exact original selected shadow/publication rows and
available signed bodies. Signature verification uses the original issuance
time and the independently loaded current revocation set. A staged candidate
can have a valid signature with no matching publication sequence. A refreshed
LKG cannot supply a missing historical publication signature. Original source
and signing key material are never created, modified or re-signed.

`checksum_verified` is file-integrity evidence, not authentication of custody.
`state_contents_validated`, `admission_authenticated` and `dependency_closure`
remain false: unselected transitions and the full dependency graph are not
semantically admitted. Missing original route intent/inventory/compile inputs,
historical signatures, independent policy custody and persistent reactivation
must remain unresolved. This diagnostic grants no activation, retention
classification, migration, routing, fencing or physical retirement authority.
