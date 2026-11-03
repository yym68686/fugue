# Longhorn S3 garbage collection

`flat-s3-gc.patch` targets Longhorn engine v1.12.1, commit
`1fa902f0061b84b92af445c38392bfeb99faffb2`.

The engine keeps ownership of its deletion lock, reference mark phase and
configuration consistency checks. The patch changes only the S3 sweep I/O:
flat paginated block inventory replaces recursive hash-directory listing, and
exact validated orphan block keys are deleted in batches of at most 1,000.
Repeated continuation tokens and per-object deletion errors fail closed.

`Dockerfile.longhorn-gc` runs the patch's HTTP-backed tests on Linux and replaces
the binary inside the matching official engine image. The normal main-push CI
lane publishes an immutable digest under the existing public package, using a
separate `longhorn-gc-` tag. No API release tag is changed. The installation lane
waits for the candidate engine image and a quiet backup/restore window before
changing Longhorn's default backup utility. Existing volume engine identities
must remain unchanged. The engine API and data format versions stay at v1.12.1.

The installer never kills a process, breaks a repository lock, strips a
finalizer, or deletes an S3 block itself. Changing the backup utility does not
upgrade already running volume engines.
