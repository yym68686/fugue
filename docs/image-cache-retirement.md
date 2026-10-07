# Image-cache retirement and LocalPV evidence

Physical inventory does not authorize deleting image content. API preview,
controller plans and task claim revalidation use `internal/imagecachepolicy`.
A plan records the policy version, content hash, first observation, protection
reason and durable image/replica retirement references. Unknown metadata is
quarantined regardless of age. In delete mode unsafe aliases are protected
before graph propagation, preserving all child manifests and shared layers.
Tag targets are bound to the expected digest at both claim and node execution.

`fugue_image_retirements` retains immutable generation decisions independently
of business image/app rows. PostgreSQL archives transitions transactionally;
existing retiring generations are backfilled idempotently. A restored live
image row overrides an older tombstone. Completed historical operations can
restore missing attribution only with an exact immutable repository/digest
and fresh complete physical graph. Recovered rows enter `lost`, never serving
or deleting; normal retention and migration gates decide their next state.
Objects without affirmative provenance remain quarantined. Their current
plans provide a reviewable inventory; age or an absent database row never
creates a retirement decision.

Upload expiry is separate from manifest retirement. The image-cache defaults
to observation; production intent explicitly enables it. Configuration:

- `FUGUE_IMAGE_CACHE_UPLOAD_GC_MODE`: `observe` or `delete`.
- `FUGUE_IMAGE_CACHE_UPLOAD_TTL`: inactivity threshold (default 24h).
- `FUGUE_IMAGE_CACHE_UPLOAD_GC_INTERVAL`: default 15m.
- `FUGUE_IMAGE_CACHE_UPLOAD_GC_MAX_BYTES`: per-pass cap (default 1GiB).

A pass treats a data file and its JSON state as one session, honors the newest
activity, skips malformed state/symlinks/unknown layouts, serializes against
HTTP writes, checks open descriptors and rechecks file identity before unlink.
Inability to establish inactivity preserves files. Metrics and expiry receipts
report the temporary bytes, state mismatches and actual deletions. Targeted
manifest prune never implicitly expires uploads. Interrupted append records
actual on-disk progress before returning the transport error.

Host LocalPV collection no longer requires kubectl credentials. Unknown PV
ownership is reported as `bound_pv_count=-1`, `bound_pv_count_known=false`.
The authenticated API joins Kubernetes node/PV facts, including Bound volumes
missing from a host LV report. Ambiguous affinity conservatively retains the
claim; failed reads cannot become zero. Only ownership failures are removed
when enrichment succeeds; host/LVM failure evidence remains. Relevant storage
nodes receive the current optional updater protocol through normal tasks.

Release the additive schema before API/controller, then image-cache through
normal component intents and GitHub Actions. Verify actual workload revisions,
not just successful builds. Review upload receipts and quarantine/deferred
counts after deployment. Neither image cleanup nor LocalPV enrichment changes
serving configuration, migration authorization or positive LKG.
