# Edge cache retention

HTTP responses and node-local system images are disposable caches, separate
from signed route/DNS artifacts, configuration intent and positive LKG state.
Neither collector modifies serving configuration.

## HTTP response cache

Each worker inventories only `FUGUE_EDGE_ASSET_CACHE_PATH` at startup and every
five minutes. `FUGUE_EDGE_ASSET_CACHE_TOTAL_MAX_BYTES` defaults to 512 MiB per
worker; `FUGUE_EDGE_ASSET_CACHE_MAX_BYTES` remains the per-response capture limit.
The total counts allocated file and directory blocks and reserves room for an atomic temporary
write before accepting it. Writes bypass caching while inventory is incomplete,
collection is running or capacity is unavailable; forwarding remains available.
Above 90% of capacity the collector removes oldest responses to 80%, after
removing entries past their expiry and stale window. Legacy entries without a
persisted stale deadline receive at least 24 hours of expiry grace. Current
policy can extend that grace. Temporary files abandoned for an hour and empty namespace directories are removed. Unknown JSON is counted but never evicted as a response. Scans have a 30-second deadline; metrics reads do not wait for them.
Symlink traversal outside the cache root is rejected. Collection never deletes
route bundles, certificates, WAL or other sibling directories.

`fugue_edge_http_cache_*` metrics report allocation, limit, readiness, successful
inventory time, removals by reason, errors and skipped writes. A collector error
does not change route readiness or invalidate the loaded LKG. Lowering the limit
converges at the next scan. Old host directories outside the configured cache
root require a separate reviewed ownership inventory; code does not discover and
delete arbitrary historic host paths.

## Edge node image cache

The independent `edge-image-gc` declarative component selects nodes labelled
`fugue.io/role.edge=true`. It does not expand the legacy node-janitor's tenant-data
or runner-cleanup scope. Its reviewed command-line configuration controls the
repository allowlist, apply/observe mode, observation interval, minimum unused
age and deletion batch size. Production starts with a 24-hour age, hourly
observations and four exact image IDs per batch.

Protection covers all local CRI containers (including exited init containers),
pinned images, every Kubernetes Pod and workload template/history, and every
retained ConfigMap digest in the release namespace. Thus desired/candidate
releases and retained rollback/LKG records are protected even with zero replicas.
Images with any repository alias outside the allowlist are retained. The default
empty allowlist cannot delete anything. The process uses a dedicated service account with list-only workload/history
permissions and release-namespace ConfigMap access. It reads its host root mount without
changing files there; removals are CRI RPCs. Only the dedicated `/state` volume
holds its own unused-since evidence.

An unavailable, truncated or malformed inventory aborts the sweep. An observation
gap longer than twice the configured interval resets the age. Before each delete
the entire protection inventory is refreshed. CRI `rmi` receives only an exact
image ID, never `--prune`; containerd manages shared layers and active snapshots.
No direct blob/snapshot deletion is permitted. This is conservative cache
maintenance, not a promise to reclaim every image absent from `crictl ps`.

`/healthz` describes process liveness; `/readyz` requires a recent complete
inventory. `/metrics` and JSON sweep logs describe
collection success, protected/waiting/candidate/deleted counts and failures.
Alert on stale last-success timestamps (over two sweep intervals), errors or
persistent disk pressure. Do not infer reclaimable bytes by summing image sizes;
measure filesystem availability after containerd finishes asynchronous GC.

Release only through `git push main` and `.github/workflows/ci.yml`. Verify the
new worker's metrics, unchanged route/LKG health, the maintenance DaemonSet and
its initial protected/waiting inventory. The first eligible image batch follows
24 hours of successful observations; verify that separately.
