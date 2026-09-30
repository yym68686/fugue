# Worker standby observations

An idle Worker may still be the immediate rollback executor. Zero connections
alone cannot authorize retiring it or moving its resource reservations.

An explicit `fugue.worker-standby-observation/v1` declaration pins a node, the
current traffic authority record and epoch, the stable code release, and the
inactive Worker source and image. The observer checks the component Lease,
fresh Guardian health, every Front on that node, and the Worker's IPv4 and IPv6
socket tables repeatedly. Front readiness must match the exact current
authority. Unknown Fronts, incomplete connection inventories, changing
authority, or an owned mutation Lease stop the observation.

The result distinguishes idle capacity from a referenced rollback executor.
`authorizes_mutation` and `retirement_ready` are always false. Retiring a
referenced executor requires a separate, validated replacement or reconstruction
mechanism that preserves positive LKG and rollback availability. The observation
is historical evidence, never a reusable lock or a deletion permit.

Update one declaration under
`deploy/environments/production/worker-standby-observation/` and push to `main`.
The normal CI entrypoint runs the read-only observer and retains its receipt.
This lane is independent of image builds and does not change Kubernetes objects,
release records, configuration artifacts, or traffic. No API credentials are
passed to the observer, and connection addresses and hostnames are omitted from
its receipt. Script changes alone do not replay old pinned declarations.

Local validation:

```sh
python3 -m unittest scripts.test_observe_worker_standby
python3 -m scripts.observe_worker_standby declaration.json --validate-only
```
