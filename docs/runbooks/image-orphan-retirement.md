# Audited orphan retirement

Image ownership and physical retirement are separate records. Unknown ownership
stays `unknown`: the controller never invents a deleted application image to
permit reclamation. A policy is configured independently of binary rollout using
`GET`/`PUT /v1/admin/image-cache/orphan-policy` (platform admin, generation CAS).
The default is observe. `GET /v1/admin/image-cache/orphans` exposes coverage and
durable observations; transitions are retained in `fugue_image_orphan_events`.

A retire policy explicitly scopes repository prefixes, offline/decommissioned
node exclusions, quarantine duration (at least 600 seconds), observations (at
least two), inventory freshness, sweep frequency and per-node limits. Excluded
nodes cannot be deleted. Every other registered cache participant needs a fresh,
complete inventory bound to its authenticated updater. Partial or reordered
reports cannot declare missing manifests absent. A policy generation change
restarts quarantine; repeated reads of the same inventory do not advance it.

The controller checks live workloads, rollback pins, migration references, active
tasks, build registrations, local pins, healthy replica requirements and complete
manifest graphs. Shared aliases and parent manifests propagate protection.
Unknown candidates become `orphan_retirement` only through a persisted decision
for the exact node/repository/target/digest and graph. API previews, automatic
scheduling and task claim revalidation use the same evaluator; node-local prune
checks still run. Changing the policy to observe suspends future claims. Failed
prunes retain the existing conservative failure cooldown and inventory recovery
requirements. A later complete inventory marks a disappeared decision `absent`;
this status does not invent a physical delete receipt.

Builders register artifacts before execution. A registration failure prevents the
push; a graph-verified immutable receipt is persisted before import completion.
These receipts survive failed deployments and business metadata deletion.
Reconciliation can retry registered builds while their authenticated completed
Job/Pod identity remains available. A missing Job or mutable tag cannot prove
ownership. Manifest pushes acknowledge success only after their replay journal
has been written, fsynced, renamed and its directory fsynced.

To validate a rollout, compare orphan decisions and `prune-plan` reasons, inspect
completed `prune-image-cache` task receipts, and request a fresh inventory. Verify
actual cache bytes/free space and retained workload/pin/graph protections. Never
report a diagnostic count reduction as physical space reclamation without node
receipts and a subsequent inventory.
