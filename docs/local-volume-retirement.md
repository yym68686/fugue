# Retire an unused local Retain volume

`fugue admin storage volume-retirement` is an operator command for one unused,
directory-backed Kubernetes `local` PV with `Retain` policy, or an explicitly
identified historical directory whose PV no longer exists. It uses explicit
Kubernetes access and root SSH access to the storage node, like the physical
backup operator commands. It does not add an HTTP API or grant access through
a Fugue API key. CSI, LVM pools, hostPath PVs and automatic bulk cleanup are
unsupported.

First identify the exact claim, Released PV or orphan directory and confirm that its retained data may be
discarded. Absence of Pod references alone does not establish that the data is
unneeded. Backup timestamps in a plan are observations, not proof of a recent
backup or a successful restore. Coordinate with other administrators and
controllers: the retirement lease serializes this command, not arbitrary
Kubernetes or root filesystem writers.

```sh
fugue admin storage volume-retirement plan archive-db-2 \
  --namespace test-system --ssh control-node --node-ssh storage-node \
  --storage-root /var/lib/storage --output-file plan.json

fugue admin storage volume-retirement execute \
  --ssh control-node --node-ssh storage-node \
  --plan-file plan.json --plan-sha256 <plan_sha256> \
  --ack-delete-data --reason 'Audited obsolete replica; retained data may be discarded'

fugue admin storage volume-retirement status \
  --namespace test-system --ssh control-node --pv-uid <original-pv-uid>
```

Historical volumes have explicit selectors; they do not recreate claims or
volumes to bypass missing ownership metadata:

```sh
fugue admin storage volume-retirement plan \
  --released-pv retained-volume --database-cluster archive-db \
  --namespace test-system --ssh control-node --node-ssh storage-node \
  --storage-root /var/lib/storage --output-file released-plan.json

fugue admin storage volume-retirement plan \
  --orphan-directory /var/lib/storage/old-replica --former-pv absent-volume \
  --node storage-node --database-cluster archive-db \
  --namespace test-system --ssh control-node --node-ssh storage-node \
  --storage-root /var/lib/storage --output-file orphan-plan.json
```

Use the same `execute` command and exact preview hash for every source. A
Released PV must retain the original claim UID and its claim name must remain
absent, including when it was reused by a new PVC. An orphan directory requires
the exact former PV name and node; that PV must remain absent and no other PV
may overlap the path. The plan records a distinct `retirement_id`, never a
fabricated PV UID. Query its saved receipt using `status --retirement-id` in
the plan's namespace. Historical modes never delete a PVC. An optional explicit
`--database-cluster` pins the current CNPG cluster's identity, primary and health;
the directory name is not interpreted as ownership of a current instance.

`plan` reads remote metadata only. It pins cluster, node, PVC and PV identities,
exact directory, boot identity, inode and a metadata tree hash. Its JSON reports
blockers and allocated bytes. A new execution needs a plan less than one hour
old. The CLI and node require Python 3.9+ on the selected Linux storage node;
Kubernetes access uses `kubectl`, locally or on `--ssh`.

Terminal Job references block by default too. After auditing their purpose,
`plan --allow-terminal-job-references` can explicitly retain that history while
retiring the data. Only `Complete=True` or `Failed=True` Jobs without active,
terminating or uncounted Pods qualify; `FailureTarget` alone does not. All Pod
references and node mounts/process handles still block. This policy is pinned
in the plan digest and does not delete any Job history.

A broad host administration mount of `/` or a storage parent is not ownership
of every child volume. Direct target/descendant hostPath mounts, actual node
mounts and open handles still block. Overlapping PV sources always block.

Execution rechecks workload references (including scaled-down controllers and
claim templates), overlapping PVs, node mounts, process handles, explicit
`fugue.pro/retirement-protected` holds and, for CNPG, detached status, cluster
health, primary identity and recovery references. It writes an audit ConfigMap
before deleting the exact PVC with UID/resourceVersion preconditions, waits
for normal protection finalizers and `Released`, and rechecks references.
It then quarantines only the pinned directory by atomic rename and removes it
using fd-safe traversal. The PV stays `Retain` until a matching node receipt
verifies both original and quarantine paths absent. PV deletion also uses UID
and resourceVersion preconditions. No finalizer is forced, and no workload is
scaled or restarted.

Failures report partial progress. Preserve the original plan and retry the same
command; do not create a different plan to bypass an existing receipt. Node
receipts and lock files remain in the storage root; the namespace ConfigMap
and Lease remain for audit. `status` reads the saved receipt, not a fresh health
check. `deleted_allocated_bytes` is the pinned tree's block allocation;
`available_bytes_delta` is a separate live filesystem measurement that can also
reflect unrelated writes. Completion means the selected directory and any
selected retained PV were removed and final checks passed. An absent historical
claim/PV stays absent, while a bound-source plan also removes its exact claim.
It does not mean other storage was reclaimed.
