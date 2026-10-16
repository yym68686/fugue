# Node Resource Policy

`policy.json` is node capacity intent, independent of application builds and
serving bundles. `targets.json` explicitly selects seven online updater nodes
and the control-plane host. Offline nodes are intentionally excluded.

The independent `node_resource_policy` CI lane validates and previews the
policy, then publishes it through administrator-authenticated node tasks.
It waits for the served updater to contain the required capability before
requesting a bounded updater upgrade. The control-plane runner installs a
separate local timer because that host has no node-updater enrollment.

`minimum_updater_version` declares the minimum diagnostic executor version.
With `--upgrade-updaters`, publication waits for fresh heartbeats to confirm
that version and never downgrades a newer updater. Already capable nodes receive
resource policy even if the required script has not been released yet; failed
code publication cannot prevent policy recovery. Without this flag, policy
recovery requires only the existing capability. Only explicit online targets
are eligible for upgrades.

Nodes persist `/etc/fugue/node-resource-policy.json` and reconcile it without
requiring the control plane. Instance capacity uses container density plus a
base reserve; observed per-UID instance/watch descriptor counts provide another
lower bound. Descriptor counts may include shared descriptors and are reported
as upper bounds. Limits only increase automatically, up to policy caps; existing
higher limits are preserved. Disabling the policy stops reconciliation while
preserving allocated capacity. No service or workload restart is required.

The root probe actually creates an inotify instance and filesystem watch and
closes both. It records EMFILE/ENOSPC failures explicitly. Usage warnings use the
policy threshold. Reaching a cap produces a capacity warning instead of claiming
sufficient capacity. DNS probes require valid nonempty UDP and TCP answers from
the selected Pod network namespace; missing prerequisites are unobserved warnings.

Capacity rollback changes policy intent, never automatically lowers a live
kernel limit. Any deliberate limit reduction requires a separately reviewed
capacity operation based on current usage. Configuration publication must not
change serving artifacts, certificates, or LKG.
