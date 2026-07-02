# Fugue image-cache and LocalPV storage pressure incident note

This note preserves the production evidence that motivated
`image-cache-localpv-storage-recovery-plan.md`. It intentionally omits secrets,
tokens, kubeconfigs, and raw customer environment values.

## Scope

The investigation covered the Fugue control plane, edge nodes, DNS/edge nodes,
and agent nodes over the previous 24 hours at the time of investigation. Older
events were treated as historical background and were not used as active
incident evidence.

## Findings

- `fortedrape8` had root filesystem pressure around 92-93%. Kubernetes events
  repeatedly reported `FreeDiskSpaceFailed`; image GC attempted to free about
  5 GiB but found 0 eligible bytes.
- `ns101351` had image filesystem pressure around 85%. Events included repeated
  `ImageGCFailed` and `FreeDiskSpaceFailed`.
- `vps-d6d20fa1` had image filesystem pressure around 86% during the initial
  event review, later observed closer to 77% root usage during path-level
  sizing.
- Primary space consumers were under `/var/lib/fugue`,
  `/var/lib/kubelet`, and `/var/lib/rancher/containerd`.
- The largest Fugue-owned contributors were node-local image-cache registry
  data and LVM LocalPV loopback backing files.
- The condition was not an active outage at investigation time, but it was a
  credible capacity bottleneck for future build, deploy, pull, and image
  replication activity.

## Safety conclusions

- Image-cache orphan cleanup is suitable for formal automation only after the
  control plane builds a protected set from live workloads, available images,
  aliases, active pins, active tasks, replica policy, local pins, and manifest
  age.
- LVM LocalPV recovery must remain an explicit maintenance action. Empty VG
  nodes may have reclaimable backing files, but deletion must require dry-run
  evidence, zero active LVs, zero bound PVs, exact loop/backing-file match,
  expected preflight values, and explicit `allow_delete=true`.
- Nodes with active LVs or bound PVs, including nodes like `ns101351` when
  active, are not eligible for LocalPV decommission.

## Implementation guardrails

- Default image-cache orphan mode is `observe`.
- Dry-run tasks must set `allow_delete=false`.
- Delete mode must require explicit controller configuration and task payload
  gates.
- LocalPV decommission is never automatic GC.
- Existing nodes are not mutated by node preparation policy changes.
