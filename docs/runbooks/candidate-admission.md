# Candidate workload admission

Managed safe rollouts admit one active candidate revision per app. Each running
replica retains its normal CPU and memory requests; the controller does not turn
historical candidates into zero-request running workloads. An app configured
with several replicas may still run that many replicas of its admitted revision.

Before creating a candidate release, preparation checks whether another candidate
or a retired revision still declares replicas or retains nonterminal Pods. Apply
checks again against fresh Kubernetes inventory, including orphan revision
resources without a release row. Deployment and ReplicaSet zero replicas must be
observed before the slot is free; Pending and terminating Pods still occupy it.
An incomplete inventory or failed read blocks admission. The error identifies the
release and remaining workload; retry deployment after retirement converges.

Cold historical Deployment/ReplicaSet records with observed zero replicas and no
nonterminal Pods do not occupy a slot. Stable and previous lifecycle roles are
protected even when their rendered names and labels still contain `candidate`.
An operation resuming its verified, bound candidate does not create another
revision or reset published traffic. Per-app operation claiming serializes apply.

This guard does not authorize retirement or modify resource requests. Existing
traffic withdrawal, retention, connection drain, ownership and deletion gates
remain responsible for historical workloads. Check those gates if admission
remains blocked; do not delete resources by name or lower requests to bypass it.
