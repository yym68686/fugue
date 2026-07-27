# K3s Node Identity Rejoin

Use this runbook when a host-level Fugue node updater is healthy but the
corresponding Kubernetes `Node` is absent.

## Safety contract

Automatic rejoin is allowed only when all of the following are true:

- the authenticated node updater advertises `rejoin-k3s-node`;
- the updater is active;
- the bound NodeKey exists and is not revoked;
- updater, machine, runtime, NodeKey, and cluster node names match exactly;
- the Kubernetes API returns `404` for that exact Node name;
- cluster join is fully configured.

The control plane issues a short-lived Kubernetes bootstrap token using the
normal cluster-join TTL. It never returns the permanent k3s server token. The
node updater validates the credential class, node name, token ID, generation,
and expiry before atomically updating `/etc/rancher/k3s/config.yaml`. Both the
desired-state cache and k3s config are mode `0600`.

Explicit edge drain state and node policy are not changed by cluster rejoin.

## Evidence

Inspect the latest deep-health result:

```text
fugue admin node-updater health show <node-updater-id>
```

The `k3s_cluster_membership` check reports:

- the control-plane observation (`node_present` or
  `kubernetes_node_not_found`);
- credential class, token ID, generation, and expiration, but never the token;
- whether the safe action was ready, suppressed, or unavailable.

Prometheus metrics:

- `fugue_node_cluster_membership_present`
- `fugue_node_cluster_rejoin_credential_ready`

Durable Fugue audit actions:

- `node_updater.cluster_rejoin.credential_issued`
- `node_updater.cluster_rejoin.credential_issue_failed`
- `node_updater.cluster_rejoin.credentials_revoked`
- `node_updater.cluster_rejoin.credentials_revoke_failed`

Every Fugue-mediated Kubernetes Node deletion also records a correlated
operation ID, exact actor, reason, resource, and outcome using:

- `cluster.node.delete.requested`
- `cluster.node.delete.succeeded`
- `cluster.node.delete.already_absent`
- `cluster.node.delete.failed`

For a deletion made outside Fugue, use the Kubernetes API audit log while it is
within retention. The automatic missing-node detection and issuance audit make
the incident visible immediately instead of relying on a later manual report.

## Expected convergence

1. The controller upgrades the updater to the current generation.
2. Desired state reports `credential_ready`.
3. The updater applies the bounded token and restarts `k3s-agent` once.
4. The Node and Node Lease reappear.
5. The next desired-state read reports `not_required/node_present`.
6. The control plane deletes all owned rejoin bootstrap Secrets.
7. Deep health reports cluster membership present.

If any identity check is suppressed or Kubernetes is unavailable, do not copy a
server token to the host. Resolve the reported binding or control-plane issue
and let the bounded flow retry.
