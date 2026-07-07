# Node DNS Failure Runbook

## Trigger

Use this runbook when `node.pod_dns_failed`, `pod_dns_to_kube_dns_service`,
`pod_dns_to_coredns_pod`, `kubernetes_default_dns`,
`same_namespace_service_dns`, or `same_namespace_service_tcp` fails.

## Read-Only Diagnosis

1. Capture the current status:

   ```bash
   fugue admin robustness check node <node-name> --json
   fugue admin node-updater health show <node-updater-id> --json
   ```

2. Confirm the failed probe name, expected value, observed value, and evidence.
3. Check whether quarantine is active and whether `quarantine_expires_at` is in
   the future.
4. Check affected runtime apps:

   ```bash
   fugue admin robustness status --json
   ```

## Safety Rules

- Treat kube-dns Service-IP failure as a hard gate for new scheduling and edge
  eligibility.
- Do not delete iptables rules until the stale rule runbook proves they are
  Fugue managed and stale.
- Stateful apps must use the stateful preflight runbook before failover.

## Recovery

1. Prefer observe-only recheck first:

   ```bash
   fugue admin robustness check node <node-name> --json
   ```

2. If the node has stale Fugue-managed DNS DNAT, follow
   `docs/runbooks/stale-iptables-managed-rule.md`.
3. If desired state drift is suspected, enqueue a dry-run or safe refresh task:

   ```bash
   fugue admin node-updater tasks create --node <node-name> --type refresh-desired-state
   fugue admin node-updater tasks create --node <node-name> --type run-deep-health
   ```

4. Keep the node quarantined until a fresh deep-health report clears all
   hard-fail checks.

## Verification

- `fugue admin node-updater health show <node-updater-id>` reports
  `overall_status=pass`.
- `quarantine_state=clear`.
- `fugue admin robustness status` has no node DNS `block_publish` incident.
- Affected services have at least one healthy eligible edge.
