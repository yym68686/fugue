# Stale Managed Iptables Rule Runbook

## Trigger

Use this runbook when node deep health reports
`managed_iptables_stale_rule` or evidence contains `suspect_rules`.

## Read-Only Diagnosis

1. Inspect the node health evidence:

   ```bash
   fugue admin node-updater health show <node-updater-id> --json
   ```

2. Confirm the suspect rule targets an old kube-dns or CNI path and is not the
   current desired target.
3. Confirm the rule has Fugue provenance or matches the legacy Fugue DNS
   escape-hatch shape.

## Safety Boundary

- Dry-run is mandatory before deletion.
- Non-dry-run deletion requires `allow_delete=true`.
- The repair deletes only provenance-scoped DNS DNAT rules; it must not flush a
  table or delete unrelated Kubernetes chains.
- A failed repair keeps quarantine active.

## Recovery

1. Run a dry-run repair task:

   ```bash
   fugue admin node-updater tasks create \
     --node <node-name> \
     --type repair-managed-iptables \
     --payload dry_run=true
   ```

2. Review task logs and confirm only stale Fugue-managed DNS DNAT rules would be
   deleted.
3. Execute only if the dry-run is correct:

   ```bash
   fugue admin node-updater tasks create \
     --node <node-name> \
     --type repair-managed-iptables \
     --payload dry_run=false \
     --payload allow_delete=true
   ```

4. Enqueue a deep-health recheck:

   ```bash
   fugue admin node-updater tasks create --node <node-name> --type run-deep-health
   ```

## Verification

- Repair task audit action is `node_repair_completed` or
  `node_repair_dry_run_completed`.
- The next deep-health report has no `managed_iptables_stale_rule`.
- `quarantine_state` returns to `clear` only after the fresh report passes.
