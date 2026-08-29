# Host Journald Retention

Fugue manages host journald retention through the node-updater task
`reconcile-host-journald-policy`. The production controller intent is:

- `MaxRetentionSec=30day`
- `SystemMaxUse=1G`

The controller schedules at most one mutating task across the active node
fleet. A node must first report node-updater `v36` and the
`reconcile-host-journald-policy` capability. Successful reconciliations are
not repeated for 24 hours; failed reconciliations are retried after a 30-minute
cooldown.

The updater writes only
`/etc/systemd/journald.conf.d/90-fugue-retention.conf`, atomically restarts
`systemd-journald`, verifies the effective values, rotates the journal, and
vacuum-cleans archived entries by both age and size. A failed journald restart
restores the previous managed file. It never removes the drop-in or vacuums
the journal in dry-run mode.

Platform administrators can inspect task ACKs with:

```text
fugue admin node-updater task ls --json
```

Filter the JSON result for `type=reconcile-host-journald-policy` when checking
policy ACKs for one node or the fleet.

Manual mutation requires all of `dry_run=false`, `allow_delete=true`, and
`allow_restart=true`; otherwise the API refuses the claim. The normal fleet
path is controller-owned and should be preferred over manually creating tasks.
