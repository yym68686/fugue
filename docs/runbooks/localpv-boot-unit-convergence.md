# LocalPV Boot Unit Convergence

The node updater maintains the boot implementation of an already installed
Fugue LocalPV loop service. It preserves the image path and volume group from
the node's recognized unit. It does not provision storage, initialize metadata,
activate volumes, or restart services. A missing unit is not an instruction to
enable LocalPV on a node.

## Source And Delivery

`internal/api/localpv_unit.py` is the shared renderer used by both
`scripts/prepare_fugue_lvm_localpv_node.sh` and the generated node updater.
The preparation script needs the repository checkout (or an explicit
`FUGUE_LOCALPV_UNIT_RENDERER`) and Python 3; it validates the rendered unit
before changing storage.

The API generator and controller must both use
`model.NodeUpdaterCurrentVersion`. Changing the embedded boot implementation
requires a monotonic version increment; `make prepush` enforces this. Publish
through the normal `main` CI workflow. The controller then schedules the
existing node-updater self-update operation. Do not distribute updater binaries
or scripts to live nodes by hand for ordinary releases.

After an update, each normal updater cycle reconciles the local boot unit
before fetching remote desired state. A control-plane outage does not erase
the installed unit or prevent this local check. Storage selection remains local
intent; an executable update does not select a new image or volume group.

## Safety Boundaries

- Only known Fugue unit templates are migrated. Arbitrary customizations,
  symlinks, writable units, masked units, and different loaded fragments are
  refused.
- Any systemd drop-in is preserved and reported as `override_conflict`. Review
  its purpose and effective command separately. Do not delete overrides just
  to make a status green.
- Candidate syntax is verified first. Concurrent file/configuration changes
  abort application. The previous file is retained as
  `<unit>.fugue-previous`, and replacement is atomic.
- Application uses only `systemctl daemon-reload`. It never invokes storage
  start, stop, restart, enable, losetup, or LVM commands.
- The updater verifies loaded ExecStart and the installed file after reload.
  Failed verification restores the previous file and reloads it, unless an
  intervening operator edit would be overwritten. Such an edit is preserved
  and the failure remains visible.
- `FUGUE_LOCALPV_UNIT_MODE=observe` in the node updater's environment suppresses
  installation while retaining drift observations. It does not require a code
  release. Unit/lock paths can be supplied through `FUGUE_LOCALPV_UNIT_PATH`
  and `FUGUE_LOCALPV_UNIT_LOCK_PATH` for an explicitly configured installation.

The generated boot command checks query output, propagates query/attach
failures, and uses `losetup --nooverlap`. Shell dollars are escaped for systemd.
This fixes the old assumption that `losetup -j` returns a failure status when
no loop device is associated.

## Evidence And Acceptance

Read `fugue admin node-updater health ls --json`, selecting the
`localpv_boot_unit` check. It is an observation, not an instruction to quarantine
a serving node. Each heartbeat obtains fresh local evidence rather than
reusing a historical success receipt.

Evidence includes the renderer revision, desired/file SHA-256, effective
ExecStart SHA-256, image/VG intent, drop-in paths, enablement, service runtime
state, and the latest in-process apply result. `converged` requires recognized
content installed and loaded, an enabled unit, and an active successful service.
`runtime_unready` means content converged but runtime/enablement still needs
review. No automatic storage start follows that observation.

For production acceptance, verify the served installer generation and node
heartbeat generation, then inspect unit evidence on the storage nodes. Compare
application/database Pod UIDs, container start times and restart counts across
the rollout, and check small read-only HTTP endpoints. Reboots and storage
detachments are not acceptance probes on a serving production node.

Offline tests exercise empty-success loop queries, query errors, idempotence,
old-to-new migration, custom overrides, disabled/failed runtime state,
concurrent edits, reload failures and rollback. A real cold boot should be
tested only on a disposable or separately drained node.
