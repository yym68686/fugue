# K3s proxy channel leak: authorized maintenance

## Identify the defect, not just a timeout

K3s `v1.35.4+k3s1` (`5dc8fe68`) has an unbuffered error channel in
`pkg/daemons/control/proxy/proxy.go`. Two pipe goroutines can both pass the
unsynchronized `done` check. Only one error is received; the other sender can
remain blocked in `proxy.(*proxy).err -> runtime.chansend`, retaining its
connection and copy buffer. A successful request or healthy API readiness
check does not rule out this leak.

Correlate a bounded, passive goroutine profile with the **running** K3s
version, PID, start time and build ID. Repeated samples in the same process
can establish accumulation at that exact stack. Total goroutine count alone
is insufficient. Do not run load tests, CPU profiles or repeated probes on a
production control plane to reproduce the race.

Upstream [PR 14811](https://github.com/k3s-io/k3s/pull/14811) replaces the
channel with a buffer of two and removes the racy flag. The 1.35 backport is
`60854880485947a2b5de8e0b5417c705b5e9bfa6`. It is included in
[`v1.35.10+k3s1`](https://github.com/k3s-io/k3s/releases/tag/v1.35.10%2Bk3s1).
Verify the [exact target source](https://github.com/k3s-io/k3s/blob/v1.35.10%2Bk3s1/pkg/daemons/control/proxy/proxy.go)
and release asset checksum; do not infer inclusion from a version number.

This establishes a K3s defect. It does **not** establish that every Agent
capture rejection, DNS candidate failure or missing log interval has that
cause. Preserve the distinct failure receipts and safety checks.

## Maintenance boundary

This is host K3s maintenance, not a Fugue image rollout. Normal Fugue
production changes still use `main` and `.github/workflows/ci.yml`. Obtain
explicit authorization for K3s binary replacement, service restart and the
possible control-plane outage before writing to a host. In a single-server
embedded-etcd cluster, there is no second API server to hide the interruption.

Before execution:

1. Inventory server/agent versions, control-plane and etcd topology, disabled
   packaged components, storage drivers, service flags, configuration hashes,
   disk space, node health and running Pod/container identities. Check
   [Kubernetes version skew](https://kubernetes.io/releases/version-skew-policy/)
   and every bundled-component change in the selected K3s release. Do not
   restart all agents merely because a server proxy fix is needed.
2. Save the current official binary, service unit, environment/configuration,
   manifests and server token in a root-only backup directory. Never print
   credentials or commit this archive. Retain the server token with the
   snapshot because it is needed to decrypt bootstrap data during recovery.
3. Create a fresh [embedded-etcd snapshot](https://docs.k3s.io/datastore/backup-restore),
   verify the snapshot format's integrity checksum, and retain its path,
   size and SHA-256. Verify a recovery procedure compatible with the exact
   versions and datastore. A checksum alone is not a restore rehearsal.
4. Download the pinned official binary to the same filesystem as the entry
   binary; verify the release checksum, size, version, owner and mode. Recheck
   the live process and configuration against the inventory immediately
   before replacement. Stop if they have changed unexpectedly.

K3s normally executes an **unpacked inner binary** under its data directory.
Its hash differs from the downloaded entry binary. Verify the entry binary
against the release asset and the running inner binary against its own
unpacked `.sha256sums`, exact version and path. Do not compare their hashes
as if they were identical files.

Use the official [manual binary upgrade](https://docs.k3s.io/upgrades/manual)
procedure: atomically replace the verified entry binary and queue one K3s
service restart. Preserve existing service arguments and configuration.
Do not use an installer that might regenerate them. Do not drain/delete
workloads, run kill-all scripts, alter DNS delegation, expand resources or
restore the datastore as part of this procedure. Ensure the restart job is
owned by the service manager and survives an SSH disconnect.

If recovery fails, inspect the exact startup failure before choosing the
[rollback procedure](https://docs.k3s.io/upgrades/roll-back). Do not blindly
downgrade an upgraded datastore or automatically restore an older snapshot:
restoration rolls Kubernetes state back and can discard newer operations.
Additional data-loss recovery decisions require explicit authorization.

## Verify after the restart

- Record binary replacement time, service start time, new PID, running inner
  version/build and first failed/successful readiness observations. A pair of
  samples bounds the observed interruption; it is not continuous uptime data.
- Require all API `/readyz?verbose` gates, including etcd, to pass. Compare
  node readiness with the saved baseline, not an assumption that every node
  was healthy. Verify packaged component rollouts and storage controllers.
- Compare saved Pod UIDs, container IDs and restart counts. Distinguish
  packaged-component replacement, leader-election restarts, preexisting
  workload crashes and newly unhealthy containers. Do not claim zero impact
  solely because the K3s service is active.
- Re-sample passive goroutine profiles in the **same new process**. Verify
  the leaking `proxy.err -> chansend` stack stays absent under ordinary
  traffic. Reset cumulative counters or a lower total are not proof.
- Keep Agent capture receipts, DNS management and background validation
  results, and telemetry global gap/error counters separate. Where their
  processes survive, compare those original counters directly. Mark the
  authorized maintenance window separately from steady-state failures.
- Never treat a truncated per-source log snapshot as global zero gaps, or
  describe already-rotated-away source bytes as recovered. Retain offline
  node evidence and ownership; unreachability is not proof of termination.

Do not weaken authorization, exact publication binding, capacity thresholds,
evidence freshness or recovery fencing to make the verification pass.
