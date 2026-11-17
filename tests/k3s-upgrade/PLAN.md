# K3s upgrade qualification and release gates

This document is an operational gate, not permission to bypass CI or change
tenant replicas, placement, storage, resource budgets, or application traffic.
The qualification runner must never receive production credentials or backups.

## Required evidence, in order

1. **Bootstrap benefit.** On disposable native x86_64 Linux, run exact K3s
   `v1.35.10+k3s1 -> v1.36.6+k3s1`. This qualification stops at the explicitly
   selected 1.36 target; it must not silently promote 1.37. Demonstrate the baseline
   refusal, resize an already completed ordinary init container, preserve the
   main container and Pod identities, and demonstrate actual freed scheduler
   capacity. A successful `/resize` response alone is insufficient.
2. **Business and storage continuity.** Separately exercise the exact deployed
   Fugue/CNPG manager and operator artifacts, Longhorn V1 engine and CSI
   components, application routing and PostgreSQL storage. Keep established
   database connections and committed writes running across each restart.
   Test single-instance and replicated database configurations; verify PID,
   postmaster start time, committed data, PVC/volume ownership and mount identity.
   Do not substitute emptyDir or local-path tests for Longhorn evidence.
3. **Recovery.** Verify the snapshot mechanism on the isolated cluster. Before
   every production control-plane minor upgrade, create a fresh durable etcd
   snapshot and verify its integrity and availability outside that host. Keep
   the matching original server token protected. Never upload these to the
   qualification workflow. Verify the previous binary/configuration and usable
   workload backup paths. A snapshot listing is not a restore rehearsal.
4. **Dependency compatibility.** Record exact CNPG, Longhorn and CSI versions
   and their documented Kubernetes support, plus the above integration results.
   Production CNPG 1.29 must not silently be labeled officially supported on
   Kubernetes 1.36. Longhorn remains at 1.12.1 for this target. Any dependency update needs its own tested
   declarative release and continuity evidence before Kubernetes promotion.
5. **Fugue readiness.** Release the fail-closed updater through main/CI; verify
   each selected node actually reports the new updater version before submitting
   upgrade tasks. Existing nodes do not gain a new script merely because an API
   image was built. No manually rewritten systemd units, join configuration,
   registry settings, service arguments or environment files are allowed.

## Production sequence, only after every gate above passes

1. Capture the live node, workload, traffic/error, connection and volume baseline.
   Classify existing NotReady nodes and intentionally hibernated databases;
   neither may be silently treated as an upgrade failure or successful target.
2. Bring older workers to the current server's minor, one node at a time.
   In this cluster a worker on 1.34 needs a 1.35 step before a 1.36 step.
3. Upgrade the sole server from 1.35 to 1.36; its brief API restart is allowed,
   tenant workload interruption is not. Preserve every running container.
4. Upgrade a qualified low-risk worker to 1.36. Observe at least 15 minutes,
   then handle the remaining healthy workers sequentially, each with the same
   before/after checks and observation window. Never drain single-instance
   databases to make an upgrade appear safe.
5. Stop at 1.36.6 and observe the completed cohort for at least 30 minutes.
   A future minor needs a new qualification and explicit release intent.
   Do not enqueue a fleet of upgrade tasks.
6. Only then consider an independently tested Fugue completed-init resize
   operation. Upgrading Kubernetes alone does not change existing Pod requests
   or teach Fugue's current resize controller to resize init containers.

## Stop conditions and failure behavior

- Any failed prerequisite, missing evidence, unsupported/untested dependency,
  stale node identity, wrong version, inactive service, unsafe KillMode,
  unverifiable artifact, or skipped minor: **do not replace the binary**.
- Any unexpected tenant HTTP failure, closed PostgreSQL connection, changed
  workload container identity/restart count, volume detach/degradation, missing
  committed write, extra primary, or storage ownership change: **stop promotion**.
- Failure to regain API/etcd/node/CSI/operator health within the measured and
  approved maintenance envelope: **do not advance to the next node**.
- Preserve the failed operation's phase, exact version, UID, previous binary
  and diagnostic evidence. Do not report success from systemd `active` alone.
- Do not automatically downgrade across a minor boundary or blindly restore
  etcd. Recovery must use the matching snapshot/token and be reviewed against
  current workload/storage state; restoring stale metadata can itself disrupt
  healthy business or violate single-writer safety.

## Primary references

- https://docs.k3s.io/upgrades/manual
- https://docs.k3s.io/upgrades/roll-back
- https://kubernetes.io/releases/version-skew-policy/
- https://kubernetes.io/docs/tasks/configure-pod-container/resize-container-resources/
- https://cloudnative-pg.io/docs/1.29/supported_releases/
- https://cloudnative-pg.io/docs/1.30/supported_releases/
- https://longhorn.io/docs/1.12.1/best-practices/
- https://longhorn.io/docs/1.12.2/best-practices/
