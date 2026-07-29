# Host zram memory safety

Fugue automatically plans host-only compressed swap when an agent node joins.
The default policy allocates 25% of physical RAM, rounded to 256 MiB and bounded
to 1–4 GiB. Nodes with less than 4 GiB of RAM are skipped.

The installer only enables zram when all of these checks pass:

- the host uses systemd and cgroup v2;
- k3s/Kubernetes is version 1.34 or newer;
- the zram kernel module and `mkswap`, `swapon`, and `swapoff` are available;
- no non-Fugue swap, zram manager, or conflicting zram artifact exists.

Fugue adds `fail-swap-on=false` to the k3s kubelet arguments before activating
zram. It does not set `LimitedSwap`, so Kubernetes keeps the default `NoSwap`
behavior for Pods while host processes can use the compressed safety net.

The managed files are:

- `/usr/local/sbin/fugue-host-zram`
- `/etc/fugue/host-zram.env`
- `/etc/systemd/system/fugue-host-zram.service`

Existing agent nodes can be reconciled one at a time after the current
node-updater generation reaches the node:

```bash
fugue admin node-updater task create \
  --cluster-node NODE_NAME \
  --type reconcile-host-zram \
  --allow-restart
```

The task snapshots the k3s configuration and Fugue-managed zram files, restarts
the k3s agent with swap-compatible kubelet settings, then activates and verifies
zram. A failed step restores the snapshots and restarts k3s with its previous
configuration. Inline YAML forms such as `kubelet-arg: [...]` are refused rather
than rewritten.

Once the managed files exist, node-updater deep-health heartbeats report a
`host_zram` check and warn if the device is inactive or its size drifts.

Useful verification commands:

```bash
systemctl status fugue-host-zram.service --no-pager
swapon --show
cat /sys/block/zram0/disksize
grep -A5 '^kubelet-arg:' /etc/rancher/k3s/config.yaml
```

Set `FUGUE_HOST_ZRAM_MODE=off` while running the join installer to disable the
automatic policy for a node. Size-policy environment variables are intended for
controlled testing; production changes should use the default bounded policy.
