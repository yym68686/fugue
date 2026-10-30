# Node service resource report

Platform administrators can inspect one node in a single read-only command:

```sh
fugue admin cluster resources --node worker-a --limit 30 --memory
fugue admin cluster resources --node worker-a --limit 0 --json
fugue admin cluster resources --node worker-a --sort memory-usage --order desc --limit 50
fugue admin cluster resources --node worker-a --sort cpu-recommended --order asc --window-hours 24 --min-samples 12
```

`--limit` defaults to 30; zero returns all services. `--sort` accepts `cpu-request` (default), `cpu-usage`, `cpu-recommended`, `cpu-capacity`, `memory-request`, `memory-usage`, `memory-recommended`, `name`, `namespace`, and `pods`. `--order` accepts `asc` or `desc` (default). Unknown values always sort last; equal values use namespace, name, kind and ID for deterministic ordering. Memory sorting also enables memory columns. JSON always includes memory fields.

Each row aggregates a managed app or database in one namespace on this exact node, including candidate replicas. Other Kubernetes workloads are grouped by controller; standalone pods retain their names. Completed and failed pods are excluded. CPU request and memory request include regular containers, restartable init sidecars, ordered init peaks, pod-level requests and overhead. Header totals cover all services before the display limit.

`CPU-USED(m)` sums available fresh container CPU usage from Metrics Server, falling back to the node's kubelet statistics when Metrics Server is unavailable; `MEM-USED(MiB)` sums memory working sets. Observations older than five minutes are excluded. Missing pods, missing main containers, or a missing CPU/memory dimension do not erase other observations, including available sidecars. CPU and memory are aggregated independently. Only a dimension with no observations is unknown (`-` / JSON `null`); an observed zero remains zero.

`USAGE` is `complete`, `partial`, or `unavailable`. A `*` after a usage value means that dimension is a partial observed sum, not the full service usage or an estimate for missing containers. `METRICS` shows fully measured pods / total pods: a fully measured pod has both dimensions for every expected regular container and restartable init sidecar. JSON also includes `container_count`, `cpu_measured_containers`, and `memory_measured_containers`. For example, when one pod has main + drain metrics and another has only drain metrics, all three observed containers contribute, while pod coverage is `1/2` and container coverage is `3/4`. Sorting uses the displayed observed sums, with unknown values last; a partial value can underestimate the full service total.

`CPU-REC(m)` is the reservation total if every managed main container on this node uses the existing right-sizing **request target**. `CPU-CAP(m)` separately shows the total using the historical **capacity estimate**, which is not the request target. `MEM-REC(MiB)` uses the memory request target. Both retain sidecars, init peaks and overhead and preserve the current replica count. These totals do not propose removing candidate replicas. System workloads without a supported policy show unknown recommendations. Missing historical dimensions remain unknown even when the other dimension has samples.

Recommendations use the same versioned per-replica samples and workload policies as `app resources recommend`, including standalone managed PostgreSQL services. The history window defaults to 168 hours (range 1–168) and the sample minimum defaults to 12. JSON includes the underlying recommendation policy, sample count and last sample time. `RECOMMENDATION=ready` describes historical recommendation readiness, not pod health or current metric completeness. These are suggestions, not an automatic resource change.

## 中文

管理员使用 `fugue admin cluster resources --node <节点> --limit 30 --memory`，即可按 CPU request 从高到低输出前 30 个服务的 request、实际 CPU、推荐 request、CPU 容量建议和内存占用。`--limit 0` 输出全部服务，`--sort` 选择排序字段，`--order asc|desc` 控制方向，`--json` 方便脚本处理。

服务用量只统计指定节点，合并该服务的全部非终态副本，包含候选副本与辅助容器。CPU 与内存分别累加已有指标，部分 Pod 或主容器缺失时仍保留已观测用量，使用 `*` 和 `USAGE=partial` 标注部分数据。`METRICS` 表示指标完整的 Pod 数/总 Pod 数；某一维度完全没有观测值时才显示 `-`，不会将缺失数据当作 0。推荐值保留当前副本数和辅助容器预留，CPU 推荐 request 与容量建议分别展示；`RECOMMENDATION=ready` 只表示历史推荐可用。本命令只查询，不修改线上资源。
