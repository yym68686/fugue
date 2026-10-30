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

`CPU-USED(m)` is the current sum of container CPU usage from Metrics Server, falling back to the node's kubelet statistics when Metrics Server is unavailable; `MEM-USED(MiB)` is the memory working set. Metrics older than five minutes, missing containers or missing pods make the affected service's usage unknown (`-` / JSON `null`). `METRICS` shows measured pods / total pods. Unknown usage is never reported as zero.

`CPU-REC(m)` is the reservation total if every managed main container on this node uses the existing right-sizing **request target**. `CPU-CAP(m)` separately shows the total using the historical **capacity estimate**, which is not the request target. `MEM-REC(MiB)` uses the memory request target. Both retain sidecars, init peaks and overhead and preserve the current replica count. These totals do not propose removing candidate replicas. System workloads without a supported policy show unknown recommendations. Missing historical dimensions remain unknown even when the other dimension has samples.

Recommendations use the same versioned per-replica samples and workload policies as `app resources recommend`, including standalone managed PostgreSQL services. The history window defaults to 168 hours (range 1–168) and the sample minimum defaults to 12. JSON includes the underlying recommendation policy, sample count and last sample time. These are suggestions, not an automatic resource change.

## 中文

管理员使用 `fugue admin cluster resources --node <节点> --limit 30 --memory`，即可按 CPU request 从高到低输出前 30 个服务的 request、实际 CPU、推荐 request、CPU 容量建议和内存占用。`--limit 0` 输出全部服务，`--sort` 选择排序字段，`--order asc|desc` 控制方向，`--json` 方便脚本处理。

服务用量只统计指定节点，合并该服务的全部非终态副本，包含候选副本与辅助容器。推荐值保留当前副本数和辅助容器预留，CPU 推荐 request 与容量建议分别展示；实际用量缺失或历史样本不足时显示未知。本命令只查询，不修改线上资源。
