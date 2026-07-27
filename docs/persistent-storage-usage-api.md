# 前端获取项目真实磁盘占用

Fugue 在 `current_resource_usage` 中分别返回临时盘和持久盘的当前占用。前端不需要读取 Kubernetes，也不要用 PVC 的声明容量代替实际占用。

## 推荐接口

读取一个项目时优先使用：

```http
GET /v1/console/projects/{project_id}?include_live_status=false
Authorization: Bearer <api-key>
```

响应中的 `apps[]` 已包含应用及其 `backing_services[]`，并为两者填充 `current_resource_usage`。也可以使用下面的兼容接口，但计算项目汇总时不要分页：

```http
GET /v1/apps?project_id={project_id}&include_resource_usage=true&include_live_status=false
Authorization: Bearer <api-key>
```

## 字段含义

- `ephemeral_storage_bytes`：工作负载所有 Pod 的临时文件系统当前占用。容器可写层、日志等通常在这里；Pod 重建后数据可能消失。
- `persistent_storage_used_bytes`：工作负载挂载的不同 PVC 当前真实占用。数据库文件包含在数据库 backing service 的这个字段里。
- `persistent_storage_capacity_bytes`：只在存储后端确实提供并执行独享容量上限时返回。`rancher.io/local-path` 共享节点文件系统且不执行 PVC 声明容量，所以该字段会省略。

字段缺失或 `null` 表示“当前无法得到准确值”，不等于 `0`。字节转 GiB 时应使用 `bytes / 1024 ** 3`。

## 项目汇总规则

项目运行时磁盘占用为：

```text
项目临时盘 = 所有应用和不同 backing service 的 ephemeral_storage_bytes 之和
项目持久盘 = 所有应用和不同 backing service 的 persistent_storage_used_bytes 之和
项目运行时总占用 = 项目临时盘 + 项目持久盘
```

同一个 backing service 可能出现在多个应用的 `backing_services[]` 中，必须按 service `id` 去重。Fugue 后端已经对同一响应中的共享 PVC 做了全局归属和去重，前端不要再次按 Pod 或副本数相乘。

下面的 TypeScript 示例保留了“未知”和“零”的区别：

```ts
type Usage = {
  ephemeral_storage_bytes?: number | null;
  persistent_storage_used_bytes?: number | null;
  persistent_storage_capacity_bytes?: number | null;
};

type Service = {
  id: string;
  type?: string;
  spec?: { postgres?: unknown | null };
  current_resource_usage?: Usage | null;
};
type App = {
  spec?: {
    workspace?: unknown | null;
    persistent_storage?: unknown | null;
  };
  current_resource_usage?: Usage | null;
  backing_services?: Service[];
};

export function projectDiskUsage(apps: App[]) {
  let ephemeralBytes = 0;
  let persistentUsedBytes = 0;
  let persistentCapacityBytes = 0;
  let hasEphemeral = false;
  let hasPersistent = false;
  let persistentUsedComplete = true;
  let capacityComplete = true;
  const seenServices = new Set<string>();

  const add = (usage: Usage | null | undefined, expectsPersistent: boolean) => {
    if (usage?.ephemeral_storage_bytes != null) {
      ephemeralBytes += usage.ephemeral_storage_bytes;
      hasEphemeral = true;
    }
    const hasPersistentSignal =
      expectsPersistent ||
      usage?.persistent_storage_used_bytes != null ||
      usage?.persistent_storage_capacity_bytes != null;
    if (!hasPersistentSignal) return;

    hasPersistent = true;
    if (usage?.persistent_storage_used_bytes != null) {
      persistentUsedBytes += usage.persistent_storage_used_bytes;
    } else {
      persistentUsedComplete = false;
    }
    if (usage?.persistent_storage_capacity_bytes != null) {
      persistentCapacityBytes += usage.persistent_storage_capacity_bytes;
    } else {
      capacityComplete = false;
    }
  };

  for (const app of apps) {
    add(
      app.current_resource_usage,
      app.spec?.workspace != null || app.spec?.persistent_storage != null,
    );
    for (const service of app.backing_services ?? []) {
      if (!service.id || seenServices.has(service.id)) continue;
      seenServices.add(service.id);
      add(
        service.current_resource_usage,
        service.type?.toLowerCase() === "postgres" || service.spec?.postgres != null,
      );
    }
  }

  return {
    ephemeralBytes: hasEphemeral ? ephemeralBytes : null,
    persistentUsedBytes:
      hasPersistent && persistentUsedComplete ? persistentUsedBytes : null,
    totalRuntimeBytes:
      hasEphemeral && (!hasPersistent || persistentUsedComplete)
        ? ephemeralBytes + persistentUsedBytes
        : null,
    persistentCapacityBytes:
      hasPersistent && capacityComplete ? persistentCapacityBytes : null,
  };
}
```

只有 `persistentCapacityBytes` 非 `null` 时才适合显示“已用 / 容量”或百分比。否则只显示“已用”，不要把服务器剩余空间或 PVC 声明值呈现成用户独享上限。

## 刷新与异常处理

- kubelet 工作负载统计通常随集群资源快照刷新，约几十秒更新一次。
- directory-backed local-path 卷的目录扫描缓存 5 分钟，以避免每个页面请求都遍历数据库目录。
- 刷新暂时失败时可能短暂返回最近一次准确值；超过容许的陈旧窗口后，Fugue 会省略该值，而不会退回错误的整块服务器磁盘占用。
- 镜像占用不属于上述运行时磁盘。如果产品要展示“项目平台总占用”，应把镜像作为独立分类读取并展示，避免与容器临时盘或 PVC 重复计算。
