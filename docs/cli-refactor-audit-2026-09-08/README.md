# CLI 调查证据

这些文件支持 [CLI 重构与删除计划](/Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-plan-2026-09-08.md)。它们记录 2026-09-08 本地工作区的调查结果，不是生产环境状态。

- `command-inventory.tsv`：递归帮助树，包含分组；不含隐藏兼容入口及别名重复路径。
- `api-inventory.tsv`：OpenAPI HTTP operation 索引，不是自动判定的 CLI 覆盖率矩阵。
- `behavior.json`：6 个等待/错误分类探针。
- `supplemental-behavior.json`：3 个弃用提示/脱敏/部分取证探针。secret-marker 是合成字符串。
- `baseline.json`：仓库 HEAD、关键文件摘要及本次被执行 CLI 的二进制摘要。

如需复现，先从要调查的源码构建独立 CLI：

```bash
cd /Users/yanyuming/Downloads/GitHub/fugue
go build -o /tmp/fugue-cli-audit-20260908 ./cmd/fugue
python3 /Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-audit-2026-09-08/collect-inventory.py
python3 /Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-audit-2026-09-08/probe-behavior.py
python3 /Users/yanyuming/Downloads/GitHub/fugue/docs/cli-refactor-audit-2026-09-08/probe-supplemental-behavior.py
```

新结果写入 `/tmp/fugue-cli-audit-20260908-data`，不会自动覆盖本目录的证据快照。探针使用随机 loopback 端口和合成 token；只向本地模拟 API 发 GET。子进程清除生产相关环境变量。帮助枚举只执行 `--help`，不执行生成的示例命令。

脚本是本次调查工具，不是正式产品测试套件。后续修复应将有意义的行为场景转为项目现有 Go 测试风格，并按改动范围运行相应验证。
