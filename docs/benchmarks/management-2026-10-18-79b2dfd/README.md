# 管理 API 默认配置性能基线（2026-10-18）

本目录保留基于提交 `79b2dfd3e043e946be2881c85a09dd0658b5e289` 的测试协议、原始证据与分析。
最终结论见 [REPORT.md](REPORT.md)，离线汇总见 [analysis.json](analysis.json)。

## 版本与隔离

测试源由 `git archive HEAD` 导出至工作区 `.artifacts/management-baseline-2026-10-18-79b2dfd/source`，
没有使用当前工作区的未提交业务改动。`provenance.json` 记录提交、tree、初始工作区状态与差异哈希；
每组 `environment.json` 记录实际导入路径、Python/SQLite/系统版本和源文件 SHA256。
本地 HEAD 和本地 `origin/master` 相同；连接 GitHub 失败，未能核验远端是否另有更新。

## 测量协议

使用提交自带 `tools/benchmark_management.py` 的 `run_benchmark()`，不传负载或超时覆盖参数。
默认每轮 100 个 `root(abs(-i)) → join(identity(root))` 工作流、300 次交错读取、
8 个并发 HTTP 客户端、4 个 Worker。读取各 100 次 overview、分页 workflows 和 metrics。
每轮使用全新临时数据库、loopback HTTP 服务和 Worker，顺序执行，无并行 Benchmark。

默认完成截止点 60 秒；客户端每次请求最多 10 秒；服务请求与管理读模型 busy timeout 2 秒；
Worker 轮询 0.05 秒、心跳 1 秒；协调器轮询 0.1 秒；关闭等待各 5 秒。
基准按客户端并发数设服务 max_concurrency=16、backlog=32，这是现有工具行为。
默认 SQLite 写连接超时 30 秒，HTTP 线程写等待遵循原来的 2 秒范围；所有值均未放宽。

主基线为 1 轮预热、5 轮正式测量；预热保留但不计入汇总，失败的正式轮仍纳入。
另以相同默认负载运行 3 轮数据库诊断，无预热，复用提交自带的 `DatabaseProbe`。
探针记录本地写队列等待、`BEGIN IMMEDIATE` 与 SQL execute 耗时及 busy/locked 错误。
诊断结果单列，不合入主基线。探针从服务构造完成后安装，至关闭后的结果校验时取样，
包括 Worker 启动、HTTP、完成轮询、关闭及校验开始的数据库操作。

`collect.py` 只包装服务创建以保存对象、内置延迟汇总以复制原始延迟列表、内置结果校验以保存逐任务证据。
主测量开启管理服务原生 INFO 日志并暂存在内存；数据库备份、指标导出和文件写入在计时区间结束后进行。
诊断组另装 `DatabaseProbe`。业务源文件、SQL、调度行为、重试和超时配置没有修改。

HTTP 吞吐沿用现有工具：完成的客户端请求（含错误）/HTTP 批次耗时；该耗时包含 Worker 启动。
工作流吞吐为 SUCCESS 数量/总完成耗时，计入启动、HTTP 与完成轮询，不计初始化和关闭。
延迟为客户端完整调用延迟，包含错误，使用 nearest-rank；没有额外预热请求或客户端重试。
`suite.md` 的分位数统计是逐轮分位数分布；`analysis.json` 另从所有正式轮的原始延迟计算合并分位数。

## 复现

从仓库根目录，在允许 127.0.0.1 客户端连接的环境执行：

```powershell
$snapshot = '.artifacts/management-baseline-reproduction/source'
New-Item -ItemType Directory -Path '.artifacts/management-baseline-reproduction' -Force
git archive --format=zip --output=.artifacts/management-baseline-reproduction/source.zip 79b2dfd3e043e946be2881c85a09dd0658b5e289
Expand-Archive .artifacts/management-baseline-reproduction/source.zip $snapshot
python docs/benchmarks/management-2026-10-18-79b2dfd/collect.py --snapshot $snapshot --output .artifacts/management-baseline-reproduction/baseline --commit 79b2dfd3e043e946be2881c85a09dd0658b5e289 --repeat 5 --warmup 1
python docs/benchmarks/management-2026-10-18-79b2dfd/collect.py --snapshot $snapshot --output .artifacts/management-baseline-reproduction/diagnostic --commit 79b2dfd3e043e946be2881c85a09dd0658b5e289 --repeat 3 --warmup 0 --probe
```

也可直接用原工具 CLI 复现同一负载（不含补充证据采集）：

```powershell
# 在上述快照目录内执行
$env:PYTHONPATH = 'src'
python tools/benchmark_management.py --repeat 5 --warmup 1 --output baseline.json --markdown baseline.md
```

## 证据文件

- `baseline/`、`diagnostic/`：逐轮 `.raw.json`（原工具结果、原始客户端延迟、节点结果、最终指标、事件计数）、
  `.server.jsonl`（原生请求/错误日志）、`.metrics.prom`（关闭后的原生指标）、`.sqlite3`（独立数据库备份）。
- 每组 `suite.json` 和 `suite.md`：原工具汇总与可读报告；`progress.json` 保留各轮完成状态。
- `collect.py`、`analyze.py`：采集和离线复核脚本；`SHA256SUMS.json`：交付文件完整性清单。
- `collection-attempt-01/`：首次采集脚本连接未显式关闭引发的归档异常、数据库备份，排除统计。
- `sandbox-attempt-02/`、`sandbox-loopback-check.json`：沙箱拒绝 loopback 导致的 URLError 和空数据库证据，排除统计；
  该次测量在失败预热后中止。此类环境错误不解释为服务容量或 HTTP 503。

只读取备份数据库进行离线校验；不删除或覆盖失败轮。报告不代表生产 SLO、跨机网络、模型推理或长期稳态容量。
