# Benchmark 使用与验证

## 检查结论

仓库已有 `tools/benchmark_management.py` 和历史结果
`management-benchmark-2026-10-13.json`，历史说明位于
[管理系统验证记录](management-validation-2026-10-13.md)。原工具支持一次完整运行，
没有多轮统计和针对 Benchmark 自身的独立测试。

本次扩展保留默认单轮命令及原 JSON 指标字段，增加预热、多轮测量、Markdown
结果报告、逐节点结果/执行次数校验及失败原因。`schema_version` 为 2；开启预热或
多轮测量时，逐轮结果存于 `warmup_runs` 和 `runs`，汇总存于 `summary`。

## 重复运行

从仓库根目录执行，无新增运行依赖：

```powershell
$env:PYTHONPATH = 'src'
python tools/benchmark_management.py --workflows 100 --reads 300 --concurrency 8 --workers 4 `
  --deadline 60 --repeat 3 --warmup 1 `
  --output docs/benchmark-results.json --markdown docs/benchmark-results.md
```

每轮使用全新临时数据库、服务及 Worker，按相同负载运行，结束后关闭线程、连接并
删除临时库。临时库位于当前运行目录的 `.management-benchmark-*` 子目录，要求该
目录可写；这避免受限环境的系统临时目录不能被 SQLite 打开。服务只监听 loopback，
不使用已有数据库或已部署服务。输出目录会自动创建。

| 参数 | 默认值 | 语义 |
| --- | ---: | --- |
| `--workflows` | 100 | 两节点工作流数量，正整数 |
| `--reads` | 300 | 混合读取次数，可为 0 |
| `--concurrency` | 8 | 并发 HTTP 客户端，1–128 |
| `--workers` | 4 | Worker 数量，1–64 |
| `--deadline` | 60 | 从 Worker 启动前开始计时的完成轮询截止点，有限正秒数 |
| `--repeat` | 1 | 测量轮数，正整数 |
| `--warmup` | 0 | 预热轮数，非负整数 |
| `--output` | 无 | JSON 文件；同时始终输出 JSON 到 stdout |
| `--markdown` | 无 | Markdown 可读报告；须与 JSON 文件不同 |

每个工作流执行 `root(abs(-index)) → join(identity(root 的结果))`。写入请求与
overview、分页列表和 metrics 请求交错执行。Worker 默认轮询 0.05 秒、心跳 1 秒，
服务协调器轮询 0.1 秒，这些参数保留在报告中。

## 结果语义

- `http` 包含请求数、错误数/类型、批次耗时、吞吐及 nearest-rank P50/P95/P99/max。
  延迟单位为毫秒，覆盖成功及失败请求；吞吐为完成请求数除以 HTTP 批次耗时。
- `execution` 包含完成工作流数、任务尝试数、重试数、总完成耗时和工作流吞吐。
  测量包含 Worker 启动、HTTP 批次及完成轮询；数据库/服务初始化和关闭不计时。
- `verification` 检查完整节点集合与数量，逐节点验证 SUCCESS、整数结果、一次尝试
  和零重试。空结果集合、缺失节点、重复节点、错误根节点结果都不能判为通过。
- `checks` 与 `failure_reasons` 提供 HTTP 状态、截止点、工作流/任务数量、执行次数、
  结果、数据库完整性和关闭状态的证据；Worker 运行/关闭异常单独记入
  `lifecycle_errors`，不会增加 HTTP 错误数。
- 预热轮保留原始证据，排除在汇总之外。任意预热或测量轮失败会使整体失败；失败
  的测量轮仍包含在统计中，避免只展示成功样本。
- 汇总吞吐按「请求/完成工作流总数 ÷ 相应耗时总和」计算。`round_statistics` 提供
  各轮指标的 min、median、max、mean、样本标准差；单轮标准差记为 0。
  各轮 P95/P99 的统计不是合并全部请求后的 P95/P99。
- 正常完成后，通过返回 0，正确性检查失败返回 1，非法参数返回 2。

`--deadline` 不会强制中断整个 HTTP 批次；单次请求客户端超时上限为 10 秒，批次
结束后才按截止点轮询完成。关闭阶段另有每个 Worker/服务 5 秒等待。

该负载用于本机控制面和持久化 DAG 容量抽样，没有调用真实模型，也不代表跨主机
网络、故障恢复或长时间 soak 的性能。复现时应保持参数、Python/SQLite 版本和
机器负载一致，单独运行 Benchmark，避免与测试并行竞争资源。

## 独立测试

三组文件可以分别运行；每组不依赖其他测试生成的状态或报告。

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_benchmark_validation.py -v
python -m unittest discover -s tests -p test_benchmark_reports.py -v
python -m unittest discover -s tests -p test_benchmark_integration.py -v
```

| 文件 | 测试数量 | 覆盖 |
| --- | ---: | --- |
| `test_benchmark_validation.py` | 7 | 非法配置无副作用、边界、nearest-rank、空/缺失/重复结果、根与汇合结果、逐任务次数 |
| `test_benchmark_reports.py` | 7 | 按耗时加权吞吐、失败轮保留、单轮统计、预热排除/失败、Markdown、CLI 文件/退出码/参数 |
| `test_benchmark_integration.py` | 4 | 真实 HTTP/SQLite/Worker 多轮隔离与清理、零读取负载、截止点失败证据、CLI JSON/Markdown 一致性 |

完整回归同时收集 unittest 与 pytest 风格用例：

```powershell
$env:PYTHONPATH = 'src'
python -m pytest tests -q
```

当执行环境限制 loopback 或子进程访问时，真实 HTTP 集成测试需要允许本机连接的
环境；纯校验和报告测试不需要网络。

## 本次实际运行结果

2026-10-13 的 [检查与验证报告](benchmark-validation-2026-10-13.md) 记录了新增
18 项测试和完整 367 项回归的证据。两组负载均进行了 1 次预热和 3 次测量：

- [8 客户端 / 4 Worker 报告](benchmark-results-2026-10-13.md)：3 轮均 FAIL，
  1200 次 HTTP 请求中 4 次超时；全部工作流与任务结果仍通过检查。
- [4 客户端 / 2 Worker 报告](benchmark-results-low-concurrency-2026-10-13.md)：
  3 轮均 PASS，1200 次 HTTP 请求零错误。

失败结果保留在报告中。可重复运行表示可以重复执行相同负载并获得可检查的结果，
不保证任意配置均通过，也不保证每次性能数值相同。
