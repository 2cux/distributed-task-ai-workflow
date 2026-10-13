# Benchmark 检查与验证报告

日期：2026-10-13。环境：Windows 11、Python 3.12.10、SQLite 3.49.1。
代码基线为 `506d8b6`，本报告对应其上的本次 Benchmark 工具及测试工作区修改。

## 检查与改进

系统已有可以单次复跑的 `tools/benchmark_management.py` 和历史 JSON 报告
`management-benchmark-2026-10-13.json`，因此本次在已有工具上扩展：

- `--repeat` 顺序运行独立测量轮，`--warmup` 保留预热证据并从统计中排除。
- `--output` 保存 JSON，`--markdown` 保存逐轮结果、统计口径和失败原因。
- 检查完整节点集合、根及汇合节点结果，以及每个任务的一次尝试和零重试。
- 记录 Worker 运行/关闭异常，单独报告 HTTP 错误和生命周期错误。
- 临时数据库改为位于可写的当前运行目录，兼容系统临时目录受限的环境。
- 保留默认单轮命令及原 JSON 指标字段，无新增运行依赖。

## 独立测试与回归

| 测试组 | 数量 | 结果 |
| --- | ---: | --- |
| 参数、计时及结果校验 `test_benchmark_validation.py` | 7 | PASS |
| 汇总、预热、Markdown 及 CLI `test_benchmark_reports.py` | 7 | PASS |
| 真实 HTTP、SQLite、Worker 与 CLI `test_benchmark_integration.py` | 4 | PASS |
| 完整既有及新增回归 | 367 | PASS，69.37 秒 |

三组均分别以 unittest 执行；完整 pytest 回归同时收集所有 unittest 和 pytest
风格测试。完整结果为 367 passed，0 failures、0 errors、0 skipped；原始证据见
[JUnit XML](benchmark-validation-2026-10-13-full.xml)，其中新增组分别为 7、7、4 项。

首次执行原工具时，系统临时目录中的 SQLite 文件无法打开；本次已修正临时库位置。
受限环境的真实 HTTP 测试发生 URLError，允许 loopback 的执行环境中同组测试全部
通过。最终回归和正式 Benchmark 使用允许本机连接的环境，Benchmark 在回归结束后
单独顺序运行，没有与回归测试并行。

## 两组重复测量

每轮为 100 个两节点 DAG、300 次读取，共 400 个 HTTP 请求；截止点 60 秒，
单次 HTTP 超时上限 10 秒。每组 1 次预热、3 次测量，使用独立临时库。
下表只汇总测量轮，保留所有失败轮；两组只有客户端并发数和 Worker 数不同。

| 指标 | 8 客户端 / 4 Worker | 4 客户端 / 2 Worker |
| --- | ---: | ---: |
| 测量轮通过 | 0 / 3（FAIL） | 3 / 3（PASS） |
| HTTP 请求总数 | 1200 | 1200 |
| HTTP 错误 | 4 次 TimeoutError | 0 |
| 汇总 HTTP 吞吐 | 30.22 请求/秒 | 31.19 请求/秒 |
| 各轮 P95 的中位数 | 403.378 ms | 189.362 ms |
| 各轮 P99 的中位数 | 3416.727 ms | 542.117 ms |
| 各轮最慢请求的最大值 | 10158.899 ms | 7008.318 ms |
| 完成工作流 | 300 / 300 | 300 / 300 |
| 任务尝试 / 重试 | 600 / 0 | 600 / 0 |
| 根及汇合节点结果 | 全部正确 | 全部正确 |
| 汇总工作流吞吐 | 7.24 / 秒 | 7.42 / 秒 |
| Worker 会话异常 | 第一测量轮记录两条 WorkerSessionMismatch | 无 |
| SQLite integrity_check | 每轮 ok | 每轮 ok |

两组预热均 PASS。8 客户端的三轮 HTTP 错误分别为 1、2、1 次；第一轮还记录了
WorkerSessionMismatch，因此判为 HTTP/生命周期检查失败。全部已提交工作流完成，
每个任务恰有一次尝试，结果正确。4 客户端的预热及三轮测量全部通过。

这说明本次本机较高并发负载下存在客户端超时及 Worker 会话稳定性问题；具体根因
尚未确定。本次扩展提供检测与报告，没有修改调度器、存储或 Worker 的执行机制。
各轮 P95/P99 中位数不是合并全部请求后的百分位数；吞吐按总数除以耗时总和计算。
结果属于本机容量抽样，未覆盖真实模型推理、跨主机网络、故障恢复性能或长期 soak。

原始结果与自动生成的可读报告：

- 8 客户端：[JSON](benchmark-results-2026-10-13.json)、[Markdown](benchmark-results-2026-10-13.md)。
- 4 客户端：[JSON](benchmark-results-low-concurrency-2026-10-13.json)、[Markdown](benchmark-results-low-concurrency-2026-10-13.md)。

## 复现命令

从仓库根目录执行：

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_benchmark_validation.py -v
python -m unittest discover -s tests -p test_benchmark_reports.py -v
python -m unittest discover -s tests -p test_benchmark_integration.py -v
python -m pytest tests -q --junitxml=docs/benchmark-validation-2026-10-13-full.xml

python tools/benchmark_management.py --workflows 100 --reads 300 --concurrency 8 --workers 4 `
  --deadline 60 --repeat 3 --warmup 1 `
  --output docs/benchmark-results-2026-10-13.json --markdown docs/benchmark-results-2026-10-13.md
python tools/benchmark_management.py --workflows 100 --reads 300 --concurrency 4 --workers 2 `
  --deadline 60 --repeat 3 --warmup 1 `
  --output docs/benchmark-results-low-concurrency-2026-10-13.json `
  --markdown docs/benchmark-results-low-concurrency-2026-10-13.md
```

高负载检查不通过时 CLI 返回 1，并保留失败报告。更多字段和边界说明见
[Benchmark 使用与验证](benchmarks.md)。
