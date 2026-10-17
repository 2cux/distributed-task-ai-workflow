# AI 工作流持续负载与质量回归

已完成的瓶颈定位、数据库优化及原配置复测见
[2026-10-17 性能优化记录](ai-workflow-optimization-2026-10-17.md)。

`tools/benchmark_ai_workflow.py` 扩展原有 management benchmark 的覆盖范围。
原有工具及 schema v2 保持兼容，继续用于有限批次的控制面抽样；新工具使用独立
schema v1，覆盖真实产品处理器的持续执行链：

管理 HTTP 提交 → SQLite DAG → ReliableWorker → `llm.chat.v1` → 本机 HTTP
模拟供应商 → `llm.validate.v1` → 工作流用量查询 → 持久化结果核对。

不读取模型环境变量，不使用真实供应商额度。服务仅绑定 127.0.0.1，每轮创建全新
临时数据库和专属 Worker；关闭后删除数据库。不要和单元测试或其他负载同时测量。

## 可重复命令

```powershell
$env:PYTHONPATH = 'src'
# 快速检查：并发 1/4，各持续提交 3 秒
python tools/benchmark_ai_workflow.py --profile benchmarks/ai-smoke.json `
  --output .artifacts/ai-smoke.json --markdown .artifacts/ai-smoke.md

# 常规回归：并发 1/4/8，各 60 秒，两轮；每次独立预热 5 秒
python tools/benchmark_ai_workflow.py --profile benchmarks/ai-regression.json `
  --output .artifacts/ai-regression.json --markdown .artifacts/ai-regression.md

# 持续负载：并发 1/4/8，各 180 秒，两轮；每次独立预热 10 秒
python tools/benchmark_ai_workflow.py --profile benchmarks/ai-soak.json `
  --output .artifacts/ai-soak.json --markdown .artifacts/ai-soak.md

# 和同机、同配置、已通过且完整的历史报告比较，输出到新文件
python tools/benchmark_ai_workflow.py --profile benchmarks/ai-soak.json `
  --baseline docs/ai-soak-baseline-2026-10-17.json `
  --output .artifacts/ai-candidate.json --markdown .artifacts/ai-candidate.md
```

持续负载有 18 分钟正式提交窗口，另加共 60 秒预热、最后在途工作流排空和关闭。
增加 profile 中 `duration` 可以执行数小时或更长的同库持续测试；设置 `repeat` 和
`concurrencies` 控制重复次数与并发档位。配置必须完整，未知键、非有限数字、重复
并发档位会在创建服务前拒绝。

## 负载和统计口径

- 固定 4 个 Worker，独立心跳；协调器轮询 0.1 秒，Worker 轮询 0.05 秒、心跳 1 秒。
- 每个客户端最多一个在途工作流；完成或失败后继续提交。这是闭环并发压测，
  不是固定到达率压测，不能用它宣称开环峰值 QPS、无限积压下的稳定性或消除
  coordinated omission。报告中并发量为客户端槽位，生成调用上限还受 Worker 数约束。
- 每个工作流恰好一个生成节点和一个校验节点；响应回显唯一工作流 ID，延迟固定
  25 ms，usage 固定 11/7/18，调用预算 1 次及 18 token，禁止重试。
- 运行时间只控制停止提交新工作流的时刻。已提交的工作流继续查询至统一的
  `duration + drain_timeout` 截止点。HTTP socket 操作超时独立为 10 秒；正在执行的
  单次 socket 操作可能越过排空截止点。初始化、关闭和最终核对不计入吞吐分母。
- HTTP 延迟包括提交和查询的成功与失败请求，另保存两类接口分布。工作流延迟从
  提交开始到客户端观察到终态，包含排队、模型模拟调用、校验和 50 ms 查询间隔。
  工作流失败率包括提交错误、失败/暂停终态、usage 不匹配、排空截止点。
- 吞吐为成功工作流数除以正式提交加排空耗时。预热使用独立库，不并入正式统计；
  预热失败仍使整个测试失败。第二轮轮换并发档位次序，减轻固定测试顺序偏差。
- 分位数采用固定大小、1% 对数直方图的 nearest-rank 桶上界，微秒以下合入零桶；
  报告毫秒保留三位小数，max 为真实观察最大值。不把各轮分位数合成总体分位数。
- JSON 保存按请求/工作流开始时刻分窗的 HTTP 和工作流计数、失败率、分位数，
  以及按数据库操作完成时刻分窗的等待/SQL 分布，可检查早晚退化。最后窗口可能
  不完整，含排空；不能把它的计数当成完整窗口吞吐。

## 数据库竞争证据

Benchmark 只包装本轮 TaskStore 和 WorkerRegistry 的连接与 FIFO writer slot，不
修改产品源代码、不全局替换 sqlite3，不改变锁超时、事务或重试策略。

`writer_wait` 测量本地 FIFO 写入槽等待，`sqlite_begin` 测量两类持久化实例的
`BEGIN IMMEDIATE` 用时，`sql` 统计 execute 返回/抛出前的耗时与错误类型。
`busy_errors` 合并 SQLITE_BUSY/LOCKED（含扩展码）及本地写入队列超时。
BEGIN 用时包含语句及锁申请开销，不能当成纯粹锁等待；没有 busy 错误也可能
存在严重等待。初始化 PRAGMA、游标遍历及独立进程不在此探针覆盖范围内。
探针本身会增加少量锁与直方图统计开销，比较时必须使用同一工具版本。

最终逐个核对所有已发起工作流的 ID、SUCCESS、精确节点集合、输出回显、token
用量、一次 attempt、零 retry、一次成功事件，以及供应商每个 ID 恰好一次调用；
检查 SQLite integrity、foreign keys、协调器错误和 Worker/服务关闭。空样本不能通过。
提交超时属于结果不明确，仍纳入预期 ID 集合，不以忽略它来降低失败率。

## 回归门禁

Profile 中 `limits` 是版本化的项目质量线，不代表生产 SLO。每个正式轮次及预热
轮次均执行绝对门禁，任一失败就返回退出码 1：

| 指标 | 默认上/下限 |
| --- | ---: |
| HTTP / 工作流失败率 | 0 |
| HTTP P95 | ≤ 2000 ms |
| 工作流 P95 | ≤ 10000 ms |
| 本地 writer 等待 P95 | ≤ 1000 ms |
| SQLite BEGIN P95 | ≤ 500 ms |
| busy/locked/队列超时 | 0 |
| 成功工作流吞吐 | ≥ 0.1/s |
| 数据、调用次数、完整性、生命周期 | 全部通过 |

指定 `--baseline` 后，每个并发档位的逐轮中位数还需满足：吞吐不低于历史的
75%，HTTP/工作流/两种数据库等待 P95 不高于历史的 130%。等待/延迟比值计算
使用 1 ms 噪声下限。比较失败率仍执行绝对门禁，不因历史有错误而容许错误。
要求 baseline 完整、已通过，且 workload/schema、全部配置、全部阈值、Python、
SQLite、平台和主机名一致；缺轮或失败基线拒绝通过。报告保存 baseline 内容 SHA256。
跨机器应先建立各自的基线。不要把失败报告重命名为基线，也不要因候选失败自动更新基线。

输出 JSON 原子写入。长跑每完成一轮保留 `.partial.json`，标记 `complete=false`；
被中断的 partial 不能用作基线。最终报告 `complete=true`。成功退出码 0，门禁
失败为 1，profile/路径等参数错误为 2。运行异常/中断保留已完成轮次的 partial，
不会产生完整成功报告。正式报告写到新文件，以保留已有基线证据。

## 自动验证

```powershell
$env:PYTHONPATH = 'src'
python -m pytest tests/test_ai_benchmark.py -q
python -m pytest tests -q
```

新增测试包括真实 SQLite 写锁竞争、错误不被探针吞掉、工具实例恢复、真实 HTTP
生成/校验/计量及资源清理、空样本拒绝、预热失败保留、相对退化和不兼容基线拒绝。
CI 自动跑快速质量线，手动工作流可选择 regression 或 soak 并上传 JSON/Markdown。

长跑结论与实际机器数据见 [2026-10-17 验证记录](ai-benchmark-validation-2026-10-17.md)。
