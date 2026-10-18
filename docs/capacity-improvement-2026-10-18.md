# 默认超时下的容量与尾延迟优化

日期：2026-10-18，Asia/Shanghai。对照源码为 Git HEAD `79b2dfd` 的独立副本，候选为本次工作区实现。运行环境为 Windows、Python 3.12.10、SQLite 3.49.1。

## 管理负载对比

每档 50 个两节点工作流、150 次交错读取、4 个 Worker，客户端并发分别为 8、16。每组独立预热 1 轮，正式测量 3 轮，每轮新建数据库、HTTP 服务和 Worker。保留默认服务等待 2 秒；客户端请求最多 10 秒；完成截止点 20 秒。没有客户端重试，没有放宽阈值或忽略失败轮。

最终对照顺序为原版 8 → 候选 8 → 原版 16 → 候选 16，串行执行。开始前确认另一组基准采集已结束。每组三轮合计 600 个请求；下表 P99 是各轮 P99 的中位数，不是合并请求后的 P99。

| 客户端并发 | 原版 / 候选 HTTP 错误 | 原版 / 候选 HTTP P99 中位数 | 原版 / 候选 HTTP 请求/秒 | 原版 / 候选完成工作流 |
| --- | --- | --- | --- | --- |
| 8 | 0 / 0 | 1015.796 / 379.346 ms | 44.42 / 39.66 | 150 / 150 |
| 16 | 21 / 0 | 2070.329 / 526.310 ms | 43.20 / 45.16 | 129 / 150 |

8 并发 P99 下降约 63%，HTTP 吞吐下降约 11%；不能据此声称所有指标都提升。该组原版也零错误，之前偶发的 503 没有在这组 8 并发对照中重现。

16 并发原版出现 21 次 HTTP 错误，未能接受全部提交，门禁失败；候选全部接受并完成 150 个工作流、300 个任务，每任务执行一次、无重试，结果与数据库完整性检查通过。候选的 P99 下降约 75%。本轮收益主要是降低尾延迟，并扩大默认等待设置下已验证能够通过的负载范围。

HTTP 吞吐包含错误响应；原版 16 并发的工作流吞吐还受缺失提交后等待截止点影响，因此没有将工作流吞吐倍数用作加速结论。

## 定位与实现

诊断表明写入排队的尾部较长，连接准备/关闭、HTTP 提交及批量工作流协调都参与了竞争。初步缩短事务和单纯连接复用仍不能稳定解决更高并发下的 503。将数据库活动请求数约束为 4 的单独实验，通过了三轮 600 请求，因此最终加入有界数据库准入。

1. 管理服务持有有界 SQLite 连接池，每个请求独占借用。默认 `database_concurrency=4`，可通过 Python 构造参数设置为 1..256，实际不超过 HTTP `max_concurrency`。连接等待按 FIFO 排队，超时沿用 `request_timeout`，返回带重试提示的 503。
2. 每次数据库租约结束时释放读取快照、回滚未提交状态，后续借用恢复 busy timeout、外键及 FULL 同步设置。服务关闭时关闭空闲句柄，已借出句柄在归还时关闭；损坏句柄和连接创建失败不会占住准入槽。
3. 连接准备、关闭移到写入 FIFO 之外，减少文件系统操作占用写入机会。任务与引擎所属 Worker 注册表继续共用写入 FIFO，跨进程仍由 SQLite 事务互斥。
4. 协调器按工作流分别提交事务，在不同工作流之间释放写入机会。单工作流仍原子协调，失败通过 savepoint 回滚并留存错误事件，后续工作流和下一次扫描仍能推进。

WAL、`synchronous=FULL`、任务所有权校验、可信停止确认、原子预算检查及非幂等恢复规则均保留。连接池只用于管理 HTTP；业务 Worker 的执行并发及 30 秒存储等待没有改变。数据库准入等待、写入槽等待和 SQLite 锁等待分别受限，`request_timeout` 仍不是整个 HTTP 请求的总时限。

## AI 工作流对比

同日串行执行原版及候选，固定并发 1/4/8、4 Worker、模拟供应商延迟 25 ms；每档两轮 10 秒正式负载和各自 1 秒预热。该独立 AI profile 的请求超时沿用 10 秒，记录于原始配置。沿用原有绝对阈值，并给候选传入原版基线，执行最低吞吐比 0.75、最大延迟比 1.3 的相对门禁，未调整门槛。

两版各 6 个正式轮次及 6 个预热轮次全部通过；候选正式轮次共完成 792 个工作流、1584 个任务、792 次本地供应商调用，输出、调用次数、预算、用量、事件及数据库完整性核对均通过，HTTP/工作流错误和 busy/locked 均为零。

| 并发 | 原版 / 候选工作流每秒（逐轮中位数） | 原版 / 候选工作流 P95 ms（逐轮中位数） |
| --- | --- | --- |
| 1 | 3.590 / 3.446 | 366.247 / 364.425 |
| 4 | 13.283 / 13.360 | 384.968 / 384.968 |
| 8 | 21.770 / 21.775 | 476.838 / 511.236 |

AI 吞吐总体相近，1 并发约低 4%，8 并发 P95 约高 7%；相对门禁通过不表示每个指标都提高。原始数据见下方证据链接。

## 完整回归

启用 `WORKFLOW_CONTAINER_TESTS=1` 后，全量测试 **499 passed、0 skipped、0 failed，171.59 秒**。本次新增 10 项回归用例，覆盖连接池独占借用、准入超时与失败后的容量恢复、快照及事务释放、关闭和迟到请求、HTTP 成功及冲突后的连接归还，以及工作流间公平推进和局部失败回滚。

四项容器测试均实际执行并通过：非幂等任务崩溃后不重放、替换 Worker 后拒绝旧会话提交、真实网络分区及重连、SIGKILL 后受监督恢复。测试完成后，`workflow-fault-` 前缀的容器、网络、卷及镜像均无残留。

```powershell
$env:PYTHONPATH = 'src'
$env:WORKFLOW_CONTAINER_TESTS = '1'
$env:WORKFLOW_DOCKER = 'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
python -m pytest tests -q --junitxml=.artifacts/capacity-full-regression-tests.xml
```

## 证据与复现

正式结果保存在 `docs/capacity-evidence-2026-10-18/`，包含校验和，便于随报告保留：

- [管理负载对照汇总、源提交与源码差异 SHA256](capacity-evidence-2026-10-18/capacity-isolated-comparison.json)
- 8 并发：[原版](capacity-evidence-2026-10-18/capacity-isolated-control-8.json)、[候选](capacity-evidence-2026-10-18/capacity-isolated-candidate-8.json)
- 16 并发：[原版](capacity-evidence-2026-10-18/capacity-isolated-control-16.json)、[候选](capacity-evidence-2026-10-18/capacity-isolated-candidate-16.json)
- AI：[配置](capacity-evidence-2026-10-18/capacity-ai-profile.json)、[原版](capacity-evidence-2026-10-18/capacity-ai-control-2026-10-18.json)、[候选及相对门禁](capacity-evidence-2026-10-18/capacity-ai-candidate-2026-10-18.json)
- [全量回归 JUnit XML](capacity-evidence-2026-10-18/capacity-full-regression-tests.xml)、[测试资源清理检查](capacity-evidence-2026-10-18/capacity-cleanup.json)、[SHA256 校验和](capacity-evidence-2026-10-18/SHA256SUMS.json)

探索阶段及失败结果另外保留在 `.artifacts/`，没有覆盖为成功结果。

```powershell
$env:PYTHONPATH = 'src'
python tools/benchmark_management.py --workflows 50 --reads 150 --concurrency 8 --workers 4 --deadline 20 --repeat 3 --warmup 1 --output .artifacts/candidate-8.json --markdown .artifacts/candidate-8.md
python tools/benchmark_management.py --workflows 50 --reads 150 --concurrency 16 --workers 4 --deadline 20 --repeat 3 --warmup 1 --output .artifacts/candidate-16.json --markdown .artifacts/candidate-16.md
```

对照需将 `PYTHONPATH` 指向 `79b2dfd` 的独立源码副本，不能让两组导入同一份工作区代码。性能测量应顺序执行，避免与另一组采集或全量测试同时运行。

这些是同机短负载结果，未测定最大可承载并发，也不能替代生产长期容量测量。更高压力下仍保留背压；`database_concurrency` 需要结合磁盘、查询复杂度及机器负载选择。

诊断的 `writer_wait` 只覆盖写入 FIFO，未计入新增的连接池准入等待，因此收益以 HTTP 和工作流端到端结果判断，不能把等待转移到另一阶段解释成等待被消除。早期探索与另一组基准的执行时间有重叠，未将这些探索结果纳入上方最终对照。
