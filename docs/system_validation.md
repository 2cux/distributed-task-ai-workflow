# 当前实现的系统集成验证

验证日期：2026-10-11。运行环境：Windows、Python 3.12.10、pytest 8.3.0。

## 结论与范围

本轮完整回归 **277 项通过**，其中新增 4 项跨入口集成测试。未发现测试失败，
没有修改生产代码。此结论表示当前实现通过了下列场景的断言，不表示已经证明
系统在任意输入、故障、机器和负载下均无缺陷。

当前产品边界是内存执行及同机共享 SQLite 的任务引擎。README 中规划的 DAG、
跨主机调度、AI 模型调用编排和硬超时尚未实现，本轮不将它们计入已验证能力。

## 覆盖矩阵

| 方面 | 本轮执行的测试文件 | 验证内容 |
| --- | --- | --- |
| 入口与生命周期 | `test_taskengine.py`、`test_system_scenarios.py`、`test_invariants.py` | 提交、批量提交、执行、终态、预算与状态约束 |
| 优先级与延迟调度 | `test_queue.py`、`test_delayed_scheduling.py`、`test_backoff.py` | FIFO、优先级、首次到期时间、重试退避、等待唤醒 |
| 并发与关闭 | `test_concurrency.py`、`test_bounded_consumption.py`、`test_worker_shutdown.py` | 槽位上限、补位、停止后保留任务、在途调用收尾 |
| 持久化与重复提交 | `test_persistence.py`、`test_persistent_idempotency.py` | JSON 校验、重启恢复、定义冲突、跨引擎原子领取 |
| 事件一致性 | `test_task_events.py`、`test_persistent_events.py` | 状态与事件共同提交、不可变快照、序号、旧库升级 |
| 超时与异常 | `test_timeout.py`、`test_persistent_failure_boundaries.py` | 超时预算、旧调用结果隔离、持久化重试不重叠、结果提交失败 |
| Worker 存活与接管 | `test_worker_registry.py`、`test_worker_takeover.py`、`test_reliable_workers.py` | 会话身份、心跳、撤权、停止确认、接管预算与旧结果拒绝 |
| 多消费者故障与竞态 | `test_multiworker_integration.py` | 混合负载、读一致性、审计事务回滚、心跳与撤权竞态、并发恢复 |
| 真实进程与崩溃 | `test_multiworker_processes.py`、`test_crash_recovery.py` | 独立进程竞争、强制终止、已提交重试恢复、业务键去重 |
| 新增组合场景 | `test_engine_boundary_integration.py` | 以下 4 项新场景 |

## 新增场景

1. **批处理入口与常驻消费者共同消费。** 三槽位 `TaskEngine.start()` 与
   `ReliableWorker` 操作同一数据库，包含一个被阻塞的任务及 16 个首次失败的任务。
   检查每个任务的业务调用次数、attempt 不重叠、重试预算、持久化结果和事件历史。
   最后消费生产者保留的旧队列快照，确认不会重放已经完成的业务。
2. **运行中的独立生产者重复提交。** 两个真实 Python 子进程各提交 96 次，
   共享 32 个业务 ID；两个消费者持续执行。检查 192 次提交只生成 32 次调用，
   两个进程都拒绝定义冲突，完成后数据库 `integrity_check` 返回 `ok`。
3. **非法结果与正常任务混合执行。** 返回任意对象、返回 NaN、业务异常及正常
   JSON 结果共存。检查结果序列化失败不重放副作用、不消耗重试，业务异常用尽
   两次重试，正常任务完成；提高重启后的策略预算并重复提交也不会重放终态任务。
4. **运行、重试、成功和重启之间的事件增量读取。** 通过 Event 控制两次真实
   业务调用，分段读取 `after_sequence`；检查没有丢失或重复事件，失败详情保留，
   重启后得到与完整历史一致的结果。

新增测试使用真实 SQLite、执行器、Worker 与操作系统子进程，没有替换核心组件。
Event 控制业务调用的可观察阶段；失败时释放阻塞并关闭线程及子进程。

## 执行结果

- 既有完整 pytest 基线：**273 passed**。
- 新增文件独立执行：**4 passed**。
- 含新增测试的完整回归：**277 passed**（22.31 秒）。
- 新增组合、既有多 Worker 集成、真实进程三个文件共 23 项，连续执行 3 轮，
  每轮 **23 passed**。两个既有事务竞态场景每轮各执行 12 次竞争。
- unittest 基线为 **259 项通过**；它不会收集 pytest 风格的测试，因此完整结果
  以 pytest 为准。

## 复现

在项目根目录运行：

```powershell
$env:PYTHONPATH = 'src'
python -m pytest tests -q
python -m pytest tests/test_engine_boundary_integration.py -q
for ($round = 1; $round -le 3; $round++) {
    python -m pytest tests/test_engine_boundary_integration.py tests/test_multiworker_integration.py tests/test_multiworker_processes.py -q
    if ($LASTEXITCODE -ne 0) { throw "Integration repetition failed: $round" }
}
```

## 验证限制

本轮没有做跨主机网络分区、真实断电或磁盘损坏、长期高负载容量测试，也没有
验证外部支付、模型服务等业务系统的幂等性。进程强制终止不能替代物理断电测试。
内存模式采用软超时，超时后旧业务代码可能仍在运行；持久化模式等待旧调用退出。
外部副作用的去重仍依赖业务键、唯一约束或外部接口的幂等机制。
