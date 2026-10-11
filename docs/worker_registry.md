# 多 Worker 执行与分阶段恢复

在本仓库 [PR #1](https://github.com/2cux/distributed-task-ai-workflow/pull/1) 的
`WorkerRegistry` 基础上扩展。保留 `register / heartbeat / stop / get /
list_workers / is_overdue`，新增持久化会话状态、历史和接管协议。

## 启动多个消费者

```python
from workflow_engine import Task, TaskEngine, RetryPolicy

engine = TaskEngine(
    database_path='tasks.db',
    task_registry={'abs': abs},
    retry_policy=RetryPolicy(2),
)
a = engine.create_worker('worker-a', heartbeat_interval=1, heartbeat_timeout=10)
b = engine.create_worker('worker-b', heartbeat_interval=1, heartbeat_timeout=10)
a.start()
b.start()
task = engine.submit(Task('absolute', abs, args=(-7,)))
# 使用 engine.get_task(task.id) 查询数据库中的权威状态。
# 提交返回的内存 Task 不会被其他消费者原地更新。
print(engine.worker_registry.list_workers())
print(engine.worker_registry.list_sessions())
# 结束服务时：
a.shutdown(timeout=5)
b.shutdown(timeout=5)
```

每个 `ReliableWorker` 有一个执行槽位和独立心跳线程；增加实例即可增加并发。
不同进程中的 `TaskEngine` 可以共享本机的同一个 SQLite 文件，消费者无需重启就
能看到其他引擎新提交的任务和已持久化的重试。业务函数名称必须在所有进程的
`task_registry` 中保持一致。任务按到期时间过滤，再按优先级和同级 FIFO 领取；
选择、领取、增加 attempt 和追加 STARTED 事件共用一个写事务。

`TaskEngine.start()` 继续提供原来的有限队列同步执行入口；
`create_worker().start()` 是注册到协调系统的常驻消费入口，二者不要混作同一
worker 的生命周期。原同步入口没有注册会话，仅能按原有离线恢复契约处理。
`max_workers` 继续控制原同步入口的线程池；常驻 worker 的数量由实例数量决定。

## 身份、会话和状态

`worker_id` 是可复用的部署标识，`session_id` 标识每次启动，领取令牌 `owner`
标识具体 attempt。任务所有权绑定 session、owner 和递增的 attempt_count。
同一个 ID 必须明确停止后才能重新注册，心跳过期不能抢占 ID。
新会话不会覆盖旧会话历史，旧会话的心跳或停止请求不能操作新会话。

为兼容 PR #1，`WorkerRecord.status` 保留 ACTIVE / STOPPED；具体协调状态从
`get_session(session_id)` / `list_sessions()` 读取：

| 会话状态 | 领取新任务 | 当前 attempt 提交 | 能否恢复 RUNNING 任务 |
| --- | --- | --- | --- |
| ACTIVE 且心跳未过期 | 允许 | 允许 | 禁止 |
| SUSPECT | 禁止 | 允许 | 禁止 |
| FENCED | 禁止 | 拒绝 | 禁止 |
| STOPPING | 禁止 | 允许 | 禁止 |
| STOPPED 且有停止依据 | 禁止 | 拒绝 | 允许按业务安全规则恢复 |

ACTIVE 心跳在检测周期之前已经过期时，领取也会被拒绝。
SUSPECT 收到有效心跳会恢复原运行/收尾状态；FENCED 不能通过心跳恢复。
健康心跳先于撤权事务提交时，旧的失联判断不能继续执行撤权。
收尾中的 worker 若崩溃，同样会被周期检测发现。

## 分阶段接管

1. **检测**：心跳超过注册时的 `heartbeat_timeout`，`detect_overdue()` 将会话
   标记 SUSPECT。任务仍保持 RUNNING，令牌不变，不安排重试。旧调用若正常完成，
   仍可以提交，从而避免误判造成重放。
2. **撤权**：协调者显式调用 `fence(worker_id, session_id)`。撤权事务与领取、
   完成事务互斥：先完成的结果保留，撤权后的旧结果和重试提交被拒绝并回滚。
   此时仍不能领取旧 worker 的 RUNNING 任务。
3. **确认停止**：受信任的 supervisor 确认旧进程退出并 join，或者常驻 worker
   等待实际调用全部退出后发送运行时收尾确认，再进入 STOPPED。
4. **恢复**：`engine.recover_worker(session_id)` 只处理该旧会话的 RUNNING 任务。
   幂等且还有预算的任务消耗一次重试机会、进入 RETRYING；否则进入 FAILED，
   携带 `UncertainExecutionError` 供人工核对。重试退避使用首次提交的策略。
   恢复、计数及事件原子提交；重复或并发恢复不重复消耗预算。

```python
registry = engine.worker_registry
for suspect in registry.detect_overdue():
    registry.fence(suspect.worker_id, suspect.session_id)
    # supervisor 在这里停止并等待对应的旧进程及全部业务调用退出。
    # 确认依据来自实际 process.join()/wait()，不能来自另一次心跳超时。
    registry.confirm_stopped(
        suspect.worker_id,
        suspect.session_id,
        evidence='supervisor observed process exit and joined all invocations',
    )
    engine.recover_worker(suspect.session_id)
```

上面的 `confirm_stopped` 是 supervisor 的可信确认接口，字符串用于审计，系统
不会根据字符串自行验证或终止远程进程。只有取得真实停止证据后才能调用。
没有证据时任务继续隔离，宁可等待也不并行重放。若业务函数自行创建后台工作，
确认停止也必须覆盖这些后台工作。幂等标记是业务承诺，外部副作用需使用稳定业务键
去重；撤销引擎数据库写入权限不会撤销已经发生的外部副作用。

`recover_interrupted=True` / 全局离线恢复会拒绝带注册会话的 RUNNING 任务，
防止绕过上述步骤。旧版无会话任务继续沿用确认所有旧 worker 停止的离线恢复契约。

## 周期观测、关闭和故障

每个 `ReliableWorker` 的心跳线程周期发送自身心跳并检测其他会话，不因长任务占用
执行线程而停发心跳。纯协调者也可以启动独立监测器：

```python
from workflow_engine import WorkerMonitor
monitor = WorkerMonitor(engine.worker_registry, interval=1)
monitor.start()
# ...
monitor.shutdown(timeout=5)
```

监测器只记录怀疑，不自动撤权、杀进程或接管任务。如果所有消费者都退出，需要
仍存活的监测器或 supervisor 驱动检测。数据库异常会让消费者停止领取新任务，
当前调用仍需等待结束；异常可通过 `last_error` 检查。

`stop()` 请求优雅停止并禁止继续领取；心跳保持到当前调用收尾。
`shutdown(timeout)` 返回 False 表示仍在等待，不能作为停止证据。
持久化软超时同样等待实际调用退出。成功收尾后自动写入 STOPPED 及停止依据；
若结果提交失败留下 RUNNING，协调者可随后按该会话恢复。
若数据库不可写，停止依据无法落库，所有权继续保留，等待 supervisor 核对。

`get_session_events(session_id)` 读取持久化状态转换和停止依据；任务 STARTED / 
INTERRUPTED 事件包含 worker_session。升级旧任务库及 PR #1 的注册表无需删库。

## 独立验证

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_worker_registry.py -v
python -m unittest discover -s tests -p test_worker_takeover.py -v
python -m unittest discover -s tests -p test_reliable_workers.py -v
python -m unittest discover -s tests -p test_multiworker_processes.py -v
python -m unittest discover -s tests -q
```

四组测试分别覆盖原注册表契约、阶段安全与事务回滚、常驻消费与实际调用收尾、
真实独立进程竞争与无清理钩子的进程退出。

更广泛的负载、故障注入、事务竞态及进程强制终止验证见
[多 Worker 集成验证](multiworker_testing.md)。完整回归应使用
`python -m pytest tests -q`，同时收集仓库的 unittest 和 pytest 风格测试。

当前支持同一主机上的多线程/多进程，使用共享本地 SQLite 文件和主机时钟。
多主机部署需要服务器数据库与明确的时间权威；时钟跳变可能误报或延迟发现，
但检测不会因此自动重放。当前没有远程 worker 通信或强制杀进程的 supervisor。
