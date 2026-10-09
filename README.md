# Distributed Task & AI Workflow Orchestration Platform

分布式任务调度与工作流编排平台。

## Overview

平台负责异步任务的执行、协调、监控与恢复。

项目从一个最小任务执行引擎起步，按阶段演进：

| 阶段 | 能力 |
| --- | --- |
| 1 | 最小任务执行引擎 |
| 2 | 分布式 Worker |
| 3 | 基于 DAG 的工作流 |
| 4 | 失败恢复 |
| 5 | 面向 AI 的工作流执行 |

## Core Design Idea

系统不提前定义具体业务功能，只提供一组通用、可组合的执行与工作流原语。业务能力由原语组合得出。

由此得到两条约束：

- 原语集合保持最小，每种原语有明确输入、输出与失败语义。
- 业务逻辑放在原语之上，平台不内置业务概念。

## 负载

同一组原语承载两类负载：

| 负载 | 关注点 |
| --- | --- |
| 异步任务 | 提交、调度、执行、重试 |
| AI 工作流 | 多步骤、多模型调用、多 Worker 协作 |

## 原语

| 原语 | 语义 | 状态 |
| --- | --- | --- |
| Task | 一份可被调度、执行、追踪的工作的描述 | 已实现 |
| Executor | 同步执行单个任务并更新其生命周期 | 已实现 |
| TaskQueue | 进程内稳定优先级待执行任务队列 | 已实现 |
| Worker | 从队列取出任务并同步交给执行器 | 已实现 |
| WorkerLoop | 在专用线程中持续驱动 Worker，直至收到停止请求 | 已实现 |
| Scheduler | 接收任务、入队并启动 Worker 执行当前队列 | 已实现 |
| RetryPolicy | 判断重试预算，计算退避延迟并安排重试时间 | 已实现 |
| TimeoutPolicy | 给出"一次执行"的超时上限，任务级优先、策略默认值兜底 | 已实现 |
| TimeoutExecutor | 同步执行单次尝试并施加超时上限的执行器 | 已实现 |
| ConcurrentWorker | 固定大小线程池并发消费队列 | 已实现 |
| TaskEngine | 装配已有原语并提供提交与启动的统一入口 | 已实现 |

## 统一入口

`TaskEngine` 是面向调用方的系统边界：它只装配 `TaskQueue`、执行器、Worker、
重试与超时策略，并把 `submit` / `start` 转发给已有组件；它不实现任务状态机、
队列策略、重试决策或具体业务逻辑。

```python
from workflow_engine import RetryPolicy, Task, TaskEngine, TimeoutPolicy

engine = TaskEngine(
    retry_policy=RetryPolicy(max_retries=2),
    timeout_policy=TimeoutPolicy(timeout=30),
    max_workers=4,
)
task = engine.submit(Task(name="call-model", callable=call_model, timeout=5))
completed_attempts = engine.start()
```

`submit()` 仅入队并返回同一个 `Task` 供状态追踪；`start()` 才开始处理当前队列。
`max_workers=1` 使用同步 `Worker`，大于 1 时使用 `ConcurrentWorker`。即使没有
设置默认超时策略，任务自身的 `timeout` 仍会通过 `TimeoutExecutor` 生效。

## 延迟调度

通过 `Task.scheduled_at` 指定首次执行的 Unix 到期时间（秒）：

```python
import time
from workflow_engine import Task, TaskEngine

engine = TaskEngine(max_workers=2)
task = engine.submit(Task(
    name="delayed-job", callable=abs, args=(-42,),
    scheduled_at=time.time() + 10,  # 十秒后才允许首次执行
))
engine.start()  # 等待到期并执行，返回时任务已完成
```

`scheduled_at=None`（默认）或已过去的时间立即就绪；时间必须是有限非负数，
不接受布尔值。延迟期间任务保持 `PENDING`，不消耗执行次数或重试预算，仍计入
`pending_count`。未到期的高优先级任务不会阻塞就绪任务；到期任务按优先级及
同级提交 FIFO 竞争执行机会，到期时间表示最早可执行时间，不保证准点开始。

同步 Worker、并发 Worker 和 WorkerLoop 共用延迟队列；等待不占用线程池槽位，
新任务入队和停止请求会唤醒等待。`start()` 会等待队列中延迟任务完成，停止后未执行
任务保留在队列。首次失败后的重试只使用 RetryPolicy 安排的 `retry_at`。

入队时根据 Unix 时间计算剩余延迟，进程内使用单调时钟等待；提交后修改
`scheduled_at` 不会重新安排已排队条目。SQLite 模式保存原始到期时间，重启后重新
计算剩余等待，并在原子领取时检查数据库中的时间。到期时间属于持久化任务定义，
同一 ID 使用不同到期时间重复提交会触发 `IdempotencyConflict`。旧库自动升级，
原有任务仍立即就绪。`SUBMITTED` 事件的 `details` 包含 `scheduled_at`。

四个独立测试类位于 `tests/test_delayed_scheduling.py`，覆盖参数校验、队列到期与
排序、执行与停止、持久化恢复及旧库升级。可单独运行：

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_delayed_scheduling.py -v
```

## 任务持久化与故障恢复

通过 `database_path` 开启 SQLite 持久化；省略该参数保留原有内存模式。
无需安装额外依赖。数据库目录须预先存在，并位于持久磁盘上；数据库和其
`-wal` / `-shm` 文件应放在同一目录，运行期间不要单独复制数据库文件作备份。
使用 WAL 和 `synchronous=FULL`，每次提交、领取、完成都在短事务中提交。
任务定义、状态、JSON 结果、异常摘要、执行次数和重试次数均保存到磁盘。

```python
from workflow_engine import RetryPolicy, Task, TaskEngine

engine = TaskEngine(
    database_path="tasks.sqlite3",
    task_registry={"absolute-value-v1": abs},
    retry_policy=RetryPolicy(max_retries=2),
    max_workers=4,
    # 服务器断电重启、确认所有旧 Worker 已停止后才开启：
    recover_interrupted=True,
)
task = engine.submit(Task(
    name="absolute-value", callable=abs, args=(-42,),
    id="request-2026-001",  # 同一业务请求始终使用同一个 ID
    idempotent=True,        # abs 可安全重复调用；有副作用的函数必须自行去重
))
engine.start()
saved = engine.get_task(task.id)
print(saved.status, saved.result, saved.attempt_count)
```

`task_registry` 将稳定名称映射到函数；重启后提供相同映射，不序列化函数、不使用
pickle、不从数据库动态导入代码。注册名称应随业务版本区分，不能更改已有名称
的业务语义。参数和结果必须是 JSON 类型，`args` 的最外层元组会转成 JSON 数组；
嵌套元组、任意对象、非字符串字典键和 NaN/Infinity 会被拒绝。异常恢复为
`StoredTaskError` 摘要，结果不确定异常恢复为 `UncertainExecutionError`。
无法序列化的返回值保存为失败并停止重试，避免重复调用已完成的业务。

幂等规则：

- `Task.id` 是数据库唯一键。相同 ID、相同定义的提交返回已存任务；若定义不同，
  抛出 `IdempotencyConflict`。终态任务不会再次进入执行队列。未指定 ID 时每次
  新建 Task 都会生成新 ID，所以请求重发必须复用业务键。
- 业务调用前，数据库原子领取将待执行状态改为 `RUNNING` 并记录执行次数。
  共享同一数据库的多个引擎中只有一个能领取成功；完成写入校验领取令牌和执行
  次数，过期执行器不能覆盖恢复后的结果。已提交定义是执行依据，修改内存对象
  不会改变持久化的业务调用。
- 成功结果或失败后的重试决策一次提交。即使在提交 `RETRYING` 后、内存入队前
  断电，重启仍会恢复该次重试。提交时保存有效重试预算，重启不会因默认策略
  改变而重置预算。未配置 RetryPolicy 时不安排重试。

恢复规则：

- 每次新建持久化引擎都会按优先级及同级 FIFO 恢复 `PENDING` / `RETRYING`。
  `SUCCESS` / `FAILED` 保留供查询，不会自动重新执行。
- 默认不接管 `RUNNING`，避免把另一个仍在运行的引擎误判为故障。确认共享该
  数据库的所有旧 Worker 已停止后，可使用 `recover_interrupted=True`，或独立
  调用 `SQLiteTaskStore.recover_interrupted()` 后创建引擎。
- 中断的非幂等任务转为 `FAILED`，错误为 `UncertainExecutionError`，需核对实际
  业务结果；声明 `idempotent=True` 且有剩余重试预算的任务进入 `RETRYING`，
  消耗一次预算。重复恢复不会重复消耗预算。
- `idempotent=True` 是业务承诺，并不会自动让任意副作用变成幂等。涉及支付、
  写入或外部 API 时，应将稳定业务键传入 `args` / `kwargs`，在业务数据库用唯一
  约束和事务去重，或传给支持幂等键的外部接口。执行结果提交前断电可能导致
  函数重放；平台不能保证任意外部操作恰好执行一次。
- 持久化模式的软超时会等待旧调用真正结束后才提交失败或重试，防止业务调用
  重叠；因此函数不退出时 `start()` 也不会立即返回。需要硬超时应采用进程隔离。

当前实现适用于共享本机 SQLite 文件的进程；多主机调度需后续增加服务器数据库
和明确的 Worker 存活协调机制。

四个独立测试类覆盖磁盘读写、重复提交与跨引擎竞争、子进程强制退出恢复，以及
结果提交失败和软超时边界：`TaskPersistenceTests`、`PersistentIdempotencyTests`、
`CrashRecoveryTests`、`PersistentFailureBoundaryTests`。

```powershell
$env:PYTHONPATH = "src"
python -m unittest discover -s tests -v
```

## 基础并发执行

`ConcurrentWorker` 是当前阶段唯一的并发模型：它在一次 `run()` 调用中创建固定
大小的进程内线程池，最多保留 `max_workers` 个已提交、尚未回收的任务尝试。
有几个空闲槽位就从队列取几个任务，任一尝试完成后立即回收并补位；其余任务留在
`TaskQueue` 中，避免在线程池内部积压。队列与在途尝试均为空时返回。不包含协程、分布式 Worker、
后台常驻服务或动态扩缩容。

```python
queue = TaskQueue()
worker = ConcurrentWorker(queue, Executor(), max_workers=4)
scheduler = Scheduler(queue, worker)
for number in range(10):
    scheduler.submit(Task(name=f"job-{number}", callable=run_job, args=(number,)))
completed = scheduler.start()
```

线程安全边界：`TaskQueue` 的入队、出队、去重与观察操作均受队列锁保护；每个
`Task` 有独立生命周期锁，`Executor` 在执行期间持有该锁，因此同一个任务不会被
同时执行两次，而不同任务不会彼此串行化。`run()` 的返回列表按提交到线程池的顺序
排列，任务实际开始与完成顺序不作保证。每次补位都重新选择队列中最高优先级任务，
相同优先级保持 FIFO，因此运行期间新提交的高优先级任务也能参与下一次槽位分配。
失败尝试完成后立即按策略重新入队，无需等待其他运行任务结束；重试保留原优先级，
同优先级时排在队尾，同一任务的两个执行器 attempt 不会重叠。软超时的业务代码
仍有下文“超时”章节所述的边界。

`ConcurrentWorker.stop()` 是非阻塞的优雅停止请求：不再取新任务，`run()` 等待
已提交的尝试全部结束后进入 `STOPPED` 并返回。未取出的任务和收尾期间产生的重试
保留在队列中，可由新的 Worker 接续处理。取任务与提交使用生命周期锁，与停止请求
保持互斥。该有界消费行为适用于 `ConcurrentWorker.run()` 及使用它的
`Scheduler.start()` / `TaskEngine.start()`；`WorkerLoop` 仍通过 `process_next()`
逐个同步处理任务。

## 后台 Worker Loop

`WorkerLoop` 为既有 `Worker` 提供一个常驻的后台线程。它只调用
`Worker.process_next()`，不导入或了解 `TaskEngine`；因此任务提交和引擎装配
仍可由调用方自由组合。空队列时 loop 阻塞等待任务；`enqueue()` 和 `stop()`
都会立即唤醒等待，不再依赖 `idle_wait` 轮询。停止是协作式的：已经开始的任务会
执行完毕，但不会开始下一项任务。

```python
queue = TaskQueue()
loop = WorkerLoop(Worker(queue, Executor()))
loop.start()
queue.enqueue(Task(name="background-job", callable=run_job))
# ...
if not loop.shutdown(timeout=5):
    # 当前任务仍在协作式收尾；可继续等待或记录超时。
    loop.join()
```

`WorkerLoop.status` 使用与状态变更相同的生命周期锁保护，状态为
`CREATED -> RUNNING -> STOPPING -> STOPPED`（空闲状态直接停止时为
`CREATED -> STOPPED`）。一个 loop 实例只能启动一次；`start()` 的并发调用中
恰有一个能完成该转换。

`shutdown(timeout)` 等同于先请求 `stop()`、再 `join(timeout)`：它不会中断已经
开始执行的任务，返回 `True` 表示该任务已完成、loop 已停止；返回 `False` 表示
超时时任务仍在运行。停止请求发出后尚未开始的任务会保留在 `TaskQueue`，可以交给
新的 Worker 继续处理。`WorkerLoop` 驱动的底层 `Worker` 使用同一生命周期，直接
调用底层 `Worker.stop()` 也会唤醒并结束对应的空闲 loop。

## 任务生命周期

```text
PENDING ──> RUNNING ──> SUCCESS
   ↑           │
   │           └─────> FAILED ──> RETRYING
   │                      ↑           │
   │                      └───────────┘
   └──────────────────────────────────┘
```

- `PENDING` / `RETRYING` 都是"等待被执行"，也是队列唯一接受的状态。
- `RETRYING` 表示已取到一次重试机会、等待重新执行；`FAILED` 与 `SUCCESS` 是终态。

### 生命周期不变量

引擎把以下条件当作运行边界上的硬约束，而不是调用方的约定：

- `PENDING` 只对应尚未开始的首次 attempt；已开始过的非重试任务不能再次
  入队或执行。
- 状态只能是 `TaskStatus` 的合法成员；执行器只接受 `PENDING` 和 `RETRYING`，
  因而 `SUCCESS`、`FAILED` 与 `RUNNING` 不会被重新执行。
- 每次重试必须先由 `RetryPolicy` 将失败任务改为 `RETRYING` 并递增
  `retry_count`；达到适用的 `max_retries` 后不再重新入队。
- 每个队列中同一 task id 最多有一个待执行条目，且队列只含 `PENDING` 或
  `RETRYING` 任务，避免同一 attempt 重复排队。
- 超时与其他失败走同一条失败 -> 重试链路；一次超时 attempt 最多消耗并产生
  一次重试。未请求停止时，`Worker.run()` / `ConcurrentWorker.run()` 返回时队列已清空，
  已处理任务均处于 `SUCCESS` 或 `FAILED`，不会遗留 `RUNNING`。

## 重试

重试链路是"失败 -> 判断剩余次数 -> 重新入队 -> 再执行"，职责划分如下：

| 组件 | 职责 |
| --- | --- |
| Task | 保存 `max_retries`、`retry_count`、`last_error` 和只读 `retry_at`，不执行重试 |
| RetryPolicy | 按任务级上限优先判断重试预算，记录次数并计算退避到期时间 |
| Worker | 编排：执行失败且策略允许时，把任务重新放回队尾 |
| TaskQueue | 到期的任务按优先级及 FIFO 出队，同一任务至多出现一次 |

对原有行为的影响：

- 队列在已到期的任务中优先取出 `priority` 数值更大的任务；相同优先级保持入队 FIFO。未到期的重试不阻塞其他任务，到期后保留原优先级参与调度。
- 每次重试都消耗一次策略允许的次数，`TaskQueue.size()`、`Worker.run()`、`Scheduler.start()` 仍在有限步内结束。
- `run()` / `start()` 返回的列表按每次尝试排列，同一个任务对象可能出现多次（每次尝试一项）。
- 不配置 `RetryPolicy` 时 Worker 行为与之前完全一致：失败任务停留在 `FAILED`。
- `Scheduler` 契约不变，仍只负责入队与启动 Worker；重试由 Worker 与策略完成。

### 重试退避

```python
policy = RetryPolicy(
    max_retries=5,
    initial_delay=0.5,  # 第一次重试等待 0.5 秒
    backoff_factor=2,  # 设置为 1 则使用固定间隔
    max_delay=4,       # 每次最多等待 4 秒
)
engine = TaskEngine(retry_policy=policy, max_workers=2)
```

第 n 次重试的延迟为 `min(initial_delay * backoff_factor ** (n - 1), max_delay)`，
上述配置依次等待 0.5、1、2、4、4 秒。默认 `initial_delay=0` 保持立即重试；
`backoff_factor` 默认 2、必须至少为 1，`max_delay` 默认 60 秒。延迟必须为有限非负
数，布尔值不作为数值接受。`policy.delay_for(n)` 可独立计算延迟，不消耗预算。

退避从失败被安排重试时开始，任务保持 `RETRYING`，`task.retry_at` 是只读 Unix
到期时间（立即重试为 `None`）。等待不会占用线程池执行槽位；同步、并发和常驻
Worker 均可在等待期间消费其他就绪任务。`dequeue()` / 默认 `process_next()` 只取
就绪任务，`dequeue_wait()` 等待到期；`size()` / `pending_count` 包含退避中的任务。
`run()` / `start()` 会等待已安排的重试完成，停止请求可唤醒等待并把任务留在队列。

SQLite 模式会在首次提交时保存退避配置，在安排重试时原子保存到期时间；重启
沿用原任务配置和剩余等待时间。旧数据库自动增加字段并保持立即重试。
`RETRY_SCHEDULED` 事件包含 `retry_delay` 和 `retry_at`，可用于查询重试计划。

## 超时

超时限制的是**一次尝试（attempt）**，不是任务生命周期的总时间：上限在每次执行
开始时重新计时，任务被重试多次时每次尝试都拿到完整的一份上限。平台当前不提供
"任务必须在某个时刻前全部完成"的总体截止时间。

| 组件 | 职责 |
| --- | --- |
| Task | 只保存 `timeout`（秒，`None` 表示由策略决定），不执行超时判定 |
| TimeoutPolicy | 只做取值：任务级上限优先、策略默认值兜底，两处都没有则不设上限 |
| TimeoutExecutor | 编排：把上限交给 `run_with_timeout`，其余生命周期沿用 Executor |
| run_with_timeout | 只做执行：限时运行一个可调用对象，到上限仍未结束就抛出 `TaskTimeoutError` |

超时的那次执行以 `FAILED` 结束，`task.error` 是 `TaskTimeoutError`（继承内置
`TimeoutError`）。它不消耗也不修改重试次数：是否重试仍由 `RetryPolicy` 判断，
超时任务被重试时，下一次尝试重新获得完整上限。超时因此只是"一次失败"，不是一种
特殊终态。

接线方式：

```python
queue = TaskQueue()
worker = Worker(queue, TimeoutExecutor(TimeoutPolicy(timeout=30)), RetryPolicy(max_retries=3))
Scheduler(queue, worker).submit(Task(name="call-model", callable=call_model, timeout=5))
```

上面的任务每次执行最多 5 秒（任务级上限覆盖策略默认的 30 秒），最多执行 4 次
（首次 + 3 次重试），每次都是独立的 5 秒。

实现与边界：超时用"独立守护线程 + 限时等待"实现，属于**软超时**。到上限时调用方
立即返回并把这次尝试判为失败，但已经开始的可调用对象不会被强行终止，它在后台
继续运行。若重试策略立即发起 attempt B，则尚未结束的 attempt A 与 B 的业务代码
**可能并行运行**；调用方必须让业务操作具备幂等性或自行支持协作式取消。A 后续的
返回值或异常只留在 A 的内部结果槽，绝不会再写回 `Task`，因此最终
`Task.status/result/error` 保留 B 的结果。因此：

- 平台保证"这一次执行被判失败"，不保证"这次执行已经停止"，例如长阻塞的 I/O 仍会
  占用线程直到自己结束。
- 平台保证旧 attempt 不会覆盖新 attempt 的任务状态；不保证两个 attempt 对外部系统
  （数据库、文件、HTTP 服务等）的副作用彼此隔离。
- 要在超时点真正终止执行，需要进程隔离与 Worker 强杀，属于后续阶段的能力；分布式
  超时同样不在当前范围内。
- 上限是"至少给到"的：判定超时只发生在上限时刻之后，刚好用完上限的执行不会被误判。

对原有行为的影响：

- 不设上限时 `TimeoutExecutor` 不引入额外线程，执行方式与 `Executor` 完全一致，
  既有任务的行为不变。
- `Executor` 的状态流转不变，只把"如何调用可调用对象"抽成 `_invoke` 扩展点。
- `RetryPolicy`、`TaskQueue`、`Worker`、`Scheduler` 的职责与契约都不变：重试链路
  不需要感知超时，超时只是一种普通失败。

## 任务执行事件记录

任务当前状态继续保存在 `Task.status/result/error` 中，同时新增只追加的历史记录。
`Task.events` 返回不可修改的事件快照；`TaskEngine.get_events(task_id)` 按任务内序号
读取历史，`after_sequence` 可增量读取。内存模式的历史随进程保存，SQLite 模式的
历史持久化到新增的 `task_events` 表，重启、重复提交和重新加载不会生成重复事件。

```python
from workflow_engine import Task, TaskEngine, RetryPolicy

engine = TaskEngine(
    database_path="tasks.db",
    task_registry={"abs": abs},
    retry_policy=RetryPolicy(2),
)
task = engine.submit(Task("absolute", abs, args=(-7,)))
engine.start()
for event in engine.get_events(task.id):
    print(event.sequence, event.timestamp, event.event_type.value,
          event.previous_status, event.status, event.attempt_count,
          event.retry_count, event.details)
new_events = engine.get_events(task.id, after_sequence=2)
```

| 事件 | 含义 |
| --- | --- |
| `CREATED` | 任务创建，持久化时保留原始创建时间 |
| `SUBMITTED` | 首次提交被接受（内存队列或数据库） |
| `STARTED` | 一次尝试开始；持久化模式已成功获得执行所有权 |
| `SUCCEEDED` | 本次尝试成功 |
| `FAILED` | 本次尝试失败，保留异常类型和消息；超时还记录上限及实际等待时间 |
| `RETRY_SCHEDULED` | 消耗一次重试预算，进入等待重试状态 |
| `INTERRUPTED` | 显式恢复发现执行中断，结果不确定；后续可重试或停留在失败状态 |
| `RESTORED` | 从已有状态建立历史起点，旧库无法补回过去的执行过程 |

例如失败后重试成功的历史为：`CREATED → SUBMITTED → STARTED → FAILED →
RETRY_SCHEDULED → STARTED → SUCCEEDED`。失败事件保留当时的错误和计数，不会被
后续成功覆盖。每个事件带有 UTC 时间、前后状态、尝试次数和重试次数；任务内序号
决定事件顺序，系统时钟调整不会改变查询顺序。`details` 每次返回独立副本。
SQLite 成功事件保存 JSON 结果，内存模式为兼容任意 Python 返回值保存 `result_repr`。

SQLite 的状态更新和对应事件写入共用一个事务：失败和重试决定同时提交，事件写入
失败会回滚状态更新，过期执行器无法追加完成事件。`get_task()` 与事件查询读取的
是已提交历史，持久化任务的 `Task.events` 也是最近一次同步的已提交快照。
事件标记引擎生命周期，不记录业务函数内部的每条语句或后台软超时线程的后续执行。
持久化软超时仍等待旧调用结束后提交失败事件，保持现有的避免重试重叠语义。

旧数据库首次打开时会自动建立事件表，并为已有任务写入一个 `RESTORED` 事件，
其中 `history_available=False` 明确表示历史缺失；以后发生的事件正常追加。
内存引擎拒绝用不同 Task 对象复用已经提交过的 id，避免替换并丢失已有历史。

独立测试类位于 `tests/test_task_events.py` 和 `tests/test_persistent_events.py`，
覆盖事件不可变性、执行与重试、超时、重启与幂等提交、事务回滚与并发竞争、中断恢复、
旧库升级和真实子进程崩溃。可单独运行：

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p '*events.py' -v
```

## 待确定

- 原语清单中其余原语与各自语义
- 任务与工作流的持久化模型
- 调度策略与 Worker 通信协议
- 重试退避与跨进程恢复策略
- 硬超时：在超时点终止执行所需的进程隔离、Worker 强杀与分布式超时
- 任务级总体截止时间，以及它与"单次尝试超时"的叠加规则

## 目录

| 路径 | 用途 |
| --- | --- |
| `src/` | 平台实现，按 src 布局组织 |
| `src/workflow_engine/` | 平台源码包，模块名 `workflow_engine` |
| `pyproject.toml` | 打包配置，声明 `src` 为源码根 |
| `referrences/` | 参考资料 |

## 运行

源码位于 `src/` 下，需把 `src` 放进导入路径，或在仓库根目录做一次可编辑安装：

```bash
pip install -e .
```

未安装时，临时导入方式：

```bash
PYTHONPATH=src python -c "from workflow_engine import Task"
```

PowerShell 下运行全部测试：

```powershell
$env:PYTHONPATH = 'src'
python -m pytest tests -q
```

`tests/test_bounded_consumption.py` 提供四个独立测试类，分别验证有界取任务与立即
补位、优先级与 FIFO、失败重试、优雅关闭。可单独运行：

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_bounded_consumption.py -v
```
