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
| Scheduler | 接收任务、入队并启动 Worker 执行当前队列 | 已实现 |
| RetryPolicy | 判断失败任务是否还有剩余重试次数 | 已实现 |
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

## 基础并发执行

`ConcurrentWorker` 是当前阶段唯一的并发模型：它在一次 `run()` 调用中创建固定
大小的进程内线程池，最多同时运行 `max_workers` 个不同任务；调用会处理已取出的
任务及其重试，直到一次取批操作观察到队列为空后返回。不包含协程、分布式 Worker、
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
排列，任务实际完成顺序不作保证。失败重试在一批任务均结束后进入下一批，避免同一
任务的两个 attempt 重叠。

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
  一次重试。`Worker.run()` / `ConcurrentWorker.run()` 正常返回时，队列已清空，
  已处理任务均处于 `SUCCESS` 或 `FAILED`，不会遗留 `RUNNING`。

## 重试

重试链路是"失败 -> 判断剩余次数 -> 重新入队 -> 再执行"，职责划分如下：

| 组件 | 职责 |
| --- | --- |
| Task | 只保存重试信息：`max_retries`、`retry_count`、`last_error`，不执行重试 |
| RetryPolicy | 只做决策：按任务级上限优先、策略默认值兜底的规则判断是否重试，并记录重试次数 |
| Worker | 编排：执行失败且策略允许时，把任务重新放回队尾 |
| TaskQueue | 只接受 `PENDING` / `RETRYING` 任务，且同一任务至多出现一次 |

对原有行为的影响：

- 队列始终优先取出 `priority` 数值更大的任务；相同优先级保持 FIFO。重试会保留任务原优先级，因此高优先级任务的重试仍会先于较低优先级的待执行任务；同优先级的重试进入同级队尾，其他同级任务不会被它阻塞。
- 每次重试都消耗一次策略允许的次数，`TaskQueue.size()`、`Worker.run()`、`Scheduler.start()` 仍在有限步内结束。
- `run()` / `start()` 返回的列表按每次尝试排列，同一个任务对象可能出现多次（每次尝试一项）。
- 不配置 `RetryPolicy` 时 Worker 行为与之前完全一致：失败任务停留在 `FAILED`。
- `Scheduler` 契约不变，仍只负责入队与启动 Worker；重试由 Worker 与策略完成。

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
