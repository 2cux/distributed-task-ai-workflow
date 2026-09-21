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
| TaskQueue | 进程内 FIFO 待执行任务队列 | 已实现 |
| Worker | 从队列取出任务并同步交给执行器 | 已实现 |
| Scheduler | 接收任务、入队并启动 Worker 执行当前队列 | 已实现 |
| RetryPolicy | 判断失败任务是否还有剩余重试次数 | 已实现 |

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

## 重试

重试链路是"失败 -> 判断剩余次数 -> 重新入队 -> 再执行"，职责划分如下：

| 组件 | 职责 |
| --- | --- |
| Task | 只保存重试信息：`max_retries`、`retry_count`、`last_error`，不执行重试 |
| RetryPolicy | 只做决策：按任务级上限优先、策略默认值兜底的规则判断是否重试，并记录重试次数 |
| Worker | 编排：执行失败且策略允许时，把任务重新放回队尾 |
| TaskQueue | 只接受 `PENDING` / `RETRYING` 任务，且同一任务至多出现一次 |

对原有行为的影响：

- `PENDING` 任务重新入队发生在队尾，因此 FIFO 顺序不被插队破坏，其他任务也不会被失败任务阻塞。
- 每次重试都消耗一次策略允许的次数，`TaskQueue.size()`、`Worker.run()`、`Scheduler.start()` 仍在有限步内结束。
- `run()` / `start()` 返回的列表按每次尝试排列，同一个任务对象可能出现多次（每次尝试一项）。
- 不配置 `RetryPolicy` 时 Worker 行为与之前完全一致：失败任务停留在 `FAILED`。
- `Scheduler` 契约不变，仍只负责入队与启动 Worker；重试由 Worker 与策略完成。

## 待确定

- 原语清单中其余原语与各自语义
- 任务与工作流的持久化模型
- 调度策略与 Worker 通信协议
- 重试退避、超时与跨进程恢复策略

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
