# 端到端集成测试与修复记录（2026-10-21）

本轮在已有未提交改动上执行测试与修复，保留原有工作。最终全量回归为
**563 passed、4 skipped、0 failed**，耗时 149.33 秒。新增 5 项测试，包含本地与
远程执行子场景。4 项跳过均为 Docker 容器隔离测试，原因见下文。

后续已恢复 Docker Engine 并补测此前跳过的 4 项：**4 passed、0 skipped、0 failed**，
耗时 84.69 秒。结合此前通过的 563 项，全部 567 个用例现已分两批执行通过。

## 测试范围

| 范围 | 已执行的主要场景 |
| --- | --- |
| 外部入口 | 管理 HTTP API 认证、提交、查询、分页、暂停/恢复、审计、健康探针，以及独立 CLI 子进程 |
| 核心编排 | DAG 并行与汇合、嵌套输入绑定、优先级、延迟执行、重试退避、超时、失败后代跳过、独立分支继续执行 |
| Worker 与恢复 | 多 Worker、多进程竞争、心跳与撤权、网络断连、进程强制退出、可信停止后接管、旧会话结果拒绝 |
| 持久化 | 并发重复提交、定义冲突、事务回滚、事件一致性、重启后读取与不重放、SQLite 完整性 |
| AI 与外部工具 | HTTP 上下文获取 → LLM 规划 → JSON 解析 → 工具调用 → LLM 汇总；调用/token 预算、用量汇总、凭据隔离、不确定写入不重放 |
| 边界与异常 | 非法定义、数值范围、未知 handler/tool、路径与索引错误、大小限制、截断响应、失败分类、重复关闭 |

使用真实 SQLite、回环 HTTP 服务及独立 Python 进程。LLM 协议由本地确定性供应商
夹具提供，没有调用收费模型 API。

## 已复现并修复的问题

| 问题 | 原行为与影响 | 修复及验证 |
| --- | --- | --- |
| LLM HTTP 正文截断误判成功 | `read(limit)` 返回的短正文如果恰好仍为合法 JSON，节点会成功并放行下游，未核实响应完整性 | 在读取前保存预期正文长度并核对实际字节数；不符时以 `LLMResponseError` / `uncertain` 失败。本地、远程均验证只调用一次、下游无副作用、健康分支完成、用量未知且重启不重放 |
| DAG 反序列化错误转换容器 | 字符串参数/路径可被拆成字符数组，部分错误对象被转换；`inputs=[]` 还导致 CLI 输出 `AttributeError` 堆栈并以 1 退出 | 对工作流、节点、引用和绑定统一检查对象字段与数组类型，在构造前拒绝错误输入；CLI 正常以配置错误码 2 退出且不创建数据库 |
| 已停止远程 Worker 重复关闭仍依赖中心 | Worker 正常关闭、中心服务下线后，再次 `shutdown()` 仍发送停止 RPC，抛出 `RemoteTransportError` | `STOPPED` 状态的 `stop()` 直接返回；验证重复 `shutdown()`/`stop()` 成功、`last_error` 保持为空、持久化会话不变 |

主要实现位于 `src/workflow_engine/llm.py`、`dag.py`、`reliable_worker.py`。
新增回归位于 `tests/test_end_to_end_fault_boundaries.py`。LLM HTTP 夹具允许显式设置
Content-Length，旧传输重试测试的模拟响应也补齐了正文长度字段。

`from_dict()` 现在要求实际 JSON 容器类型并拒绝未知字段；`to_dict()` 的有效序列化
结构不变。Python 构造函数中的元组参数约定保持不变。

复核工具文档后保留了空 HTTP 200 正文映射为 `json=null` 的既有契约，并增加
GET/POST 不重复执行的兼容性验证；同时验证空 204/205、HEAD 和空文本响应。

## 回归结果与证据

| 验证 | 结果 | 证据 |
| --- | --- | --- |
| 修改前完整基线 | 558 passed，4 skipped | `.artifacts/e2e-2026-10-21-baseline.xml` |
| 核心链路与 Worker 定向回归 | 93 passed | `.artifacts/e2e-2026-10-21-targeted.xml` |
| 最终故障边界及 HTTP 兼容性 | 19 passed | `.artifacts/e2e-2026-10-21-boundaries.xml` |
| 失败分类、重试及新增故障用例 | 25 passed | `.artifacts/e2e-2026-10-21-failure-policies.xml` |
| 最终完整回归 | 563 passed，4 skipped，149.33 秒 | `.artifacts/e2e-2026-10-21-final.xml`、同名 `.log` |
| 后续容器补测 | 4 passed，0 skipped，84.69 秒 | `.artifacts/e2e-2026-10-21-containers.xml`、同名 `.log` |
| 管理 API 混合负载 | 预热 1 轮、测量 2 轮全部通过 | `.artifacts/e2e-2026-10-21-management.json`、同名 `.md` |
| AI 工作流 smoke 门禁 | 并发 1 / 4 全部通过 | `.artifacts/e2e-2026-10-21-ai-smoke.json`、同名 `.md` |
| 改动格式检查 | `git diff --check` 通过 | 无空白错误 |

各定向测试集合有重叠，不应相加作为总测试数。最终完整回归是在所有实现与测试
夹具修正后重新执行的结果。证据目录 `.artifacts/` 按仓库现有规则不纳入 Git。

## 负载验证

管理 API：每轮 100 个两节点 DAG、300 次读取、8 个并发客户端、4 个 Worker，
预热 1 轮后单独测量 2 轮。测量合计完成 200 个工作流、800 次 HTTP 请求、
400 次任务执行，HTTP 错误及重试均为 0。每个节点结果、单次执行及数据库完整性
均通过校验；两轮 HTTP P95 分别为 387.121 ms、563.081 ms。

AI 工作流：使用仓库 `benchmarks/ai-smoke.json`，并发 1 与 4 各持续接收 3 秒，
随后排空，共完成 12 / 40 个工作流。两组 HTTP 及工作流失败率均为 0，busy 错误
为 0；工作流 P95 为 368.024 / 418.897 ms，全部通过预设门禁。

这两组负载在完整回归结束后顺序执行，没有与全量测试并发。AI 门禁采用配置中的
绝对阈值，未提供历史性能基线；短期本机负载结果不代表生产模型推理性能或 SLO。

## 复现方式

环境：Windows、Python 3.12.10、pytest 8.3.0、SQLite 3.49.1。
测试需要允许本机回环网络及子进程。

```powershell
$env:PYTHONPATH = 'src'
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD = '1'
$env:PYTHONIOENCODING = 'utf-8'
python -u -m pytest tests -q --tb=short --junitxml=.artifacts/e2e-2026-10-21-final.xml
python -u tools/benchmark_management.py --repeat 2 --warmup 1 --output .artifacts/e2e-2026-10-21-management.json --markdown .artifacts/e2e-2026-10-21-management.md
python -u tools/benchmark_ai_workflow.py --profile benchmarks/ai-smoke.json --output .artifacts/e2e-2026-10-21-ai-smoke.json --markdown .artifacts/e2e-2026-10-21-ai-smoke.md

# 容器故障测试（Docker Engine 就绪后）
$env:WORKFLOW_CONTAINER_TESTS = '1'
$env:WORKFLOW_DOCKER = 'C:/Program Files/Docker/Docker/resources/bin/docker.exe'
python -u -m pytest tests/test_deployment_containers.py -v --tb=short --junitxml=.artifacts/e2e-2026-10-21-containers.xml
```

## Docker 启动恢复与容器补测

首次全量回归时 Docker Engine 未启动，无法连接 `dockerDesktopLinuxEngine` 命名管道。
后续启动 Docker Desktop 时发现两个 10 月 18 日遗留的 Windows Unix 套接字无法移除，
分别阻塞 Inference Manager 和 Secrets Engine 初始化。

停止 Docker 后，逐项核实 `Docker/run` 和 `docker-secrets-engine` 只含零字节
套接字，再将运行时目录原地改名备份并重建，恢复 Engine 29.1.2。未重置 Docker，
未删除既有镜像、容器、卷或业务数据。备份保留在本机 LocalAppData：

- `Docker/run.e2e-backup-20261021-3dc8ed29`
- `Docker/run.e2e-backup-20261021-fd531c32`
- `docker-secrets-engine.e2e-backup-20261021-231e889f`

`tests/test_deployment_containers.py` 显式启用后，以下 4 项全部通过：

| 用例 | 结果 |
| --- | --- |
| 非幂等任务崩溃后不重放 | PASS |
| 旧会话在替换后不能提交结果 | PASS |
| 真实容器网络断连与重连 | PASS |
| SIGKILL 后的监督恢复 | PASS |

每项使用独立 Compose 项目，验证不同内核网络命名空间、可信退出依据、恢复次数、
旧结果拒绝、中心重启后的结果与事件保持。4 项测试的收尾均完成；补测没有发现新的
项目代码缺陷。

## 仍未覆盖的环境

本轮未验证物理跨主机部署、真实供应商模型行为或长时间 soak；相关结论不包含
这些环境。
