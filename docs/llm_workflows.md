# 真实 LLM API 与 DAG 工作流

`llm.chat.v1` 是内置的文本生成处理器。节点通过 HTTP 调用 OpenAI 兼容的
Chat Completions API，回答和供应商返回的 token 用量通过既有任务事务保存到
SQLite。支持本地 Worker、RemoteWorker、管理 API 提交的 DAG 和重启后查询。
实现只使用 Python 标准库，无需安装模型 SDK。

## 配置与直接运行

在仓库根目录打开 PowerShell，设置执行进程的环境变量：

```powershell
$env:PYTHONPATH = 'src'
$env:LLM_API_KEY = '<你的 API Key>'
$env:LLM_BASE_URL = 'https://api.openai.com/v1'
$env:LLM_MODEL = '<供应商提供的模型名称>'
$env:LLM_TIMEOUT = '60'

python -m workflow_engine.llm_cli --database llm.sqlite3 --prompt '用三句话解释 DAG 工作流'
python -m workflow_engine.llm_cli --database llm.sqlite3 --workflow examples/llm_workflow.json
```

安装项目后也可以使用 `workflow-llm` 命令。成功退出码为 0，节点执行失败为 1，
命令行或配置错误为 2；标准输出包含工作流状态、节点回答和用量的 JSON。
`--base-url`、`--model` 可以覆盖环境变量；`--max-workers` 设置本地并发数，
`--max-retries` 默认为 0。每次 `--prompt` 创建新工作流；JSON 文件中的固定 ID
支持重启接续和重复查询，已成功的节点不会再次调用模型。修改定义后要使用新的 ID。

| 配置 | 含义 |
| --- | --- |
| `LLM_API_KEY` | 必填，执行进程使用的 Bearer API Key |
| `LLM_BASE_URL` | API 根地址，默认 `https://api.openai.com/v1` |
| `LLM_MODEL` | 必填，默认模型名；允许节点覆盖 |
| `LLM_TIMEOUT` | 正数秒，HTTP 连接和读操作超时，默认 60 |

Base URL 应包含供应商要求的版本或兼容路径，例如 `/v1`；处理器追加
`/chat/completions`。也接受以 `/chat/completions` 结尾的完整接口地址，配置时会
归一化为根地址，避免重复追加路径。API Key 不得放入 URL。
每个 `LLM_*` 字段不存在时，回退读取对应的 `APP_LLM_*` 字段；显式设置的
`LLM_*` 优先，包括空字符串，空值会报配置错误，不会悄悄切换到备用配置。
远程服务应使用 HTTPS；本机兼容服务也支持 HTTP。
不自动读取 `.env` 文件；使用环境变量、部署系统的 secret 注入或下方的 Python 配置。
HTTP 客户端使用系统代理配置，且不跟随重定向，避免将凭据转发到其他地址。

## Python 接入和上下游引用

```python
from workflow_engine import (
    DAGNode, DAGWorkflow, LLMConfig, LLM_HANDLER, OutputRef,
    TaskEngine, WorkflowOrchestrator, create_llm_registry,
)

config = LLMConfig.from_env()
# 也支持应用注入：
# config = LLMConfig(api_key=secret, base_url="https://provider.example/v1", model="my-model")

engine = TaskEngine(
    database_path="llm.sqlite3",
    task_registry=create_llm_registry(config),
    max_workers=2,
)
orchestrator = WorkflowOrchestrator(engine)
workflow = DAGWorkflow((
    DAGNode("draft", LLM_HANDLER, kwargs={"prompt": "写一份三点实施计划"}),
    DAGNode("review", LLM_HANDLER,
            kwargs={"system_prompt": "审查这份计划并改进它"},
            inputs={"prompt": OutputRef("draft", ("text",))}),
))
orchestrator.submit(workflow)
run = orchestrator.run(workflow.id)
print(run.status)
for node in run.nodes.values():
    print(node.id, node.status, node.result, node.error)
```

`create_llm_registry()` 注册 `llm.chat.v1`（模型调用）和 `llm.validate.v1`（输出校验）。
可以与自己的检索、工具、校验函数注册表合并。不同供应商可分别构造
`OpenAIChatHandler(LLMConfig(...))`，以不同稳定名称注册到同一引擎。
相同名称在各 Worker 上应使用相同配置；重新配置模型或供应商应为新任务使用新的
注册名或明确的节点 `model`，避免待执行任务在重启后意外切换模型。

节点 kwargs 支持：

| 参数 | 用途 |
| --- | --- |
| `prompt` | 用户文本，可通过 `OutputRef` 接收上游 `text` |
| `system_prompt` | 可选系统指令，与 `prompt` 配合 |
| `messages` | 替代 prompt 的文本消息数组；不能与上述两个参数混用 |
| `model` | 可选，覆盖配置中的模型名 |
| `temperature` | 可选，0 至 2；默认不发送，由供应商决定 |
| `max_completion_tokens` | 可选，正整数输出 token 上限 |
| `max_tokens` | 兼容旧接口的上限参数，与 `max_completion_tokens` 二选一 |

`messages` 每项只包含 `role` 和字符串 `content`；支持 `system`、`developer`、
`user`、`assistant`。本阶段提供非流式文本调用，不包括图片、工具调用或自动执行
模型生成的代码。参数是否受某一模型支持仍由供应商决定。
协议参考：[OpenAI Chat Completions API](https://developers.openai.com/api/reference/resources/chat)。

## 保存和查询的结果

每个成功节点的 `result` 结构如下（数值仅为格式示例）：

```json
{
  "text": "模型生成的回答",
  "model": "provider-reported-model",
  "requested_model": "configured-model",
  "response_id": "chatcmpl-example",
  "finish_reason": "stop",
  "usage": {
    "prompt_tokens": 100,
    "completion_tokens": 40,
    "total_tokens": 140,
    "prompt_tokens_details": {"cached_tokens": 20}
  }
}
```

供应商未返回的计数保存为 `null`，不估算、不记为 0；额外的缓存／推理 token
明细按原值保存。`model` 为供应商报告的模型，未报告时使用请求的模型。
`finish_reason` 可用于识别 `length` 等截断情况，截断文本仍会保存。

工作流节点、对应任务的 `result` 和 `SUCCEEDED` 事件中的 `details.result` 使用
同一个已提交结果，成功结果与任务状态原子写入。可以使用：

- `engine.get_workflow(workflow_id).nodes[node_id].result`
- `engine.get_task(task_id).result`
- `engine.get_events(task_id)`
- 管理 API 的 `GET /api/v1/workflows/{id}`、`GET /api/v1/tasks/{id}` 及事件接口

API Key 保留在处理器配置中，不作为 DAG kwargs、数据库字段或 HTTP 错误消息保存。
配置的 `repr` 隐藏 Key。模型输入和回答属于工作流数据，会持久化；不要将凭据写进
prompt、messages 或工作流 JSON。

## 模型输出校验

HTTP 200 只说明收到响应。对完整性、固定标识或数字格式有要求时，在模型后增加
`llm.validate.v1` 节点，再让业务节点引用校验节点的输出：

```python
DAGNode("checked", "llm.validate.v1",
        kwargs={"pattern": r"[0-9]+", "expected_text": "42"},
        inputs={"result": OutputRef("generate")})
```

校验节点不调用模型，成功时原样返回整个结果，保留响应 ID、文本和 token 用量。
统计模型用量时只汇总生成节点，或按响应 ID 去重，不要重复计算校验节点复制的用量。
它默认拒绝空白回答及 `finish_reason != "stop"` 的结果，包括被 token 上限截断的回答。
`expected_text` 使用逐字符匹配，`pattern` 使用完整正则匹配；二者可分别使用，也可同时使用。
不会自动删除空白、补前缀、修复文本或再次请求模型。需要容许尾部空白时，应在正则中明确表达。
供应商不报告结束原因时，可显式 `require_stop=False`，同时配置业务需要的文本约束。

校验失败抛出脱敏的 `LLMOutputError`，不会把不匹配的文本或校验规则写进错误消息。
上游模型节点保持 SUCCESS，已收到的回答及用量继续保存在数据库中；
下游受依赖门控制。管理服务开启恢复协调时，失败会产生事故并暂停工作流，
需按事故处理流程决定后续操作。重跑校验节点不需要再次生成上游已成功的模型结果。
完整示例见 [`examples/llm_validated_workflow.json`](../examples/llm_validated_workflow.json)。

## 管理服务和远程 Worker

已有管理服务可直接加载环境配置工厂：

```powershell
$env:WORKFLOW_API_TOKEN = '<管理服务凭据>'
python -m workflow_engine --database llm.sqlite3 --registry workflow_engine.llm:create_llm_registry
```

管理服务只推进和查询工作流，还需要启动 Worker。单机共享本地数据库时，用相同
registry 创建 `TaskEngine`，再运行 `engine.create_worker('llm-worker').start()`。
跨主机时，在中心启动 `engine.create_recovery_service(rpc_token)`，在远端配置
LLM 环境变量并构造：

```python
from workflow_engine import RemoteWorker, create_llm_registry

worker = RemoteWorker(
    server_url, create_llm_registry(), rpc_token, "llm-worker-1",
)
worker.start()
# 保持进程存活；退出时调用 worker.shutdown()。
```

中心和远端须注册相同的 handler 名称，工厂现在包含上述两个 handler；升级时两端应一起更新。
SQLite 留在中心。中心若只提供调度服务，可以为每个名称分别注册拒绝本地执行的占位函数，
无需配置 LLM 凭据；真正的
`OpenAIChatHandler` 和 API Key 只需存在于执行模型调用的 Worker 中。

## 失败、重试与计量边界

LLM 失败分为三类，公开枚举为 `FailureCategory`。即使配置了 `RetryPolicy` 或
CLI `--max-retries`，也只自动重试明确的临时性失败：

| 分类 | 故障示例 | 自动处理 |
| --- | --- | --- |
| `permanent` 永久性错误 | 非法参数；HTTP 400、401、403、404、413、422；重定向；501、505；429 的额度耗尽；证书验证失败、访问被禁止、无法解析的域名；输出契约不匹配 | 直接失败，不消耗重试次数 |
| `transient` 临时性错误 | 普通 HTTP 429 限流、503 服务不可用；DNS `EAI_AGAIN`；连接被拒绝（尚未发送请求） | 使用现有指数退避和任务／引擎次数预算 |
| `uncertain` 执行结果不确定 | HTTP 408、500、502、504 及其他未明确分类的响应；请求超时、连接重置、通用 TLS 故障、读取中断；HTTP 200 后响应非法或超限；任务软超时；进程在提交结果前中断 | 停止自动重试，核实供应商结果后走人工恢复流程 |

429 响应只在内存中限量检查 `error.code` / `error.type`，将 `insufficient_quota`、
`billing_hard_limit_reached`、`billing_not_active`、`credit_balance_exhausted` 识别为
永久性错误。HTTP 错误消息仅包含状态码；底层网络异常、响应体和供应商消息不会写入
错误或事件。已开始读取成功响应后的传输故障一律按结果不确定处理。
对于兼容供应商，没有其明确的未执行证明时，500／网关错误采用保守策略，避免重复生成。

处理器自身不重试，一次 attempt 至多发起一次模型请求。分类和 `retryable` 标志保存到
任务错误、`last_error` 和失败事件，经过 SQLite 重启与远程 RPC 仍保留。
`retryable` 表示该类故障可安全自动重试，实际是否调度还取决于次数预算；耗尽后仍停止。
旧远程 Worker 未提供分类时，LLM 未知故障停止自动重试；升级时建议中心和 Worker 一起更新。
非 LLM 业务任务沿用原有重试行为。下游跳过与事故处理沿用既有 DAG 机制。

`LLMPermanentError`、`LLMTransientError` 可用于自定义模型处理器；`LLMError`、
`LLMTimeoutError`、`LLMResponseError` 默认表示不确定结果，`LLMOutputError` 表示永久失败。
内置处理器使用其他注册名时仍保留分类。对 LLM，即使设置 `idempotent=True`，也不会让
不确定结果自动重放。人工恢复仍要求现有的操作人、原因、幂等性与次数预算检查；
非幂等调用应核实外部结果后通过 `succeed` 提交已确认结果，或选择 `fail`。

LLM 请求可能收费且输出不确定，因此示例保持 `idempotent=False`。
客户端超时、连接中断或结果提交前崩溃时，供应商可能已生成并计费；引擎无法保证
外部请求恰好一次。只有收到且成功提交的响应拥有可确认的 token 用量，失败尝试
的未知用量不包含在成功节点计数中，不能把这些计数当作完整账单。
客户端超时限制连接／读操作，并非整个工作流的硬截止时间；DAG `timeout` 仍沿用
既有软超时语义。

## 验证

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_llm.py -v
python -m unittest discover -s tests -p test_llm_failure_retry.py -v
```

测试启动本机 HTTP 供应商模拟服务，实际经过 HTTP 客户端、DAG Worker、SQLite、
远程 RPC 和管理 API，覆盖凭据与请求参数、上下游回答传递、用量缺失、错误脱敏、
超时、重试、重启去重和 CLI。测试不消耗外部模型额度；供应商实测需配置自己的 Key、
Base URL 和模型后运行上述 CLI。
独立的 `test_llm_failure_retry.py` 进一步验证三类失败、额度耗尽与限流区分、
实际 HTTP 调用次数、预算与退避、软超时、进程中断隔离、持久化与远程分类传递、
人工恢复及非 LLM 重试兼容性。

需要验证真实供应商、管理 HTTP、两个独立 RemoteWorker 进程、并行 DAG、供应商拒绝、
事故解决及服务重启去重时，可显式运行：

```powershell
$env:PYTHONPATH = 'src'
python tools/validate_real_llm.py
```

该工具直接使用产品的配置解析与归一化逻辑，并在 CLI 和远程 DAG 中执行输出校验节点。
它会实际调用模型，预期 5 次成功调用及 1 次不存在模型的拒绝，
不自动重试。每轮在新的 `.artifacts/real-llm-*` 目录保留 JSON、SQLite 和调用记录；
`--output` 可指定尚不存在的输出目录。默认测试不运行它。
实测结果及限制见 [2026-10-15 真实 LLM 验证报告](real-llm-validation-2026-10-15.md)。
