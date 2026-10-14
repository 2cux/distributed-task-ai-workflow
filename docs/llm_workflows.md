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
`/chat/completions`。不要把完整 endpoint 或 API Key 放入 URL。
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

`create_llm_registry()` 返回 `{ "llm.chat.v1": OpenAIChatHandler(config) }`。
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

中心和远端须注册相同的 handler 名称，SQLite 留在中心。中心若只提供调度服务，
可以为该名称注册一个拒绝本地执行的占位函数，无需配置 LLM 凭据；真正的
`OpenAIChatHandler` 和 API Key 只需存在于执行模型调用的 Worker 中。

## 失败、重试与计量边界

HTTP 失败只持久化状态码；网络错误、客户端超时和非法响应使用脱敏错误。
节点失败后的下游跳过、退避、次数限制均沿用现有 DAG 机制。处理器自身不重试，
一次节点 attempt 只发起一次模型请求。若显式配置 `RetryPolicy` 或 CLI
`--max-retries`，当前引擎对这些失败统一按预算重试，包括 401 等配置错误。
不设置自动重试可避免配置错误反复请求；修正配置后按已有恢复流程处理任务。

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
```

测试启动本机 HTTP 供应商模拟服务，实际经过 HTTP 客户端、DAG Worker、SQLite、
远程 RPC 和管理 API，覆盖凭据与请求参数、上下游回答传递、用量缺失、错误脱敏、
超时、重试、重启去重和 CLI。测试不消耗外部模型额度；供应商实测需配置自己的 Key、
Base URL 和模型后运行上述 CLI。
