# DAG、LLM 与 HTTP 工具协同工作流

`create_workflow_registry()` 将已有 LLM、HTTP 和工具执行器接入同一个任务注册表。
工作流继续由 `WorkflowOrchestrator → TaskEngine → Worker / RemoteWorker` 执行，
沿用依赖放行、参数绑定、持久化、重试、恢复和 LLM 预算，没有新增运行依赖。

## 统一入口

```python
from workflow_engine import (
    HTTPConfig, LLMConfig, TaskEngine, WorkflowOrchestrator,
    ToolRegistry, create_workflow_registry,
)

tools = ToolRegistry()
tools.register('normalize.v1', lambda text: {'text': text.strip()})
handlers = create_workflow_registry(
    LLMConfig.from_env(),
    http_config=HTTPConfig(
        allowed_origins=('http://127.0.0.1:8090',),
        timeout=10,
    ),
    tools=tools,
)
orchestrator = WorkflowOrchestrator(TaskEngine(
    database_path='mixed.sqlite3', task_registry=handlers, max_workers=2,
))
```

统一注册表包含以下处理器：

| 名称 | 输入与输出 |
| --- | --- |
| `llm.chat.v1` | `prompt` 或 `messages` → 文本、响应元数据和 token 用量 |
| `llm.validate.v1` | `result` → 通过文本/结束原因校验的原模型结果 |
| `llm.parse_json.v1` | `result`、可选 `required_keys` → 解析后的 JSON 对象 |
| `http.request.v1` | HTTP 参数 → `status_code`、选定响应头、`text`、`json`；配置 HTTP 后注册 |
| `tool.call.v1` | `tool_name`、`arguments` → 注册工具的 JSON 结果 |
| 自定义工具名称 | 原有 `ToolRegistry` 工具的直接调用入口 |

注册名称冲突会在创建注册表时失败。动态工具调用只包含已注册工具及配置后的 HTTP
执行器；LLM 和校验节点不会自动进入工具集合。已有 `create_llm_registry()` 和
`create_http_registry()` 接口保持兼容。

## 串联节点

完整可序列化示例位于 [`examples/http_llm_workflow.json`](../examples/http_llm_workflow.json)：

1. `fetch` 通过 HTTP GET 获取业务上下文。
2. `plan` 通过 `InputBinding` 将上下文写入 `messages[1].content`，调用 LLM 生成工具计划。
3. `parse` 将模型结果转换为对象，要求包含 `name`、`arguments`。
4. `execute` 使用 `OutputRef` 把计划交给 `tool.call.v1`，执行真实 HTTP 请求。
5. `summarize` 引用 HTTP 响应文本，再次调用 LLM 生成汇总。
6. `check` 验证最终回答完整结束。

`inputs` 绑定整个关键字参数；`bindings` 绑定嵌套字典/列表中的字段。引用自动建立
DAG 依赖，放行后的具体参数保存在任务定义中。每个节点独立持久化和重试。

`llm.parse_json.v1` 接收完整的模型结果，例如
`inputs={"result": OutputRef("plan")}`。它要求非空文本、`finish_reason="stop"`，
且文本必须是完整 JSON 对象。代码围栏、尾随文字、重复键、NaN/Infinity、数字溢出、
缺少必需字段都会产生 permanent 错误，下游工具不会执行。它不修复模型文本，
`required_keys` 只检查顶层字段存在性；具体工具名、参数签名和 HTTP origin 继续由
已有工具执行器检查。模型原始回答与 token 用量仍保留在上游节点中。

如果模型只需生成 HTTP 请求体，可以将解析结果直接绑定为
`http.request.v1` 的 `json_body`，并在 DAG 中固定 URL 和 method。

## 命令行与环境配置

```powershell
$env:PYTHONPATH = 'src'
$env:LLM_API_KEY = '<模型 API Key>'
$env:LLM_BASE_URL = '<OpenAI 兼容 API Base URL>'
$env:LLM_MODEL = '<模型名称>'
python -m workflow_engine.llm_cli --database mixed.sqlite3 --workflow examples/http_llm_workflow.json --http-allowed-origin http://127.0.0.1:8090
```

示例要求应用服务在 `8090` 端口提供 `GET /context` 和 `POST /actions`，且响应为
JSON。请将 JSON 中的 URL、规划提示中的 URL 与允许的 origin 一起改为实际业务服务。
`--http-allowed-origin` 可以重复指定，显式参数覆盖环境中的 origin 列表。
工作流有固定 ID，使用同一数据库和相同定义重复运行会返回已保存结果；修改定义后
应更换 ID，避免幂等定义冲突。原有 `--prompt` 模式继续可用。

无参数注册工厂支持以下 worker 环境变量：

| 变量 | 含义/默认值 |
| --- | --- |
| `LLM_*` / `APP_LLM_*` | 沿用现有模型配置 |
| `HTTP_ALLOWED_ORIGINS` | JSON 数组，例如 `["https://service.example"]`；未设置时不注册 HTTP |
| `HTTP_DEFAULT_HEADERS` | JSON 对象，默认 `{}`，用于 worker 本地注入认证头 |
| `HTTP_TIMEOUT` | socket 超时，默认 `10` 秒 |
| `HTTP_MAX_REQUEST_BYTES` | 默认 `1048576` |
| `HTTP_MAX_RESPONSE_BYTES` | 默认 `1048576` |

HTTP 配置错误会在提交工作流之前失败。凭据应通过 worker 配置注入；节点只携带业务参数。
允许的 origin 来自部署配置，不根据工作流或模型输出自动扩展。

## 远程 Worker 与管理 API

在现有 `workflow-management --registry`、`workflow-deploy worker --registry` 中使用：

```text
workflow_engine.ai_workflow:create_workflow_registry
```

它是无参数工厂，读取上述环境变量。自定义工具可由应用自己的无参数工厂构造
`ToolRegistry`，再传入 `create_workflow_registry(tools=...)`。

中心可以使用已有 `--handlers` 占位注册方式，名称须与 worker 一致：
`llm.chat.v1 llm.validate.v1 llm.parse_json.v1 http.request.v1 tool.call.v1`。
通过管理 API 提交的仍是标准 DAG JSON；中心调度，远程 worker 执行所有节点。
HTTP 和 LLM 的认证只需存在于实际执行的 worker。

LLM 预算仅统计生成节点。解析、校验、工具节点不重复统计 token；预算会阻止后续
模型调用，但不会撤销此前已完成的 HTTP 操作。HTTP 写请求结果不确定时不自动重试，
已完成工作流重新打开也不会重放已保存的外部操作。中断恢复仍遵循已有恢复规则。

## 独立测试

四个测试文件均可独立运行，不依赖另一个测试套件的测试类，不需要公网或真实凭据：

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_ai_workflow_registry.py -v
python -m unittest discover -s tests -p test_ai_workflow_integration.py -v
python -m unittest discover -s tests -p test_ai_workflow_remote.py -v
python -m unittest discover -s tests -p test_ai_workflow_cli.py -v
```

后三组启动本机回环 HTTP 服务，并使用真实的模型 HTTP adapter、工具 HTTP adapter、
SQLite、Worker 和子进程 CLI；运行环境需要允许 Python 连接自身的本机端口。
覆盖内容包括注册冲突、严格 JSON、完整数据传递、认证隔离、失败传播、读取/模型重试、
不确定写入不重放、LLM 预算、数据库重新打开、管理 API 和远程执行。
