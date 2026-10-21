# 工具执行原语

工具节点会真实调用应用注册的函数或 HTTP 服务，结果作为严格 JSON 保存到既有
任务日志，再传给下游 DAG 节点。执行路径仍是 `WorkflowOrchestrator → TaskEngine
→ Worker / RemoteWorker → handler`，沿用任务事件、并发、重试预算和恢复机制。
没有新增运行依赖，也不需要修改 TaskEngine 构造参数。

## 工具注册与调用

```python
from workflow_engine import ToolRegistry, TOOL_HANDLER

tools = ToolRegistry()
tools.register('inventory.lookup.v1', lookup_inventory, description='查询库存')
handlers = tools.to_task_registry()

# 固定工具：handler 名称直接用 inventory.lookup.v1。
result = handlers['inventory.lookup.v1'](sku='item-7')
# 动态选择：规划节点给出名称与 JSON 参数对象，只能调用已注册工具。
result = handlers[TOOL_HANDLER](
    tool_name='inventory.lookup.v1', arguments={'sku': 'item-7'},
)
```

`register(name, handler, description='')` 拒绝重复、空名称和保留名称
`tool.call.v1`。工具必须是具有可检查签名的同步 callable。调用前检查参数是否
满足函数签名，并检查输入、输出是否为严格 JSON；它不执行 JSON Schema 类型校验，
业务参数约束由 adapter 检查。`describe()` 返回名称和说明目录，可供规划层使用。

`to_task_registry()` 返回普通字典，可直接传入 `TaskEngine(task_registry=...)`
和 `RemoteWorker(..., task_registry=...)`。导出时会固定当前工具集合，之后再注册
工具不会改变已有 dispatcher。工具别名各自拥有独立 callable，满足持久化层的
唯一注册名称约束。重启和跨主机部署必须保留名称、签名与行为契约的一致性。

输入和输出在调用边界复制为 JSON 快照，工具对参数的修改不会影响其他节点。
未知工具和签名不匹配会在执行工具前失败；工作流输入不能注册工具、动态导入模块
或执行任意代码。工具函数本身是受信任的部署代码。

## HTTP 请求执行器

```python
from workflow_engine import HTTPConfig, HTTPRequestHandler, HTTP_HANDLER, ToolRegistry

tools = ToolRegistry()
tools.register(HTTP_HANDLER, HTTPRequestHandler(HTTPConfig(
    allowed_origins=('https://inventory.example.com',),
    timeout=10,
    max_request_bytes=1024 * 1024,
    max_response_bytes=1024 * 1024,
    default_headers={'Authorization': 'Bearer ' + service_token},
)))
handlers = tools.to_task_registry()
```

如果只需要 HTTP 工具，可直接使用 `create_http_registry(config)`，导出
`http.request.v1` 和 `tool.call.v1`。混合 LLM、HTTP 和自定义工具时，使用
`create_workflow_registry(llm_config, http_config=config, tools=tools)` 统一组合，
自动拒绝名称冲突并固定动态工具集合，详见 [AI 协同工作流](ai_workflows.md)。

HTTP handler 参数如下：

| 参数 | 含义 |
| --- | --- |
| `url` | 完整 HTTP(S) URL，必须属于配置的服务 origin |
| `method` | GET（默认）、HEAD、OPTIONS、POST、PUT、PATCH、DELETE，大小写不敏感 |
| `params` | 查询参数对象；值为 JSON 标量（不含 null）或标量列表，保留 URL 已有查询参数 |
| `headers` | 本次请求头；不能覆盖 worker 配置头或 Host、Content-Length 等传输头 |
| `json_body` | JSON 请求体，可显式传入 null |
| `text_body` | UTF-8 文本请求体；与 `json_body` 互斥 |
| `response_format` | `json`（默认）或 `text` |

GET、HEAD 不接受请求体。HTTPConfig 的超时和大小限制由 worker 配置，节点不能
放宽限制。`timeout` 是底层连接/读取的 socket 超时，不是整个流式响应的总时长限制。
请求体超过限制时不会发送；响应最多读取上限加一个字节，超出即失败。

成功结果结构：

```json
{
  "status_code": 200,
  "headers": {"content-type": "application/json; charset=utf-8"},
  "text": "{\"items\":[{\"id\":\"item-7\"}]}",
  "json": {"items": [{"id": "item-7"}]}
}
```

所有 2xx 都视为成功。`text` 按响应声明的 charset 解码，默认 UTF-8；`json`
保存解析结果。文本模式不做 JSON 解析，`json` 为 null。空响应的 `text` 为空串、
`json` 为 null。非 JSON、非有限数值、无效编码和超过限制的响应会失败。
响应头只保留 Content-Type、Content-Length、ETag、Last-Modified、Retry-After，
避免将服务端 Set-Cookie 等信息自动写入日志。

允许列表精确匹配协议、主机和有效端口；不支持通配符或 URL 前缀匹配。URL 中的
用户密码、fragment、空白与控制字符会被拒绝；不跟随重定向，也不读取环境代理。
默认请求头的值不会出现在配置 repr 中，调用错误只记录状态码和固定说明，不包含
远端错误体或 URL。凭据应从 worker 环境或密钥服务注入，不要放入 DAG 参数。
需要不同服务凭据时，为各服务注册独立 adapter；同一 HTTPConfig 的默认头会用于
其所有允许的 origin。允许列表基于主机名，不替代网络层的 DNS/IP 出站访问控制。

## DAG 中传递工具数据

`inputs={'argument': OutputRef('node', ('json', 'field'))}` 继续用于绑定整个关键字
参数。新增的 `bindings` 可以将输出填入嵌套对象，不需要额外写一个拼装请求的节点：

```python
from workflow_engine import (
    DAGNode, DAGWorkflow, HTTPConfig, HTTP_HANDLER, InputBinding, OutputRef,
    RetryPolicy, TaskEngine, WorkflowOrchestrator, create_http_registry,
)

# 示例服务约定：GET /items 返回 {"items": [{"id": "item-7"}]}；
# POST /actions 接受 {"operation": "archive", "item_id": "item-7"}。
origin = 'http://127.0.0.1:8081'
engine = TaskEngine(
    database_path='tools.sqlite3',
    task_registry=create_http_registry(HTTPConfig((origin,))),
    retry_policy=RetryPolicy(2, initial_delay=0.1),
)
workflow = DAGWorkflow((
    DAGNode('fetch', HTTP_HANDLER, kwargs={'url': origin + '/items'}),
    DAGNode('act', HTTP_HANDLER,
        kwargs={'url': origin + '/actions', 'method': 'POST',
                'json_body': {'operation': 'archive'}},
        bindings=(InputBinding(
            ('json_body', 'item_id'), OutputRef('fetch', ('json', 'items', 0, 'id')),
        ),)),
))
orchestrator = WorkflowOrchestrator(engine)
orchestrator.submit(workflow)
run = orchestrator.run(workflow.id, timeout=30)
print(run.status, run.nodes['act'].result)
```

`InputBinding(target, source)` 的 target 是以关键字参数名开头的路径，字符串访问
字典字段、非负整数访问列表项。父容器须已存在于静态 kwargs 中；可以新增字典叶子，
列表只能替换已有索引。它可以更新静态 kwargs 的指定字段，但不能与另一 binding
或 `inputs` 的目标重叠。所有引用都会自动产生依赖边，参与环和未知节点校验。

JSON 定义中的对应写法：

```json
{
  "bindings": [{
    "target": ["json_body", "item_id"],
    "source": {"node_id": "fetch", "path": ["json", "items", 0, "id"]}
  }]
}
```

绑定与放行仍在同一数据库事务中完成。缺失输出字段、目标父容器不存在、类型不匹配
或索引越界，会让节点 FAILED，实际尝试次数为 0，后代 SKIPPED；独立分支继续运行。
绑定结果保存到任务定义中，重启后不重新拼装已放行的参数。无 bindings 的旧工作流
保持原有序列化格式和幂等定义比较行为，无需数据库 schema 迁移。

动态工具调用可以使用规划节点输出（例如已解析的模型 JSON）：

```python
DAGNode('call', 'tool.call.v1', inputs={
    'tool_name': OutputRef('plan', ('name',)),
    'arguments': OutputRef('plan', ('arguments',)),
})
```

此处规划结果约定为 `{"name": "http.request.v1", "arguments": {"url": "..."}}`。
`llm.chat.v1` 返回文本；统一注册表中的 `llm.parse_json.v1` 可将模型结果解析为
上述对象，再交给工具节点。应用也可注册自己的解析/校验节点。
每次模型和工具调用都是显式 DAG 节点。

## 失败、重试与副作用

每次 HTTP handler 调用只发送一次请求，不在 adapter 内重试。引擎统一消耗
`RetryPolicy` 和节点 `max_retries` 预算，失败类别会持久化并通过远程协议传递。

| 情形 | 类别与自动重试 |
| --- | --- |
| 未注册工具、输入/签名错误、不允许的 origin、TLS 证书无效 | permanent，不重试 |
| GET/HEAD/OPTIONS 的网络失败、408/425/429、大多数 5xx | transient，受现有预算约束 |
| POST/PUT/PATCH/DELETE 的上述失败 | uncertain，不自动重试 |
| 其他非 2xx（包括重定向、501/505） | permanent，不重试 |
| 响应格式/大小失败、工具返回非 JSON、任务软超时 | uncertain，不自动重试 |
| 自定义工具抛出普通异常 | 转成固定消息的 ToolExecutionError，uncertain |

只读方法的重试安全性依赖外部服务遵守 HTTP 方法语义。自定义 adapter 明确知道
调用未产生副作用时，可以抛出 `ToolTransientError`；已知不可恢复的输入错误使用
`ToolInputError`，其他不确定结果使用 `ToolError`。这些异常的消息由 adapter 保证
不含凭据。普通业务 handler 仍沿用已有重试行为。

设置 `idempotent=True` 不会让不确定 HTTP 写请求自动重试，也不提供外部副作用
exactly-once 保证。恢复仍走现有的可信停止、隔离和人工核实流程。Task.timeout
是软超时，不终止底层调用；HTTP adapter 自身也必须配置网络超时。

## 独立测试

四个测试文件均可单独运行，不需要模型凭据或公网访问；HTTP/远程测试需要允许
Python 连接它自行启动的本机回环服务：

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_tool_registry.py -v
python -m unittest discover -s tests -p test_http_tool.py -v
python -m unittest discover -s tests -p test_tool_dag_integration.py -v
python -m unittest discover -s tests -p test_tool_remote_integration.py -v
```

分别覆盖注册与 JSON 调用边界；真实 HTTP 协议、请求体、查询参数、超时和限制；
获取数据→嵌套绑定→外部副作用→汇合、重启、失败传播与重试；以及中心占位 registry
配合远程 Worker 的真实执行、参数传递和失败类别保留。

远程部署时，中心与 worker 必须具有相同的顶层 handler 名称。中心可使用已有
`deployment.contract_registry` 占位实现；worker 使用可信的无参数 registry factory
从环境读取 HTTPConfig，再调用 `create_http_registry(config)`。动态工具集合和
凭据在 worker 本地配置，中心的占位实现不会执行 HTTP 请求。
