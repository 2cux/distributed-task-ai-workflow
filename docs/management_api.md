# 工作流管理 API 与可观测系统

`ManagementService` 为外部应用提供 `/api/v1` HTTP JSON 接口。实现只使用 Python
标准库，复用持久化 DAG、Worker 会话和 `RecoverySystem`，现有调用入口继续可用。
工作流定义及所有节点任务原子提交；业务函数由独立配置的本地或远程 Worker 执行。

## 启动与接入

```python
from workflow_engine import TaskEngine, RetryPolicy

engine = TaskEngine(database_path="tasks.db", task_registry={"abs-v1": abs},
                    retry_policy=RetryPolicy(2))
api = engine.create_management_service(
    token="replace-with-admin-secret",
    readonly_token="replace-with-read-secret",
    port=8080,
)
api.start()
worker = engine.create_worker("worker-1")
worker.start()
# 应用退出时：先让业务调用收尾，再关闭服务。
worker.shutdown()
assert api.shutdown(5)
```

可运行的示例 `examples/management_server.py` 注册 `abs-v1`、`multiply-v1`，启动
两个本地 Worker。在仓库根目录运行：

```powershell
$env:PYTHONPATH = 'src'
$env:WORKFLOW_API_TOKEN = 'replace-with-admin-secret'
$env:WORKFLOW_API_READ_TOKEN = 'replace-with-read-secret'
python examples/management_server.py
```

安装项目后也可独立运行控制面，Worker 由部署单独提供：

```powershell
$env:PYTHONPATH = 'src;.' # 示例 registry 在仓库根目录下；生产应使用已安装的业务模块。
workflow-management --database tasks.db --registry examples.management_server:registry --port 8080
```

未安装时，可在仓库根目录使用 `python -m workflow_engine` 及相同参数。

`--registry` 是部署方配置的可信 `module:factory`，返回名称到函数的映射；HTTP
请求只允许使用已注册名称。CLI 默认重试预算为 0，节点可显式设置 `max_retries`。
已有库中的预算和退避配置沿用其持久化值。

外部应用携带 `Authorization: Bearer <token>`；管理令牌允许所有操作，只读令牌
只允许 GET。仅 `/health/live` 和 `/health/ready` 无需认证。令牌不进入响应或请求
日志，两个令牌必须不同。工作流数据、业务结果和错误详情可通过认证的读取接口访问，
因此只读令牌应只授予有权查看这些数据的应用。

提交示例：

```powershell
$headers = @{ Authorization = "Bearer $env:WORKFLOW_API_TOKEN" }
$body = @'
{
  "id": "external-order-001",
  "name": "absolute-and-multiply",
  "nodes": [
    {"id": "root", "handler": "abs-v1", "args": [-7], "idempotent": true},
    {"id": "double", "handler": "multiply-v1", "idempotent": true,
     "inputs": {"value": {"node_id": "root"}}}
  ]
}
'@
Invoke-RestMethod -Method Post -Uri http://127.0.0.1:8080/api/v1/workflows `
  -Headers $headers -ContentType application/json -Body $body
Invoke-RestMethod -Uri http://127.0.0.1:8080/api/v1/workflows/external-order-001 -Headers $headers
```

POST 返回 **202** 表示已持久化接受，不代表执行完成。外部调用方必须提供稳定的
`id`；同 ID 同定义重发仍返回 202，不增加任务或执行次数；同 ID 不同定义返回
409。工作流 ID 限 1..256 字符，名称限 1..512 字符。请求丢失响应时，先按 ID 查询，再按原定义重发。定义不可修改，需要新的 ID
提交新版本。节点支持 `depends_on`、`inputs` 输出路径绑定、`priority`、
`max_retries`、`timeout`、`scheduled_at` 和 `idempotent`，含义沿用 DAG 文档。

## 接口清单

完整机器可读契约：`GET /openapi.json`（OpenAPI 3.1，允许只读令牌）。

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/v1/workflows` | 原子提交 DAG，幂等重发 |
| GET | `/api/v1/workflows` | 分页列出工作流，可按 `status` 过滤 |
| GET | `/api/v1/workflows/{id}` | 定义、节点状态/结果、暂停来源、LLM 用量与预算状态 |
| POST | `/api/v1/workflows/{id}/pause` | 阻止新领取，记录操作审计 |
| POST | `/api/v1/workflows/{id}/resume` | 移除人工暂停，重新推进依赖 |
| GET | `/api/v1/workflows/{id}/events` | 汇总工作流所有节点的执行事件 |
| GET | `/api/v1/tasks` | 任务分页，可按 `status`、`workflow_id` 过滤 |
| GET | `/api/v1/tasks/{id}` | 持久化定义、结果、错误、尝试/重试计数 |
| GET | `/api/v1/tasks/{id}/events` | 按任务内序号增量读取事件 |
| GET | `/api/v1/workers`、`/api/v1/workers/{id}` | Worker 当前身份和会话 ID |
| GET | `/api/v1/sessions`、`/api/v1/sessions/{id}` | 当前及历史 Worker 会话 |
| GET | `/api/v1/sessions/{id}/events` | 会话状态变化事件 |
| POST | `/api/v1/sessions/{id}/stop` | 优雅停止指定会话，返回 202 |
| GET | `/api/v1/incidents`、`/api/v1/incidents/{id}` | 查询故障恢复事故 |
| POST | `/api/v1/incidents/{id}/resolve` | 安全重试、补录成功或接受失败 |
| GET | `/api/v1/audit` | 持久化恢复动作和人工操作审计 |
| GET | `/api/v1/handlers` | 可提交的注册函数名称 |
| GET | `/api/v1/overview` | 同一数据库快照的状态、积压、执行和事故汇总 |
| GET | `/metrics` | Prometheus 文本指标 |
| GET | `/health/live`、`/health/ready` | 存活与就绪探针，返回 200 或 503 |

`{id}` 须按单个 URL 路径片段编码，例如 Python `quote(id, safe='')`。GET 只读取
已提交数据，后台协调器推进节点与工作流汇总，默认最多间隔 0.1 秒扫描一次；负载
较大时需要加上扫描及事务等待时间。

列表和事件接口使用 `limit`（默认 50，范围 1..100）和 `cursor`（默认 0）：

```json
{"items": [], "next_cursor": null}
```

非空 `next_cursor` 用于读取下一页。列表按插入顺序，采用键集分页，过滤在分页前
执行；跨页不是同一个长期事务，期间状态变化会影响过滤结果。事件项还包含自身
`cursor`：读到末页后保留最后一项的 cursor，下次从该值继续轮询，避免重新读取。
任务 cursor 是任务内事件序号，工作流 cursor 是所有任务事件的数据库追加顺序，
会话和审计使用各自持久化序号；cursor 是不透明续读位置，不应加减或用时间替代。
轮询接口无需维持长连接。现有历史一直保留，暂无自动事件清理。

暂停、恢复及停止请求的 body 为：

```json
{"operator": "ops-user-01", "reason": "maintenance window"}
```

`operator` 是管理令牌持有方声明的审计标识，不是独立验证的个人身份。人工暂停
阻止新领取，不中断已开始的函数。恢复只移除 `manual` hold，事故产生的暂停继续
保留，直到事故得到安全处理。优雅停止面向确切 `session_id`，防止停止同名 Worker
的新会话；Worker 需继续连通控制面才能看到停止请求。

事故处理示例：

```json
{"action": "retry", "operator": "ops-user-01", "reason": "safe retry confirmed", "extra_retries": 1}
```

`action` 为 `retry`、`succeed` 或 `fail`。重试要求原任务声明幂等，且有剩余预算，
不足时明确授予 `extra_retries`；`succeed` 的 `result` 是经业务核对的 JSON 结果。
过期或已解决的决定返回 409。HTTP API 沿用既有恢复校验；运行中或仅失联的调用
不能直接重试。本接口没有远程“停止证明”入口，确切旧调用退出的证明应由可信
监督器通过既有 `RecoverySystem` 提供。参见 [故障恢复系统](failure_recovery.md)。

## 错误、请求关联和运行限制

错误格式：

```json
{"error": {"code": "conflict", "message": "definition or recovery state conflicts"}, "request_id": "..."}
```

400 为非法 JSON/参数/定义，401 为未认证，403 为权限不足，404 为资源缺失，405
为不支持的操作，409 为幂等或恢复冲突，413 为大小限制，415 为非 JSON body，
503 为数据库/控制面暂不可用或并发饱和。503 带 `Retry-After: 1`。错误响应不暴露
数据库路径、堆栈或内部异常文本；事件与任务详情保留业务错误，供授权调用方排查。

正常及错误响应携带 `X-Request-ID`。调用方可传入 1..64 字符的字母、数字、点、
下划线或连字符，否则生成随机 ID。结构化 JSON 日志记录 `request_id`、方法、
路由模板、状态码及耗时；不记录原始 URL、查询字符串、业务 body、结果或令牌。
SQLite 忙或锁冲突额外记录 `failure_kind="database_busy"`，其他 SQLite 错误为
`failure_kind="database_error"`，可通过 request ID 关联客户端的 503。
按需启用 `workflow_engine.management` 的 INFO 日志；CLI 默认启用。

默认请求体上限 1 MiB，响应上限 4 MiB，每个连接读写超时 2 秒，最多 32 个 HTTP
处理线程（可配置 1..256）。拒绝分块 body、重复 JSON 字段、重复 query、非有限
数值及未知定义字段。饱和连接立即返回 503，并对连接收尾设置短时间上限。
管理 HTTP 请求使用服务持有的连接池，每个请求独占借用连接；默认同时允许 4 个
请求进入数据库操作。Python 构造参数 `database_concurrency` 可设置为 1..256，
实际值不超过 `max_concurrency`。连接等待按 FIFO 排队，借用连接、等待写入槽以及
SQLite 锁等待分别按 `request_timeout` 限时，读写均适用；
持续争用时返回带 `Retry-After: 1` 的 503。等待策略按线程隔离，后台协调器和
Worker 保留原有 30 秒存储等待。socket 超时和数据库锁等待上限不是业务/事务取消，
也不是 HTTP 请求的总时限；多次数据库操作可能累积等待。响应超限可能
发生在提交之后，调用方应按原 ID 查询；不能以错误响应断言写入一定未发生。
每次数据库租约结束时释放读取快照并回滚未提交更改，归还连接时不保留事务，
后续借用会恢复超时、外键和 FULL 同步设置。服务关闭时关闭空闲连接；尚在执行的
请求归还连接后关闭，已接收但尚未开始访问数据库的请求仍可完成排空。
同一个 `SQLiteTaskStore` 的任务、工作流及其 Worker 注册表写事务共用 FIFO，
避免持续轮询反复抢先；读请求不经过写入 FIFO。连接准备和关闭位于写入 FIFO
之外，协调器在不同工作流之间提交并释放写入槽。跨进程、其他 store 和独立
Worker 注册表仍通过 SQLite 文件锁协调，持久化领取令牌和事务仍是去重与安全边界。

服务默认绑定 loopback。标准库 HTTP 服务应放在可信网络及带 TLS、访问限流和
请求总超时的反向代理之后。跨主机绑定须提供 `ssl_context`，或在确实有保护隧道/
TLS 代理时显式 `allow_insecure=True`。数据库仍保留在中心本地盘。当前令牌模型
服务于可信应用，暂无多租户隔离、SSO、自动密钥轮换或浏览器跨域 CORS 配置。

`start()` 启动一个恢复/依赖协调循环，只能调用一次。可传入同一 store 的
`recovery=engine.create_recovery_system(stop_verifier=...)`，由服务驱动其 tick。
协调器故障会重试，失败期间 readiness 为 503；连续无成功扫描或扫描停滞超过
`coordinator_stale_after`（默认 10 秒）也不就绪。健康检查不会通过领取任务来探测。
服务关闭停止接受请求、等待在途处理和协调器，`shutdown(timeout)` 返回 False
表示尚未收尾，可以稍后继续等待；业务 Worker 由部署单独管理。

## 指标与告警

HTTP 指标是进程内 counter/histogram，服务重启归零：

| 指标 | 含义 |
| --- | --- |
| `workflow_api_requests_total{method,route,status}` | 完成请求数，路由使用模板，无 task/workflow ID |
| `workflow_api_request_duration_seconds` | 含序列化与响应写入的处理耗时直方图 |
| `workflow_api_overloads_total` | 在解析前因容量限制拒绝的连接数 |
| `workflow_api_database_busy_total` | 管理 HTTP 因 SQLite 忙或锁冲突返回 503 的次数 |
| `workflow_api_uptime_seconds` | 当前服务运行时长 |
| `workflow_coordinator_ticks_total`、`workflow_coordinator_errors_total` | 扫描次数与失败次数 |
| `workflow_coordinator_last_success_timestamp_seconds` | 最后成功扫描时间 |

数据库指标为 gauge，重启从同一库继续读取；多个服务读取同一个库时，不应把这些
指标简单相加，避免重复统计：

| 指标 | 含义 |
| --- | --- |
| `workflow_engine_tasks{status}`、`workflow_engine_workflows{status}` | 持久化状态数量 |
| `workflow_engine_sessions{status}` | 会话状态数量，含历史停止会话 |
| `workflow_engine_attempts`、`workflow_engine_retries` | 库内累计尝试/重试计数 |
| `workflow_engine_dispatchable_tasks` | 已到期、依赖放行、没有恢复/人工 hold 的可领取任务 |
| `workflow_engine_overdue_sessions` | 心跳逾期且尚未停止/撤权的会话数 |
| `workflow_engine_open_incidents` | 待处理事故数量 |

`tasks{status="PENDING"}` 包含依赖阻塞、延迟、暂停及被跳过节点对应的底层任务，
排队压力优先查看 `dispatchable_tasks`。时间定位使用持久化执行事件，当前没有
业务函数内部 span 或 OpenTelemetry exporter。HTTP 和扫描指标为有限标签集合，
业务 ID 不作为指标标签。

`examples/monitoring/prometheus.yml` 和 `alerts.yml` 提供可修改的抓取与告警配置。
在 Prometheus 工作目录创建 `workflow-api-read-token.txt`，内容只放只读令牌，
按实际部署修改目标和认证文件路径。可用 PromQL：

```promql
sum(rate(workflow_api_requests_total[5m]))
histogram_quantile(0.95, sum by (le) (rate(workflow_api_request_duration_seconds_bucket[5m])))
workflow_engine_dispatchable_tasks
workflow_engine_open_incidents
```

配置依据 [Prometheus 配置文档](https://prometheus.io/docs/prometheus/latest/configuration/)
和 [直方图文档](https://prometheus.io/docs/practices/histograms/)。仓库提供配置，
没有自动启动 Prometheus、Grafana 或外部通知服务。

## 独立测试与压测

四组测试可分别执行，使用真实 HTTP、SQLite 和 Worker；部分故障通过确定性注入
模拟，以便重复验证边界：

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_management_api.py -v
python -m unittest discover -s tests -p test_management_observability.py -v
python -m unittest discover -s tests -p test_management_stability.py -v
python -m unittest discover -s tests -p test_management_performance.py -v
python -m pytest tests -q
```

独立压测工具创建临时库及专用服务，运行并发 HTTP 写入/查询/指标抓取和两节点
DAG，记录吞吐、P50/P95/P99、错误率、执行次数与数据库完整性。退出码非 0 表示
请求、结果、执行次数或完整性验证失败：

```powershell
$env:PYTHONPATH = 'src'
python tools/benchmark_management.py --workflows 100 --reads 300 --concurrency 8 --workers 4 `
  --output docs/management-benchmark.json
```

可追加 `--repeat 3 --warmup 1 --markdown docs/management-benchmark.md`，顺序运行
预热和测量轮，保留逐轮原始数据并生成统计报告。三组 Benchmark 工具独立测试、
临时库位置和汇总规则见 [Benchmark 使用与验证](benchmarks.md)。

`--deadline` 限制请求批次结束后等待执行完成的总计时截止点，默认 60 秒；HTTP
单次客户端超时上限 10 秒。测试里的延迟门槛是宽松回归防线，不能当成生产 SLO。
容量数据应结合数据库大小、磁盘、业务延迟和机器配置判断。工具使用服务默认的
0.1 秒协调间隔、0.05 秒 Worker 轮询和 1 秒心跳，全部计入报告；过于频繁的轮询
会增加 SQLite 写锁竞争。计时使用高精度单调 `perf_counter`。
