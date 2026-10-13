# 管理 API、可观测性与性能稳定性验证

日期：2026-10-13；环境：Windows 11、Python 3.12.10。新增实现无运行时第三方依赖。

## 交付内容

- 版本化 REST 接口、管理员/只读令牌、稳定工作流 ID 幂等提交、键集分页及 OpenAPI 3.1。
- 复用既有 DAG 和恢复控制面：暂停/恢复、事故核对和预算校验、精确会话停止请求。
- 事件增量读取、操作审计、请求关联 JSON 日志、健康探针和 Prometheus 指标/告警配置。
- 有界 HTTP 处理线程、输入/响应上限、连接超时、协调器错误/停滞检测及可等待的关闭。
- CLI、本地运行示例、接入文档和可独立运行的压测工具。

## 测试覆盖

| 独立文件 | 数量 | 场景 |
| --- | --- | --- |
| `test_management_api.py` | 8 | 并发幂等提交、DAG 输出绑定、分页、事件续读、权限、非法协议/定义、暂停审计、编码 ID/OpenAPI、生命周期 |
| `test_management_observability.py` | 5 | 持久化统计、耗时直方图、标签和日志脱敏、请求关联、协调失败/停滞就绪变化、延迟与暂停队列 |
| `test_management_stability.py` | 8 | 重启、移除完成任务的 handler、慢客户端与过载、未收尾关闭、重试预算和过期决定、失联禁止重放、非幂等结果核对、真实控制面进程强制终止、存储错误隔离 |
| `test_management_performance.py` | 2 | 8 客户端混合请求/3 Worker、40 工作流单次执行、75 项分页遍历与数据库完整性 |

独立场景共 **23 项**。完整既有及新增回归 **349 passed（73.72 秒）**，证据见
`management-validation-2026-10-13-full.xml`。独立及重复测试证据见对应 XML 文件。
最终独立执行 **23 passed（16.70 秒）**；性能/稳定性两组另重复两轮，分别为
**10 passed（10.49 秒）**、**10 passed（12.17 秒）**，以验证此前 Windows
过载连接复位问题的修复。

测试使用真实 loopback HTTP、SQLite、Worker 和子进程。协调器停滞、存储异常通过
确定性注入产生；失联场景使用可控时钟及真实领取事务。测试不使用业务假执行器。
首次沙箱执行因禁止本机连接失败；最终验证在获准使用 loopback 的执行环境运行。

监控 YAML 已解析，OpenAPI 全部 component schema 通过 JSON Schema 2020-12
结构校验，operationId 无重复；本机没有 promtool，未实际启动 Prometheus。

## 独立压测数据

配置：100 个两节点工作流、300 次混合查询/抓取、8 个 HTTP 客户端、4 个 Worker。
服务默认 0.1 秒协调间隔，Worker 默认 0.05 秒轮询和 1 秒心跳。基于临时数据库，
压测没有与回归测试同时运行。原始报告为 `management-benchmark-2026-10-13.json`。

| 数据 | 结果 |
| --- | --- |
| HTTP 请求 | 400 次，0 错误 |
| HTTP 吞吐 | 46.70 请求/秒 |
| HTTP P50 / P95 / P99 | 103.469 / 236.858 / 2396.412 ms |
| 最慢请求 | 8323.652 ms |
| 工作流完成 | 100 / 100 |
| 执行尝试 / 重试 | 200 / 0，无重复执行 |
| 总完成时间 | 9.508 秒 |
| 工作流吞吐 | 10.52 / 秒 |
| SQLite integrity_check | ok |

该数据验证本机有限混合负载下的完成与正确性，并非生产容量承诺。P99 及最长请求
仍表现出显著尾延迟；SQLite 单写者事务竞争和本机 FULL 同步磁盘成本是进一步
容量分析的重点，不能把 P95 当成最坏响应时间。客户端及代理超时要覆盖实际尾延迟，
写请求丢失响应需按稳定 ID 核对。监听 backlog 已按线程限额调整，避免默认 5 个
连接积压容量造成不必要的连接等待。

本轮没有验证长期 soak、跨真实主机网络故障、物理断电、磁盘损坏、TLS 证书部署
或公网防护；此前已有远程 Worker/进程恢复测试随全量回归执行。可观测系统目前
提供事件、审计、指标和 JSON 日志，业务内部 OpenTelemetry span 仍需后续接入。
标准库服务面向可信部署，生产应配合受保护的 TLS 反向代理。

## 复现

```powershell
$env:PYTHONPATH = 'src'
python -m pytest tests -q
python -m unittest discover -s tests -p test_management_api.py -v
python -m unittest discover -s tests -p test_management_observability.py -v
python -m unittest discover -s tests -p test_management_stability.py -v
python -m unittest discover -s tests -p test_management_performance.py -v
python tools/benchmark_management.py --workflows 100 --reads 300 --concurrency 8 --workers 4 `
  --output docs/management-benchmark-2026-10-13.json
```
