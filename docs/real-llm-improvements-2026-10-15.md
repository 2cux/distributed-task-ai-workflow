# 真实 LLM 验证后的改进

根据 [首次验证报告](real-llm-validation-2026-10-15.md) 修改产品代码，并重新验证。
基线提交为 `c7b8488`；本报告验证的是其上的工作区改动，尚未提交。

最终完整回归 **397 passed，96.12 秒，0 failures / 0 errors / 0 skipped**。
最终真实模型复验和三轮原性能用例复测也全部通过。

## 改进内容

### 配置兼容移入正式入口

`LLMConfig.from_env()` 按字段优先使用 `LLM_*`，不存在时读取 `APP_LLM_*`。
显式空值会报错，不会意外切换到另一套配置。
`LLMConfig` 同时接受 API 根地址和以 `/chat/completions` 结尾的完整地址，统一归一化。
CLI、管理服务注册工厂及远程 Worker 使用相同逻辑；真实验证脚本已删除自己的变量和 URL 转换。
配置检查仍在工作流提交前执行，Key 不进入任务参数。

### 独立模型输出校验节点

新增 `llm.validate.v1`、`validate_llm_output()` 和 `LLMOutputError`。
默认校验非空白文本、完整结束原因 `stop`；可添加精确文本和完整正则匹配。
校验失败不自动修复或再次请求模型，上游已生成的回答、响应 ID、结束原因和 token 用量继续保留。
成功时原样向下游传递结果。业务节点可依赖校验节点，避免 HTTP 200 的不合格内容直接进入业务。

`create_llm_registry()` 现在同时包含模型与校验处理器，中心和 RemoteWorker 应同步更新注册表。
统计用量应只累加模型生成节点或按响应 ID 去重，避免把校验节点复制的用量计算两次。
新增可直接运行的 [校验示例](../examples/llm_validated_workflow.json)。

### 有界数据库等待及同进程写入排队

管理 HTTP 中的 SQLite 连接按 `request_timeout` 限制锁等待。
持续争用时返回带 `Retry-After: 1` 的 503，并记录请求 ID 和脱敏的
`failure_kind=database_busy`，新增指标 `workflow_api_database_busy_total`。
等待配置按线程隔离，后台 Worker 和协调器继续使用原有存储等待策略。

只增加等待上限时，原有混合负载用例再次失败。独立诊断日志确认多个提交在约
2.27～2.35 秒后返回 `database_busy`，将问题定位到 SQLite 写入争用。
随后为同一个 `SQLiteTaskStore` 的写事务增加先到先服务的等待队列，避免持续轮询的
本地 Worker/协调器反复抢先。超时票据会移除，不留下迟到写入或阻塞后续请求。
只对同一 store 的写事务排队，读取仍可并行；跨进程及其他 store 继续由 SQLite 协调。

这轮诊断确认了复现请求的直接失败原因，不能据此断言首次报告中那一次客户端超时
也有完全相同的根因。没有修改客户端超时、并发量、性能断言或添加请求重试来使测试通过。

### 失败诊断

LLM 网络错误可区分 DNS、TLS 证书、TLS 握手、连接拒绝及网络访问权限，
只持久化固定类别，不回显底层异常中的地址、凭据或响应体。
真实验证工具在 CLI 失败时先保存返回结果；远程流程进入 FAILED/PAUSED 时保存
节点、事件、事故和概览，避免只留下“超时”或“Worker 提前退出”。

## 已完成验证

- 最终全量回归：**397 passed，96.12 秒**。
  [日志](../.artifacts/improvements-2026-10-15/full-final.log)、
  [XML](../.artifacts/improvements-2026-10-15/full-final.xml)
- 配置、HTTP、校验、持久化、远程 RPC、管理稳定性和观测定向测试：
  **41 passed，13.15 秒**。[日志](../.artifacts/improvements-2026-10-15/targeted-fixed.log)
- 公平排队改动后的原性能及稳定性用例：**12 passed，13.85 秒**。
  [日志](../.artifacts/improvements-2026-10-15/writer-fairness.log)
- 原性能测试使用三个全新进程连续复测，每轮均 **2 passed**，分别为
  **8.57 / 10.25 / 10.17 秒**；没有失败请求重试。
  [第 1 轮](../.artifacts/improvements-2026-10-15/performance-repeat-1.log)、
  [第 2 轮](../.artifacts/improvements-2026-10-15/performance-repeat-2.log)、
  [第 3 轮](../.artifacts/improvements-2026-10-15/performance-repeat-3.log)
- 最终真实模型链路：**PASSED，15.86 秒**。通过现有兼容网关调用供应商报告的
  `claude-opus-4-6`，5 次成功调用、1 次预期不存在模型的 HTTP 403；
  CLI 三节点流程及远程六节点流程均经过正式输出校验处理器。
  [结构化结果](../.artifacts/improvements-2026-10-15/live-final/summary.json)
- 最终真实运行再次验证了两个独立 Worker 的并行调用、`42 + 56 → 98`、
  token/事件持久化、只读及错误令牌、暂停/恢复、供应商拒绝的事故解决、
  服务重启去重、Worker 正常退出和两库完整性。

最终真实运行已确认用量 **193 tokens（输入 167、输出 26）**。
本次改进还执行了一轮较早的真实复验，确认用量 200 tokens；两轮合计 10 次成功响应、
2 次预期拒绝，已确认成功用量 **393 tokens**。未知或拒绝请求的供应商用量未计入。

确定性数据库锁竞争测试还确认：三个并发提交返回 503 时没有新工作流，释放锁后
不会迟到提交；后续提交和 Worker 执行正常，普通存储连接的等待值仍为 30 秒。

## 保留的失败证据

- 有界等待改动后的首次完整回归为 396 passed / 1 failed，失败是混合负载提交 503：
  [日志](../.artifacts/improvements-2026-10-15/full.log)。
- 同一性能用例的独立复现及数据库忙诊断：
  [日志](../.artifacts/improvements-2026-10-15/performance-diagnostic.log)。

产物保存在已忽略的 `.artifacts/improvements-2026-10-15/` 下，分享报告时需附带相关文件。
最终产品源文件和验证脚本的 SHA-256 保存在 [源码清单](../.artifacts/improvements-2026-10-15/source-manifest.json)，
可用于核对本次尚未提交的具体代码版本。
用法及边界已同步到 [LLM 文档](llm_workflows.md) 和 [管理 API 文档](management_api.md)。
