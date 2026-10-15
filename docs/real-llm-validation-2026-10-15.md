# 真实 LLM 端到端验证

后续改进及复验见 [真实 LLM 验证后的改进](real-llm-improvements-2026-10-15.md)。以下保留首次验证时的结果与边界。

验证时间：2026-10-15，Asia/Shanghai。代码基线：`c7b8488f396fb24cd00ee498d95527441d528567`。
运行环境：Windows、Python 3.12.10；使用已有环境凭据，未安装额外依赖。

真实模型端到端验证通过，最终独立运行耗时 **17.156 秒**。
最终完整回归 **387 passed，66.72 秒，0 failures / 0 errors / 0 skipped**。
本次通过配置的 HTTPS 兼容网关 `api.xiaocaseai.com` 发起请求，响应报告的模型为
`claude-opus-4-6`。模型身份以网关返回的字段为依据，未单独验证上游服务身份。

## 最终验证结果

| 检查 | 结果 |
| --- | --- |
| CLI 两节点模型链路及下游文本传递 | SUCCESS，2 次真实调用 |
| 相同 JSON 定义重复启动 CLI | 回答、响应 ID、用量、attempt 不变 |
| 管理 API → SQLite → RPC → 两个独立 RemoteWorker 进程 → 真实模型 → API 查询 | SUCCESS，6 个业务节点 |
| 并行模型分支及汇总 | `19+23 → 42`、`7*8 → 56`，下游模型汇总 `98`，校验 `valid=True` |
| 并行执行证据 | PID 1816、4240；两次 HTTP 调用的客户端时间区间重叠 |
| 请求权限和暂停门 | 只读令牌提交 403、错误令牌提交 401；暂停期间 attempt 为 0 |
| 任务、节点、成功事件的结果一致性 | 6 个成功节点全部一致，均仅执行一次，未重试 |
| 监控和事件 | 指标与 6 次成功业务 attempt 一致；事件游标无重复 |
| 真实供应商拒绝 | 不存在模型请求返回 HTTP 403，错误脱敏，1 次 attempt、0 次重试 |
| 事故处理 | 失败产生事故并暂停；经管理 API 接受失败后为 FAILED，下游 SKIPPED、0 次执行 |
| 管理服务重启和幂等重发 | 结果、事件、审计不变；累计 attempt 仍为 7，无新增执行 |
| Worker 停止 | 两个会话均 STOPPED，进程退出码 0，`last_error` 为空 |
| 数据库 | 两个 SQLite 库 `integrity_check=ok`，无外键违规 |
| 凭据检查 | 模型 Key 未出现在 SQLite dump 中；另检查了本轮已有的 62 个运行产物，未发现 Key |

中心仅注册拒绝业务执行的占位函数。真实模型处理器及校验函数在 Worker 子进程中运行。
Worker 的包装器只记录调用时间、PID、响应及并行进入屏障，模型调用由项目原有
`OpenAIChatHandler` 实际发出，没有返回预设模型回答。

管理链路定义为：

```text
retrieve → 并行 llm.chat.v1(a, b) → join → llm.chat.v1(judge) → validate
```

CLI 额外使用随机标记检验完整文本在上下游和进程重启后保持一致。
模型调用记录含响应 ID、供应商模型字段、`finish_reason` 和原始 token 明细。
最终 5 次成功调用具有不同响应 ID，均以 `stop` 结束，用量合计：
**输入 172、输出 28、总计 200 tokens**。

## 验证过程中保留的问题和证据

1. 初次默认沙箱请求出现连接失败。允许网络访问后，相同配置通过真实调用。
   证据：`.artifacts/real-llm-2026-10-15/probe.json`、`probe-network.json`。
2. 原环境提供 `APP_LLM_*`，URL 包含完整 `/chat/completions`。验证工具将变量映射为
   系统要求的 `LLM_*`，并去掉 endpoint 后缀。产品仍使用 README 规定的显式配置，
   没有改变现有配置协议。
3. 第一轮并行分支中，模型返回了正确数值 `42`，但没有遵守要求的文本前缀。
   下游严格格式校验失败，控制面产生事故并将工作流置为 PAUSED。
   证据保留在 `.artifacts/real-llm-2026-10-15-run2/remote.sqlite3` 和 Worker 日志。
   后续把该算术场景的验收条件明确为整数结果，未跳过结果校验，也未增加自动重试。
   这说明业务应显式校验模型输出，单纯 HTTP 200 不能保证业务成功。
4. 验证工具本身曾遇到 Windows 缺少时区数据库，以及主动停止 Worker 后仍检查其存活的问题。
   已分别改用 UTC+08:00 和区分主动收尾；未修改产品执行逻辑。
5. 初次完整回归 **386 passed / 1 failed，80.17 秒**：混合并发管理请求中出现一次
   5 秒客户端超时。独立复查 `test_management_performance.py` 为 **2 passed，6.32 秒**。
   目前没有证据确认首次超时的根因，初次失败日志保留。
   真实 LLM 验证结束后，再次独立运行全量回归得到 **387 passed，66.72 秒**，未重现超时。

本会话包含预检、未完成轮次及最终通过轮次，共记录 **15 个不同的成功响应 ID**、
**2 次预期 HTTP 拒绝**，已确认成功用量 **595 tokens（输入 503、输出 92）**。
另有 1 次沙箱连接失败。未知用量与拒绝请求的用量未计入，以上不是供应商完整账单。

## 证据与复现

- 最终结构化报告：[summary.json](../.artifacts/real-llm-2026-10-15-final/summary.json)
- 管理链路结果：[remote-result.json](../.artifacts/real-llm-2026-10-15-final/remote-result.json)
- 请求与响应时间记录：[provider-journal.json](../.artifacts/real-llm-2026-10-15-final/provider-journal.json)
- 事故暂停和最终失败：[暂停](../.artifacts/real-llm-2026-10-15-final/provider-failure-paused.json)、[失败](../.artifacts/real-llm-2026-10-15-final/provider-failure-result.json)
- 重启前审计和会话：[审计](../.artifacts/real-llm-2026-10-15-final/remote-audit.json)、[会话](../.artifacts/real-llm-2026-10-15-final/remote-sessions.json)
- 本会话调用计数：[session-call-audit.json](../.artifacts/real-llm-2026-10-15-final/session-call-audit.json)
- 初次回归失败：[日志](../.artifacts/real-llm-2026-10-15-run2/regression.log)、[XML](../.artifacts/real-llm-2026-10-15-run2/regression.xml)
- 性能测试独立复查：[日志](../.artifacts/real-llm-2026-10-15-final/performance-recheck.log)
- 最终完整回归：[日志](../.artifacts/real-llm-2026-10-15-final/regression.log)、[XML](../.artifacts/real-llm-2026-10-15-final/regression.xml)

新增可重复运行的真实验证工具：[tools/validate_real_llm.py](../tools/validate_real_llm.py)。
它不加入默认 pytest 测试，避免普通回归隐式消耗真实模型额度。每轮使用新目录和新工作流 ID；
指定的产物目录必须不存在，以避免复用历史结果造成误判。

```powershell
$env:PYTHONPATH = 'src'
# 使用现有 LLM_*，或回退到 APP_LLM_* 环境配置；Key 只保留在环境变量中。
python tools/validate_real_llm.py

python -m pytest tests -q
```

运行产物保存在 `.artifacts/`，该目录已加入 `.gitignore`，可在本机直接查看；
分享报告时需另行附带相关产物。未提交 Git 变更。

## 运行边界

本次验证覆盖真实外部模型 HTTP、同机独立 Worker 进程、本机管理/RPC 服务及本地 SQLite。
没有验证实际跨主机部署、公网 TLS 入口、供应商计费准确性、请求中断后的上游执行状态、
物理断电或长期负载容量。输出格式存在随机性，严格格式需求仍需业务校验。
