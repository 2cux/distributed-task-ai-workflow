# 端到端集成测试报告

验证日期：2026-10-13（Asia/Shanghai）。环境：Windows 11、Python 3.12.10、pytest 8.3.0、SQLite 3.49.1。

最终完整回归 **370 passed，64.14 秒，0 failures / 0 errors / 0 skipped**。
新增完整链路及两个缺陷回归场景连续独立重复三轮，每轮 **3 passed**。

## 完整业务链路

新增 `tests/test_end_to_end_management_remote.py`，所有业务提交、查询、暂停、恢复和停止操作经真实管理 HTTP 接口执行。

```text
管理 HTTP 提交 DAG
  → SQLite 原子持久化
  → 管理服务内的恢复协调器推进 DAG
  → 中心 HTTP RPC 领取任务
  → 两个独立 Python 进程的 RemoteWorker 执行业务
  → 中心持久化结果与事件
  → 管理 HTTP 查询和停止
  → 重启管理服务，查询历史结果并幂等重发
```

场景使用五个节点：检索 → 两个并行模型适配器 → 汇合 → 校验。模型适配器为确定性业务函数，无外部模型凭据或调用。
中心注册函数若被误执行会直接报错；子进程 PID 和并行屏障证明两条分支由不同进程并行执行。

验收断言包括：

- 只读令牌和错误令牌无法提交工作流；失败请求不产生任务。
- 同 ID 同定义重复提交不增加节点任务；提交返回 202。
- 暂停期间远程 Worker 已注册，但尝试数为零；恢复后开始执行。
- 两分支接收上游输出，汇合及校验得到 `['E2E-a', 'E2E-b']` 和 `valid=True`。
- 分支 a 注入一次瞬时异常后重试成功，实际退避不少于 0.05 秒；其他节点只执行一次。
- 共 5 个节点、6 次尝试、1 次重试，无开放事故；管理查询和指标一致。
- 增量事件与完整历史拼接一致，无重复游标；失败、安排重试及成功事件顺序准确。
- API 发出的两个精确会话停止请求生效，子进程以退出码 0 结束，会话为 STOPPED。
- 暂停、恢复及停止请求留下持久化审计。
- 重启后结果、任务 ID、事件和审计保持一致，幂等重发不增加执行次数，队列无待执行任务。
- SQLite `integrity_check=ok`，`foreign_key_check` 无违规。

## 本轮发现并修复的问题

### 管理 API 停止被记录为远程 Worker 异常

首次真实链路运行已正确完成业务，但 API 停止把会话改为 STOPPING 后，Worker 的下一次领取抛出 `WorkerSessionMismatch`，导致 `last_error` 非空并以退出码 1 结束。

修复在注册表中为当前会话的明确优雅停止返回 `WorkerStopRequested`，作为 `WorkerSessionMismatch` 的子类保持已有捕获兼容；RPC 保留该异常类型，Worker 消费循环按正常退出处理。已撤权或被替换会话仍走原有拒绝与错误逻辑。

除远程链路外，新增本地回归验证：控制面停止时当前函数继续收尾，下一任务保持 PENDING、执行次数为零，最终会话 STOPPED 且无异常。现有撤权、可信停止、旧结果拒绝和远程恢复测试全部通过。

失败证据：[链路运行 XML](e2e-validation-2026-10-13-chain-before-fix.xml)、[日志](e2e-validation-2026-10-13-chain-before-fix.log)。

### Windows 拒绝上传时偶发连接中止

修复停止问题后的第一次全量运行 **368 passed / 1 failed**。非法 Content-Type 请求本应返回 415，但客户端收到 Windows `ConnectionAbortedError / WinError 10053`：服务在未读完上传内容时关闭连接，错误响应可能被连接复位掩盖。

修复复用原过载处理的半关闭及有界丢弃连接数据机制，推广到 HTTP 错误响应。收尾最多等待 100 ms，避免慢客户端长期占用处理线程。

新增测试逐类发送未认证、权限不足、超限、错误 Content-Type 和不支持的 Transfer-Encoding 请求，覆盖 401 / 403 / 413 / 415 / 400，每类重复 8 次，其中部分请求体为 64 KiB。客户端必须读到预期 JSON 错误，数据库无工作流、执行数为零。既有过载、慢客户端、未收尾关闭和性能回归均通过。

失败证据：[全量运行 XML](e2e-validation-2026-10-13-full-before-http-fix.xml)、[日志](e2e-validation-2026-10-13-full-before-http-fix.log)。

## 最终证据

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| 全量回归，包含所有 unittest 和 pytest 用例 | 370 passed / 64.14 s | [XML](e2e-validation-2026-10-13-full.xml)、[日志](e2e-validation-2026-10-13-full.log) |
| 停止、撤权、远程恢复和完整链路定向回归 | 30 passed / 9.05 s | [XML](e2e-validation-2026-10-13-targeted.xml)、[日志](e2e-validation-2026-10-13-targeted.log) |
| HTTP 修复后的 API、稳定性及完整链路回归 | 18 passed / 12.14 s | [XML](e2e-validation-2026-10-13-http.xml)、[日志](e2e-validation-2026-10-13-http.log) |
| 三个新增场景重复第 1 轮 | 3 passed / 4.03 s | [XML](e2e-validation-2026-10-13-repeat-1.xml) |
| 三个新增场景重复第 2 轮 | 3 passed / 4.09 s | [XML](e2e-validation-2026-10-13-repeat-2.xml) |
| 三个新增场景重复第 3 轮 | 3 passed / 3.97 s | [XML](e2e-validation-2026-10-13-repeat-3.xml) |

定向和重复用例也包含在全量回归中，上述数量不能相加作为唯一用例数量。日志显示 pytest 总耗时，XML 的 suite 时间略有差异。
最终 `git diff --check` 通过；测试临时目录已清理。

全量覆盖任务生命周期、并发、优先级、延迟、重试、超时、幂等提交、事件持久化、DAG/AI 适配器、多 Worker/多进程、崩溃恢复、可信停止、非幂等结果核对、管理权限、分页、监控、过载、有限混合负载及 benchmark 工具的集成行为。

## 复现与运行边界

在仓库根目录执行：

```powershell
$env:PYTHONPATH = 'src'
python -m pytest tests -q --junitxml=docs/e2e-validation-2026-10-13-full.xml
python -m pytest tests/test_end_to_end_management_remote.py -q
python -m pytest tests/test_management_stability.py tests/test_reliable_workers.py -q
```

首次沙箱运行因本机 loopback 被禁止而出现 WinError 10013；该受阻全量运行已终止，相关临时目录已清理。表中的最终结果均在获准访问本机 HTTP 的环境执行。

测试使用本机真实 HTTP、SQLite 和独立进程。没有验证真实跨主机网络、公网/TLS 部署、外部大模型 API、物理断电、磁盘损坏或长期 soak；也未重新进行独立的高负载 benchmark，当前结果不构成生产容量或延迟保证。
