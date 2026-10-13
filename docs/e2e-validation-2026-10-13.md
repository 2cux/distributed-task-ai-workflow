# 系统端到端集成测试（2026-10-13）

环境：Windows、Python 3.12.10、pytest 8.3.0。测试使用独立临时数据库、本机 HTTP 服务、真实线程和 Python 子进程。

## 最终结果

- 全量回归：**326 passed，59.95 秒**；无失败或跳过。
- 集成与系统场景重复执行三轮：每轮 **76 passed**，耗时分别为 **49.77 / 40.85 / 28.97 秒**。
- 三轮均包含新增整体组合流程，以及 DAG、恢复控制面、真实 HTTP、进程崩溃、多消费者竞态等场景。
- 本轮新增一项可重复执行的端到端测试，没有修改生产代码。
- 原始结果：`e2e-validation-2026-10-13-full.xml` 和 `e2e-validation-2026-10-13-repeat-1.xml` 至 `repeat-3.xml`（JUnit XML）。

## 验证范围

| 系统路径 | 验证内容 |
| --- | --- |
| TaskEngine / 队列 / 执行器 | 提交、生命周期、优先级、FIFO、延迟、退避、重试预算、并发槽位、优雅停止 |
| SQLite / 事件 | 重启读取、重复提交、原子领取、结果及事件共同提交、故障回滚、旧库升级 |
| Worker / 注册表 | 多消费者及多进程竞争、心跳、撤权、停止证据、旧结果拒绝、接管 |
| DAG | 输出引用、并行与汇合、依赖门、失败后代跳过、进程中断后继续执行 |
| RecoverySystem | 后台协调、事故暂停、人工核对结果、额外预算、过期决定拒绝、多协调器竞争 |
| RecoveryService / RemoteWorker | 真实 HTTP、认证、注册表校验、网络中断、远端进程退出、中心权威预算 |
| 新增整体组合流程 | DAG → 后台恢复协调 → HTTP → 两个独立 Worker 进程 → 重试 → 汇合校验 → 持久化及重启读取 |

## 新增端到端用例

`tests/test_end_to_end_remote_workflow.py` 提交五节点工作流：延迟检索、两个并行模型分支、结果汇合、最终校验。中心注册函数只作为执行契约，若在中心被调用会直接失败；业务处理由两个子进程通过 HTTP 领取并执行，子进程不获得中心数据库路径。

分支通过可观察屏障确保首次调用在两个不同进程中并行运行。其中一个分支第一次抛出异常，中心保存 50 毫秒退避并执行一次重试。测试检查输出、进程身份、每个节点的 attempt 和 retry 次数、完整失败及重试事件链、重试开始时间、Worker STOPPED 状态和无未解决事故。

关闭 Worker、协调器和 HTTP 服务后重新打开数据库，重复提交同一工作流，确认终态结果和事件不变、无任务重放；SQLite `integrity_check` 返回 `ok`，`foreign_key_check` 无违规。测试在异常路径也关闭进程、后台协调器和服务。

## 环境问题与处理

第一次沙箱内全量运行：318 passed、7 failed（既有 325 项）。七项失败均为远端 HTTP 控制面无法连接。在允许本机回环连接的执行环境中，既有八项远端集成用例全部通过。本轮未修改生产实现。

新增测试初次运行发现两处测试断言需调整：事件时间为 ISO 字符串，需要转换后与 Unix 到期时间比较；重试允许换 Worker，因此并行性按首次调用的进程身份验证，最终结果只要求来自有效 Worker。修正后重新执行验证。

## 复现

需使用允许本机回环 HTTP 连接和启动子进程的环境。在项目根目录执行：

```powershell
$env:PYTHONPATH = 'src'
python -m pytest tests -q --durations=10 --junitxml=docs/e2e-validation-2026-10-13-full.xml
$integrationFiles = @(Get-ChildItem tests/*integration.py | ForEach-Object { $_.FullName }) + @(
    'tests/test_multiworker_processes.py',
    'tests/test_system_scenarios.py',
    'tests/test_end_to_end_remote_workflow.py'
)
for ($round = 1; $round -le 3; $round++) {
    python -m pytest @integrationFiles -q --tb=short "--junitxml=docs/e2e-validation-2026-10-13-repeat-$round.xml"
    if ($LASTEXITCODE -ne 0) { throw "Integration repetition failed: $round" }
}
```

## 验证限制

远端协议通过本机回环 HTTP 和独立进程验证，没有进行真实多台主机部署或 TLS 证书部署测试。模型与工具处理使用确定性函数，没有调用外部模型 API。已有故障用例注入网络服务中断、进程退出和事务错误，没有进行物理断电、磁盘损坏或长期容量压测；中心复制与自动主备切换不属于当前实现。
