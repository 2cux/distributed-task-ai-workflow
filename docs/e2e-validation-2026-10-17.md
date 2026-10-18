# 端到端集成测试与修复记录

日期：2026-10-17（Asia/Shanghai）。环境：Windows、Python 3.12.10、pytest 8.3.0。

## 回归结果

- 修改前完整基线：480 passed、4 skipped，168.55 秒。
- 修复相关专项回归：32 passed，12.50 秒。
- 首轮完整回归：485 passed、4 skipped，151.72 秒；没有失败或执行错误。
- Docker 启动故障修复后，四个独立容器故障场景已全部通过，原始证据保存于 `.artifacts/e2e-container-scenarios.json`。
- 启用容器测试后的最终全量回归：**489 passed，0 skipped，170.26 秒**；原先跳过的四项均实际执行并通过，XML 为 `.artifacts/e2e-regression-with-containers-tests.xml`。
- 核心组合场景连续运行三轮，每轮 18 passed，分别耗时 13.40、14.49、13.08 秒。
- AI 短负载：并发 1、4 均通过，32 个工作流、64 个任务、32 次模型 HTTP 调用；HTTP 和工作流错误均为零，调用次数、结果、事件、token 用量、数据库完整性和外键检查均通过。
- 管理接口同一负载在显式 `request_timeout=5` 秒配置下：预热及三轮测量全部通过；测量轮共 600 个 HTTP 请求、150 个工作流、300 次任务执行，零 HTTP 错误、零重试，结果及数据库检查通过。默认 2 秒配置的负载门禁未通过，详见下方容量边界。

XML 和负载原始结果保存在项目 `.artifacts/` 中；该目录按仓库规则不纳入版本控制。

## 覆盖范围

| 方面 | 实际验证的行为 |
| --- | --- |
| 核心链路 | 管理 HTTP 提交 → SQLite → DAG 调度 → 独立远程 Worker 进程 → 结果提交 → 管理 API 读取 |
| 调度和依赖 | 并行分支、汇合、输出绑定、优先级与 FIFO、首次延迟、重试退避、失败后代跳过 |
| 重复请求与竞争 | 同 ID 重复提交、定义冲突、多个进程与消费者竞争、原子领取、过期结果拒绝 |
| 异常和恢复 | 超时、非法返回值、提交失败、进程强制终止、心跳异常、可信停止确认、人工结果修复、暂停和恢复 |
| HTTP 边界 | 认证和只读权限、请求体与分页校验、过载、慢客户端、锁竞争、异常数值输入、失败请求原子回滚 |
| 持久化和观测 | 服务重启、旧库恢复、事件增量读取与顺序、审计、指标、SQLite 完整性和外键检查 |
| AI 工作流 | 本地供应商 HTTP 协议、回答校验、失败分类、选择性重试、调用和 token 预算、用量去重、多 Worker 共享预算 |

## 发现和修复

### 1. 超范围优先级可能中断消费者

原实现允许提交任意大小的 Python 整数优先级。两个 `priority=2**100` 的 LLM 节点共用 token 预算时，第一个节点被领取后，第二个节点因等待用量而被跳过；继续扫描队列时把原始超大整数绑定到 SQLite 游标参数，触发 `OverflowError: Python int too large to convert to SQLite INTEGER`，阻塞其他健康工作流的消费。

修复：持久化提交只接受有符号 64 位优先级，避免超范围值的排序精度损失及运行时溢出；生效的重试上限同样校验 SQLite 整数范围。旧数据库已有的超范围优先级使用 SQLite 提取出的比较值推进扫描游标，避免中断消费。旧数据仍沿用 SQLite 原有的数值比较语义。

新增测试覆盖：提交前拒绝、工作流事务回滚、正负合法极值、预算等待期间健康任务继续被领取、旧数据库中超大优先级兼容。README 和 OpenAPI 已同步数值边界。

### 2. 异常数值输入被误报为 HTTP 500

极大的整数形式 `timeout`、`scheduled_at` 等在数值转换时可能抛出 `OverflowError`，原 HTTP 异常分支没有将其识别为输入错误。

修复：统一返回 HTTP 400。新增真实 HTTP 测试提交异常数值，确认数据库没有部分工作流或任务残留，再提交正常工作流，验证 Worker 成功执行且重启后结果一致。

### 3. 队列等待参数没有拒绝 NaN/Infinity

原等待接口只检查负数，没有拒绝 NaN 和正无穷；NaN 会破坏截止时间比较，存在忙循环风险，正无穷则会产生非预期长期等待。

修复：在出队和停止判断前检查有限非负数。新增测试覆盖空队列、有就绪任务及已请求停止时的校验，确保异常参数不消费任务。

上述新增回归用例位于 `tests/test_end_to_end_boundaries.py`。初版复现测试修复前为 3 failed、1 passed；随后增加旧库兼容场景，最终 5 项均在专项及全量回归中通过。

## 管理接口负载边界

后续容量改进及 2026-10-18 的独立对照、499 项完整回归见[容量优化报告](capacity-improvement-2026-10-18.md)。下方保留优化前的历史结果。

负载使用 50 个两节点工作流、150 次查询、8 个并发客户端、4 个 Worker，包含一轮预热及三轮测量。

默认服务 `request_timeout=2` 秒时，首次测量三轮共 600 个请求中有 7 个 HTTP 503，完成 143 个工作流，负载门禁失败；预热另外有 2 个 HTTP 503。独立复现采集的请求日志将失败明确归类为 `database_busy`，持续约 2.04～2.11 秒。所有已创建任务的结果、执行次数、数据库完整性及关闭检查均正确，没有发现部分提交或已接受任务被遗漏。

这是当前机器和该配置下的写入等待容量边界。没有放宽默认超时，也没有自动重放被拒绝的写请求。使用临时测试夹具将服务的 `request_timeout` 显式设为 5 秒后，同一并发和负载的预热及三轮测量全部通过，测量轮 600 个请求、150 个工作流、300 次执行均成功。该结果不能解释成默认配置的负载门禁通过。

原始失败结果保留在 `.artifacts/e2e-management-load.json`，对应报告为 `.artifacts/e2e-management-load.md`；5 秒配置结果保存在 `.artifacts/e2e-management-configured-load.json` 和同名 Markdown 中，其 configuration 记录了显式等待配置。

## 执行与证据

```powershell
$env:PYTHONPATH = 'src'
python -m pytest tests -q --junitxml=.artifacts/e2e-regression-tests.xml
python -m pytest tests/test_end_to_end_boundaries.py tests/test_llm_budget.py tests/test_queue_wait.py tests/test_persistence.py tests/test_management_api.py -q --junitxml=.artifacts/e2e-targeted-tests.xml
for ($round = 1; $round -le 3; $round++) {
    python -m pytest tests/test_end_to_end_boundaries.py tests/test_end_to_end_remote_workflow.py tests/test_end_to_end_management_remote.py tests/test_recovery_workflow_integration.py tests/test_engine_boundary_integration.py -q --junitxml=.artifacts/e2e-repeat-$round.xml
    if ($LASTEXITCODE -ne 0) { throw "Regression failed: $round" }
}
python tools/benchmark_ai_workflow.py --profile benchmarks/ai-smoke.json --output .artifacts/e2e-ai-smoke.json --markdown .artifacts/e2e-ai-smoke.md
```

## Docker 容器补测

Docker Desktop 4.54.0 首次启动失败，原因是 Windows 上遗留的 `dockerInference` 和 Secrets Engine `engine.sock` 运行时 socket 无法访问。确认相关目录只含零字节运行时 socket 后，将旧目录原样改名备份，再一次性重新启动，Docker Engine 29.1.2 恢复可用。未执行恢复出厂设置，也未删除现有镜像、容器数据卷或修改 Docker 配置。

保留的运行时备份为 `%LOCALAPPDATA%\Docker\run.stale-20261017-190509`、`%LOCALAPPDATA%\Docker\run.stale-20261017-190656` 和 `%LOCALAPPDATA%\docker-secrets-engine.stale-20261017-190607`。

| 容器场景 | 结果 | 关键断言 |
| --- | --- | --- |
| 真实断网、重连 | 通过 | 不凭网络失联重放；会话撤权后重连仍拒绝旧结果；可信停止确认后由替代 Worker 完成，执行 2 次、重试 1 次 |
| SIGKILL、监督恢复 | 通过 | 旧容器退出码 137；停止证明前不接管，证明后重试成功；已完成上游只执行 1 次 |
| 旧会话与同名 Worker 新实例 | 通过 | 旧结果、心跳、领取请求均被拒绝；同名 Worker 新会话保持 ACTIVE；恢复结果不被旧消息覆盖 |
| 非幂等任务崩溃 | 通过 | 退出码 137；业务尝试保持 1 次、重试为 0；先暂停等待处理，人工接受失败后工作流终止为 FAILED |

四个场景均核对了中心和 Worker 使用不同的内核网络命名空间，Worker 不挂载中心数据库或共享控制文件；中心重启后工作流结果和任务事件一致。每个场景使用新建的随机 Compose 项目，已清理本轮项目的容器、网络、数据卷和项目本地镜像，报告没有 cleanup_errors。

随后设置 `WORKFLOW_CONTAINER_TESTS=1` 再运行完整测试套件，四个容器测试再次通过，全量 **489 passed，0 skipped**。清理审计 `.artifacts/e2e-container-cleanup.json` 确认测试前缀下没有残留容器、网络、数据卷或镜像。本轮未发现需要新增应用代码修复的问题，Docker Engine 保持运行。

```powershell
$env:PYTHONPATH = 'src'
python tools/validate_cross_host.py --scenario all --docker 'C:\Program Files\Docker\Docker\resources\bin\docker.exe' --output .artifacts/e2e-container-scenarios.json
$env:WORKFLOW_CONTAINER_TESTS = '1'
$env:WORKFLOW_DOCKER = 'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
python -m pytest tests -q --junitxml=.artifacts/e2e-regression-with-containers-tests.xml
```

## 本次未验证的范围

- 初次沙箱运行因 loopback 连接被拒绝（WinError 10013）而中断；确认原因后，在允许本机网络的环境重新执行上述测试，最终结果以成功完成的 XML 为准。
- 首轮因 Docker Engine 未启动而跳过的 4 项已补测，详见上方 Docker 容器补测记录。
- 远程执行和故障测试使用本机独立进程、HTTP 服务及独立容器网络命名空间，没有使用两台物理主机。
- 模型协议使用本地可控供应商服务，没有调用付费外部模型。短负载检查不能替代长期 soak、生产容量或物理断电验证。
- AI 冒烟与全量回归的末段存在时间重叠；延迟数据仅为现场样本，不作为独立性能基线或性能提升结论。
