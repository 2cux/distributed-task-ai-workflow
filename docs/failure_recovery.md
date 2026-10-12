# 独立故障恢复系统

`RecoverySystem` 是持久化执行引擎旁的恢复控制面。它整合 Worker 存活观察、分阶段撤权、
中断任务恢复、DAG 状态重建和人工决策，不执行具体业务函数。通过
`TaskEngine.create_recovery_system(...)` 创建；内存模式不支持该模块。

已有的 `RetryPolicy`、任务事件、`WorkerRegistry`、`PersistentExecutor` 与
`WorkflowStore` 仍负责各自的执行边界，恢复系统复用它们，不另建一套任务状态机。
调用方可以手动执行一次 `tick()`，也可以显式 `start()` 启动后台扫描。

## 组成与持久化边界

| 组件 | 责任 |
| --- | --- |
| `WorkerRegistry` | 保存每次注册生成的 `session_id`、`host_id`、心跳、撤权状态及停止证据 |
| `SQLiteTaskStore` / `PersistentExecutor` | 原子领取与结果提交；拒绝过期 owner、attempt 或撤权会话写入 |
| `RecoverySystem` | 可重复扫描、确认停止后的有界恢复、逐任务错误隔离及工作流协调 |
| `WorkflowStore` | 重建节点状态、绑定上游输出、放行依赖与汇总工作流状态 |
| `recovery_incidents` | 按 `(task_id, attempt_count)` 去重保存事故、旧会话/主机与人工处理结果 |
| `recovery_holds` | 保存任务/工作流暂停来源，阻止尚未开始的业务调用 |
| `recovery_events` | 保存恢复动作、操作人、理由、停止证据和递增审计序号 |

恢复表随持久化引擎初始化，即使尚未启动协调器，数据库领取也遵守已写入的 hold。
hold 在待领取查询和原子领取事务中都检查，另一进程的旧内存队列不能绕过暂停。
任务及工作流事故、人工暂停和人工决策保存在同一数据库，重启后继续生效。

## 安全恢复协议

注册 Worker 的旧执行按以下流程处理：

```text
ACTIVE --心跳过期--> SUSPECT --达到撤权期限--> FENCED
                                                |
                                         可信停止确认
                                                |
                                             STOPPED
                                                |
                           幂等且有剩余预算：RETRYING
                           否则：FAILED，等待人工核对

ACTIVE --优雅停止请求--> STOPPING --全部调用排空--> STOPPED
```

- `SUSPECT` 暂停新领取，尚未撤权的旧调用仍可提交结果。恢复心跳可返回 `ACTIVE`；
  如果已请求优雅停止，则返回 `STOPPING`。相应等待停止事故可在恢复扫描中关闭。
- `fence_after` 是心跳超时期限之后的额外等待秒数，默认为 `0`，必须是有限非负数。
  达到期限后转为 `FENCED`，拒绝新领取与旧结果提交。已撤权的会话不能靠心跳复活，
  Worker 重启必须注册新会话。
- 心跳超时、网络断开、数据库写入撤权都不能证明业务已退出。系统为对应 attempt
  打开 `WAIT_STOP` 事故；任务保持 `RUNNING`，不消耗重试预算，也不自动重放。
- 只有 `STOPPED` 且带有可信停止证据的会话，才允许恢复它拥有的 `RUNNING` attempt。
  没有 `stop_verifier` 时，协调器仍自动检测与撤权，但等待人工或运行时确认停止。

**可信证据必须来自对确切旧会话的监督**：例如确认执行进程已退出并完成 process join、
收到可信运行时“全部业务调用排空”的确认，或可靠隔离旧主机且确保它不能继续产生外部
副作用。隔离只切断控制面连接、再等待一段超时、观察不到心跳、撤销 SQLite 写权限，
都不满足这一条件。字符串 `evidence` 是审计记录，系统不会验证字符串描述的事实，
因此确认 API 和验证器必须只交给可信监督者。

任务函数启动的后台线程、子进程或异步外部操作也属于监督范围。仅父进程退出，不能
自动证明这些操作停止。已经完成但未提交结果的外部副作用，仍需要业务幂等键或人工核对。

## 自动协调与跨进程恢复

```python
from workflow_engine import RetryPolicy, Task, TaskEngine

engine = TaskEngine(
    database_path="tasks.sqlite3",
    task_registry={"abs-v1": abs},
    retry_policy=RetryPolicy(2, initial_delay=0.5),
)
engine.submit(Task("absolute-value", abs, id="business-request-001",
                   args=(-42,), idempotent=True))

worker = engine.create_worker("worker-a", host_id="host-a")
recovery = engine.create_recovery_system(interval=1, fence_after=5)
worker.start()
recovery.start()

# 应用退出时检查两个返回值；False 表示对应后台线程仍未结束。
worker_stopped = worker.shutdown(timeout=30)
recovery_stopped = recovery.shutdown(timeout=30)
```

`ReliableWorker` 从持久化队列持续领取任务，能够消费另一进程的恢复结果。新建引擎
加载已有 `PENDING` / `RETRYING`，不会默认接管仍为 `RUNNING` 的旧任务。
协调器负责重新安排恢复后的尝试，Worker 负责实际执行；创建协调器不会自动创建 Worker。

`stop_verifier(session)` 在数据库写锁外调用，仅为 `FENCED` 会话检查停止。确认后同轮
扫描即可恢复任务。验证器返回非空证据字符串；无法证明停止时返回 `None`。它必须有
时间上限且允许重复调用，因为多个协调器可能同时验证同一会话。例如，应用已有一个
可信监督映射 `supervised_processes`，精确关联旧 `session_id` 与受监督的进程：

```python
def verify_joined_process(session):
    process = supervised_processes.get(session.session_id)
    if process is None:
        return None
    process.join(timeout=0)
    if process.exitcode is None:
        return None
    # 监督者还须保证该进程没有留下继续产生副作用的后台执行。
    return f"joined supervised process; exitcode={process.exitcode}"

recovery = engine.create_recovery_system(
    interval=1, stop_verifier=verify_joined_process, fence_after=5,
)
```

### 多协调器与崩溃重入

系统不选举长期 leader；每个动作在 SQLite `BEGIN IMMEDIATE` 短事务内重新检查状态。
多个进程可对同一中心本地数据库运行协调器，事务串行化实际的恢复决定。停止确认提交
后协调器崩溃，下一次扫描仍能发现已停止会话并继续；同一个中断 attempt 只能提交一次
恢复状态、重试预算扣减与对应任务事件。

恢复一个任务时使用 savepoint。缺失注册函数、损坏定义或事件写入失败会回滚该任务
的恢复，打开 `RECOVERY_ERROR`，保留原执行状态和预算，其他任务仍可继续恢复。
工作流状态协调也逐工作流使用 savepoint。依赖于事务的这些保证适用于同一数据库；
协调器不会在数据库不可用时凭内存猜测执行状态。

缺少注册函数的 `PENDING` / `RETRYING` 任务在扫描中隔离为 `RECOVERY_ERROR`，
不被领取；部署恢复完整注册表后，后续扫描自动关闭“handler unavailable”事故并
解除其 hold。已可信停止但无法解码的 `RUNNING` 任务仍保留执行状态和预算，恢复
注册后重新扫描可继续恢复，无须人工追加预算。其他错误须先修复原因，避免反复失败。

### 有界重试与业务幂等

中断恢复仅在 `idempotent=True` 且持久化预算尚有剩余时安排重试，并消耗一次预算。
已提交的 `retry_count`、有效 `retry_limit`、退避配置与 `retry_at` 是恢复依据，换进程、
修改引擎默认策略或重复扫描都不会重置预算。预算耗尽或非幂等的中断任务转为
`FAILED` / `UncertainExecutionError`，等待核对结果。

`idempotent=True` 是业务声明。函数可能已完成外部写入而来不及提交成功结果，平台
不能替任意外部接口实现恰好一次副作用；应使用稳定业务键、业务唯一约束或外部接口的
幂等键。非幂等结果不确定时，人工 `retry` 也被拒绝，应核对外部结果后决定成功或失败。
持久化软超时会等待旧调用实际结束，不能用超时返回代替停止证明。

## 工作流级恢复

每轮扫描重新协调尚未成功的 DAG：已完成分支保留结果；满足依赖的等待节点继续绑定
输出并进入可领取状态。任务事故在任务本身和所属工作流分别写入 hold，工作流显示
`PAUSED`，阻止新的节点领取。暂停不会强制中止已经开始的调用。

人工确认成功或安排重试时，修复对应节点，并将从未执行、因失败被 `SKIPPED` 的节点
重新置为等待，再按实际依赖推进。已经成功的节点不重放；其他失败分支保留各自事故。
人工最终判失败则保留失败节点并继续跳过后代，工作流最终为 `FAILED`。

`resume_workflow()` 只移除人工暂停来源，不会清除未处理事故产生的 hold。解决一个
事故也只移除该事故的 hold，不会取消人工暂停或其他事故。因而多个故障和维护暂停
能够同时存在，任何入口都不会因一个恢复动作错误地放行整个工作流。

## 跨主机执行与部署

`remote.py` 提供中心 `RecoveryService` 与执行端 `RemoteWorker`。中心服务在本地持久
磁盘上打开 SQLite，远端 Worker 通过带认证的 HTTP(S) JSON RPC 注册、心跳、领取和
提交结果，并在自己的主机执行预先注册的业务函数。远端无需数据库文件访问权限，
`RemoteTaskStore` 也不会打开本地或网络 SQLite。

```text
主机 B：RemoteWorker ── HTTPS JSON RPC ──┐
                                      ├── 中心主机：RecoveryService
主机 C：RemoteWorker ── HTTPS JSON RPC ──┘          │
                                             本地 SQLite
                                                  │
                                       RecoverySystem / 人工监督
```

中心创建示例（证书、路径、域名与密钥由部署提供）：

```python
import os
import ssl
from workflow_engine import RetryPolicy, Task, TaskEngine

engine = TaskEngine(
    database_path="tasks.sqlite3",
    task_registry={"abs-v1": abs},
    retry_policy=RetryPolicy(2),
)
tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
tls.load_cert_chain("server.crt", "server.key")
service = engine.create_recovery_service(
    os.environ["WORKFLOW_RECOVERY_TOKEN"],
    host="0.0.0.0", port=8443, ssl_context=tls,
)
recovery = engine.create_recovery_system(interval=1, fence_after=5)
service.start()
recovery.start()
engine.submit(Task("absolute-value", abs, args=(-42,),
                   id="remote-business-request-001", idempotent=True))
```

在另一主机启动执行端；URL 使用客户端实际可达、与证书匹配的服务域名：

```python
import os
import ssl
from workflow_engine.remote import RemoteWorker

worker = RemoteWorker(
    "https://recovery.example.internal:8443",
    task_registry={"abs-v1": abs},
    token=os.environ["WORKFLOW_RECOVERY_TOKEN"],
    worker_id="host-b-worker-1", host_id="host-b",
    heartbeat_interval=1, heartbeat_timeout=10,
    poll_interval=0.1, request_timeout=2,
    ssl_context=ssl.create_default_context(cafile="internal-ca.crt"),
)
worker.start()
# 常驻宿主保持运行；退出时调用 worker.shutdown(timeout=30) 并检查返回值。
```

中心及每个远端 Worker 的注册函数名称集合必须完全一致，对应版本也须具有相同业务
语义。传输只携带 JSON 定义、结果和异常摘要，不传递函数、pickle 或动态导入指令。
重试预算、退避时间和状态由中心持久化记录决定；中心结果提交重新检查旧会话、领取
owner 与 attempt，远端不能自行覆盖预算或越过撤权状态。任务提交、工作流操作、
恢复扫描和人工确认由中心应用执行。

网络错误会使请求结果不确定，传输层不会自动重复写请求。远端控制面请求失败后停止
新领取，仍等待已经开始的业务调用排空；停止确认若也无法送达，中心保留旧会话及
`RUNNING` 任务，随后进入 `WAIT_STOP`。恢复网络连接、部署替换 Worker、切换 `host_id`
或中心服务重启，都不能代替旧调用停止确认。可靠监督者确认旧主机会话停止后，健康
主机的 Worker 可领取允许重试的任务。

部署约束：

- SQLite 数据库、`-wal` 与 `-shm` 仅放在中心主机本地盘；其他主机通过 RPC 访问。
  多协调器仅在能安全访问这一中心本地数据库的进程间共同工作。
- 服务默认绑定 loopback，`port=0` 可选临时端口，`address` / `url` 可读取监听地址。
  对外绑定时必须提供 `ssl_context`，或者仅在完整连接已经被可信 TLS 代理、VPN 或
  SSH 隧道保护时显式设置 `allow_insecure=True`。客户端也须验证服务端证书。
- 认证在所有传输模式下均要求共享 Bearer token；使用部署密钥管理提供高熵 ASCII
  密钥。拥有密钥的 Worker 属于可信执行和监督域，权限不按租户或 Worker 身份隔离。
  人工停止确认接口同样只应交给可信监督者。
- `request_timeout` 是有限正数；`max_request_bytes` 默认 1 MiB，限制请求与响应。
  大结果和随任务累积的事件历史可能触及限制，应按真实负载统一配置中心和客户端。
- `RecoveryService` 的启动和停止独立于 `RecoverySystem` 与 Worker；关闭服务也不会
  终止其他主机已开始的业务。应用需分别监督这些生命周期。

此部署支持执行主机失效后的安全接管，中心服务和它的本地数据库仍是单一故障域。
中心进程退出而持久磁盘完整时可重新打开同一数据库继续协调；中心主机或数据库磁盘
不可用时不能继续调度。实现没有数据库复制、中心自动主备切换或磁盘灾难恢复；需
另外提供可验证的持久盘与一致备份方案，不能将复制 WAL 文件当作故障切换协议。

## 人工干预 API

所有人工写操作要求非空 `operator` 与 `reason` 或 `evidence`，在同一事务中记录审计。
确认停止和处理业务结果是两个步骤：对仍在 `RUNNING` 的旧 attempt，先取得可信停止
证据，确认停止并 `tick()`，再对恢复后的失败事故做决定。

```python
# 找到事故及其确切旧会话。
incident = next(item for item in recovery.list_incidents()
                if item.kind == "WAIT_STOP" and item.session_id is not None)

# 监督者已完成真实停止确认；仅有心跳超时不能调用此接口。
recovery.confirm_session_stopped(
    incident.session_id,
    operator="process-supervisor",
    evidence="joined exact old process; all business invocations stopped",
)
report = recovery.tick()
```

主机故障使用 `confirm_host_stopped(host_id, session_ids=[...], operator=..., evidence=...)`。
`session_ids` 必须是非空且无重复的明确快照，每个会话均须属于指定主机并已经撤权或
排空中；一个不匹配会话使整个确认事务回滚。确认不包含该主机未来注册的新会话。
主机 ID 本身是部署标识，不能替代对主机可靠隔离及外部副作用停止的核验。

| 操作 | 适用情况与约束 |
| --- | --- |
| `resolve_task(id, action="succeed", result=..., operator=..., reason=...)` | 已核对外部结果，将 JSON 结果提交为成功，修复下游输入；不调用业务函数 |
| `resolve_task(id, action="retry", operator=..., reason=...)` | 同一失败 attempt、声明幂等且还有剩余预算；消耗一次预算并沿用持久化退避 |
| `resolve_task(id, action="retry", extra_retries=1, ...)` | 明确授予额外预算并记录审计；不能绕过非幂等限制 |
| `resolve_task(id, action="fail", operator=..., reason=...)` | 接受最终失败，关闭事故；重复扫描不会重新打开同一已接受的失败 |
| `pause_workflow(id, operator=..., reason=...)` | 持久化人工暂停，停止新的节点领取 |
| `resume_workflow(id, operator=..., reason=...)` | 仅移除人工暂停，未解决事故仍阻止领取 |

`resolve_task()` 的第一个参数是事故 ID。它要求事故仍为 `OPEN`、任务仍为 `FAILED`，
且 `attempt_count` 与事故一致；重复决策或过期页面上的决定抛出 `RecoveryConflict`。
人工重试不清零历史或预算，人工补录成功也不伪造新的业务执行次数。

输入绑定在领取前失败的节点不能直接重试：原定义仍不可绑定，重试会留下无法领取的
任务。该情况应接受失败，或核对业务结果后补录成功。若旧调用已可信停止，但删除的
注册函数使自动解码失败，`RECOVERY_ERROR` 对应的确切 `RUNNING` attempt 也允许
人工补录成功或接受失败；系统会重新核验已持久化的停止证据并记录中断事件，不调用
已经移除的业务代码。仍在运行且未经停止确认的 attempt 不接受这些操作。

### 旧执行入口兼容

`TaskEngine.start()` 等旧入口可能留下没有 `worker_session` 的中断 attempt。协调器
对它打开 `WAIT_STOP`，不能用会话或主机确认代替针对该 attempt 的监督。确认旧调用
已经结束后使用：

```python
recovery.confirm_task_stopped(
    task_id, attempt_count=known_attempt,
    operator="offline-supervisor", evidence="joined exact legacy invocation",
)
```

该接口只接受确切、仍为 `RUNNING` 且没有注册会话的 attempt，随后原子恢复。
已有 `recover_interrupted=True` / `SQLiteTaskStore.recover_interrupted()` 是离线兼容
入口，要求调用方先保证所有旧调用已停止；只要存在注册 Worker 持有的 `RUNNING`
任务就拒绝全局恢复。在线部署应使用协调器的分阶段恢复及可审计确认。
兼容入口成功安排重试时也会关闭该 attempt 已有的恢复事故、解除其 hold 并记录审计，
避免混合使用新旧恢复入口时留下永久暂停。

## 后台生命周期与观测

- `interval` 为有限正数；`start()` 显式启动后台 daemon 线程，一个实例只能启动一次。
  停止后需要新建实例；未启动就 `shutdown()` 的实例也不能再启动。
- `shutdown(timeout=None)` 请求停止并等待当前扫描结束，返回 `True` 表示线程已退出，
  `False` 表示超时仍未结束。它不强制打断停止验证器或正在执行的数据库操作，也不
  停止 Worker。关闭时须分别监督 Worker 与协调器。
- `tick()` 返回 `RecoveryReport`：`recovered_task_ids` 为本轮成功提交恢复的任务，
  `workflow_ids` 为本轮已协调的工作流，`incident_ids` 为当前未解决事故，`errors`
  为逐任务、逐工作流及停止验证失败。`workflow_ids` 不表示这些工作流已经成功。
- `last_report` 保存最近完成扫描的报告。后台循环通过 `last_error` 暴露整轮异常及报告
  中的局部错误，下一轮成功可清除它；直接调用 `tick()` 应检查返回报告并捕获整轮异常。
- `list_incidents(open_only=False)` 可读取已处理事故；`get_events(after_sequence=...)`
  可增量读取恢复审计。`engine.get_events(task_id)` 和 Worker 会话历史继续提供任务及
  会话事件。验证器失败记录 `STOP_VERIFICATION_FAILED`，工作流协调失败记录
  `WORKFLOW_RECOVERY_FAILED`；失败不会被当作停止证据。

## 独立回归集成测试

以下文件分别使用新的临时数据库，不依赖其他测试的执行顺序，可单独运行：

| 测试文件 | 覆盖场景 |
| --- | --- |
| `tests/test_recovery_process_integration.py` | 真实子进程异常退出、停止确认前不重放、重启恢复与退避、多协调器进程竞争、事件失败回滚预算 |
| `tests/test_recovery_workflow_integration.py` | 核对成功结果修复 DAG、保留完成分支、重启后暂停仍阻止领取、额外重试预算、最终失败及过期决策拒绝 |
| `tests/test_recovery_host_integration.py` | 主机会话标识和明确快照隔离、健康会话继续工作、新 incarnation 不被旧确认影响、验证器拒绝证明时不重放；使用本机数据库模拟主机故障域 |
| `tests/test_recovery_remote_integration.py` | 实际 HTTP RPC 与独立 Worker 子进程、远端本地函数和中心重试、网络分区时不重放、认证/注册表拒绝、撤权和过期 owner 拒绝；测试通过 loopback 验证协议 |

```powershell
$env:PYTHONPATH = 'src'
python -m unittest discover -s tests -p test_recovery_process_integration.py -v
python -m unittest discover -s tests -p test_recovery_workflow_integration.py -v
python -m unittest discover -s tests -p test_recovery_host_integration.py -v
python -m unittest discover -s tests -p test_recovery_remote_integration.py -v
python -m unittest discover -s tests -p 'test_recovery*integration.py' -v
python -m unittest discover -s tests -v
```

基础协议与既有回归还可参见 [Worker 注册与分阶段恢复](worker_registry.md)、
[DAG 工作流](dag_workflows.md) 和 [系统验证](system_validation.md)。
